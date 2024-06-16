import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, datediff, date_format, explode, sum
from pyspark.sql.types import FloatType, ArrayType, StringType, DateType

# UDF definitions and registrations
udf_five_digit_pc = udf(lambda x: x[0:5], StringType())


def explode_start(bill_start, bill_days):
    from datetime import timedelta
    explode_list = []
    for day in range(1, bill_days + 1):
        x = bill_start + timedelta(days=day)
        explode_list.append(x)
    return explode_list


udf_explode_start = udf(explode_start, ArrayType(DateType()))


def aggregate_hdd_cdd(bills):
    from pyspark.sql.functions import avg
    return bills.groupBy([
        'tenant_id',
        'account_id',
        'location_id',
        'channel_id',
        'fuel_type',
        'consumption_scaled',
        'bill_yearmonth',
        'bill_adc'
    ]) \
        .agg(
        avg('cdd_sum').alias('mean_cdd'),
        avg('hdd_sum').alias('mean_hdd')
    )


def apply_model(bills):
    from pyspark.sql.functions import udf, collect_list, isnan, lit
    from pyspark.sql.types import ArrayType, FloatType

    def ols(adc, hdd, cdd):
        import numpy as np
        X = np.array([[1, t[0], t[1]] for t in zip(hdd, cdd)])
        y = np.array(adc)
        try:
            if len(y) >= 8:
                coefs = np.dot(np.dot(np.linalg.inv(np.dot(X.T, X)), X.T), y)
                ymu = np.mean(y)
                rsq = 1 - (sum((yh[0] - yh[1]) * (yh[0] - yh[1]) for yh in zip(y, np.dot(X, coefs))) /
                           sum((yb - ymu) * (yb - ymu) for yb in y))
                if rsq == rsq:
                    return float(rsq), float(coefs[0]), float(coefs[1]), float(coefs[2])
                else:
                    return None, float(coefs[0]), float(coefs[1]), float(coefs[2])
            else:
                return None, None, None, None
        except Exception:
            pass

    ols_udf = udf(ols, ArrayType(FloatType()))

    model_df = bills \
        .groupBy([
            'tenant_id',
            'account_id',
            'location_id',
            'channel_id',
            'fuel_type',
        ]) \
        .agg(
            collect_list('bill_adc').alias('adc_list'),
            collect_list('mean_hdd').alias('hdd_list'),
            collect_list('mean_cdd').alias('cdd_list'),
        )

    model_df = model_df \
        .withColumn(
            'model_coefs',
            ols_udf(
                model_df['adc_list'],
                model_df['hdd_list'],
                model_df['cdd_list']
            )
        )

    model_df = model_df \
        .withColumn('rsquare', model_df['model_coefs'].getItem(0)) \
        .withColumn('intercept', model_df['model_coefs'].getItem(1)) \
        .withColumn('hdd_coef', model_df['model_coefs'].getItem(2)) \
        .withColumn('cdd_coef', model_df['model_coefs'].getItem(3))

    # Filter out NaNs, -Infinity, and other empty results
    model_df = model_df \
        .filter(isnan('rsquare') == lit(False)) \
        .filter(model_df.rsquare != '-Infinity') \
        .dropna(thresh=2, subset=('hdd_coef', 'cdd_coef', 'rsquare'))

    return model_df \
        .select([
            'tenant_id',
            'account_id',
            'location_id',
            'channel_id',
            'fuel_type',
            'rsquare',
            'intercept',
            'hdd_coef',
            'cdd_coef',
        ])


# STEP 0: Register tables, determine date range etc.
spark = SparkSession.builder.getOrCreate()

max_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
max_date = max_date.replace(day=1)
min_date = max_date - datetime.timedelta(days=365)

bills = spark.table('bills')
locations = spark.table('locations')
channels = spark.table('channels')
weather = spark.table('weather')

# Keeping column names as they were before weather changed
weather = weather \
    .withColumnRenamed('date_local', 'date') \
    .withColumnRenamed('postal_code', 'PostalCode') \
    .withColumnRenamed('cdd_avg', 'cdd_sum') \
    .withColumnRenamed('hdd_avg', 'hdd_sum')

# STEP 1: Clean/prep bills and locations, merge
bill_field_subset = [
    'tenant_id',
    'account_id',
    'location_id',
    'channel_id',
    'bill_start',
    'bill_end',
    'consumption_scaled',
    'fuel_type',
    ]

bills = bills.filter(bills['bill_end'] >= min_date) \
    .filter(bills['bill_end'] < max_date) \
    .select(bill_field_subset) \
    .withColumn('bill_days', datediff(bills['bill_end'], bills['bill_start']))

bills = bills.withColumn('bill_adc', bills['consumption_scaled'] / bills['bill_days']) \
    .selectExpr('*', 'date_add(bill_start, bill_days / 2) as bill_yearmonth')

df_total_con = bills \
    .groupBy([
        'tenant_id',
        'account_id',
        'location_id',
        'channel_id',
    ]) \
    .agg(sum('consumption_scaled').alias('total_consumption'))

locations = locations.withColumnRenamed('id', 'location_id') \
    .select(
    'tenant_id',
    'account_id',
    'location_id',
    'postal_code'
    ) \
    .filter(
    col('postal_code').isNotNull() &
    (col('postal_code') != '')
    )

channels = channels \
    .withColumnRenamed('id', 'channel_id') \
    .select([
        'channel_id',
        'location_id',
    ])

bills_channel = bills.withColumn('bill_yearmonth', date_format(bills['bill_yearmonth'], 'yyyyMM').cast('integer')) \
    .join(locations, ['tenant_id', 'account_id', 'location_id'], how='inner')

bills_no_channel = bills.withColumn('bill_yearmonth', date_format(bills['bill_yearmonth'], 'yyyyMM').cast('integer')) \
    .drop('location_id') \
    .join(locations, ['tenant_id', 'account_id'], how='inner')

bills_no_channel = bills_no_channel.where(bills_no_channel.channel_id.isNull())

bills_no_channel = bills_no_channel \
    .drop('channel_id') \
    .join(
        channels,
        ['location_id'],
        how='left',
    )

# STEP 2: Extract HDD/CDD values from weather, explode the date range of each bill, then join weather.
# NOTE: Weather is being appended on BOTH date and postal_code.
weather = weather.select(
    'PostalCode',
    'date',
    'hdd_sum',
    'cdd_sum'
    )

weather = weather.withColumn('date', weather['date'].cast(DateType())) \
    .withColumn('PostalCode', weather['PostalCode'].cast(StringType())) \
    .withColumn('hdd_sum', weather['hdd_sum'].cast(FloatType())) \
    .withColumn('cdd_sum', weather['cdd_sum'].cast(FloatType()))

weather = weather.withColumnRenamed('PostalCode', 'postal_code')

weather = weather.na.drop(how='any')

bills_channel = bills_channel.withColumn('date',
                                         udf_explode_start(bills_channel['bill_start'], bills_channel['bill_days'])) \
    .withColumn('postal_code', udf_five_digit_pc('postal_code'))

bills_no_channel = bills_no_channel.withColumn('date', udf_explode_start(bills_no_channel['bill_start'],
                                                                         bills_no_channel['bill_days'])) \
    .withColumn('postal_code', udf_five_digit_pc('postal_code'))

bills_channel = bills_channel.select(
    'tenant_id',
    'account_id',
    'location_id',
    'channel_id',
    'postal_code',
    'consumption_scaled',
    'bill_yearmonth',
    'bill_adc',
    'fuel_type',
    explode(bills_channel['date']).alias('date')
    )

bills_no_channel = bills_no_channel.select(
    'tenant_id',
    'account_id',
    'location_id',
    'channel_id',
    'postal_code',
    'consumption_scaled',
    'bill_yearmonth',
    'bill_adc',
    'fuel_type',
    explode(bills_no_channel['date']).alias('date')
    )

bills_channel = bills_channel.join(weather, ['date', 'postal_code'], 'inner')
bills_no_channel = bills_no_channel.join(weather, ['date', 'postal_code'], 'inner')

bills_agg_channel = aggregate_hdd_cdd(bills_channel)
bills_agg_no_channel = aggregate_hdd_cdd(bills_no_channel)

df_coefs_channel = apply_model(bills_agg_channel)
df_coefs_no_channel = apply_model(bills_agg_no_channel)

df_output_channel = df_coefs_channel \
    .join(
        df_total_con,
        [
            'tenant_id',
            'account_id',
            'location_id',
            'channel_id',
        ],
        how='inner'
    )

df_total_con = df_total_con.drop('location_id', 'channel_id')

df_output_no_channel = df_coefs_no_channel \
    .join(
        df_total_con,
        [
            'tenant_id',
            'account_id',
        ],
        how='inner'
    )

df_new_output = df_output_channel.union(df_output_no_channel)
df_new_output = df_new_output.dropDuplicates()
df_new_output.createOrReplaceTempView('output')
