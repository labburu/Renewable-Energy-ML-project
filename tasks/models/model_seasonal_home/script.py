"""Seasonal home model."""

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
    date_format,
    first,
    greatest,
    kurtosis,
    least,
    stddev,
    udf,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def assign_region(state):
    """Assign arbitrary region to state abbreviation."""
    if (state == 'NC') | (state == 'SC'):
        return 'NC-SC'
    elif (state == 'FL'):
        return 'FL'
    elif (state == 'OH') | (state == 'KY') | (state == 'IN') | (state == 'MI') | (state == 'IL') | (state == 'IA'):
        return 'MIDWEST'
    elif (state == 'PA') | (state == 'NY') | (state == 'NJ'):
        return 'PA-NY-NJ'
    else:
        return 'UNKNOWN'


udf_assign_region = udf(assign_region, StringType())


def filter_by_region(state):
    """Filter by region."""
    from pyspark.sql import Row

    if (state == 'NC') | (state == 'SC'):
        return Row('Out1', 'Out2', 'Out3', 'Out4')(-1.4, -1.1, 5.0, 3.5)
    elif (state == 'FL'):
        return Row('Out1', 'Out2', 'Out3', 'Out4')(-1.6, -1.1, 4.0, 3.0)
    elif (state == 'OH') | (state == 'KY') | (state == 'IN') | (state == 'MI') | (state == 'IL') | (state == 'IA'):
        return Row('Out1', 'Out2', 'Out3', 'Out4')(-1.5, -1.2, 5.5, 4.0)
    elif (state == 'PA') | (state == 'NY') | (state == 'NJ'):
        return Row('Out1', 'Out2', 'Out3', 'Out4')(-1.5, -1.2, 6.5, 4.5)


udf_filter_by_region = udf(
    filter_by_region,
    StructType([
        StructField('kurtosis_vacation', DoubleType(), False),
        StructField('kurtosis_maybe_vacation', DoubleType(), False),
        StructField('maxmin_vacation', DoubleType(), False),
        StructField('maxmin_maybe_vacation', DoubleType(), False),
    ]))


def bin_seasonal_homes(k, k_vacation, k_maybe_vacation, maxmin, maxmin_vacation, maxmin_maybe_vacation):
    """Bin seasonal home output.

    TODO: Explain the logic here.
    """
    if all([k < k_vacation, maxmin > maxmin_vacation]):
        return 2
    elif all([k < k_maybe_vacation, k >= k_vacation, maxmin > maxmin_maybe_vacation, maxmin <= maxmin_vacation]):
        return 1
    else:
        return 0


udf_bin_seasonal_homes = udf(bin_seasonal_homes, IntegerType())

spark = SparkSession.builder.getOrCreate()

max_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
max_date = max_date.replace(day=1)
min_date = max_date - datetime.timedelta(days=365)

bills = spark.table('bills')
locations = spark.table('locations')

locations = locations \
    .select([
        'account_id',
        'tenant_id',
        'state',
        'postal_code',
    ]) \
    .filter(
        (col('postal_code').isNotNull()) &
        (col('postal_code') != '')
    )

# Tenant filter: Select tenants for whom this model is supported
bills = bills.filter(bills['tenant_id'].isin([11, 12, 41, 43, 79, 83, 97, 103]))

states = locations.filter(
    (locations['tenant_id'].isin([11, 12, 41, 43, 79, 83, 97, 103])) &
    (locations['state'].isin(['OH', 'IN', 'KY', 'NC', 'SC', 'FL', 'PA', 'NY', 'IL', 'IA', 'MI', 'NJ']))
)

bills = bills \
    .join(states, ['account_id', 'tenant_id'], how='inner') \
    .select([
        'account_id',
        'location_id',
        'state',
        'tenant_id',
        'bill_start',
        'bill_end',
        'consumption_scaled',
        'fuel_type'
    ]) \
    .filter((bills.fuel_type == 'ELECTRIC') | ((bills.fuel_type == 'GAS') & (bills.tenant_id.isin([97, 103]))))

bills = bills \
    .withColumn('bill_start', bills.bill_start.cast('date')) \
    .withColumn('bill_end', bills.bill_end.cast('date')) \
    .withColumn('bill_consumption', bills.consumption_scaled.cast('double')) \
    .drop(bills['fuel_type']) \
    .drop(bills['consumption_scaled'])

bills = bills.withColumn('bill_days', datediff(bills.bill_end, bills.bill_start))
bills = bills.selectExpr('*', 'date_add(bill_start, bill_days / 2) as bill_midpoint')

# Note: These columns seem like things that could go into transform_bills.
# Other data products might be interested in yearmonth, midpoint, adc, etc
bills = bills \
    .withColumn('bill_month', date_format('bill_midpoint', 'MM').cast('string')) \
    .withColumn('bill_adc', bills.bill_consumption / bills.bill_days)

bills = bills.filter(bills.bill_midpoint >= min_date) \
    .filter(bills.bill_midpoint < max_date)

bills = bills \
    .select([
        'account_id',
        'location_id',
        'state',
        'tenant_id',
        'bill_month',
        'bill_adc'
    ])

b_p = bills \
    .groupby('account_id', 'location_id', 'tenant_id', 'state') \
    .pivot('bill_month', [
        '01',
        '02',
        '03',
        '04',
        '05',
        '06',
        '07',
        '08',
        '09',
        '10',
        '11',
        '12'
    ]) \
    .agg(first('bill_adc'))

b_p = b_p \
    .withColumn('spring', b_p['03']+b_p['04']+b_p['05']) \
    .withColumn('summer', b_p['06']+b_p['07']+b_p['08']) \
    .withColumn('fall', b_p['09']+b_p['10']+b_p['11']) \
    .withColumn('winter', b_p['12']+b_p['01']+b_p['02'])


b_p = b_p.withColumn(
    'max_min_ratio',
    greatest(b_p.spring, b_p.summer, b_p.fall, b_p.winter) /
    least(b_p.spring, b_p.summer, b_p.fall, b_p.winter)
)

a = bills.select('account_id', 'state', 'bill_adc')

a = a.withColumn('region', udf_assign_region(a.state))

# z score calc
av = a.groupby('region').agg({'bill_adc': 'avg'})
sd = a.groupby('region').agg(stddev('bill_adc'))

sdav = sd.join(av, 'region', how='inner')
a = a.join(sdav, 'region', how='full_outer')

a = a.withColumn('z_bill_adc', (a['bill_adc'] - a['`avg(bill_adc)`']) / a['`stddev_samp(bill_adc)`'])
a = a.select('account_id', 'z_bill_adc')

a = a.groupby('account_id').agg(kurtosis('z_bill_adc').alias('kurtosis'))

z = a.join(b_p, 'account_id')

znomiss = z.where(
    (col('summer').isNotNull()) &
    (col('winter').isNotNull()) &
    (col('max_min_ratio').isNotNull())
)

# Raw model output
znomiss = znomiss.select([
    'account_id',
    'location_id',
    'tenant_id',
    'state',
    'kurtosis',
    'max_min_ratio',
])

# Begin purple-boxification
df = znomiss.withColumn('stats', udf_filter_by_region(znomiss['state']))

# Select out the regional filter columns
df = df.select([
    'account_id',
    'location_id',
    'tenant_id',
    'state',
    'kurtosis',
    'max_min_ratio',
    'stats.*',
])

assert df.dtypes == [
    ('account_id', 'string'),
    ('location_id', 'string'),
    ('tenant_id', 'bigint'),
    ('state', 'string'),
    ('kurtosis', 'double'),
    ('max_min_ratio', 'double'),
    ('kurtosis_vacation', 'double'),
    ('kurtosis_maybe_vacation', 'double'),
    ('maxmin_vacation', 'double'),
    ('maxmin_maybe_vacation', 'double'),
]

# Apply binning function and tidy up purple box output
df_output = df \
    .withColumn(
        'is_seasonal',
        udf_bin_seasonal_homes(
            df['kurtosis'],
            df['kurtosis_vacation'],
            df['kurtosis_maybe_vacation'],
            df['max_min_ratio'],
            df['maxmin_vacation'],
            df['maxmin_maybe_vacation'],
        )
    ) \
    .drop(
        'kurtosis_vacation',
        'kurtosis_maybe_vacation',
        'maxmin_vacation',
        'maxmin_maybe_vacation',
    )

assert df_output.dtypes == [
    ('account_id', 'string'),
    ('location_id', 'string'),
    ('tenant_id', 'bigint'),
    ('state', 'string'),
    ('kurtosis', 'double'),
    ('max_min_ratio', 'double'),
    ('is_seasonal', 'int'),
]

df_output.createOrReplaceTempView('output')
