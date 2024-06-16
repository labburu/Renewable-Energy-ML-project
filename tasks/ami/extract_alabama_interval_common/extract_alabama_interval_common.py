
from pyspark.sql.functions import (
    col,
    dayofmonth,
    explode,
    input_file_name,
    lit,
    month,
    to_timestamp,
    to_utc_timestamp,
    udf,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

COMMON_ODOMETER_AMI_SCHEMA = StructType([
    StructField('tenant_id', LongType(), False),
    StructField('channel_uuid', StringType(), False, metadata={'maxlength': 36}),
    StructField('reading_ts_utc', TimestampType(), False),
    StructField('time_zone', StringType(), False, metadata={'maxlength': 36}),
    StructField('seconds_per_reading', IntegerType(), False),
    StructField('consumption', DoubleType(), True),
    StructField('direction', StringType(), False, metadata={'maxlength': 1}),
    StructField('consumption_unit', StringType(), True, metadata={'maxlength': 20}),
    StructField('consumption_code', IntegerType(), True),
    StructField('source_file_name', StringType(), False, metadata={'maxlength': 255}),
    StructField('updated_time_utc', TimestampType(), False),
    StructField('year', IntegerType(), False),
    StructField('month', IntegerType(), False),
    StructField('day', IntegerType(), False)
])


class ExtractAmiAlabama:

    def __init__(self, execution_date):
        """Set up job.

        :param str execution_date: The date on which the job was run.
            Running from Airflow, this is the airflow DagRun execution
            date.
        """
        self.execution_date = execution_date

    @staticmethod
    def clean_file_name(full_name, default='UNKNOWN'):
        """Return file name without leading path and file extension."""
        if full_name is None or len(full_name) < 1:
            fname = default
        else:
            fname = full_name.split('/')[-1]
            dotidx = fname.find('.')
            if dotidx > 0:
                fname = fname[0:dotidx]
        return fname

    def add_source_file_name(self, df):
        """Add column that contains parent file name."""
        clean_udf = udf(ExtractAmiAlabama.clean_file_name, StringType())
        return df.withColumn('source_file_name', clean_udf(input_file_name()))

    def run(self, spark):
        """Process Alabama AMI JSON data and return as parquet."""
        df_ami = spark.table('ami')
        df_channels = spark.table('channels')
        df_linkage = spark.table('linkage')

        # Keep track of the original file name
        df_ami = self.add_source_file_name(df=df_ami)

        # Join AMI to Zeus channels via AMI linkage files
        df_meters = df_ami.alias('a') \
            .join(
                df_linkage.alias('l'),
                on=[col('l.premisemeter.meterno') == col('a.meterreadings.meterno')],
                how='inner',
            ) \
            .join(
                df_channels.alias('c'),
                on=[col('c.external_channel_id') == col('l.premisemeter.realmeterno')],
                how='inner',
            )

        # NOTE: Residential meters are always hourly right now.
        # This could change in the future.
        # TODO: Handle multiplier
        df_meters \
            .withColumn('tenant_id', lit(109)) \
            .withColumn('channel_uuid', col('channel_uuid')) \
            .withColumn('interval_usage', explode(df_meters.meterreadings.intervalusage)) \
            .withColumn('reading', explode(col("interval_usage.intervals"))) \
            .withColumn('time_zone', lit('UTC')) \
            .withColumn('reading_ts_utc', to_utc_timestamp(to_timestamp(col("reading.timestamp")), 'UTC')) \
            .withColumn('seconds_per_reading', lit(3600)) \
            .withColumn('consumption', col('reading.usagevalue')) \
            .drop("reading") \
            .drop("exp_reading_ts_utc") \
            .withColumn('direction', lit('D')) \
            .withColumn('consumption_unit', lit('KWH')) \
            .withColumn('consumption_code', lit(1)) \
            .withColumn('source_file_name', col('source_file_name')) \
            .withColumn('updated_time_utc', to_timestamp(lit(self.execution_date), 'yyyy-MM-dd')) \
            .withColumn('year', year(col('reading_ts_utc'))) \
            .withColumn('month', month(col('reading_ts_utc'))) \
            .withColumn('day', dayofmonth(col('reading_ts_utc'))) \
            .select(COMMON_ODOMETER_AMI_SCHEMA.fieldNames()) \
            .drop_duplicates() \
            .createOrReplaceTempView('output')
