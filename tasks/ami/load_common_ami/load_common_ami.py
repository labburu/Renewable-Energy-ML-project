
import logging
import sys

from pyspark.sql.functions import (
    col,
    concat_ws,
    to_date,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)

# Metadata output schema
OUTPUT_SCHEMA = StructType([
    StructField('tenant_id', LongType(), False),
    StructField('date_utc', DateType(), False),
    StructField('row_count', IntegerType(), False),
])

# Common AMI should follow this structure
COMMON_AMI_SCHEMA = StructType([
    StructField('tenant_id', LongType(), False),
    StructField('channel_uuid', StringType(), False, metadata={'maxlength': 36}),
    StructField('interval_start_utc', TimestampType(), False),
    StructField('interval_end_utc', TimestampType(), False),
    StructField('time_zone', StringType(), False, metadata={'maxlength': 36}),
    StructField('seconds_per_interval', IntegerType(), False),
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


class LoadCommonAmi:

    def __init__(
        self,
        out_prefix,
        use_odometer=False,
        partition_columns=[
            'tenant_id',
            'year',
            'month',
            'day',
        ],
        write_mode='append',
        *args, **kwargs
    ):
        self.out_prefix = out_prefix
        self.use_odometer = use_odometer
        self.partition_columns = partition_columns
        self.write_mode = write_mode

    def _common_schema(self):
        if self.use_odometer:
            return COMMON_ODOMETER_AMI_SCHEMA
        else:
            return COMMON_AMI_SCHEMA

    def run(self, spark):
        """
        Re-bin and append common AMI data to year/month/day partitions
        """
        log.info('Load Common AMI start...')
        df_a = spark.table('ami')

        # Repartition and append to existing common AMI piles
        df_out = spark.createDataFrame(
            data=df_a.select(self._common_schema().fieldNames()).rdd,
            schema=self._common_schema(),
        )

        # Persist partitioned data to avoid recalculating df_dates
        df_out = df_out.persist()

        # Write to output buckets partitioned by t/y/m/d/file_name
        df_out.write \
            .partitionBy(self.partition_columns) \
            .parquet(self.out_prefix, mode=self.write_mode)

        log.info('Wrote partitioned data to {} ok'.format(self.out_prefix))

        # Output DataFrame is actually *metadata* of the dates and counts
        # Use above partitions to minimize shuffling when grouping for
        # performance then convert partition columns back into a DateType
        df_dates = df_out \
            .groupBy('tenant_id', 'year', 'month', 'day') \
            .count() \
            .withColumn(
                'date_utc',
                to_date(concat_ws('-', col('year'), col('month'), col('day')))
            ) \
            .withColumnRenamed('count', 'row_count') \
            .select(OUTPUT_SCHEMA.fieldNames())

        spark \
            .createDataFrame(
                data=df_dates.rdd,
                schema=OUTPUT_SCHEMA,
            ) \
            .coalesce(1) \
            .createOrReplaceTempView('output')

    def report(self, spark, default_tags):
        # Add sanity checks here
        pass
