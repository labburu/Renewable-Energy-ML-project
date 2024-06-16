
import logging
import sys
from datetime import datetime

from pyspark.sql.functions import (
    col,
    dayofmonth,
    from_unixtime,
    input_file_name,
    lit,
    month,
    regexp_replace,
    to_utc_timestamp,
    trim,
    udf,
    unix_timestamp,
    when,
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

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)

# The four items required to arrive at a unique channel
JOIN_QUAD = [
    'external_location_id',
    'external_account_id',
    'external_channel_id',
    'direction',
]

# Tendril common AMI format
OUTPUT_SCHEMA = StructType([
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

ERROR_SCHEMA = StructType([
    StructField('tenant_id', LongType(), True),
    StructField('external_location_id', StringType(), True, metadata={'maxlength': 20}),
    StructField('external_account_id', StringType(), True, metadata={'maxlength': 20}),
    StructField('external_channel_id', StringType(), True, metadata={'maxlength': 20}),
    StructField('direction', StringType(), True, metadata={'maxlength': 1}),
    StructField('timestamp', TimestampType(), True),
    StructField('read_code', StringType(), True, metadata={'maxlength': 20}),
    StructField('source_file_name', StringType(), True, metadata={'maxlength': 255}),
    StructField('error_code', IntegerType(), True),
    StructField('error_message', StringType(), True, metadata={'maxlength': 255})
])


class ExtractCommonAmi:

    def __init__(self,
                 tenant_id,
                 col_external_account_id,
                 col_external_channel_id,
                 col_external_location_id,
                 col_timestamp,
                 col_interval,
                 col_consumption,
                 col_consumption_unit,
                 col_consumption_code,
                 col_direction,
                 col_time_zone=None,
                 default_time_zone='UTC',
                 errors_save_to=None,
                 errors_save_format='parquet',
                 errors_save_partitions='9',
                 file_pattern='',
                 file_pattern_replacement='',
                 *args, **kwargs):
        """Set up transform with tenant column mappings."""
        self.tenant_id = tenant_id
        self.col_external_account_id = col_external_account_id
        self.col_external_channel_id = col_external_channel_id
        self.col_external_location_id = col_external_location_id
        self.col_timestamp = col_timestamp
        self.col_interval = col_interval
        self.col_consumption = col_consumption
        self.col_consumption_unit = col_consumption_unit
        self.col_consumption_code = col_consumption_code
        self.col_direction = col_direction
        self.col_time_zone = col_time_zone
        self.default_time_zone = default_time_zone
        self.errors_save_to = errors_save_to
        self.errors_save_format = errors_save_format
        self.errors_save_partitions = int(errors_save_partitions)
        self.file_pattern = file_pattern
        self.file_pattern_replacement = file_pattern_replacement

    @staticmethod
    def clean_file_name(full_name, default='UNKNOWN'):
        """Return file name with leading path removed and trailing extension removed"""
        if full_name is None or len(full_name) < 1:
            fname = default
        else:
            fname = full_name.split('/')[-1]
            dotidx = fname.find('.')
            if dotidx > 0:
                fname = fname[0:dotidx]
        return fname

    def add_source_file_name(self, df):
        """Add column that contains parent file name stripped of path/extension"""
        clean_udf = udf(ExtractCommonAmi.clean_file_name, StringType())
        return df.withColumn('source_file_name', clean_udf(input_file_name()))

    def add_updated_timestamp(self, df, pattern, replacement):
        """Regex parse local updated timestamp out of file name.

        E.g., Duke AMI file names get replaced like so:
        AMI_NC01_1800_01032019T1206_03 -> 2019-03-01T12:06

        Then translate to UTC timestamp as Column: `updated_time_utc`.
        """
        default_datetime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M')
        return df \
            .withColumn(
                'tmp_ts_str',
                regexp_replace(
                    str=col('source_file_name'),
                    pattern=pattern,
                    replacement=replacement,
                )
            ) \
            .withColumn(
                'tmp_ts_str',
                when(col('tmp_ts_str').like('20%'), col('tmp_ts_str'))
                .otherwise(lit(default_datetime))
            ) \
            .withColumn(
                'updated_time_utc',
                to_utc_timestamp(
                    timestamp=col('tmp_ts_str'),
                    tz=self.default_time_zone
                )
            ) \
            .drop('tmp_ts_str')

    def read_ami_data(self, spark):
        """Read AMI data and add file metadata.

        The functions in here need to run prior to the tenant AMI data
        being joined with Tendril internal data.
        """
        df_ami = spark.table('ami')

        log.info('Adding column: source_file_name')
        df_ami = self.add_source_file_name(df_ami)

        log.info('Adding column: updated_time_utc')
        df_ami = self.add_updated_timestamp(
            df=df_ami,
            pattern=self.file_pattern,
            replacement=self.file_pattern_replacement,
        )

        log.info('Adding column: direction')
        df_ami = self.add_direction(
            df=df_ami,
            col_direction=self.col_direction,
        )

        if self.col_external_location_id is not None:
            log.info('ami column %s->external_location_id', self.col_external_location_id)
            df_ami = df_ami.withColumnRenamed(self.col_external_location_id, 'external_location_id')
        if self.col_external_account_id is not None:
            log.info('ami column %s->external_account_id', self.col_external_account_id)
            df_ami = df_ami.withColumnRenamed(self.col_external_account_id, 'external_account_id')
        if self.col_external_channel_id is not None:
            log.info('ami column %s->external_channel_id', self.col_external_channel_id)
            df_ami = df_ami.withColumnRenamed(self.col_external_channel_id, 'external_channel_id')

        df_ami = df_ami.withColumn('external_location_id', trim(col('external_location_id')))

        return df_ami

    def read_channels_data(self, spark):
        """Read output from tenant channel ingestor.

        This allows us to attach internal channel id to external ids.

        Schema:
         |-- external_location_id: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
         |-- channel_uuid: string (nullable = true)
         |-- tenant_id: bigint (nullable = true)
         |-- time_zone: string (nullable = true)
        +--------------------+-------------------+-------------------+----------+------------+----------+----------+
        |external_location_id|external_account_id|external_channel_id|direction |channel_uuid|tenant_id |time_zone |
        +--------------------+-------------------+-------------------+----------+------------+----------+----------+
        |1689351373          |1863512696         |1080489625         |D         |00000000-...|99        |US/Eastern|
        |1725044             |1910839714         |1112807745         |D         |00000000-...|99        |US/Eastern|
        |1094042706          |2137510950         |1113521201         |R         |00000000-...|99        |US/Eastern|
        +--------------------+-------------------+-------------------+----------+------------+----------+----------+
        """
        return spark.table('channels') \
            .where(col('tenant_id') == self.tenant_id) \
            .select([
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'channel_uuid',
                'tenant_id',
                'time_zone'
            ])

    def join_ami_to_channels(self, df_ami, df_channels, join_on=None):
        """Join Tenant AMI data to ingested Tendril IDs.

        This attaches Tendril ID "join quad" to tenant external quad.
        Each "direction" gets its own channel, so to get a unique match
        you need to join location, account, channel, AND direction.

        Source columns are renamed to make Tenant AMI data schema match
        the channel ingest output.
        """
        if join_on is None:
            # Fill in default join columns if unspecified
            join_on = JOIN_QUAD
        df = df_ami \
            .join(df_channels, on=join_on, how='left_outer')

        # Persist joined data to reduce re-scans of the csv for each count/write below
        df = df.persist()

        # Start errors dataframe with all unmatched rows, exclude unmatched from output
        df_err = df.where(col('channel_uuid').isNull() | col('tenant_id').isNull())
        df_err.createOrReplaceTempView('errors')

        # Proceed with matched data
        return df.where(col('channel_uuid').isNotNull() & col('tenant_id').isNotNull())

    def add_time_zone(self, df, col_time_zone):
        """Add time_zone column to existing DataFrame.

        If we get a time zone with the AMI data, that column should be
        used here. Otherwise, we default to 'time_zone', which comes in
        through the channel ingest, which pulls from Zeus locations.

        This column will allow downstream consumers of this data to get
        from the UTC timestamp back to the local timestamp.
        """
        if not col_time_zone:
            col_time_zone = 'time_zone'

        return df.withColumn(
            'time_zone',
            when(col(col_time_zone).isNull(), lit(self.default_time_zone))
            .when(col(col_time_zone).isin(['EST', 'EDT', 'EST5EDT']), lit('US/Eastern'))
            .when(col(col_time_zone).isin(['CST', 'CDT', 'CST6CDT']), lit('US/Central'))
            .when(col(col_time_zone).isin(['MST', 'MDT', 'MST7MDT']), lit('US/Mountain'))
            .when(col(col_time_zone).isin(['PST', 'PDT', 'PST8PDT']), lit('US/Pacific'))
            .otherwise(col(col_time_zone))
        )

    def add_timestamps(self, df, col_timestamp,
                       col_interval='seconds_per_interval',
                       col_time_zone=None):
        """Add timestamp information.

        DataFrame must have existing timestamp and time zone columns.

        This assumes that we're operating on interval data, where the
        timestamp given represents the _end_ timestamp of the interval.
        In order to roll up consumption to a coarser resolution, we need
        the interval start time so that the consumption occurs at the
        correct time. We calculate the interval start time as the end
        time in epoch seconds minus the duration of the interval.

        Also adds year/month/day output partitioning columns on the
        interval start time.

        Notes:
            - requires Spark 2.4+, which allows second argument of
              `to_utc_timestamp` to be Column
        """
        if self.tenant_id == 11:
            # Duke timestamps need to be converted to UTC with a fixed
            # offset. Duke does not give us a timezone column, but
            # rather two timestamp columns:
            #   - one (_c3) with a fixed 5-hour offset from UTC,
            #   - the other (_c4) that varies with US/Eastern DST rules
            # Since Daylight Savings timestamps skip and hour in the
            # spring and have two 2am readings in the fall, we should
            # choose the fixed-offset column to convert to UTC. And we
            # use a fixed "Etc/GMT+5" posix timezone name to do the
            # conversion with no daylight savings rules. That adds a
            # fixed 5-hour offset to get to UTC.
            df = df.withColumn(
                'interval_end_utc',
                to_utc_timestamp(timestamp=col(col_timestamp), tz='Etc/GMT+5')
            )
        elif self.tenant_id == 12:
            # AEP timestamps already come in UTC, formatted as a string
            df = df.withColumn(
                'interval_end_utc',
                col(col_timestamp).cast('timestamp')
            )
        elif self.tenant_id == 79:
            # PSEG-LI timestamps come in the time_zone
            # specified in the *.lse files
            df = df.withColumn(
                'interval_end_utc',
                to_utc_timestamp(timestamp=col_timestamp, tz=col_time_zone)
            )
        elif self.tenant_id == 41:
            # Alliant timestamps need to be converted to UTC with a fixed
            # offset. Duke does not give us a timezone column, but
            # rather two timestamp columns:
            #   - one (_c3 ?) with a fixed 5-hour offset from UTC,
            #   - the other (_c4 ?) that varies with US/Eastern DST rules
            # Since Daylight Savings timestamps skip and hour in the
            # spring and have two 2am readings in the fall, we should
            # choose the fixed-offset column to convert to UTC. And we
            # use a fixed "Etc/GMT+5" posix timezone name to do the
            # conversion with no daylight savings rules. That adds a
            # fixed 5-hour offset to get to UTC.
            df = df.withColumn(
                'interval_end_utc',
                to_utc_timestamp(timestamp=col(col_timestamp), tz='Etc/GMT+5')
            )
        else:
            raise Exception('tenant_id is required')

        return df \
            .withColumn(
                'interval_start_utc',
                from_unixtime(
                    unix_timestamp(col('interval_end_utc')) - col(col_interval)
                ).cast('timestamp')
            ) \
            .withColumn('year', year(col('interval_start_utc')).cast('int')) \
            .withColumn('month', month(col('interval_start_utc')).cast('int')) \
            .withColumn('day', dayofmonth(col('interval_start_utc')).cast('int'))

    def add_interval(self, df, col_interval):
        """Add column that contains the AMI interval length.

        Typically 900 or 1800 (seconds).
        """
        return df.withColumn(
            'seconds_per_interval',
            col(col_interval).cast('int')
        )

    def add_consumption(self, df, col_consumption):
        """Add column that contains the consumption value."""
        return df.withColumn(
            'consumption',
            col(col_consumption).cast('double')
        )

    def add_consumption_unit(self, df, col_unit):
        """Add column that contains the unit of consumption.

        This is usually 'KWH'.
        """
        return df.withColumn(
            'consumption_unit',
            col(col_unit).cast('string')
        )

    def add_consumption_code(self, df, col_code):
        """Add consumption code in Tendril internal code format.

        The code column has contained leading/trailing whitespace in the
        past, so we pass it through trim() here.
        """
        codes_actual = [
            'A',    # AEP
            'ACT',  # Duke and ALLIANT
            'M',    # PSEG-LI Origin metered
        ]
        codes_estimated = [
            'E',    # AEP
            'EST',  # Duke and ALLIANT
            'P',    # PSEG-LI Origin profiled
        ]
        codes_prorated = [
            'PRO',  # Duke
            'C',    # PSEG-LI Origin computed
            'S',    # PSEG-LI Origin statistic
        ]
        codes_missing = [
            'MIS',  # Duke
        ]
        return df \
            .withColumn(col_code, trim(col(col_code))) \
            .withColumn(
                'consumption_code',
                (when(col(col_code).isin(codes_actual), lit(1))
                 .when(col(col_code).isin(codes_estimated), lit(2))
                 .when(col(col_code).isin(codes_prorated), lit(3))
                 .when(col(col_code).isin(codes_missing), lit(0))
                 .otherwise(lit(None))).cast('int')
            )

    def add_direction(self, df, col_direction):
        """Add consumption direction column.

        Note: This is from the perspective of the power company
        """
        valid_directions = [
            'D',  # "delivered": regular consumption
            'R',  # "received": generation (i.e., solar, wind, etc)
        ]
        return df \
            .withColumn('direction', trim(col(col_direction))) \
            .where(col('direction').isin(valid_directions))

    def register_output(self, spark, df):
        """Register output DataFrame.

        Use final OUTPUT_SCHEMA and register temp view: `output`.

        :return: None
        """
        spark.createDataFrame(data=[], schema=OUTPUT_SCHEMA) \
            .union(df.select(OUTPUT_SCHEMA.fieldNames())) \
            .createOrReplaceTempView('output')

    def run(self, spark):
        """Transform tenant AMI data to Tendril common format."""
        log.info('Reading raw AMI data')
        df_ami = self.read_ami_data(spark)

        log.info('Reading ami-to-zeus channel mapping')
        df_channels = self.read_channels_data(spark)

        log.info('Adding columns: tenant_id, channel_uuid')
        df = self.join_ami_to_channels(df_ami, df_channels)

        log.info('Adding column: seconds_per_interval')
        df = self.add_interval(df, col_interval=self.col_interval)

        log.info('Adding column: time_zone')
        df = self.add_time_zone(df, col_time_zone=self.col_time_zone)

        log.info('Adding columns: interval_start_utc, interval_end_utc, year, month, day')
        df = self.add_timestamps(df, col_timestamp=self.col_timestamp, col_interval=self.col_interval)

        log.info('Adding column: consumption')
        df = self.add_consumption(df, col_consumption=self.col_consumption)

        log.info('Adding column: consumption_unit')
        df = self.add_consumption_unit(df, col_unit=self.col_consumption_unit)

        log.info('Adding column: consumption_code')
        df = self.add_consumption_code(df, col_code=self.col_consumption_code)

        log.info('Register DataFrame as output')
        self.register_output(spark, df)

    def report(self, spark, default_tags=None):
        """Report job errors.

        TODO: Send something to DataDog from here.
        """
        # Pull in unmatched rows from raw AMI data
        df_errors = spark \
            .table('errors') \
            .select([
                'external_channel_id',
                'external_account_id',
                'external_location_id',
                'direction',
                'source_file_name',
                col(self.col_timestamp).cast('timestamp').alias('timestamp'),
                col(self.col_consumption).cast('double').alias('consumption'),
                col(self.col_consumption_code).cast('string').alias('read_code'),
            ])

        # Pull in errors from channel ingest job
        df_dd = spark.table('ingest_errors').dropDuplicates(JOIN_QUAD)

        df_ingest_errors = df_dd \
            .select([
                'external_channel_id',
                'external_account_id',
                'external_location_id',
                'direction',
                'error_code',
                'error_message',
            ])

        error_count = df_errors.count()
        log.info('Error rows (i.e., incomplete matches): %s', error_count)

        # Join unmatched rows to ingest errors to get error messages
        df = df_errors \
            .join(df_ingest_errors, on=JOIN_QUAD, how='left_outer') \
            .withColumn('tenant_id', lit(self.tenant_id).cast(LongType()))

        # Registering this as a spark sql view makes testing easier
        spark.createDataFrame(data=[], schema=ERROR_SCHEMA) \
            .union(df.select(ERROR_SCHEMA.fieldNames())) \
            .createOrReplaceTempView('error_output')

        # Save error output if configured
        if self.errors_save_to:
            log.info('Writing error output: %s', self.errors_save_to)
            spark.table('error_output') \
                .coalesce(self.errors_save_partitions) \
                .write.save(
                    path=self.errors_save_to,
                    format=self.errors_save_format,
                    mode='overwrite',
                )
