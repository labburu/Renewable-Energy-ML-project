import logging
import sys

from pyspark.sql.functions import col, lit

try:
    # Spark likes it this way
    from extract_common_ami import ExtractCommonAmi
    from extract_common_ami import ERROR_SCHEMA as PSEGLI_ERROR_SCHEMA
except Exception:
    # Tests like it this way
    from .extract_common_ami import ExtractCommonAmi
    from .extract_common_ami import ERROR_SCHEMA as PSEGLI_ERROR_SCHEMA

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class ExtractCommonAmiPsegli(ExtractCommonAmi):

    def read_ami_data(self, spark):
        """
        Override base method since file name already added etc by
        `extract_ami_psegli`.
        """
        df_ami = spark.table('ami')

        log.info('Adding column: updated_time_utc')
        df_ami = self.add_updated_timestamp(
            df=df_ami,
            pattern=self.file_pattern,
            replacement=self.file_pattern_replacement,
        )

        # Basic column renames
        df_ami = df_ami \
            .withColumnRenamed('account_id', 'external_account_id')

        # Drop columns that are duplicated from psegli channel_ingest
        df_ami = df_ami.drop(col('time_zone'))

        return df_ami

    def run(self, spark):
        """Transform tenant AMI data to Tendril common format."""
        log.info('Reading raw AMI data')
        df_ami = self.read_ami_data(spark)

        log.info('Reading ami-to-zeus channel mapping')
        df_channels = self.read_channels_data(spark)

        log.info('Adding columns: tenant_id, channel_uuid')
        # PSEG-LI ONLY is able to join to data based on external_account_id since the
        # "service_point_id" supplied in AMI data has no match to previously-ingested
        # external IDs in zeus.
        df = self.join_ami_to_channels(df_ami, df_channels, join_on=[
            'external_account_id'])

        log.info('Adding column: seconds_per_interval')
        df = self.add_interval(df, col_interval=self.col_interval)

        log.info('Adding column: time_zone')
        df = self.add_time_zone(df, col_time_zone='time_zone')

        log.info('Adding columns: interval_start_utc, interval_end_utc, year, month, day')
        df = self.add_timestamps(df, col_timestamp=self.col_timestamp,
                                 col_interval=self.col_interval,
                                 col_time_zone=self.col_time_zone)

        log.info('Adding column: consumption')
        df = self.add_consumption(df, col_consumption=self.col_consumption)

        log.info('Adding column: consumption_unit')
        df = self.add_consumption_unit(df, col_unit=self.col_consumption_unit)

        log.info('Adding column: consumption_code')
        df = self.add_consumption_code(df, col_code=self.col_consumption_code)

        log.info('Register DataFrame as output')
        self.register_output(spark, df)

    def report(self, spark, default_tags=None):
        """Report job errors using PSEG-LI join-only-on-external-account-id"""
        # Pull in unmatched rows from extract_ami_psegli-format AMI data
        df_errors = spark \
            .table('errors') \
            .select([
                'external_account_id',
                'source_file_name',
                col(self.col_timestamp).cast('timestamp').alias('timestamp'),
                col(self.col_consumption).cast('double').alias('consumption'),
                col(self.col_consumption_code).cast('string').alias('read_code'),
            ])

        # Pull in errors from channel ingest job
        df_ingest_errors = spark \
            .table('ingest_errors') \
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
            .join(df_ingest_errors, on=['external_account_id'], how='left_outer') \
            .withColumn('tenant_id', lit(self.tenant_id).cast('long')) \

        # Registering this as a spark sql view makes testing easier
        spark.createDataFrame(data=[], schema=PSEGLI_ERROR_SCHEMA) \
            .union(
                df.select(PSEGLI_ERROR_SCHEMA.fieldNames())
                .where(col('error_code').isNotNull())
                .dropDuplicates()
            ) \
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
