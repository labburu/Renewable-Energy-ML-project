"""Join PSEGLI Zeus data."""

from pyspark.sql.functions import (
    col,
    collect_set,
    concat,
    length,
    lit,
    size,
    trim,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel

try:
    # Spark likes it this way
    from channel_ingest import ChannelIngest
except Exception:
    # Tests like it this way
    from .channel_ingest import ChannelIngest


ERR_OUTPUT_SCHEMA_PSEGLI = StructType([
    StructField('file_name', StringType(), False, metadata={'maxlength': 255}),
    StructField('external_location_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_account_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_channel_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('direction', StringType(), True, metadata={'maxlength': 1}),
    StructField('tenant_id', LongType(), False),
    StructField('account_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('location_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('channel_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('error_code', IntegerType(), False),
    StructField('error_message', StringType(), False, metadata={'maxlength': 255})
])


class ChannelIngestPsegli(ChannelIngest):
    """
    Currently we are ONLY able to map from PSEG-LI external `account_id` in the AMI
    data to a single Zeus channel_uuid for zeus accounts that only have a single
    type ELECTRIC_METER direction DELIVERY.  All other accounts end up in the error
    file.  Channels MUST have been pre-ingested via the PSEG-LI Zeus ingest - this
    ingestor does not do any calls to zeus to ingest channels.

    The `service_point_id` supplied in PSEG-LI AMI *.lse files does not match any
    known account, location/premise or channel/meter ID that is processed by the zeus
    or bill ingestors so cannot be used for distinct channel ingesting.  This may
    lead to possibly duplicate readings per account per day if the site has multiple
    meters.
    """

    def load_ami(self, spark, table_name='ami'):
        """Load DataFrame with data from PSEG-LI output from `extract_ami_psegli`

        Do column filtering, distinct-ing, naming, and types to the
        ami_* columns.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name to load
        :return: DataFrame with following schema
         |-- file_name: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        return spark \
            .table(table_name) \
            .where(col('error_type').isNull() & col('code').isNotNull()) \
            .select([
                'source_file_name',
                'account_id',
                'service_point_id',
                'time_zone',
            ]) \
            .dropDuplicates() \
            .withColumnRenamed('source_file_name', 'file_name') \
            .withColumnRenamed('account_id', 'external_account_id') \
            .withColumnRenamed('service_point_id', 'external_channel_id') \
            .withColumnRenamed('time_zone', 'ami_time_zone') \
            .withColumn(
                'external_account_id',
                trim(col('external_account_id')).cast(StringType())
            ) \
            .withColumn(
                'external_channel_id',
                trim(col('external_channel_id')).cast(StringType())
            ) \
            .withColumn(
                'ami_time_zone',
                trim(col('ami_time_zone')).cast(StringType())
            ) \
            .where(
                (length(col('external_account_id')) > lit(0)) &
                (length(col('external_channel_id')) > lit(0))
            ) \
            .select([
                'file_name',
                'external_account_id',
                'external_channel_id',
                'ami_time_zone',
            ])

    def join_psegli_ami_to_zeus_ids(
        self,
        df_ami_ids,
        df_zeus_accounts,
        df_zeus_locations,
        df_zeus_channels,
        tenant_id,
    ):
        """Left-Join the AMI IDS with matching Zeus external IDs.

        This is a PSEG-LI-specific join function to match up AMI IDs with zeus
        IDs.  In the case of PSEG-LI, *only* the zeus external_account_id
        matches the AMI "customer_id" column.  The service_point_id column does
        *not* match any previously-ingested zeus location or channel ids - it
        remains a mystery where that comes from.

        :param df_ami_ids: DataFrame loaded from `extract_ami_psegli` task output.
        :param df_zeus_accounts: Dataframe with output from `load_zeus_accounts`
        :param df_zeus_locations: DataFrame with output from `load_zeus_locations`
        :param df_zeus_channels: Dataframe with output from `load_zeus_channels`
        :param int tenant_id: Tendril tenant identifier to add to result as tenant_id
        :return: DataFrame with following schema
         |-- file_name: string (nullable = true)
         |-- external_location_id: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
         |-- channel_uuid: string (nullable = true)
         |-- location_uuid: string (nullable = true)
         |-- account_uuid: string (nullable = true)
         |-- is_inactive: boolean (nullable = true)
         |-- time_zone: string (nullable = true)
         |-- tenant_id: long (nullable = false)
         |-- n_account_uuids: integer (nullable = false)
         |-- n_location_uuids: integer (nullable = false)
         |-- n_channel_uuids: integer (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        window = Window.partitionBy('za.account_uuid')
        df_zids = df_zeus_accounts.alias('za') \
            .join(
                df_zeus_locations
                .withColumnRenamed('time_zone', 'loc_time_zone')
                .alias('zl'),
                on=[
                    (col('za.account_uuid') == col('zl.account_uuid')),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_channels.alias('zc'),
                on=[
                    (col('zl.location_uuid') == col('zc.location_uuid')) &
                    (col('zc.direction_char') == lit('D'))
                ],
                how='left_outer',
            ) \
            .withColumn(
                'n_account_uuids',  # fill in with 1 because of zeus unique constraint
                lit(1)
            ) \
            .withColumn(
                'n_location_uuids',  # number zeus locations in account
                size(collect_set('zl.location_uuid').over(window))
            ) \
            .withColumn(
                'n_channel_uuids',  # number zeus channels in account
                size(collect_set('zc.channel_uuid').over(window))
            )
        return df_ami_ids.alias('ami') \
            .join(
                df_zids,
                on=[
                    col('ami.external_account_id') == col('za.external_account_id'),
                ],
                how='left_outer',
            ) \
            .withColumn(
                'tenant_id',
                lit(self.tenant_id).cast(LongType())
            ) \
            .withColumn(
                'direction',
                lit('D')
            ) \
            .withColumn(
                'time_zone',
                when(
                    col('ami_time_zone').isNull() | (length(col('ami_time_zone')) < lit(3)),
                    col('loc_time_zone')
                ).otherwise(col('ami_time_zone'))
            ) \
            .select([
                'ami.file_name',
                'zl.external_location_id',
                'ami.external_account_id',
                'ami.external_channel_id',
                'direction',
                'zc.channel_uuid',
                'zl.location_uuid',
                'za.account_uuid',
                'za.is_inactive',
                'time_zone',
                'tenant_id',
                'n_account_uuids',
                'n_location_uuids',
                'n_channel_uuids',
            ])

    def start_psegli_err_rows(self, spark, df_join):
        """Start DataFrame of error rows.

        Start DataFrame of Error row results, adding error_code and
        error_message columns and populating them where either the Zeus
        account_uuid or location_uuid are missing.

        :param array base_columns: starting (success) columns to begin the output
        :param df_after_rpc_find: Dataframe results of the call to
            `zeus_rpc_find_channels` to look for rows where account_uuid
            or location_uuid are missing.
        :return: Dataframe to be output as "error" rows following ERR_OUTPUT_SCHEMA_PSEGLI
        :rtype: pyspark.sql.DataFrame
        """
        df_errors = spark.createDataFrame([], ERR_OUTPUT_SCHEMA_PSEGLI)
        df_errors_join = df_join \
            .where(
                col('account_uuid').isNull() |
                col('location_uuid').isNull() |
                col('channel_uuid').isNull() |
                (col('n_account_uuids') > lit(1)) |
                (col('n_location_uuids') > lit(1)) |
                (col('n_channel_uuids') > lit(1))
            ) \
            .withColumn(
                'error_code',
                (
                    when(
                        condition=col('account_uuid').isNull(),
                        value=lit(4041)
                    )
                    .when(
                        condition=col('is_inactive') == lit(True),
                        value=lit(5141)
                    )
                    .when(
                        condition=col('location_uuid').isNull(),
                        value=lit(4042)
                    )
                    .when(
                        condition=col('channel_uuid').isNull(),
                        value=lit(4043)
                    )
                    .when(
                        condition=col('n_account_uuids') > lit(1),
                        value=lit(5241)
                    )
                    .when(
                        condition=col('n_location_uuids') > lit(1),
                        value=lit(5242)
                    )
                    .when(
                        condition=col('n_channel_uuids') > lit(1),
                        value=lit(5243)
                    )
                    .otherwise(
                        value=lit(5000)
                    )
                ).cast(IntegerType())
            ) \
            .withColumn(
                'error_message',
                (
                    when(
                        condition=col('account_uuid').isNull(),
                        value=lit('Unmatched account id')
                    )
                    .when(
                        condition=col('is_inactive') == lit(True),
                        value=lit('Inactive account')
                    )
                    .when(
                        condition=col('location_uuid').isNull(),
                        value=lit('Zeus location not found')
                    )
                    .when(
                        condition=col('channel_uuid').isNull(),
                        value=lit('Zeus channel not found')
                    )
                    .when(
                        condition=col('n_account_uuids') > lit(1),
                        value=concat(col('n_account_uuids'), lit(' account matches'))
                    )
                    .when(
                        condition=col('n_location_uuids') > lit(1),
                        value=concat(col('n_location_uuids'), lit(' location matches'))
                    )
                    .when(
                        condition=col('n_channel_uuids') > lit(1),
                        value=concat(col('n_channel_uuids'), lit(' channel matches'))
                    )
                    .otherwise(
                        value=lit('ERR_UNKNOWN')
                    )
                ).cast(StringType())
            ) \
            .select(ERR_OUTPUT_SCHEMA_PSEGLI.fieldNames())

        df_errors = df_errors.union(df_errors_join)
        return df_errors

    def run(self, spark):
        """Join Zeus data without ingest."""
        df_ami = self.load_ami(spark)
        df_zeus_accounts = self.load_zeus_accounts(spark, self.tenant_id)
        df_zeus_channels = self.load_zeus_channels(spark)
        df_zeus_locations = self.load_zeus_locations(spark, self.tenant_id, self.default_time_zone)
        df = self.join_psegli_ami_to_zeus_ids(
            df_ami_ids=df_ami,
            df_zeus_accounts=df_zeus_accounts,
            df_zeus_locations=df_zeus_locations,
            df_zeus_channels=df_zeus_channels,
            tenant_id=self.tenant_id,
        )

        # Cache the output of the join to avoid re-calculating for
        # every count/collect/write operation
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # Start output dataframe with all successfully joined rows where
        # we have matching zeus IDs and no secondary/tertiary channels
        df_complete = ChannelIngest.start_complete_rows(
            df.where(col('n_channel_uuids') == lit(1)))

        # Add count of existing channels matched either from airflow
        # export or zeus RPC to metrics
        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=ChannelIngest.start_metrics(spark),
            metric_name='channel_exist',
            df_to_count=df_complete,
        )
        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=df_metrics,
            metric_name='channel_ok',
            df_to_count=df_complete,
        )

        # These rows cannot be procssed so need to go to the "error"
        # output file.  Add error_* columns.
        df_err_rows = self.start_psegli_err_rows(spark, df)

        # Add final error metrics before outputting metrics dataframe
        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=df_metrics,
            metric_name='channel_error',
            df_to_count=df_err_rows,
        )
        df_metrics = ChannelIngest.add_error_code_metrics(
            df_metrics,
            df_err_rows
        )
        df_metrics = df_metrics.coalesce(1).cache()
        df_metrics.createOrReplaceTempView('metrics')

        # Save error rows if configured
        self.save_error_rows(df_err_rows)

        self.register_output(spark, df_complete)
