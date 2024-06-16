"""Ingest and map all AEP location/account/meter identifiers to Tendril channel_id"""

import logging

from pyspark.sql.functions import col, input_file_name, length, lit, trim, udf
from pyspark.sql.types import StringType
from pyspark.storagelevel import StorageLevel

try:
    # Spark likes it this way
    from channel_ingest import ChannelIngest
except Exception:
    # Tests like it this way
    from .channel_ingest import ChannelIngest


log = logging.getLogger(__name__)


class ChannelIngestAep(ChannelIngest):

    @staticmethod
    def load_ami(spark, table_name='ami'):
        """Load DataFrame with data from AEP AMI CSVs.

        Do column filtering, distinct-ing, naming, and types to the
        ami_* columns.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name created by the DAG
            current run that has pattern-matched the execution date's
            raw AEP unencrypted files.
        :return: DataFrame with following schema
         |-- file_name: string (nullable = true)
         |-- external_location_id: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        clean_udf = udf(ChannelIngest.clean_file_name, StringType())

        return spark \
            .table(table_name) \
            .withColumn('file_name', clean_udf(input_file_name())) \
            .select([
                'file_name',
                'location_id',
                'account_id',
                'meter_id',
                'direction_meter_type',
            ]) \
            .dropDuplicates() \
            .withColumnRenamed('account_id', 'external_account_id') \
            .withColumnRenamed('location_id', 'external_location_id') \
            .withColumnRenamed('meter_id', 'external_channel_id') \
            .withColumnRenamed('direction_meter_type', 'direction') \
            .withColumn(
                'external_account_id',
                trim(col('external_account_id')).cast(StringType())
            ) \
            .withColumn(
                'external_location_id',
                trim(col('external_location_id')).cast(StringType())
            ) \
            .withColumn(
                'external_channel_id',
                trim(col('external_channel_id')).cast(StringType())
            ) \
            .withColumn(
                'direction',
                trim(col('direction')).cast(StringType())
            ) \
            .where(
                (length(col('external_location_id')) > lit(0)) &
                (length(col('external_account_id')) > lit(0)) &
                (length(col('external_channel_id')) > lit(0)) &
                (length(col('direction')) > lit(0))
            ) \
            .select([
                'file_name',
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
            ])

    def run(self, spark):
        """Main Entry Point for AEP Channel ingest."""
        df_ami_ids = ChannelIngestAep \
            .load_ami(spark)
        df_zeus_channels = ChannelIngest \
            .load_zeus_channels(spark)
        df_zeus_accounts = ChannelIngest \
            .load_zeus_accounts(spark, tenant_id=self.tenant_id)
        df_zeus_locations = ChannelIngest \
            .load_zeus_locations(spark, tenant_id=self.tenant_id, default_time_zone=self.default_time_zone)

        df = self.join_tenant_ami_to_zeus_ids(
            df_ami_ids=df_ami_ids,
            df_zeus_accounts=df_zeus_accounts,
            df_zeus_locations=df_zeus_locations,
            df_zeus_channels=df_zeus_channels,
            tenant_id=self.tenant_id,
        )

        # Cache the output of the csv join to avoid re-calculating for
        # every count/collect/write operation
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # Start summary statistics for metrics/audit rows
        df_metrics = ChannelIngest.start_metrics(spark)

        # Start output dataframe with all successfully joined rows where
        # we have matching zeus IDs
        df_complete = ChannelIngest.start_complete_rows(df)

        # Get rows that have incomplete channel information
        df_incomplete = ChannelIngest.select_incomplete_rows(df)

        # Fill in Zeus channel_uuid, location_uuid, account_uuid for all
        # incomplete rows
        df_rpc_find = self.zeus_rpc_find_channels(spark, df_incomplete)

        # After zeus RPC lookup, these are our complete rows that can be
        # part of success output
        df_complete = ChannelIngest.add_complete_rows(df_complete, df_rpc_find)

        # Add count of existing channels matched either from airflow
        # export or zeus RPC to metrics
        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=df_metrics,
            metric_name='channel_exist',
            df_to_count=df_complete,
        )

        # These rows cannot be procssed so need to go to the "error"
        # output file.  Add error_* columns.
        df_err_rows = ChannelIngest.start_err_rows(spark, df, df_rpc_find)

        # Get the rows that need to be ingested
        # These are rows where we are missing channel_uuid but *do* have
        # a zeus location/account
        df_ingestable = ChannelIngest.select_ingestable_rows(df_rpc_find)

        # Try to create new channels via zeus RPC for ingestable rows
        df_rpc_created = self.zeus_rpc_create_channels(spark, df_ingestable)

        # Add count of newly-ingested ingested channels to metrics
        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=df_metrics,
            metric_name='channel_ingest',
            df_to_count=df_rpc_created,
        )

        # After Zeus RPC create, these are our complete rows to be added
        # to success output
        df_complete = ChannelIngest.add_complete_rows(df_complete, df_rpc_created)

        df_metrics = ChannelIngest.add_metric_count(
            df_metrics=df_metrics,
            metric_name='channel_ok',
            df_to_count=df_complete,
        )

        # For rows unable to create channel, add to error rows
        df_err_rows = ChannelIngest.add_err_rows_from_rpc_create(
            df_err_rows,
            df_rpc_created
        )

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
