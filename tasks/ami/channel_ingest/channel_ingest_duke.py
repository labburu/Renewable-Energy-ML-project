"""Ingest new channels into Zeus from Duke AMI."""

import logging

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    collect_set,
    input_file_name,
    length,
    lit,
    lpad,
    size,
    trim,
    udf,
)
from pyspark.sql.types import LongType, StringType
from pyspark.storagelevel import StorageLevel

try:
    # Spark likes it this way
    from channel_ingest import ChannelIngest
except Exception:
    # Tests like it this way
    from .channel_ingest import ChannelIngest

log = logging.getLogger(__name__)


class ChannelIngestDuke(ChannelIngest):

    @staticmethod
    def load_ami(spark, table_name='ami', column_map=None):
        """
        Load DataFrame with data from Duke AMI csvs then apply column
        filtering, distinct-ing, naming, types, and zero-prefixing the
        ami_* columns.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name created by the DAG
            current run that has pattern-matched the execution date's
            raw Duke unencrypted files.
        :param column_map: The mapping of the column number and column name
            that will be used when loading the AMI data.
        :return: DataFrame with following schema
         |-- file_name: string (nullable = true)
         |-- external_location_id: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
         |-- raw_external_location_id: string (nullable = true)
         |-- raw_external_account_id: string (nullable = true)
         |-- raw_external_channel_id: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        clean_udf = udf(ChannelIngest.clean_file_name, StringType())

        if not column_map:
            column_map = {
                'external_location_id': '_c0',
                'external_account_id': '_c1',
                'external_channel_id': '_c2',
                'direction': '_c9'
            }

        return spark \
            .table(table_name) \
            .withColumn(
                'file_name',
                clean_udf(input_file_name())
            ) \
            .select(['file_name'] + list(column_map.values())) \
            .dropDuplicates() \
            .withColumnRenamed(column_map['external_location_id'], 'external_location_id') \
            .withColumnRenamed(column_map['external_account_id'], 'external_account_id') \
            .withColumnRenamed(column_map['external_channel_id'], 'external_channel_id') \
            .withColumnRenamed(column_map['direction'], 'direction') \
            .withColumn(
                'external_location_id',
                trim(col('external_location_id')).cast(StringType())
            ) \
            .withColumn(
                'external_account_id',
                trim(col('external_account_id')).cast(StringType())
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
            .withColumn('raw_external_location_id', col('external_location_id')) \
            .withColumn('raw_external_account_id', col('external_account_id')) \
            .withColumn('raw_external_channel_id', col('external_channel_id')) \
            .withColumn(
                'external_location_id',
                lpad(col('external_location_id'), 20, '0')
            ) \
            .withColumn(
                'external_account_id',
                lpad(col('external_account_id'), 20, '0')
            ) \
            .withColumn(
                'external_channel_id',
                lpad(col('external_channel_id'), 20, '0')
            ) \
            .select([
                'file_name',
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'raw_external_location_id',
                'raw_external_account_id',
                'raw_external_channel_id',
            ])

    @staticmethod
    def join_duke_ami_to_zeus_ids(
        df_ami_ids,
        df_zeus_account_hub_ids,
        df_zeus_accounts,
        df_zeus_locations,
        df_zeus_channels,
        tenant_id,
    ):
        """Left-Join the AMI IDS with matching Zeus external IDs.

        :param df_ami_ids: DataFrame from load_ami
        :param df_account_hub_ids: Dataframe with output from `load_zeus_account_hub_ids`
        :param df_zeus_accounts: Dataframe with output from `load_zeus_accounts`
        :param df_zeus_locations: DataFrame with output from `load_zeus_locations`
        :param df_zeus_channels: Dataframe with output from `load_zeus_channels`
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
         |-- n_account_uuids: int (nullable = false)
         |-- n_location_uuids: int (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        window = Window.partitionBy(
            'ami.external_location_id',
            'ami.external_account_id',
            'ami.external_channel_id',
            'ami.direction'
        )
        return df_ami_ids.alias('ami') \
            .join(
                df_zeus_account_hub_ids.alias('zh'),
                on=[
                    col('ami.external_account_id') == col('zh.hub_id'),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_accounts.alias('za'),
                on=[
                    col('zh.account_uuid') == col('za.account_uuid'),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_locations.alias('zl'),
                on=[
                    col('ami.external_location_id') == col('zl.external_location_id'),
                    col('zh.account_uuid') == col('zl.account_uuid'),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_channels.alias('zc'),
                on=[
                    col('ami.external_channel_id') == col('zc.external_channel_id'),
                    col('ami.direction') == col('zc.direction_char'),
                    col('zl.location_uuid') == col('zc.location_uuid'),
                ],
                how='left_outer',
            ) \
            .withColumn(
                'tenant_id',
                lit(tenant_id).cast(LongType())
            ) \
            .withColumn(
                'n_account_uuids',  # number distinct zeus account matches
                size(collect_set('zh.account_uuid').over(window))
            ) \
            .withColumn(
                'n_location_uuids',  # number distinct zeus location matches
                size(collect_set('zl.location_uuid').over(window))
            ) \
            .select([
                'ami.file_name',
                'ami.external_location_id',
                'ami.external_account_id',
                'ami.external_channel_id',
                'ami.direction',
                'zc.channel_uuid',
                'zl.location_uuid',
                'zh.account_uuid',
                'za.is_inactive',
                'zl.time_zone',
                'tenant_id',
                'n_account_uuids',
                'n_location_uuids',
            ])

    def run(self, spark):
        """Main Entry Point for Duke Channel ingest."""
        df_ami_ids = ChannelIngestDuke.load_ami(spark, column_map=self.column_map)
        df_zeus_channels = ChannelIngest.load_zeus_channels(spark)
        df_zeus_account_hub_ids = ChannelIngest.load_zeus_account_hub_ids(
            spark,
            tenant_id=self.tenant_id
        )
        df_zeus_accounts = ChannelIngest.load_zeus_accounts(
            spark,
            tenant_id=self.tenant_id
        )
        df_zeus_locations = ChannelIngest.load_zeus_locations(
            spark,
            tenant_id=self.tenant_id,
            default_time_zone=self.default_time_zone,
        )

        # Join AMI IDs to Zeus IDs
        df = ChannelIngestDuke.join_duke_ami_to_zeus_ids(
            df_ami_ids=df_ami_ids,
            df_zeus_account_hub_ids=df_zeus_account_hub_ids,
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

        # Replace zero-padded IDs in error output with original IDs
        # We needed the zero-padded IDs to interact with Zeus, but now
        # that that's over with, we want to return the original IDs for
        # downstream consumers.
        df_err_rows = df_err_rows.alias('err') \
            .join(
                df_ami_ids.alias('in'),
                on=[
                    col('in.external_channel_id') == col('err.external_channel_id'),
                    col('in.external_location_id') == col('err.external_location_id'),
                    col('in.external_account_id') == col('err.external_account_id'),
                    col('in.direction') == col('err.direction'),
                ],
                how='inner',
            ).select([
                col('err.file_name'),
                col('in.raw_external_location_id').alias('external_location_id'),
                col('in.raw_external_account_id').alias('external_account_id'),
                col('in.raw_external_channel_id').alias('external_channel_id'),
                col('err.direction'),
                col('err.tenant_id'),
                col('err.account_uuid'),
                col('err.location_uuid'),
                col('err.channel_uuid'),
                col('err.error_code'),
                col('err.error_message'),
            ])

        # Save error rows if configured
        self.save_error_rows(df_err_rows)

        # Replace zero-padded IDs with original IDs
        # We needed the zero-padded IDs to interact with Zeus, but now
        # that that's over with, we want to return the original IDs for
        # downstream consumers.
        df_complete = df_complete.alias('out') \
            .join(
                df_ami_ids.alias('in'),
                on=[
                    col('in.external_channel_id') == col('out.external_channel_id'),
                    col('in.external_location_id') == col('out.external_location_id'),
                    col('in.external_account_id') == col('out.external_account_id'),
                    col('in.direction') == col('out.direction'),
                ],
                how='inner',
            ) \
            .select([
                col('in.raw_external_location_id').alias('external_location_id'),
                col('in.raw_external_account_id').alias('external_account_id'),
                col('in.raw_external_channel_id').alias('external_channel_id'),
                col('out.direction'),
                col('out.tenant_id'),
                col('out.account_uuid'),
                col('out.location_uuid'),
                col('out.channel_uuid'),
                col('out.time_zone'),
            ])

        self.register_output(spark, df_complete)
