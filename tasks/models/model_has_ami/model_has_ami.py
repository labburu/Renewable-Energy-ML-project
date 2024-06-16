from datetime import datetime, timedelta

from pyspark.sql.functions import (
    col,
    max,
    sum as sqlsum,
    when,
)
from nydus.task import NydusTask
from nydus.utilities import filter_ymd_partitions


class ModelHasAMI(NydusTask):
    def __init__(self, evaluation_date):
        self.evaluation_date = datetime.strptime(evaluation_date, '%Y-%m-%d')
        self.evaluate_previous_days = 30

    # channel_id based has_ami model
    def has_ami_calculation(self, df_ami):
        df = filter_ymd_partitions(
            df_ami, self.evaluation_date, comparison_operator='<', is_local=True
        )
        if self.evaluate_previous_days:
            window_start = self.evaluation_date - timedelta(days=self.evaluate_previous_days)
            df = filter_ymd_partitions(
                df, window_start, comparison_operator='>=', is_local=True
            )

        df = df \
            .withColumnRenamed('channel_uuid', 'channel_id') \
            .withColumnRenamed('consumption_total', 'consumption') \
            .select(
                'channel_id',
                'tenant_id',
                'consumption',
                'num_reads_actual',
                'num_reads_total',
            )

        df_has_ami = df.groupBy('channel_id', 'tenant_id') \
            .agg(
            sqlsum('num_reads_actual').alias('row_count'),
        ) \
            .withColumn('has_ami', when(col('row_count') > 0, True).otherwise(False)) \
            .select(['channel_id', 'tenant_id', 'has_ami'])

        return df_has_ami

    def run(self, spark):

        df_ami = spark.table('ami')
        df_ami = self.has_ami_calculation(df_ami)

        df_channels = spark.table('channels').select(
            col('id').alias('channel_id'),
            col('location_id')
        )

        df_final = df_ami.join(df_channels, 'channel_id', how='inner')
        df_final = df_final.drop('channel_id')

        df_final \
            .groupBy('tenant_id', 'location_id') \
            .agg(
                max('has_ami').alias('has_ami'),
            ) \
            .select(
                'tenant_id',
                'location_id',
                'has_ami'
            )

        df_final.createOrReplaceTempView('output')
