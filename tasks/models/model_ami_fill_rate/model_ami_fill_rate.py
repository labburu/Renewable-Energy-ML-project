from datetime import datetime, timedelta

from pyspark.sql.functions import (
    avg as sqlavg,
    col
)
from nydus.task import NydusTask
from nydus.utilities import filter_ymd_partitions


class ModelAMIFillRate(NydusTask):
    def __init__(self, evaluation_date):
        self.evaluation_date = datetime.strptime(evaluation_date, '%Y-%m-%d')
        self.evaluate_previous_days = 75

    # channel_id based ami fill rate calculation
    def ami_fill_rate_calculation(self, df_ami):
        df_ami = filter_ymd_partitions(df_ami, self.evaluation_date, comparison_operator='<', is_local=True)
        if self.evaluate_previous_days:
            window_start = self.evaluation_date - timedelta(days=self.evaluate_previous_days)
            df_ami = filter_ymd_partitions(df_ami, window_start, comparison_operator='>=', is_local=True)

        df = df_ami \
            .withColumnRenamed('channel_uuid', 'channel_id') \
            .withColumnRenamed('consumption_total', 'consumption') \
            .select(
                'channel_id',
                'tenant_id',
                'consumption',
                'pct_complete'
            )

        df_ami_fill_rate = df.groupBy('channel_id', 'tenant_id') \
            .agg(
            sqlavg('pct_complete').alias('avg_pct_complete'),
        ) \
            .withColumn('fill_rate', col('avg_pct_complete') / 100) \
            .select(['channel_id',
                     'tenant_id',
                     'fill_rate'
                     ])

        return df_ami_fill_rate

    def run(self, spark):

        df_ami = spark.table('ami')
        df_ami = self.ami_fill_rate_calculation(df_ami)

        df_channels = spark.table('channels').select(
            col('id').alias('channel_id'),
            col('location_id')
        )

        df_final = df_ami.join(df_channels, 'channel_id', how='inner')
        df_final = df_final.drop('channel_id')

        df_final \
            .groupBy('tenant_id', 'location_id') \
            .agg(
                sqlavg('fill_rate').alias('fill_rate')
            ) \
            .select(
                'tenant_id',
                'location_id',
                'fill_rate'
            )

        df_final.createOrReplaceTempView('output')
