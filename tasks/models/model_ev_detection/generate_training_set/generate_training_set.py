import logging
import sys
from functools import reduce

from nydus.task import NydusTask
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rand, sum
from pyspark.sql.types import IntegerType

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class GenerateTrainingSet(NydusTask):

    def __init__(self, **kwargs):
        self.tenants_with_ami = kwargs.get('tenants_with_ami', [])
        self.model_interval_minutes_lst = kwargs.get('model_interval_minutes_lst', [])

    @staticmethod
    def _add_unlabeled_data_to_training_set(df, interval_minutes, n_add):
        n_unlabeled = df.filter(col('interval_minutes') == interval_minutes)\
            .agg(sum(col('has_ev').isNull().cast(IntegerType())))\
            .head(1)[0][0]
        df = df.withColumn('has_ev', when(col('interval_minutes') != interval_minutes, col('has_ev'))
                           .otherwise(when(col('has_ev').isNotNull(), col('has_ev'))
                                      .otherwise(when(rand(seed=1234567) < n_add / n_unlabeled, lit(0))
                                                 .otherwise(None))))
        return df

    def run(self, spark):

        channels_df = spark \
            .table('channels') \
            .withColumnRenamed('id', 'channel_id') \
            .select('channel_id', 'location_id')

        feature_dfs = []
        for tenant_id in self.tenants_with_ami:
            feature_dfs.append(spark.table('features_{}'.format(tenant_id)))

        feature_df = reduce(DataFrame.union, feature_dfs)

        feature_df = feature_df.withColumn('interval_minutes', when(col('tenant_id') == 109, 60)
                                           .otherwise(col('interval_minutes')))

        lpt_df = spark\
            .table('location_profiles_tabular') \
            .filter(col('electric_vehicle__value').isNotNull()) \
            .filter(col('electric_vehicle__priority') < 3)\
            .withColumnRenamed('electric_vehicle__value', 'has_ev')\
            .withColumn('has_ev', when(col('has_ev') == 'true', lit(1)).otherwise(lit(0)))\
            .select('location_id', 'has_ev')

        joined_df = feature_df.join(channels_df, on='channel_id', how='inner')\
                              .join(lpt_df, on='location_id', how='left')

        label_counts_by_interval_minutes = joined_df.groupBy('interval_minutes', 'has_ev').count()
        label_counts_by_interval_minutes.cache()

        for interval_minutes in self.model_interval_minutes_lst:
            try:
                pos_count = label_counts_by_interval_minutes \
                    .filter(col('has_ev') == 1) \
                    .filter(col('interval_minutes') == interval_minutes) \
                    .select('count').head(1)[0][0]
            except IndexError:
                log.warning('No positive labels available for {} minute model. '
                            'Dropping negative labels, cannot generate training set'.format(interval_minutes))
                joined_df = joined_df.filter(col('interval_minutes') != interval_minutes)
                continue
            try:
                neg_count = label_counts_by_interval_minutes \
                    .filter(col('has_ev') == 0) \
                    .filter(col('interval_minutes') == interval_minutes) \
                    .select('count').head(1)[0][0]
            except IndexError:
                log.info('No negative labels available for {} minute model. '
                         'Unlabeled data will be added to the training set.'.format(interval_minutes))
                joined_df = self._add_unlabeled_data_to_training_set(joined_df, interval_minutes, pos_count)
                continue

            if pos_count > neg_count:
                log.info('Positive labels exceed negative labels for {} minute model. '
                         'Unlabeled data will be added to the training set.'.format(interval_minutes))
                joined_df = self._add_unlabeled_data_to_training_set(joined_df, interval_minutes, pos_count - neg_count)

        label_counts_by_interval_minutes.unpersist()

        joined_df = joined_df.filter(col('has_ev').isNotNull()) \
            .filter(col('interval_minutes').isin(self.model_interval_minutes_lst))

        joined_df.createOrReplaceTempView('output')
