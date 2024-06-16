import io
import logging
import os.path as op
from functools import reduce

import boto3
import joblib
import numpy as np
from nydus.task import NydusTask
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max, when, udf
from pyspark.sql.types import FloatType, IntegerType, StringType

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

_trained_pipelines_dict = None


class RunModels(NydusTask):
    def __init__(self, trained_pipelines_dict=None, **kwargs):
        self.tenants_with_ami = kwargs.get('tenants_with_ami', [])
        self.model_interval_minutes_lst = kwargs.get('model_interval_minutes_lst', [])
        self.features = kwargs.get('features',  [])

        self.s3_bucket_name = kwargs.get('s3_bucket_name')
        self.s3_prefix_trained_model = op.join('dags', kwargs.get('dag_id'),
                                               kwargs.get('train_task_id'),
                                               kwargs.get('run_date'))
        self.s3_client = boto3.client('s3', verify=boto3.__file__.replace('boto3/__init__.py', 'certifi/cacert.pem'))

        global _trained_pipelines_dict
        if trained_pipelines_dict is None:
            _trained_pipelines_dict = self.load_pickle_models_to_dict()
        else:
            _trained_pipelines_dict = trained_pipelines_dict

    def load_pickle_models_to_dict(self):

        trained_pipelines_dict = {}

        for interval_minutes in self.model_interval_minutes_lst:
            bytes_buffer = io.BytesIO()
            prefix = self.s3_prefix_trained_model.format(interval_minutes)
            key = prefix + '/' + (prefix
                                  .replace('dags/', '')
                                  .replace('/', '__')
                                  .replace('-', '')) + '.joblib'
            try:
                self.s3_client.download_fileobj(Bucket=self.s3_bucket_name,
                                                Key=key,
                                                Fileobj=bytes_buffer)
                trained_pipelines_dict[interval_minutes] = joblib.load(bytes_buffer)
            except ClientError:
                log.warning('Could not load {} minute model. No predictions will be made on'
                            ' {} minute interval data'.format(interval_minutes, interval_minutes))
                self.model_interval_minutes_lst.remove(interval_minutes)
                continue

        log.info("Pickles locked and loaded!")
        return trained_pipelines_dict

    @staticmethod
    def run_models(interval_minutes, *args):

        features = np.array(args).reshape(1, -1)

        try:
            pipeline = _trained_pipelines_dict[interval_minutes]
            prediction = pipeline.predict_proba(features)
            return str(prediction[0][1])

        except KeyError:
            return str(0)

    @staticmethod
    def post_filter_results(df_final, df_locs, df_experian, df_lpt):
        return df_final.join(df_lpt, on='location_id', how='left')\
                        .join(df_locs, on='location_id', how='left')\
                        .join(df_experian, on='location_id', how='left')\
                        .withColumn('ground_truth__ev_yes', when(col('electric_vehicle__value').isNotNull(),
                                                                 when(col('electric_vehicle__value') == 'true', lit(1))
                                                                 .otherwise(lit(0))).otherwise(lit(None)))\
                        .withColumn('score__ev_yes', when((col('experian_dwelling_type') == 'MULTI_FAMILY') |
                                                          (col('address').contains('LOT')) |
                                                          col('supervised_score').isNull() |
                                                          col('ground_truth__ev_yes').isNotNull(), lit(-1))
                                    .otherwise(col('supervised_score')))\
                        .select('tenant_id', 'location_id', 'interval_minutes', 'supervised_score',
                                'heuristic_score', 'ground_truth__ev_yes', 'score__ev_yes')

    def run(self, spark):
        df_channels = spark.table('channels')
        df_channels = df_channels \
            .withColumnRenamed('id', 'channel_id') \
            .select('channel_id', 'location_id')
        df_channels.persist()

        df_locs = spark.table('locations') \
            .withColumnRenamed('id', 'location_id') \
            .select('state', 'address', 'location_id')

        df_lpt = spark.table('location_profiles_tabular') \
            .filter(col('electric_vehicle__priority') < 3)\
            .select('location_id', 'electric_vehicle__value')

        df_experian = spark.table('experian') \
            .select('location_id', 'experian_dwelling_type')

        feature_dataframes = []
        for tenant_id in self.tenants_with_ami:
            feature_dataframes.append(spark.table('features_{}'.format(tenant_id)))

        df_features = reduce(DataFrame.union, feature_dataframes)

        udf_run_models = udf(self.run_models, StringType())
        df_scores = df_features \
            .filter(col('interval_minutes').isin(self.model_interval_minutes_lst))\
            .withColumn('supervised_score', udf_run_models(
               col('interval_minutes'), *[col(feature) for feature in self.features]
            )) \
            .withColumn('interval_minutes', col('interval_minutes').cast(IntegerType())) \
            .withColumn('supervised_score', col('supervised_score').cast(FloatType())) \
            .withColumn('heuristic_score', col('heuristic_score').cast(FloatType())) \
            .select(
                'tenant_id',
                'channel_id',
                'interval_minutes',
                'supervised_score',
                'heuristic_score'
            )

        df_final = df_scores.join(df_channels, 'channel_id', how='inner')

        df_final \
            .groupBy('tenant_id', 'location_id', 'interval_minutes') \
            .agg(
                max('supervised_score').alias('supervised_score'),
                max('heuristic_score').alias('heuristic_score')
            ) \
            .select(
                'tenant_id',
                'location_id',
                'interval_minutes',
                'supervised_score',
                'heuristic_score'
            )

        df_final = self.post_filter_results(df_final, df_locs, df_experian, df_lpt)

        df_final.createOrReplaceTempView('output')
