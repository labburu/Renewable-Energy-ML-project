import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import (
    avg,
    col,
    datediff,
    max,
    min,
    pandas_udf,
    PandasUDFType,
    sum as sqlsum,
)

from nydus.task import NydusTask
from nydus.utilities import filter_before, filter_after

log = logging.getLogger(__name__)

COMPLETENESS_LOWER = 0.9
MINUTES_PER_DAY = 1440
LOCATION_DAYS_THRESHOLD = 100


class HotWHModel(NydusTask):
    def register_pickles(self, pickles):
        pass

    def __init__(self, **kwargs):
        evaluation_date = kwargs.get('evaluation_date')
        if evaluation_date and evaluation_date != 'None':
            date_format = kwargs.get('date_format', '%Y-%m-%d')
            self.evaluation_date = datetime.strptime(evaluation_date, date_format)
        else:
            self.evaluation_date = datetime.now()

        self.evaluate_previous_days = 150

        self.tenant_id_whitelist = kwargs.get('tenant_id_whitelist', None)

    # new channel_id filtering based on hourly roll up data
    def filter_dataset(self, df_ami):
        df = filter_before(df_ami, self.evaluation_date)
        if self.evaluate_previous_days:
            window_start = self.evaluation_date - timedelta(days=self.evaluate_previous_days)
            df = filter_after(df, window_start)

        df = df \
            .withColumnRenamed('channel_uuid', 'channel_id') \
            .withColumnRenamed('date_utc', 'read_timestamp') \
            .withColumnRenamed('consumption_actual', 'consumption') \
            .withColumn('interval_minutes', col('min_seconds') / 60) \
            .select(
                'channel_id',
                'tenant_id',
                'read_timestamp',
                'interval_minutes',
                'hour_utc',
                'month',
                'year',
                'consumption',
                'num_reads_actual',
                'num_reads_total',
            )

        df_good_locations = df.groupBy('channel_id', 'interval_minutes') \
            .agg(
            sqlsum('num_reads_actual').alias('row_count'),
            sqlsum('num_reads_total').alias('interval_count'),
            min('read_timestamp').alias('timestamp_min'),
            max('read_timestamp').alias('timestamp_max'),
        ) \
            .withColumn('location_days', datediff(col('timestamp_max'), col('timestamp_min'))) \
            .withColumn('percent_complete', col('row_count') / col('interval_count')) \
            .where(col('percent_complete') > COMPLETENESS_LOWER) \
            .where(col('location_days') > LOCATION_DAYS_THRESHOLD) \
            .select('channel_id')

        return df \
            .join(df_good_locations, 'channel_id', how='inner')

    measure_wh_score_schema = StructType([
        StructField('channel_id', StringType()),
        StructField('tenant_id', IntegerType()),
        StructField('interval_minutes', IntegerType()),
        StructField('wh_score', ArrayType(DoubleType())),
        StructField('wh_scale_score', ArrayType(DoubleType())),
        StructField('avg_score', FloatType()),
        StructField('wh_yes', IntegerType())
    ])

    @staticmethod
    @pandas_udf(measure_wh_score_schema, functionType=PandasUDFType.GROUPED_MAP)
    def waterheater_score(df_ami):
        df_ami = df_ami.sort_values(by=['read_timestamp', 'hour_utc'], ascending=True)
        channel_id = df_ami.at[0, 'channel_id']
        tenant_id = df_ami.at[0, 'tenant_id']
        interval_minutes = df_ami.at[0, 'interval_minutes']
        df_ami = df_ami['consumption']
        df_ami = pd.to_numeric(df_ami, errors='coerce')
        df_ami = df_ami.fillna(0)
        df_ami = df_ami * 1000

        Y_diff = np.diff([df_ami])
        Y_diff = np.column_stack(Y_diff)
        Y = np.array([df_ami])
        Y = np.column_stack(Y)

        for i in range(0, len(Y_diff)):
            if Y_diff[i] < 0:
                Y_diff[i] = 1

        for i in range(0, len(Y)):
            if Y[i] < 0:
                Y[i] = 1

        # num_of_windows = 10
        # windows length is determined by how sparse the ground truth sequence are, 72 equals to 3 days of AMI data
        windows_length = 72
        # average windows thresh score acceptance boundary
        upper_thresh_score = 400
        lower_thresh_score = 300
        scale_score_thresh = 10
        scale_score_stdev_thresh = 10
        window_score_check = 200
        window_scale_score_check = 1000
        length_ratio = 1

        n_components = 3

        # parameter init
        # generic init model for hourly water heater
        trans_mat = np.array([[0.8, 0.17, 0.03],
                              [0.92, 0.07, 0.01],
                              [0.95, 0.04, 0.01]])
        # The means of each component
        means = np.array([[0],
                          [600],
                          [1800]])
        # The covariance of each component
        covs = np.array([[1],
                         [100],
                         [200]])

        training_length = np.floor(len(Y) / windows_length) * length_ratio

        mean_score_list = []
        scale_score_list = []
        # Y_train_final = np.array([[0]])

        for i in range(0, int(training_length)):
            tmp = i * windows_length
            Y_train = Y_diff[tmp:(tmp + windows_length - 1)]
            # Y_train_agg = Y[tmp:(tmp + windows_length - 1)]
            # Construct a GaussianHMM where we provide initial parameters for the model except for starting prob
            # During iterations we only update covariances of the model, by fixing transition matrix and means
            # we are estimating the similar windows to the training data parameters
            model = GaussianHMM(n_components, n_iter=1000, tol=0.0001, init_params='s', params='c',
                                covariance_type='spherical')
            model.means_ = means
            model.transmat_ = trans_mat
            model.covars_ = covs
            model.fit(Y_train)
            # sometimes a 3-state model will return a nan matrix if the diff readings are too sparse
            # discard nan matrices
            try:
                temp_score = abs(model.score(Y_train))
            except ValueError:
                continue

            # trying to rescale scores based on consumption reads difference by each house to have a
            # relatively fair comparison
            # TODO: need a better generalizable threshodling metric
            adj_score = float(temp_score / np.mean(Y_train))
            rescale_score = (adj_score / np.mean(Y_train)) * 1000

            # check if the low likelihood score is caused by extremely low house consumption or not
            if temp_score < window_score_check and rescale_score > window_scale_score_check:
                continue
            else:
                mean_score_list.append(temp_score)
                scale_score_list.append(rescale_score)

        if mean_score_list:
            # if the likelihood score list is not null, calculate statististics
            avg_score = np.mean(mean_score_list)
            avg_scale_score = np.mean(scale_score_list)
            stdev_scale_score = np.std(scale_score_list)
        else:
            avg_score = 0
            avg_scale_score = 0
            stdev_scale_score = 0

        # Changed method, after fine-tuning by using Duke and AEP's 7,000 electric water heat houses, not only the fixed
        # sample size matters, also the average consumption reads would affect the maximum-likelihood scores obtained
        # from HMM (the larger the average consumption is, the low the score is, and also eligible windows count is
        # not a reliable metric, therefore we developed a new scaled score to standardize window's consumption reads,
        # also standard deviation of the scaled score is very effective on detection because hot water heater is used
        # all the time, the less volatile the combined standard deviations are, the more likely this household
        # will have a hot water heater
        if lower_thresh_score < avg_score < upper_thresh_score and 0 < avg_scale_score < scale_score_thresh \
                and 0 < stdev_scale_score < scale_score_stdev_thresh:
            wh_yes = 1
        else:
            wh_yes = 0

        return pd.DataFrame([[channel_id, tenant_id, interval_minutes, mean_score_list, scale_score_list, avg_score,
                              wh_yes]])

    def wh(self, df_ami):
        df = self.filter_dataset(df_ami)
        location_week_group_columns = ['channel_id', 'tenant_id', 'interval_minutes']

        df_wh_score = df.\
            groupBy(location_week_group_columns).\
            apply(self.waterheater_score)

        return df_wh_score

    def run(self, spark):
        df_ami = spark.table('ami')

        if self.tenant_id_whitelist:
            log.warning('tenant_id_whitelist is set. Limiting to tenants {}'.format(self.tenant_id_whitelist))
            df_ami = df_ami.filter(df_ami.tenant_id.isin(self.tenant_id_whitelist))
        df_ami = df_ami.persist()

        channels = spark.table('channels')
        channels = channels.persist()
        channels = channels \
            .withColumnRenamed('id', 'channel_id') \
            .select('channel_id', 'location_id')

        wh_score = self.wh(df_ami)
        wh_score = wh_score \
            .join(channels, 'channel_id', how='inner')

        output = wh_score \
            .groupBy('tenant_id', 'location_id', 'interval_minutes') \
            .agg(
                max('wh_yes').alias('wh_yes'),
                avg('avg_score').alias('wh_avg_likelihood_score')
            )

        output = output.dropDuplicates()

        output.createOrReplaceTempView('output')
