import io
import logging
import os.path as op
import sys
import time

import boto3
import joblib
import numpy as np
from datadog import api
from nydus.task import NydusTask
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, count, last, avg, sum, lit, rank, when, max, pow
from pyspark.sql.window import Window
from sklearn.ensemble import VotingClassifier, RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class TrainModel(NydusTask):
    LABEL_COL = 'has_ev'
    CLASSIFIER_PARAMS = {'svc': {'class_weight': 'balanced',
                                 'probability': True},
                         'lr': {'solver': 'lbfgs',
                                'random_state': 1,
                                'class_weight': 'balanced'},
                         'rf': {'n_estimators': 100,
                                'min_samples_split': 6,
                                'min_samples_leaf': 3,
                                'max_depth': 50},
                         'gbt': {'n_estimators': 100,
                                 'min_samples_split': 6,
                                 'min_samples_leaf': 3,
                                 'max_depth': 20,
                                 'random_state': 0},
                         }
    INTERVAL_MINUTES_ENSEMBLE = {15: {'lr': 1,
                                      'gbt': 2,
                                      'rf': 1},
                                 30: {'svc': 1,
                                      'lr': 2,
                                      'rf': 1},
                                 60: {'lr': 1,
                                      'gbt': 2,
                                      'rf': 1}}
    ID_COLS = ['location_id', 'channel_id', 'tenant_id']
    DATADOG_GROUPING_IDS = ['tenant_id', 'state']

    def __init__(self, **kwargs):
        self.interval_minutes = kwargs.get('interval_minutes', 60)
        self.features = kwargs.get('features', [])

        self.s3_bucket_name = kwargs.get('s3_bucket_name')
        self.s3_prefix = op.join('dags', kwargs.get('dag_id'), kwargs.get('task_id'), kwargs.get('run_date'))
        self.s3_client = boto3.client('s3', verify=boto3.__file__.replace('boto3/__init__.py', 'certifi/cacert.pem'))

    def build_pipeline(self):
        scaler = StandardScaler()
        svc = SVC(**self.CLASSIFIER_PARAMS['svc'])
        lr = LogisticRegression(**self.CLASSIFIER_PARAMS['lr'])
        rf = RandomForestClassifier(**self.CLASSIFIER_PARAMS['rf'])
        gbt = GradientBoostingClassifier(**self.CLASSIFIER_PARAMS['gbt'])
        estimators = [('svc', svc), ('lr', lr), ('rf', rf), ('gbt', gbt)]
        estimators = [model_tuple for model_tuple in estimators
                      if model_tuple[0] in self.INTERVAL_MINUTES_ENSEMBLE[self.interval_minutes].keys()]
        weights = [self.INTERVAL_MINUTES_ENSEMBLE[self.interval_minutes][model_tuple[0]] for model_tuple in estimators]
        pipeline = Pipeline([('scaler', scaler),
                             ('voting_classifier', VotingClassifier(estimators, weights=weights, voting='soft'))])

        return pipeline

    def fit_pipeline(self, pipeline, training_pddf):
        return pipeline.fit(training_pddf.loc[:, self.features], training_pddf[self.LABEL_COL])

    @staticmethod
    def balance_classes_undersampling(X, y, pos_neg_class_ratio=1):
        num_pos = np.sum(y)
        num_neg = np.sum(1-y)
        pos_idx = np.nonzero(np.array(y))[0]
        neg_idx = np.nonzero(1 - np.array(y))[0]

        if num_pos / num_neg < pos_neg_class_ratio:
            neg_idx = np.random.choice(neg_idx, int(num_pos / pos_neg_class_ratio), replace=False)
        elif num_pos / num_neg > pos_neg_class_ratio:
            pos_idx = np.random.choice(pos_idx, int(num_neg * pos_neg_class_ratio), replace=False)

        idx = list(neg_idx) + list(pos_idx)
        X_balanced = X.iloc[idx, :]
        y_balanced = y.iloc[idx]
        return X_balanced, y_balanced

    def generate_cv_predictions(self, spark, pipeline, training_pddf, n_folds=5):

        cv = StratifiedKFold(n_splits=n_folds)
        X = training_pddf.loc[:, self.features]
        y = training_pddf.loc[:, self.LABEL_COL]

        cv_predictions_pddf = training_pddf.copy()
        cv_predictions_pddf['cv_probability'] = np.nan
        cv_col_idx = [idx for idx, col in enumerate(cv_predictions_pddf.columns) if col == 'cv_probability']
        for train_idx, test_idx in cv.split(X, y):
            X_train, y_train = self.balance_classes_undersampling(X.iloc[train_idx, :], y.iloc[train_idx])
            pipeline.fit(X_train, y_train)
            cv_predictions_pddf.iloc[test_idx, cv_col_idx] = pipeline.predict_proba(X.iloc[test_idx, :])[:, 1]

        cv_predictions_pddf = cv_predictions_pddf.loc[:, [self.LABEL_COL, "cv_probability"] + self.ID_COLS]
        cv_predictions_df = spark.createDataFrame(cv_predictions_pddf)
        return cv_predictions_df

    def save_model(self, fitted_pipeline):
        buffer = io.BytesIO()
        joblib.dump(fitted_pipeline, buffer)
        key = self.s3_prefix + '/' + (self.s3_prefix
                                      .replace('dags/', '')
                                      .replace('/', '__')
                                      .replace('-', '')) + '.joblib'

        self.s3_client.put_object(Body=buffer.getvalue(), Bucket=self.s3_bucket_name, Key=key)

        log.info('Pickle file was written to s3: {}/{}'.format(self.s3_bucket_name, key))

    def report(self, spark, default_tags):

        temp_all_group = 'temp_id'

        cv_predictions_df = spark.table('output')
        cv_predictions_df = cv_predictions_df.withColumn(temp_all_group, lit(1))
        cv_predictions_df.persist()

        now = time.time()
        tags = default_tags + ['interval:{}'.format(self.interval_minutes)]
        tags = [tag for tag in tags if 'task_id' not in tag] + ['task_id:train_model']

        grouping_ids = self.DATADOG_GROUPING_IDS + [temp_all_group]
        metrics = []

        def _create_tags(grp, grping_id, non_grping_ids):

            group_tag = ['{}:{}'.format(grping_id, grp)] if grping_id != temp_all_group else []
            all_tags = ['{}:all'.format(non_grouping_id) for non_grouping_id in grouping_ids
                        if non_grouping_id in non_grping_ids]
            return group_tag + all_tags

        for grouping_id in grouping_ids:
            temp_cv_predictions_df = cv_predictions_df.toDF(*cv_predictions_df.columns)
            counts_by_grouping_df = temp_cv_predictions_df.groupBy(grouping_id).agg(
                count('*').alias('total'),
                sum(col(self.LABEL_COL)).alias('total_pos'),
                sum(1 - col(self.LABEL_COL)).alias('total_neg')
            )
            window = Window.partitionBy(grouping_id)\
                           .orderBy(col('cv_probability').desc())\
                           .rowsBetween(Window.unboundedPreceding, 0)
            temp_cv_predictions_df = temp_cv_predictions_df.join(counts_by_grouping_df, on=grouping_id, how='left')
            temp_cv_predictions_df = temp_cv_predictions_df.withColumn('tpr', sum(col(self.LABEL_COL)).over(window) /
                                                                       col('total_pos'))
            temp_cv_predictions_df = temp_cv_predictions_df.withColumn('fpr',
                                                                       sum(1 - col(self.LABEL_COL)).over(window) /
                                                                       col('total_neg'))
            temp_cv_predictions_df = temp_cv_predictions_df.withColumn('percentile', rank().over(window)/col('total'))

            metrics_by_grouping_df = temp_cv_predictions_df\
                .groupBy(grouping_id)\
                .agg(max(when(col('fpr') < 0.01, col('tpr'))).alias('tpr_at_fpr_1'),
                     max(when(col('fpr') < 0.05, col('tpr'))).alias('tpr_at_fpr_5'),
                     max(when(col('fpr') < 0.1, col('tpr'))).alias('tpr_at_fpr_10'),
                     max(when(col('fpr') < 0.2, col('tpr'))).alias('tpr_at_fpr_20'),
                     max(when(col('fpr') < 0.5, col('tpr'))).alias('tpr_at_fpr_50'),
                     avg(when(col('percentile') < 0.01, col(self.LABEL_COL))).alias('tpr_top_1'),
                     avg(when(col('percentile') < 0.05, col(self.LABEL_COL))).alias('tpr_top_5'),
                     avg(when(col('percentile') < 0.1, col(self.LABEL_COL))).alias('tpr_top_10'),
                     avg(when(col('percentile') < 0.2, col(self.LABEL_COL))).alias('tpr_top_20'),
                     avg(when(col('percentile') < 0.5, col(self.LABEL_COL))).alias('tpr_top_50'),
                     avg(col(self.LABEL_COL)).alias('tpr_top_100'),
                     avg(pow(col(self.LABEL_COL) - col('cv_probability'), 2)).alias('brier_score'),
                     avg(when(col(self.LABEL_COL) == 1,
                              pow(col(self.LABEL_COL) - col('cv_probability'), 2))).alias('pos_brier_score'),
                     avg(when(col(self.LABEL_COL) == 0,
                              pow(col(self.LABEL_COL) - col('cv_probability'), 2))).alias('neg_brier_score'),
                     last(col('total')).alias('cv_count'))

            metrics_by_grouping_df.cache()

            auc_evaluator = BinaryClassificationEvaluator(rawPredictionCol='cv_probability', labelCol=self.LABEL_COL)
            groups = [row[grouping_id] for row in metrics_by_grouping_df.select(grouping_id).collect()]

            auc_dict = {group: auc_evaluator.evaluate(temp_cv_predictions_df.filter(col(grouping_id) == group))
                        for group in groups}

            metrics_names = [col_name for col_name in metrics_by_grouping_df.columns if col_name != grouping_id]
            non_grouping_ids = [i for i in grouping_ids if i not in [grouping_id, temp_all_group]]
            metrics = metrics + [{'metric': 'model.{}'.format(metric_name),
                                  'points': (now, metrics_by_grouping_df
                                             .filter(col(grouping_id) == group)
                                             .select(metric_name).head(1)[0][0]),
                                  'tags': tags + _create_tags(group, grouping_id, non_grouping_ids)}
                                 for metric_name in metrics_names for group in groups]
            metrics = metrics + [{'metric': 'model.area_under_roc',
                                  'points': (now, auc_dict[group]),
                                  'tags': tags + _create_tags(group, grouping_id, non_grouping_ids)}
                                 for group in groups]

            metrics_by_grouping_df.unpersist()

        cv_predictions_df.unpersist()
        api.Metric.send(metrics)

    def run(self, spark):
        training_df = spark.table('training')\
                        .filter(col('interval_minutes') == self.interval_minutes)

        locations_df = spark.table('locations')\
                            .withColumnRenamed('id', 'location_id')\
                            .select('tenant_id', 'location_id', 'state')
        training_pddf = training_df.toPandas()
        pipeline = self.build_pipeline()
        cv_predictions_df = self.generate_cv_predictions(spark, pipeline, training_pddf)

        cv_predictions_df = cv_predictions_df.join(locations_df, on=['tenant_id', 'location_id'], how='left')

        training_pddf, y_training_pddf = self.balance_classes_undersampling(training_pddf.loc[:, self.features],
                                                                            training_pddf.loc[:, self.LABEL_COL])
        training_pddf[self.LABEL_COL] = y_training_pddf
        fitted_pipeline = self.fit_pipeline(pipeline, training_pddf)
        self.save_model(fitted_pipeline)

        cv_predictions_df.createOrReplaceTempView('output')
