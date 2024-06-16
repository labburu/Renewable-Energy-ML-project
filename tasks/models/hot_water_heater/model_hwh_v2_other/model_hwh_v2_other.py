import time
import logging

import numpy as np
from datadog import api
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer
from pyspark.sql.functions import col, when, udf, lit, floor, rand, avg, last, sum
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, StructType, StructField
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

NULL_ID = '000000000000000000'


class ModelHWHv2:

    def __init__(self):
        self.id_cols = ['tenant_id', 'location_id']
        self.features = ['location_id', 'tenant_id', 'wh_avg_likelihood_score', 'mean_kurtosis', 'mean_skewness',
                         'mean_peak', 'experian_home_building_square_footage', 'experian_home_bathrooms',
                         'experian_home_estimated_current_value', 'experian_new_parent_indicator_last_three_years',
                         'intercept', 'electric_hdd_coef_ami', 'electric_cdd_coef_ami']
        self.label_col = 'wh_gt'

        # hyperparameters - static for now
        self.num_trees = 100
        self.max_depth = 9
        self.DATADOG_GROUPING_IDS = ['tenant_id']

    def preprocess(self, df):

        df = df.fillna(NULL_ID, subset=self.id_cols)

        categorical_features = [item[0] for item in df.dtypes if
                                item[1].startswith('string') and not (item[0] in (self.id_cols + [self.label_col]))]
        boolean_features = [item[0] for item in df.dtypes if
                            item[1].startswith('boolean') and not (item[0] in (self.id_cols + [self.label_col]))]

        numeric_features = [item[0] for item in df.dtypes if
                            (item[1].startswith('int') or item[1].startswith('double') or item[1].startswith('float'))
                            and not (item[0] in (self.id_cols + [self.label_col]))]

        # index categorical columns with StringIndexer, don't need to one-hot-encode for tree models
        preprocess_stages = []
        for feature in categorical_features:
            string_indexer = StringIndexer(inputCol=feature, outputCol=feature + '__index', handleInvalid='keep',
                                           stringOrderType="alphabetDesc")
            preprocess_stages += [string_indexer]

        preprocess_pipeline = Pipeline(stages=preprocess_stages)
        df = preprocess_pipeline.fit(df).transform(df)

        categorical_features = [feature + '__index' for feature in categorical_features]

        for feature in numeric_features + boolean_features:
            df = df.withColumn(feature, col(feature).cast(DoubleType()))
        df = df.withColumn(self.label_col, df[self.label_col].cast(IntegerType()))

        return df, categorical_features, boolean_features, numeric_features

    def build_model_dfs(self, input_df, features):
        filtered_df = input_df.na.drop(subset=features,
                                       how='all')
        training_df = filtered_df.filter(~col(self.label_col).isNull())
        unlabeled_df = filtered_df.filter(col(self.label_col).isNull())

        return training_df, unlabeled_df

    def build_pipeline(self, categorical_features, boolean_features, numeric_features):
        numeric_imputer = Imputer(inputCols=numeric_features, outputCols=numeric_features, strategy='mean')
        boolean_imputer = Imputer(inputCols=boolean_features, outputCols=boolean_features, strategy='median')
        assembler = VectorAssembler(
            inputCols=categorical_features + numeric_features + boolean_features,
            outputCol='features'
        )
        rf = RandomForestClassifier(
            labelCol=self.label_col,
            featuresCol='features',
            numTrees=self.num_trees,
            maxDepth=self.max_depth)

        pipeline = Pipeline(stages=[numeric_imputer, boolean_imputer, assembler, rf])

        return pipeline

    @staticmethod
    def fit(pipeline, training_df):
        return pipeline.fit(training_df)

    def predict(self, fitted_pipeline, unlabeled_df, input_df):
        predictions_df = fitted_pipeline.transform(unlabeled_df).select(self.id_cols + ['probability'])
        extract_probability_udf = udf(lambda prob: float(prob[1]), FloatType())
        predictions_df = predictions_df.withColumn('probability',
                                                   extract_probability_udf(predictions_df['probability']))
        predictions_df = input_df.join(predictions_df, self.id_cols, how='left')
        predictions_df = predictions_df.withColumn(self.label_col + '_probability', (when(~col('probability')
                                                                                          .isNull(), col('probability'))
                                                                                     .otherwise(col(self.label_col)
                                                                                                .cast(IntegerType()))))
        predictions_df = predictions_df.select(self.id_cols + [self.label_col, self.label_col + '_probability'])

        return predictions_df

    def balance_classes_undersampling(self, spark, X, y, pos_neg_class_ratio=1):
        num_pos = np.sum(y)
        num_neg = np.sum(1 - y)
        pos_idx = np.nonzero(np.array(y))[0]
        neg_idx = np.nonzero(1 - np.array(y))[0]

        if num_pos / num_neg < pos_neg_class_ratio:
            neg_idx = np.random.choice(neg_idx, int(num_pos / pos_neg_class_ratio), replace=False)
        elif num_pos / num_neg > pos_neg_class_ratio:
            pos_idx = np.random.choice(pos_idx, int(num_neg * pos_neg_class_ratio), replace=False)

        idx = list(neg_idx) + list(pos_idx)
        X_balanced = X.iloc[idx, :]
        y_balanced = y.iloc[idx]
        X_balanced[self.label_col] = y_balanced

        X_balanced = spark.createDataFrame(X_balanced)

        return X_balanced

    def calculate_cv_predictions(self, spark, pipeline, training_df, n_folds=5):
        training_df = training_df.withColumn('fold', floor(rand() * n_folds).cast(IntegerType()))
        output_df = spark.createDataFrame(training_df.rdd, training_df.schema)
        output_df = output_df.withColumn('cv_probability', lit(None))
        output_df = output_df.select(self.id_cols + ['cv_probability', self.label_col, 'fold'])
        extract_probability_udf = udf(lambda prob: float(prob[1]), FloatType())

        for fold in range(n_folds):
            test_df = training_df.filter(training_df.fold == fold)
            train_df = training_df.filter(training_df.fold != fold)

            fitted_pipeline = pipeline.fit(train_df)
            predict_df = fitted_pipeline.transform(test_df)
            predict_df = predict_df.withColumn('fold_probability', extract_probability_udf(predict_df['probability']))
            output_df = output_df.join(predict_df.select(self.id_cols + ['fold_probability']),
                                       self.id_cols, how='left')
            output_df = output_df.withColumn('cv_probability', (
                when(col('fold') == fold, col('fold_probability')).otherwise(col('cv_probability'))))
            output_df = output_df.select(self.id_cols + ['cv_probability', self.label_col, 'fold'])

        output_df = output_df.select(self.id_cols + ['cv_probability', self.label_col])
        output_df = output_df.withColumn('cv_probability', col('cv_probability').cast(DoubleType()))
        output_df.createOrReplaceTempView('cv_predictions')
        return output_df

    def report(self, spark, default_tags):
        """Send Metrics reporting to datadog, called as part of Nydus transform."""
        # if cv_predictions table does not exist return to prevent errors
        cv_predictions_exists = spark.sql("SHOW TABLES").filter("tableName == 'cv_predictions'").head()
        if not cv_predictions_exists:
            log.warning("cv_predictions table doesn't exist, return without report")
            return

        cv_predictions_df = spark.table('cv_predictions')
        cv_predictions_df.persist()

        # if there is no data return to prevent errors
        if len(cv_predictions_df.head(1)) == 0:
            log.warning("cv_predictions table contains no data, return without report")
            return

        cv_predictions_count = cv_predictions_df.select('cv_probability').count()
        total_pos = cv_predictions_df.agg(sum(col(self.label_col)).alias('total_pos')).head(1)[0]['total_pos']
        total_neg = cv_predictions_df.agg(sum(1 - col(self.label_col)).alias('total_neg')).head(1)[0]['total_neg']

        cv_predictions_df = cv_predictions_df.withColumn('tpr', sum(col(self.label_col)).over(
            Window.orderBy(col('cv_probability').desc()).rowsBetween(Window.unboundedPreceding, 0)) / total_pos)
        cv_predictions_df = cv_predictions_df.withColumn('fpr', sum(1 - col(self.label_col)).over(
            Window.orderBy(col('cv_probability').desc()).rowsBetween(Window.unboundedPreceding, 0)) / total_neg)

        cv_predictions_df = cv_predictions_df.orderBy('cv_probability', ascending=False)
        tpr_at_fpr_1 = cv_predictions_df.filter(col('fpr') < 0.01).agg(last(col('tpr')).alias('tpr')).head(1)[0]['tpr']
        tpr_at_fpr_5 = cv_predictions_df.filter(col('fpr') < 0.05).agg(last(col('tpr')).alias('tpr')).head(1)[0]['tpr']
        tpr_at_fpr_10 = cv_predictions_df.filter(col('fpr') < 0.1).agg(last(col('tpr')).alias('tpr')).head(1)[0]['tpr']
        tpr_at_fpr_20 = cv_predictions_df.filter(col('fpr') < 0.2).agg(last(col('tpr')).alias('tpr')).head(1)[0]['tpr']
        tpr_at_fpr_50 = cv_predictions_df.filter(col('fpr') < 0.5).agg(last(col('tpr')).alias('tpr')).head(1)[0]['tpr']

        b_score = cv_predictions_df \
            .groupBy(self.DATADOG_GROUPING_IDS) \
            .agg(avg(pow(col(self.label_col) - col('cv_probability'), 2)).alias('brier_score'),
                 avg(when(col(self.label_col) == 1,
                          pow(col(self.label_col) - col('cv_probability'), 2))).alias('pos_brier_score'),
                 avg(when(col(self.label_col) == 0,
                          pow(col(self.label_col) - col('cv_probability'), 2))).alias('neg_brier_score'))

        auc_evaluator = BinaryClassificationEvaluator(rawPredictionCol='cv_probability', labelCol=self.label_col)
        auc = auc_evaluator.evaluate(cv_predictions_df)

        cv_predictions_df = cv_predictions_df.orderBy('cv_probability', ascending=False)
        tpr_top_1 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.01)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_5 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.05)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_10 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.1)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_20 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.2)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_50 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.5)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_100 = cv_predictions_df.agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']

        cv_predictions_df.unpersist()

        tags = default_tags
        now = time.time()

        tenant_11_brier = b_score \
            .filter(col('tenant_id') == 11) \
            .select('brier_score').head(1)

        report_metrics = [
            {'metric': 'model.tpr_at_fpr_1', 'points': (now, tpr_at_fpr_1), 'tags': tags},
            {'metric': 'model.tpr_at_fpr_5', 'points': (now, tpr_at_fpr_5), 'tags': tags},
            {'metric': 'model.tpr_at_fpr_10', 'points': (now, tpr_at_fpr_10), 'tags': tags},
            {'metric': 'model.tpr_at_fpr_20', 'points': (now, tpr_at_fpr_20), 'tags': tags},
            {'metric': 'model.tpr_at_fpr_50', 'points': (now, tpr_at_fpr_50), 'tags': tags},
            {'metric': 'model.area_under_roc', 'points': (now, auc), 'tags': tags},
            {'metric': 'model.tpr_top_1', 'points': (now, tpr_top_1), 'tags': tags},
            {'metric': 'model.tpr_top_5', 'points': (now, tpr_top_5), 'tags': tags},
            {'metric': 'model.tpr_top_10', 'points': (now, tpr_top_10), 'tags': tags},
            {'metric': 'model.tpr_top_20', 'points': (now, tpr_top_20), 'tags': tags},
            {'metric': 'model.tpr_top_50', 'points': (now, tpr_top_50), 'tags': tags},
            {'metric': 'model.tpr_top_100', 'points': (now, tpr_top_100), 'tags': tags},
            {'metric': 'model.cv_count', 'points': (now, cv_predictions_count), 'tags': tags}
        ]

        # Only add model.tenant_11_brier to metrics if there is data for tenant 11
        if len(tenant_11_brier) > 0:
            report_metrics.append({'metric': 'model.tenant_11_brier', 'points': (now, tenant_11_brier[0][0]),
                                   'tags': tags})
        else:
            log.warning("No data for tenant 11, returning report without model.tenant_11_brier metric")

        # Send metrics to datadog
        api.Metric.send(report_metrics)

        return

    def run(self, spark):

        munged_df = spark.table('munged_data')
        input_df, categorical_features, boolean_features, numeric_features = self.preprocess(munged_df)

        training_df, unlabeled_df = self.build_model_dfs(input_df,  features=(categorical_features + boolean_features +
                                                                              numeric_features))

        training_pddf = training_df.toPandas()
        pipeline = self.build_pipeline(categorical_features, boolean_features, numeric_features)

        # If there is no training data output empty table
        if len(training_pddf) > 0:
            training_df = self.balance_classes_undersampling(spark,
                                                             training_pddf.loc[:, self.features],
                                                             training_pddf.loc[:, self.label_col])
            training_df.persist()

            fitted_pipeline = self.fit(pipeline, training_df)
            predictions_df = self.predict(fitted_pipeline, unlabeled_df, input_df)
            self.calculate_cv_predictions(spark, pipeline, training_df)
            training_df.unpersist()

            for id_col in self.id_cols:
                predictions_df = predictions_df.withColumn(id_col,
                                                           when(col(id_col) != NULL_ID, col(id_col)).otherwise(None))
        else:
            log.info("No training data, creating empty DF for output")
            output_schema = StructType([
                StructField("location_id", StringType(), True),
                StructField("tenant_id", IntegerType(), True),
                StructField("wh_gt", IntegerType(), True),
                StructField("wh_gt_probability", DoubleType(), True),
            ])

            predictions_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), output_schema)

        predictions_df.createOrReplaceTempView('output')
