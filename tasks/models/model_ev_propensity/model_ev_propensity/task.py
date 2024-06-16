import time

from datadog import api
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer
from pyspark.sql.functions import col, count, ceil, when, udf, lit, rand, floor, avg, last, sum, row_number
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.window import Window

NULL_ID = '000000000000000000'


class ModelEVPropensity:

    def __init__(self):
        self.id_cols = ['account_id', 'location_id', 'tenant_id']
        self.label_col = 'has_ev'

    def preprocess(self, df):

        df = df.fillna(NULL_ID, subset=self.id_cols)

        categorical_features = [item[0] for item in df.dtypes if
                                item[1].startswith('string') and not (item[0] in (self.id_cols + [self.label_col]))]
        boolean_features = [item[0] for item in df.dtypes if
                            item[1].startswith('boolean') and not (item[0] in (self.id_cols + [self.label_col]))]

        numeric_features = [item[0] for item in df.dtypes if
                            (item[1].startswith('int') or item[1].startswith('double') or item[1].startswith('float')
                             or item[1].startswith('long') or item[1].startswith('bigint'))
                            and not (item[0] in (self.id_cols + [self.label_col]))]

        df = df.na.drop(subset=categorical_features + boolean_features + numeric_features, how='all')

        # index categorical columns with StringIndexer, don't need to one-hot-encode for tree models
        preprocess_stages = []
        categorical_features_indices = []
        for feature in categorical_features:
            string_indexer = StringIndexer(inputCol=feature, outputCol=feature + '__index', handleInvalid='keep',
                                           stringOrderType="alphabetDesc")
            preprocess_stages += [string_indexer]
            categorical_features_indices += [feature + '__index']

        preprocess_pipeline = Pipeline(stages=preprocess_stages)
        df = preprocess_pipeline.fit(df).transform(df)

        for feature in numeric_features + boolean_features:
            df = df.withColumn(feature, col(feature).cast(DoubleType()))
        df = df.withColumn(self.label_col, df[self.label_col].cast(IntegerType()))

        return df, categorical_features_indices, boolean_features, numeric_features

    def build_model_dfs(self, input_df):

        training_df = input_df.filter(~col(self.label_col).isNull())
        unlabeled_df = input_df.filter(col(self.label_col).isNull())

        return training_df, unlabeled_df

    def build_pipeline(self, categorical_features, boolean_features, numeric_features):
        numeric_imputer = Imputer(inputCols=numeric_features, outputCols=numeric_features, strategy='mean')
        boolean_imputer = Imputer(inputCols=boolean_features, outputCols=boolean_features, strategy='median')
        assembler = VectorAssembler(
            inputCols=categorical_features + numeric_features + boolean_features,
            outputCol='features'
        )
        gbt = GBTClassifier(
            labelCol=self.label_col,
            featuresCol='features')

        pipeline = Pipeline(stages=[numeric_imputer, boolean_imputer, assembler, gbt])

        return pipeline

    @staticmethod
    def fit(pipeline, training_df):
        return pipeline.fit(training_df)

    def predict(self, fitted_pipeline, input_df):
        predictions_df = fitted_pipeline.transform(input_df).select(self.id_cols + ['probability'])
        extract_probability_udf = udf(lambda prob: float(prob[1]), FloatType())
        predictions_df = predictions_df.withColumn(self.label_col + '_probability',
                                                   extract_probability_udf(predictions_df['probability']))
        predictions_df = input_df.join(predictions_df, self.id_cols, how='left')
        predictions_df = predictions_df.select(self.id_cols + [self.label_col, self.label_col + '_probability'])

        return predictions_df

    def calculate_cv_predictions(self, spark, pipeline, training_df, n_folds=5):
        training_df = training_df.withColumn('fold', floor(rand() * n_folds).cast(IntegerType()))
        training_df.persist()
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

        training_df.unpersist()
        output_df = output_df.select(self.id_cols + ['cv_probability', self.label_col])
        output_df = output_df.withColumn('cv_probability', col('cv_probability').cast(DoubleType()))
        output_df.createOrReplaceTempView('cv_predictions')

        return output_df

    # Positive bucket numbers correspond to rank by probability, in descending order (i.e. 1 is highest propensity)
    # Bucket=-1: no probability modeled
    # Bucket=0: user already has EV
    def bucketize_predictions(self, predictions_df, num_buckets=5):
        predictions_df = predictions_df.withColumn('is_ranked',
                                                   col(self.label_col + '_probability').isNotNull() &
                                                   (col(self.label_col).cast(IntegerType()).isNull() |
                                                    (col(self.label_col).cast(IntegerType()) == 0)))
        row_counts_by_tenant = predictions_df.filter(col('is_ranked'))\
            .groupBy('tenant_id').agg(count('*').alias('row_count'))
        predictions_df = predictions_df.withColumn('rank', row_number()
                                                   .over(Window.partitionBy(['is_ranked', 'tenant_id'])
                                                         .orderBy(col(self.label_col + '_probability').desc())))
        predictions_df = predictions_df.join(row_counts_by_tenant, on='tenant_id', how='left')

        predictions_df = predictions_df.withColumn(self.label_col + '_bucket', lit(1))
        predictions_df = predictions_df.withColumn(self.label_col + '_bucket',
                                                   ceil(col('rank')/col('row_count') * num_buckets))

        predictions_df = predictions_df.withColumn(self.label_col + '_bucket',
                                                   when(col(self.label_col + '_probability').isNull(), lit(-1))
                                                   .otherwise(col(self.label_col + '_bucket')))
        predictions_df = predictions_df.withColumn(self.label_col + '_bucket',
                                                   when(col(self.label_col).cast(IntegerType()) == 1, lit(0))
                                                   .otherwise(col(self.label_col + '_bucket')))
        predictions_df = predictions_df.drop('rank').drop('is_ranked').drop('row_count')

        return predictions_df

    def report(self, spark, default_tags):
        cv_predictions_df = spark.table('cv_predictions')
        cv_predictions_df.persist()

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

        auc_evaluator = BinaryClassificationEvaluator(rawPredictionCol='cv_probability', labelCol=self.label_col)
        auc = auc_evaluator.evaluate(cv_predictions_df)

        cv_predictions_df = cv_predictions_df.orderBy('cv_probability', ascending=False)
        tpr_top_1 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.01)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_5 = cv_predictions_df \
            .limit(int(cv_predictions_count * 0.05)) \
            .agg(avg(col(self.label_col)).alias('avg')).head(1)[0]['avg']
        tpr_top_10 = cv_predictions_df  \
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

        api.Metric.send([
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
        ])

    def run(self, spark):

        munged_df = spark.table('munged_data')
        input_df, categorical_features, boolean_features, numeric_features = self.preprocess(munged_df)

        training_df, unlabeled_df = self.build_model_dfs(input_df)
        training_df.persist()
        pipeline = self.build_pipeline(categorical_features, boolean_features, numeric_features)
        fitted_pipeline = self.fit(pipeline, training_df)
        predictions_df = self.predict(fitted_pipeline, input_df)
        self.calculate_cv_predictions(spark, pipeline, training_df)

        for id_col in self.id_cols:
            predictions_df = predictions_df.withColumn(id_col,
                                                       when(col(id_col) != NULL_ID, col(id_col)).otherwise(None))
        predictions_df = self.bucketize_predictions(predictions_df)
        predictions_df.createOrReplaceTempView('output')
