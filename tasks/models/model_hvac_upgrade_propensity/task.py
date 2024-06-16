"""Demand Response propensity model."""

import time
import logging
from datadog import api

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import (
    IndexToString,
    StringIndexer,
    VectorIndexer,
    VectorAssembler,
)
from pyspark.sql.functions import lit, udf, col, when
from pyspark.sql.types import FloatType
from pyspark.ml.evaluation import BinaryClassificationEvaluator

log = logging.getLogger(__name__)


__version__ = '2018.10.26a'


class HVACUpgradePropensity:
    def __init__(self, test=False):
        """Pass arbitrary variables from Nydus into task."""

        # Set Hyperparameter inputs
        if test:
            self.num_trees = 1
            self.max_depth = 1
            self.input_cols = [
                'age_1',
                # 'zipcode_first_3'
                ]
            self.model_train_tenant_id = '92'
            self.non_kcpl = 3
        else:
            self.num_trees = 100
            self.max_depth = 7
            self.input_cols = [
                'rsquare',
                'cdd_coef',
                'hdd_coef',
                'intercept',
                'age_1',
                'education_model_1',
                'green_aware',
                'income_lower_bound',
                'income_upper_bound',
                'is_democrat',
                'is_republican',
                'is_independent',
                'is_nonregistered',
                'political_spectrum',
                'unregistered_and_not_engaged',
                'unregistered_and_engaged',
                'on_the_fence_lib',
                'green_traditionalist',
                'length_residence',
                'is_singlefamily',
                'has_heat_pump',
                'has_gas_furnace',
                'marital_model_1',
                'has_central_air_conditioner',
                'home_sq_footage',
                'year_built',
                'total_adults',
                'total_children',
                'total_occupancy',
                # 'zipcode_first_3'
            ]
        self.max_bins = 10
        self.non_kcpl = 7000
        self.seed = 1
        self.feat_subset_strategy = 'auto'
        self.train_pct = .7

        # only train model for specific tenant (92 is KCPL)
        self.model_train_tenant_id = '92'

    def run(self, spark):

        # Input Data Products
        experian_normalized = spark.table('experian_normalized')
        locations = spark.table('locations')
        weather_sensitivity = spark \
            .table('weather_sensitivity')

        accounts = spark.table('accounts')
        kcpl = spark.table('kcpl')

        # Data Prep
        kcpl_yes, kcpl_no, model_data, applied_data = self.model_data_prep(
                                        experian_normalized, locations, weather_sensitivity, accounts, kcpl)

        # Model
        model, test_data, train_data = self.fit_model(model_data)

        # Apply model to new data
        new_data_predictions = self.apply_model(model, applied_data)

        # Only want kcpl end users for now
        new_data_predictions = new_data_predictions.filter(new_data_predictions.tenant_id == '92')
        # new_data_predictions is the new data set not used for traininng that the model is applied to
        # test_data is the test dataset used to validate model performance
        new_data_predictions.createOrReplaceTempView('output')
        test_data.createOrReplaceTempView('test_output')

    def report(self, spark, default_tags):
        """Report model metrics."""
        df = spark.table('test_output')
        evaluator = BinaryClassificationEvaluator()
        evaluation = evaluator.evaluate(df)

        # Extract true positive, false positive, true negative, false negative counts
        # used for sensitivity and specificity metrics
        TP = df.filter((df.label == 1) & (df.prediction_binary == '1')).count()
        FP = df.filter((df.label == 0) & (df.prediction_binary == '1')).count()
        TN = df.filter((df.label == 0) & (df.prediction_binary == '0')).count()
        FN = df.filter((df.label == 1) & (df.prediction_binary == '0')).count()

        log.info('true positives: %s', TP)
        log.info('false positives: %s', FP)
        log.info('true negatives: %s', TN)
        log.info('false negatives: %s', FN)

        try:
            # True positive rate: Rate of correctly predicting locations
            # that truly have elected into a DR program
            sensitivity = TP/(TP+FN)
        except ZeroDivisionError:
            sensitivity = 0.0

        try:
            # True negative rate: Rate of correctly predicting locations
            # that truly have NOT elected into a DR program
            specificity = TN/(TN+FP)
        except ZeroDivisionError:
            specificity = 0.0

        log.info('sensitivity: %s', sensitivity)
        log.info('specificity: %s', specificity)

        # Mean propensity and mean prediction binary
        # Submit point with a timestamp with metrics
        tags = default_tags + ['version:{}'.format(__version__)]

        now = time.time()

        # Get % high/med/low count
        khigh = df.filter(df.propensity_hvac_score == 'high').count()
        kmed = df.filter(df.propensity_hvac_score == 'medium').count()
        klow = df.filter(df.propensity_hvac_score == 'low').count()

        tot = df.count()

        phigh = khigh / tot
        pmed = kmed / tot
        plow = klow / tot

        api.Metric.send([
            {'metric': 'model_hvac_upgrade.area_under_roc', 'points': (now, evaluation), 'tags': tags},
            {'metric': 'model_hvac_upgrade.sensitivity', 'points': (now, sensitivity), 'tags': tags},
            {'metric': 'model_hvac_upgrade.specificity', 'points': (now, specificity), 'tags': tags},
            {'metric': 'model_hvac_upgrade.num_trees', 'points': (now, self.num_trees), 'tags': tags},
            {'metric': 'model_hvac_upgrade.max_depth', 'points': (now, self.max_depth), 'tags': tags},
            {'metric': 'model_hvac_upgrade.non_kcpl',
                'points': (now, self.non_kcpl), 'tags': tags},
            {'metric': 'model_hvac_upgrade.percent_high_prop', 'points': (now, phigh), 'tags': tags},
            {'metric': 'model_hvac_upgrade.percent_med_prop', 'points': (now, pmed), 'tags': tags},
            {'metric': 'model_hvac_upgrade.percent_low_prop', 'points': (now, plow), 'tags': tags},
        ])

        return

    def model_data_prep(self, experian_normalized, locations, weather_sensitivity, accounts, kcpl):
        # Select
        accounts = accounts.select('id', 'external_account_id')
        accounts = accounts.dropDuplicates(['external_account_id'])

        accounts = accounts.withColumnRenamed('id', 'account_id')
        locations = locations.select('account_id', 'id', 'tenant_id')
        locations = locations.withColumn('location_id', locations['id'])
        accounts = accounts.join(locations, 'account_id', how='inner')
        kcpl = kcpl.withColumnRenamed('account_id', 'customer_id')
        kcpl = kcpl.join(accounts, kcpl.customer_id == accounts.external_account_id, how='inner')
        kcpl = kcpl.drop('id', 'account_id', 'tenant_id').dropDuplicates(['customer_id', 'premise_id'])
        # kcpl2 = kcpl.filter(kcpl.work_order_date > work_order_date).dropDuplicates(['customer_id', 'premise_id'])

        # Set up double types
        weather_sensitivity = weather_sensitivity.na.fill(0)
        weather_sensitivity = weather_sensitivity \
            .withColumn('rsquare', weather_sensitivity.rsquare.cast('double')) \
            .withColumn('cdd_coef', weather_sensitivity.cdd_coef.cast('double'))\
            .withColumn('hdd_coef', weather_sensitivity.hdd_coef.cast('double'))

        # Select out pre-specified features inputs from above and
        # only necessary id fields before join so there are not multiple of the same
        # field name
        experian_normalized_columns_to_select = list(set(self.input_cols) & set(experian_normalized.columns))
        experian_normalized = experian_normalized.select('location_id', *experian_normalized_columns_to_select)

        weather_sensitivity_columns_to_select = list(set(self.input_cols) & set(weather_sensitivity.columns))
        weather_sensitivity = weather_sensitivity.select('account_id', *weather_sensitivity_columns_to_select)

        locations = locations.select('id', 'account_id', 'tenant_id')

        # Joins
        locations = locations.join(weather_sensitivity, 'account_id', 'outer')

        experian_normalized = experian_normalized \
            .join(locations, experian_normalized.location_id == locations.id, how='inner')

        kcpl_yes = experian_normalized \
            .join(kcpl, 'location_id', how='inner')

        # Outer join with weather sensitivity because only ~ half of location_ids have
        # weather sensitivity data as of 2018-09-19
        kcpl_no = experian_normalized \
            .join(kcpl, experian_normalized.location_id == kcpl.location_id, how='leftouter')\
            .drop(experian_normalized.location_id)

        kcpl_no = kcpl_no.where('location_id is Null')\
            .drop(kcpl_no.location_id)

        # Set binary output field for those who have OE/SmartT/DR and those who do not
        kcpl_yes = kcpl_yes.withColumn('hvac_upgrade_flag', lit(1))
        kcpl_no = kcpl_no.withColumn('hvac_upgrade_flag', lit(0))

        # Limit number of Non KCPL particpant locations used for training to pre-specified
        # non_kcpl input
        kcpl_only = kcpl_no.filter(kcpl_no.tenant_id == self.model_train_tenant_id)
        kcpl_only_subset = kcpl_only.sample(False, 0.5, seed=0)\
            .limit(self.non_kcpl)
        kcpl_yes = kcpl_yes.select(kcpl_no.columns)
        applied_data = kcpl_yes.union(kcpl_no)
        model_data = kcpl_only_subset.union(kcpl_yes)

        return kcpl_yes, kcpl_no, model_data, applied_data

    def fit_model(self, model_data):

        # ensure train only with specified tenant_id data
        model_data = model_data.filter(model_data.tenant_id == self.model_train_tenant_id)
        model_data = model_data.select(
            'account_id', 'tenant_id', 'id', 'hvac_upgrade_flag', *self.input_cols
            )

        assembler = VectorAssembler(
            inputCols=self.input_cols,
            outputCol='features'
        )

        model_data = model_data.na.fill(0)
        model_data = assembler.transform(model_data)
        model_data = model_data.withColumnRenamed('hvac_upgrade_flag', 'label')

        label_indexer = StringIndexer(inputCol='label', outputCol='indexed_label').fit(model_data)

        # Decide which features should be categorical based on the number of distinct values,
        # where features with at most maxCategories are declared categorical.
        feature_indexer = VectorIndexer(inputCol='features',
                                        outputCol='indexedFeatures',
                                        handleInvalid='skip',
                                        maxCategories=self.max_bins).fit(model_data)

        # Extract train and test
        train, test = model_data.randomSplit([self.train_pct, 1-self.train_pct], seed=self.seed)

        # Train a RandomForest model.
        rf = RandomForestClassifier(
            labelCol='indexed_label',
            featureSubsetStrategy=self.feat_subset_strategy,
            featuresCol='indexedFeatures',
            numTrees=self.num_trees,
            maxDepth=self.max_depth,
            seed=self.seed
        )

        # Convert indexed labels back to original labels.
        labelConverter = IndexToString(
            inputCol='prediction',
            outputCol='prediction_binary',
            labels=label_indexer.labels
        )

        # Chain indexers and forest in a Pipeline
        pipeline = Pipeline(stages=[label_indexer, feature_indexer, rf, labelConverter])

        # Train model.  This also runs the indexers.
        model = pipeline.fit(train)

        udf_extract_propensity = udf(lambda v: float(v[1]), FloatType())

        test_data = model.transform(test)
        test_data = test_data.withColumn(
            "propensity_hvac_score",
            udf_extract_propensity('probability')
            )
        test_data = test_data.withColumnRenamed('id', 'location_id')
        test_data = test_data.select(
            'account_id', 'location_id', 'tenant_id', 'prediction_binary', 'propensity_hvac_score',
            'label', 'rawPrediction'
            )

        train_data = model.transform(train)
        train_data = train_data.withColumn(
            "propensity_hvac_score",
            udf_extract_propensity('probability')
            )
        train_data = train_data.withColumnRenamed('id', 'location_id')
        train_data = train_data.select(
            'account_id', 'location_id', 'tenant_id', 'prediction_binary', 'propensity_hvac_score',
            'label', 'rawPrediction'
            )
        return model, test_data, train_data

    def apply_model(self, model, applied_data):

        assembler = VectorAssembler(
            inputCols=self.input_cols,
            outputCol='features'
        )

        applied_data = applied_data.select(
            'account_id', 'tenant_id', 'id', 'hvac_upgrade_flag',
            *self.input_cols
            )

        applied_data = applied_data.na.fill(0)
        applied_data = assembler.transform(applied_data)
        applied_data = applied_data.withColumnRenamed('hvac_upgrade_flag', 'label')

        # Make predictions.
        new_data_predictions = model.transform(applied_data)

        udf_extract_propensity = udf(lambda v: float(v[1]), FloatType())
        new_data_predictions = new_data_predictions.withColumn(
            "propensity_hvac_score",
            udf_extract_propensity('probability')
            )

        # Extract data outputs
        # new_data_predictions is the new data set not used for traininng that the model is applied to
        # test_data is the test dataset used to validate model performance
        new_data_predictions = new_data_predictions.withColumnRenamed('id', 'location_id')

        new_data_predictions = new_data_predictions.select(
            'account_id', 'location_id', 'tenant_id', 'prediction_binary', 'propensity_hvac_score',
            'label',
            )

        new_data_predictions = new_data_predictions \
            .withColumn('propensity_hvac',
                        when((col('propensity_hvac_score') >= .6), lit('high'))
                        .when((col('propensity_hvac_score') >= .3) &
                              (col('propensity_hvac_score') < .6), lit('medium'))
                        .otherwise(lit('low')))

        return new_data_predictions
