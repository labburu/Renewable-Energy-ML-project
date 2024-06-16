from unittest.mock import Mock, call

import pytest
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.sql import Row, Window
from pyspark.sql.functions import count, col

from tasks.model_smart_thermostat_v2.task import ModelSmartThermostatV2

SAMPLE_MUNGED_DATA = [
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408802',
        se_account_uuid='acc_2',
        email_address='email2@fake.com',
        categorical_var_1='var_1_val_1',
        categorical_var_2='var_2_val_1',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408805',
        se_account_uuid='acc_5',
        email_address='email5@fake.com',
        categorical_var_1='var_1_val_2',
        categorical_var_2='var_2_val_2',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=None,
        smart_thermostat_purchaser=False),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408807',
        se_account_uuid=None,
        email_address=None,
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=None,
        double_var_2=None,
        int_var_1=None,
        int_var_2=None,
        boolean_var_1=None,
        boolean_var_2=None,
        smart_thermostat_purchaser=False),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408809',
        se_account_uuid='acc_9',
        email_address='email9@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e44088011',
        email_address='email11@fake.com',
        se_account_uuid='acc_11',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=None),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408813',
        se_account_uuid=None,
        email_address=None,
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=None,
        double_var_2=None,
        int_var_1=None,
        int_var_2=None,
        boolean_var_1=None,
        boolean_var_2=None,
        smart_thermostat_purchaser=None)
]

SAMPLE_PREPROCESSED_DATA = [

    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408802',
        se_account_uuid='acc_6',
        email_address='email2@fake.com',
        categorical_var_1='var_1_val_1',
        categorical_var_2='var_2_val_1',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=0,
        categorical_var_2__index=0),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408805',
        se_account_uuid='acc_5',
        email_address='email5@fake.com',
        categorical_var_1='var_1_val_2',
        categorical_var_2='var_2_val_2',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=None,
        smart_thermostat_purchaser=False,
        categorical_var_1__index=1,
        categorical_var_2__index=1),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408807',
        se_account_uuid=None,
        email_address=None,
        categorical_var_1='var_1_val_1',
        categorical_var_2=None,
        double_var_1=None,
        double_var_2=None,
        int_var_1=None,
        int_var_2=None,
        boolean_var_1=None,
        boolean_var_2=None,
        smart_thermostat_purchaser=False,
        categorical_var_1__index=0,
        categorical_var_2__index=2),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408809',
        se_account_uuid='acc_9',
        email_address='email9@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=2,
        categorical_var_2__index=2),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e44088011',
        se_account_uuid='acc_11',
        email_address='email11@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=None,
        categorical_var_1__index=2,
        categorical_var_2__index=2),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408813',
        se_account_uuid=None,
        email_address=None,
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=None,
        double_var_2=None,
        int_var_1=None,
        int_var_2=None,
        boolean_var_1=None,
        boolean_var_2=None,
        smart_thermostat_purchaser=None,
        categorical_var_1__index=2,
        categorical_var_2__index=2)
]


SAMPLE_TRAINING_DATA = [
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408805',
        se_account_uuid='acc_5',
        email_address='email5@fake.com',
        categorical_var_1='var_1_val_2',
        categorical_var_2='var_2_val_2',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=None,
        smart_thermostat_purchaser=False,
        categorical_var_1__index=1,
        categorical_var_2__index=1),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408809',
        se_account_uuid='acc_9',
        email_address='email9@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=2,
        categorical_var_2__index=2)
]

SAMPLE_CV_PREDICTIONS_DATA = [
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408805',
        se_account_uuid='acc_5',
        email_address='email5@fake.com',
        categorical_var_1='var_1_val_2',
        categorical_var_2='var_2_val_2',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=None,
        smart_thermostat_purchaser=False,
        categorical_var_1__index=1,
        categorical_var_2__index=1,
        probability=[0.15, 0.85]),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408809',
        se_account_uuid='acc_9',
        email_address='email9@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=2,
        categorical_var_2__index=2,
        probability=[0.8, 0.2])
]

SAMPLE_UNLABELED_DATA = [
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408802',
        se_account_uuid='acc_2',
        email_address='email2@fake.com',
        categorical_var_1='var_1_val_1',
        categorical_var_2='var_2_val_1',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=0,
        categorical_var_2__index=0),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e44088011',
        se_account_uuid='acc_11',
        email_address='email11@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=None,
        categorical_var_1__index=2,
        categorical_var_2__index=2)
]

SAMPLE_PREDICTIONS_DATA = [
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e4408802',
        se_account_uuid='acc_2',
        email_address='email2@fake.com',
        categorical_var_1='var_1_val_1',
        categorical_var_2='var_2_val_1',
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=True,
        categorical_var_1__index=0,
        categorical_var_2__index=0,
        probability=[0.15, 0.85]),
    Row(tenant_id=1111,
        location_id='00000000-0000-000b-0271-c078e44088011',
        se_account_uuid='acc_11',
        email_address='email11@fake.com',
        categorical_var_1=None,
        categorical_var_2=None,
        double_var_1=2.34,
        double_var_2=5.67,
        int_var_1=1.23,
        int_var_2=5.43,
        boolean_var_1=True,
        boolean_var_2=False,
        smart_thermostat_purchaser=None,
        categorical_var_1__index=2,
        categorical_var_2__index=2,
        probability=[0.1, 0.9])
]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_preprocess(spark_session):
    # Arrange
    test_munged_df = spark_session.createDataFrame(SAMPLE_MUNGED_DATA)

    test_class_instance = ModelSmartThermostatV2()

    # Act
    test_input_df, categorical_features, boolean_features, numeric_features = \
        test_class_instance.preprocess(test_munged_df)

    # Assert
    row_count = test_input_df.count()
    assert row_count == 6

    assert set(categorical_features) == {'categorical_var_1__index', 'categorical_var_2__index'}

    assert set(numeric_features) == {'double_var_1', 'double_var_2', 'int_var_1', 'int_var_2'}

    assert set(boolean_features) == {'boolean_var_1', 'boolean_var_2'}

    assert {tup[1] for tup in test_input_df.select(categorical_features).dtypes} == {'double'}
    assert {tup[1] for tup in test_input_df.select(numeric_features).dtypes} == {'double'}
    assert {tup[1] for tup in test_input_df.select(boolean_features).dtypes} == {'double'}

    assert {tup[1] for tup in test_input_df.select(test_class_instance.label_col).dtypes} == {'int'}

    for schema in test_input_df.schema:
        if schema.name in categorical_features:
            assert schema.metadata['ml_attr']['type'] == 'nominal'


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_build_model_dfs(spark_session):
    # Arrange
    test_input_df = spark_session.createDataFrame(SAMPLE_PREPROCESSED_DATA)
    features = ['categorical_var_1__index', 'categorical_var_2__index', 'double_var_1', 'double_var_2',
                'int_var_1', 'int_var_2', 'boolean_var_1', 'boolean_var_2']
    test_class_instance = ModelSmartThermostatV2()

    # Act

    training_df, unlabeled_df = test_class_instance.build_model_dfs(test_input_df, features)

    # Assert
    assert training_df.count() == training_df.na.drop(subset=features, how='all').count()
    assert unlabeled_df.count() == unlabeled_df.na.drop(subset=features, how='all').count()

    assert training_df.filter(training_df.smart_thermostat_purchaser.isNull()).count() == 0
    assert training_df.dropDuplicates(subset=['location_id']).count() == training_df.count()

    assert (unlabeled_df.withColumn("count", count("*").over(Window.partitionBy("location_id")))
            .where(col("count") == 1).drop("count").filter(~col('smart_thermostat_purchaser').isNull())).count() == 0


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_build_pipeline(spark_session):
    # Arrange
    categorical_features = ['cat_feature_1', 'cat_feature_2']
    boolean_features = ['bool_feature_1', 'bool_feature_2']
    numeric_features = ['num_feature_1', 'num_feature_2']

    test_class_instance = ModelSmartThermostatV2()

    # Act
    pipeline = test_class_instance.build_pipeline(categorical_features, boolean_features, numeric_features)

    # Assert
    stages = pipeline.getStages()
    assert len(stages) == 4
    assert isinstance(pipeline.getStages()[0], Imputer)
    assert isinstance(pipeline.getStages()[1], Imputer)
    assert isinstance(pipeline.getStages()[2], VectorAssembler)
    assert isinstance(pipeline.getStages()[3], GBTClassifier)


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_predict(spark_session):
    # Arrange
    test_predictions_df = spark_session.createDataFrame(SAMPLE_PREDICTIONS_DATA)
    test_input_df = spark_session.createDataFrame(SAMPLE_PREPROCESSED_DATA)
    test_unlabeled_df = spark_session.createDataFrame(SAMPLE_UNLABELED_DATA)

    mock_fitted_pipeline = Mock()
    mock_fitted_pipeline.transform.return_value = test_predictions_df

    test_class_instance = ModelSmartThermostatV2()

    # Act
    predictions_df = test_class_instance.predict(mock_fitted_pipeline, test_unlabeled_df, test_input_df)

    # Assert
    assert (predictions_df.count() == test_input_df.count())
    assert set(predictions_df.columns) == set(test_class_instance.id_cols + [test_class_instance.label_col] +
                                              [test_class_instance.label_col + '_probability'])
    unlabeled_loc = predictions_df.filter(col('location_id') == '00000000-0000-000b-0271-c078e44088011').head(1)[0]
    assert round(unlabeled_loc[test_class_instance.label_col + '_probability'], 5) == 0.9
    labeled_loc = predictions_df.filter(col('location_id') == '00000000-0000-000b-0271-c078e4408809').head(1)[0]
    assert labeled_loc[test_class_instance.label_col + '_probability'] == 1


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_calculate_cv_predictions(spark_session):
    # Arrange
    test_training_df = spark_session.createDataFrame(SAMPLE_TRAINING_DATA)
    test_cv_predictions_df = spark_session.createDataFrame(SAMPLE_CV_PREDICTIONS_DATA)

    n_folds = 2
    mock_pipeline = Mock()
    mock_fitted_pipeline = Mock()
    mock_fitted_pipeline.transform.return_value = test_cv_predictions_df
    mock_pipeline.fit.return_value = mock_fitted_pipeline

    test_class_instance = ModelSmartThermostatV2()

    # Act
    cv_predictions_df = test_class_instance.calculate_cv_predictions(spark_session, mock_pipeline,
                                                                     test_training_df, n_folds)

    # # Assert
    assert mock_pipeline.fit.call_count == n_folds
    assert mock_fitted_pipeline.transform.call_count == n_folds

    for fit_call, transform_call in zip(mock_pipeline.fit.call_args_list,
                                        mock_fitted_pipeline.transform.call_args_list):
        assert len(fit_call[0]) == 1
        assert len(transform_call[0]) == 1
        fold_training_df = fit_call[0][0]
        fold_testing_df = transform_call[0][0]
        assert fold_training_df.join(fold_testing_df, test_class_instance.id_cols,
                                     how='outer').count() == test_training_df.count()
        assert fold_training_df.join(fold_testing_df, test_class_instance.id_cols,
                                     how='inner').count() == 0

    assert cv_predictions_df.count() == test_training_df.count()
    assert set(cv_predictions_df.columns) == set(['cv_probability', 'smart_thermostat_purchaser'] +
                                                 test_class_instance.id_cols)
    assert cv_predictions_df.filter(cv_predictions_df.cv_probability.isNull()).count() == 0
