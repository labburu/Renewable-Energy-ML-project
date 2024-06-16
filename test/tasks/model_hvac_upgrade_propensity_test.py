import pytest
from pyspark.sql import Row
from tasks.model_hvac_upgrade_propensity.task import HVACUpgradePropensity

accounts = [
    Row(id='00000000-0000-000b-019c-ad21d0110801',
        external_account_id='a',
        tenant_id='92'),
    Row(id='0000000-0000-004f-0275-69ec31d10401',
        external_account_id='b',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0180-f20ef3f10001',
        external_account_id='c',
        tenant_id='92'),
    Row(id='00000000-0000-000b-019c-ad21d0110804',
        external_account_id='d',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf84910405',
        external_account_id='e',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c06',
        external_account_id='f',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0180-f20ef3f10006',
        external_account_id='g',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c0b',
        external_account_id='h',
        tenant_id='92'),
    Row(id='00000000-0000-004f-0275-9bb5d701000c',
        external_account_id='i',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf8491040d',
        external_account_id='j',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c0e',
        external_account_id='k',
        tenant_id='64'),
]

kcpl = [
    Row(account_id='a',
        premise_id='z',
        work_order_date='2017-11-01'),
    Row(account_id='b',
        premise_id='y',
        work_order_date='2017-11-01'),
    Row(account_id='c',
        premise_id='x',
        work_order_date='2017-11-01'),
    Row(account_id='d',
        premise_id='w',
        work_order_date='2017-11-01'),
    Row(account_id='e',
        premise_id='u',
        work_order_date='2017-11-01'),
    Row(account_id='f',
        premise_id='t',
        work_order_date='2017-11-01'),
    # Row(account_id='g',
    #     premise_id='v',
    #     work_order_date='2017-11-01'),
    # Row(account_id='h',
    #     premise_id='q',
    #     work_order_date='2017-11-01'),
    # Row(account_id='i',
    #     premise_id='r',
    #     work_order_date='2017-11-01'),
    # Row(account_id='j',
    #     premise_id='s',
    #     work_order_date='2017-11-01'),
    # Row(account_id='k',
    #     premise_id='p',
    #     work_order_date='2017-11-01'),
]

weather_sensitivity = [
    Row(account_id='00000000-0000-000b-019c-ad21d0110801',
        rsquare=0.7023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.5),
    Row(account_id='00000000-0000-004f-0275-69ec31d10401',
        rsquare=0.9823426294326782,
        cdd_coef=1.046294391155243,
        hdd_coef=.7),
    Row(account_id='00000000-0000-000b-0180-f20ef3f10001',
        rsquare=0.1023426294326782,
        cdd_coef=1.4046294391155243,
        hdd_coef=.8),
    Row(account_id='00000000-0000-000b-019c-ad21d0110804',
        rsquare=0.023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.9),
    Row(account_id='00000000-0000-004f-0275-69ec31d10405',
        rsquare=0.9823426294326782,
        cdd_coef=0.046294391155243,
        hdd_coef=0.2),
    Row(account_id='00000000-0000-000b-0180-f20ef3f10006',
        rsquare=0.1023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.1),
    Row(account_id='00000000-0000-000b-019c-ad21d0110801',
        rsquare=0.7023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.5),
    Row(account_id='00000000-0000-004f-0275-69ec31d10401a',
        rsquare=0.9823426294326782,
        cdd_coef=1.046294391155243,
        hdd_coef=.7),
    Row(account_id='00000000-0000-000b-0180-f20ef3f10001b',
        rsquare=0.1023426294326782,
        cdd_coef=1.4046294391155243,
        hdd_coef=.8),
    Row(account_id='00000000-0000-000b-019c-ad21d0110804c',
        rsquare=0.023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.9),
    Row(account_id='00000000-0000-000b-0194-63bf84910405d',
        rsquare=0.9823426294326782,
        cdd_coef=0.046294391155243,
        hdd_coef=0.2),
    Row(account_id='00000000-0000-002b-02bb-06bae3d10c06e',
        rsquare=0.1023426294326782,
        cdd_coef=0.4046294391155243,
        hdd_coef=.1),
]


experian_normalized = [
    Row(location_id='00000000-0000-004f-0275-9bb5d7010002',
        tenant_id='92',
        length_residence=9,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=80123),
    Row(location_id='00000000-0000-000b-0194-63bf84910403',
        tenant_id='92',
        length_residence=0,
        age_1=19,
        education_model_1=4,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=80224),
    Row(location_id='00000000-0000-002b-02bb-06bae3d10c03',
        tenant_id='92',
        length_residence=0,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='heyhihodiho',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=55433,
        zipcode_first_3=80325),
    Row(location_id='00000000-0000-004f-0275-9bb5d7010004',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=80426),
    Row(location_id='00000000-0000-000b-0194-63bf84910405',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=90127),
    Row(location_id='00000000-0000-002b-02bb-06bae3d10c06',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=90223),
    Row(location_id='00000000-0000-000b-0194-63bf8491040a',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=True,
        is_independent=False,
        is_nonregistered=True,
        political_spectrum=4,
        unregistered_and_not_engaged=True,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=90323),
    Row(location_id='00000000-0000-002b-02bb-06bae3d10c0b',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=True,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=True,
        green_traditionalist=False,
        a_second_dummy_property=27,
        zipcode_first_3=90423),
    Row(location_id='00000000-0000-004f-0275-9bb5d701000c',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=True,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=True,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=True,
        a_second_dummy_property=27,
        zipcode_first_3=90523),
    Row(location_id='00000000-0000-000b-0194-63bf8491040d',
        tenant_id='92',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=False,
        is_independent=False,
        is_nonregistered=True,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=True,
        green_traditionalist=True,
        a_second_dummy_property=27,
        zipcode_first_3=None),
    Row(location_id='00000000-0000-002b-02bb-06bae3d10c0e',
        tenant_id='64',
        length_residence=2,
        age_1=1,
        education_model_1=2,
        green_aware=3,
        income_lower_bound=1000,
        income_upper_bound=15000,
        is_democrat=True,
        some_other_property='boogabooga',
        is_republican=True,
        is_independent=False,
        is_nonregistered=False,
        political_spectrum=4,
        unregistered_and_not_engaged=False,
        unregistered_and_engaged=False,
        on_the_fence_lib=False,
        green_traditionalist=True,
        a_second_dummy_property=27,
        zipcode_first_3=90223),
]

locations = [
    Row(id='00000000-0000-004f-0275-9bb5d7010002',
        account_id='00000000-0000-000b-019c-ad21d0110801',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf84910403',
        account_id='00000000-0000-004f-0275-69ec31d10401',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c03',
        account_id='00000000-0000-000b-0180-f20ef3f10001',
        tenant_id='92'),
    Row(id='00000000-0000-004f-0275-9bb5d7010004',
        account_id='00000000-0000-004f-0275-9bb5d7010004',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf84910405',
        account_id='00000000-0000-000b-0194-63bf84910405',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c06',
        account_id='00000000-0000-002b-02bb-06bae3d10c06',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf8491040a',
        account_id='00000000-0000-004f-0275-69ec31d10401a',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c0b',
        account_id='00000000-0000-000b-0180-f20ef3f10001b',
        tenant_id='92'),
    Row(id='00000000-0000-004f-0275-9bb5d701000c',
        account_id='00000000-0000-004f-0275-9bb5d7010004c',
        tenant_id='92'),
    Row(id='00000000-0000-000b-0194-63bf8491040d',
        account_id='00000000-0000-000b-0194-63bf84910405d',
        tenant_id='92'),
    Row(id='00000000-0000-002b-02bb-06bae3d10c0e',
        account_id='00000000-0000-002b-02bb-06bae3d10c06e',
        tenant_id='64'),

]

oe_participants = [
    Row(location_uuid='00000000-0000-004f-0275-9bb5d7010002',
        is_test_location='false',
        tenant='IM_HIDDEN'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='XCEL_EE_HIDDEN'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='DUKE_HIDDEN'),
    Row(location_uuid='00000000-0000-004f-0275-9bb5d7010002',
        is_test_location='false',
        tenant='XCEL_EE'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='IM_REGISTRATION'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='SUNPOWER'),
    Row(location_uuid='00000000-0000-004f-0275-9bb5d7010002',
        is_test_location='false',
        tenant='KCPL_HIDDEN'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='KCPL'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='SUNPOWER_HIDDEN'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='TENDRIL_HIDDEN'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='DUKE'),
    Row(location_uuid='00000000-0000-004f-0275-9bb5d7010002',
        is_test_location='false',
        tenant='SUNPOWER_SANDBOX'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='XCEL_TOU'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='TENDRIL'),
    Row(location_uuid='00000000-0000-002b-02bb-06bae3d10c03',
        is_test_location='false',
        tenant='IM'),
    Row(location_uuid='00000000-0000-000b-0194-63bf84910403',
        is_test_location='false',
        tenant='IM'),
    Row(location_uuid='00000000-0000-004f-0275-9bb5d7010002',
        is_test_location='false',
        tenant='IM'),
]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_hvac_upgrade_propensity(spark_session):

    # ARRANGE
    experian = spark_session.createDataFrame(experian_normalized)
    loc = spark_session.createDataFrame(locations)
    ws = spark_session.createDataFrame(weather_sensitivity)
    acts = spark_session.createDataFrame(accounts)
    kc = spark_session.createDataFrame(kcpl)

    test_class_instance = HVACUpgradePropensity(test=True)

    # Data Prep Assertions
    kcpl_yes, kcpl_no, model_data, applied_data = test_class_instance.model_data_prep(experian, loc, ws, acts, kc)

    assert kcpl_yes.count() > 0
    assert kcpl_no.count() > 0

    # assert kcpl smart thermostat participants and non-participants are mutually exclusive
    a = kcpl_yes.join(kcpl_no, 'account_id', how='inner')
    assert a.count() == 0

    row_yes = kcpl_yes.filter(kcpl_yes['id'] == '00000000-0000-004f-0275-9bb5d7010002').head(1)[0]

    # make sure participants and non-participants are given appropriate flag:  1 and 0 respecitvely
    assert row_yes['hvac_upgrade_flag'] == 1

    # model only fit with kcpl data - one tenant for now for consistency (and accuracy?)
    assert row_yes['tenant_id'] == '92'

    row_no = kcpl_no.filter(kcpl_no['account_id'] == '00000000-0000-002b-02bb-06bae3d10c06e').head(1)[0]
    assert row_no['hvac_upgrade_flag'] == 0

    kcpl_yes_columns = kcpl_yes.columns
    assert 'age_1' in kcpl_yes_columns
    assert 'account_id' in kcpl_yes_columns
    assert 'id' in kcpl_yes_columns
    assert 'tenant_id' in kcpl_yes_columns
    assert 'customer_id' in kcpl_yes_columns
    assert 'premise_id' in kcpl_yes_columns
    assert 'work_order_date' in kcpl_yes_columns
    assert 'external_account_id' in kcpl_yes_columns
    assert 'hvac_upgrade_flag' in kcpl_yes_columns

    # Model Fit and Apply Assertions
    test_class_instance = HVACUpgradePropensity(test=True)

    model, test_data, train_data = test_class_instance.fit_model(model_data)
    new_data_predictions = test_class_instance.apply_model(model, applied_data)
    assert new_data_predictions.count() > 0

    # model only fit with kcpl data including the non program participants
    # one tenant for now for consistency (and accuracy?)
    assert test_data.select('tenant_id').distinct().count() == 1

    # apply model to all tenants
    assert new_data_predictions.select('tenant_id').distinct().count() > 1

    new_data_predictions_columns = new_data_predictions.columns
    assert 'account_id' in new_data_predictions_columns
    assert 'location_id' in new_data_predictions_columns
    assert 'tenant_id' in new_data_predictions_columns
    assert 'prediction_binary' in new_data_predictions_columns
    assert 'propensity_hvac_score' in new_data_predictions_columns
    assert 'label' in new_data_predictions_columns

    test_data_columns = test_data.columns
    assert 'account_id' in test_data_columns
    assert 'location_id' in test_data_columns
    assert 'tenant_id' in test_data_columns
    assert 'prediction_binary' in test_data_columns
    assert 'propensity_hvac_score' in test_data_columns
    assert 'label' in test_data_columns
    assert 'rawPrediction' in test_data_columns

    row = new_data_predictions.filter(
        new_data_predictions['location_id'] == '00000000-0000-004f-0275-9bb5d7010004').head(1)[0]
    assert row.account_id == '00000000-0000-004f-0275-9bb5d7010004'
    assert row.label == 0
    assert row.location_id == '00000000-0000-004f-0275-9bb5d7010004'
    assert row.tenant_id == '92'
    assert row.prediction_binary == '1'
    assert row.propensity_hvac_score > 0.0
    assert new_data_predictions.select('propensity_hvac').dtypes[0][1] == 'string'

    # ensure sum of propensity_hvac levels add up to total row count for the applied data
    assert sum(new_data_predictions
               .groupBy('propensity_hvac')
               .count().toPandas()['count']) == new_data_predictions.count()

    # # ensure extract correct score
    # row = applied_data.filter(applied_data['prediction_binary'] == '1').head(1)[0]
    # assert row.propensity_hvac_upgrade_score > 0.5

    # row = applied_data.filter(applied_data['prediction_binary'] == '0').head(1)[0]
    # assert row.propensity_hvac_upgrade_score < 0.5
