import pytest
from pyspark.sql import Row
from nydus.core import transform_task


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_tou_propensity(spark_session):

    experian_normalized = [
        Row(location_id='00000000-0000-000b-01c4-991125610404',
            tenant_id=11,
            green_aware=2,
            marital_model_1=None,
            total_children=0,
            length_residence=3,
            age_1=58,
            home_sq_footage=12),
        Row(location_id='00000000-0000-005c-02f7-61fe53111802',
            tenant_id=92,
            green_aware=1,
            marital_model_1=0,
            total_children=1,
            length_residence=6,
            age_1=41,
            home_sq_footage=None),
        Row(location_id='00000000-0000-005c-02f7-605408311802',
            tenant_id=92,
            green_aware=3,
            marital_model_1=2,
            total_children=1,
            length_residence=6,
            age_1=43,
            home_sq_footage=None),
        Row(location_id='00000000-0000-005c-02f7-5deb88213002',
            tenant_id=92,
            green_aware=3,
            marital_model_1=2,
            total_children=1,
            length_residence=9,
            age_1=41,
            home_sq_footage=None),
        Row(location_id='00000000-0000-005c-02f7-6396fc011802',
            tenant_id=92,
            green_aware=2,
            marital_model_1=2,
            total_children=5,
            length_residence=16,
            age_1=40,
            home_sq_footage=None)
    ]

    ev = [
        Row(location_id='00000000-0000-000b-01c4-991125610404',
            has_ev_bucket=4),
        Row(location_id='00000000-0000-005c-02f7-61fe53111802',
            has_ev_bucket=-1),
        Row(location_id='00000000-0000-005c-02f7-605408311802',
            has_ev_bucket=2),
        Row(location_id='00000000-0000-005c-02f7-5deb88213002',
            has_ev_bucket=1),
        Row(location_id='00000000-0000-005c-02f7-6396fc011802',
            has_ev_bucket=0)
    ]

    spark_session \
        .createDataFrame(experian_normalized) \
        .createOrReplaceTempView('experian_normalized')

    spark_session \
        .createDataFrame(ev) \
        .createOrReplaceTempView('ev')

    transform_config = {
        'task': 'tasks.model_tou_propensity.task.GetTOUProp',
        'script': 'task.py',
        'type': 'task'
    }

    output = transform_task(spark_session, {}, transform_config)

    row = output.filter(
        output['location_id'] == '00000000-0000-005c-02f7-605408311802') \
        .head(1)[0]
    assert row.location_id == '00000000-0000-005c-02f7-605408311802'
    assert row.tenant_id == 92
    assert row.tou_score >= 18
    assert row.propensity_tou == 'high'

    row = output.filter(
        output['location_id'] == '00000000-0000-005c-02f7-61fe53111802') \
        .head(1)[0]
    assert row.location_id == '00000000-0000-005c-02f7-61fe53111802'
    assert row.tenant_id == 92
    assert row.tou_score < 10
    assert row.propensity_tou == 'low'

    assert output.dtypes == [
        ('location_id', 'string'),
        ('tenant_id', 'bigint'),
        ('tou_score', 'int'),
        ('propensity_tou', 'string')
    ], 'DataFrame types do not match expected'
