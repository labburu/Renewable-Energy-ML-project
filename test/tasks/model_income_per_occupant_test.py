import pytest
from pyspark.sql import Row

from nydus.core import transform_task

SAMPLE_MATCHED_EXPERIAN = [
    Row(tenant_id=1,
        location_id='0000000-0000-000b-0271-c078e4408806',
        lu_ehi_v5='A',
        number_of_adults=1,
        number_of_children=0,
        dummy_prod=5.462),
    Row(tenant_id=1,
        location_id='0000000-0000-000b-0271-c078e4408805',
        lu_ehi_v5='B',
        number_of_adults=3,
        number_of_children=0,
        dummy_prod=1.23),
    Row(tenant_id=2,
        location_id='0000000-0000-000b-0271-c078e4408804',
        lu_ehi_v5='D',
        number_of_adults=2,
        number_of_children=5,
        dummy_prod=3.221),
    Row(tenant_id=3,
        location_id='0000000-0000-000b-0271-c078e4408802',
        lu_ehi_v5='E',
        number_of_adults=0,
        number_of_children=7,
        dummy_prod=9.24),
]

@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_eco_disagg(spark_session):
    spark_session \
        .createDataFrame(SAMPLE_MATCHED_EXPERIAN) \
        .createOrReplaceTempView('demogs')

    transform_config = {
        'task': 'tasks.model_income_per_occupant.income_per_occupant.ModelIncomePerOccupant',
        'script': 'model_income_per_occupant.py',
        'type': 'task',
    }

    output = transform_task(spark_session, {}, transform_config)

    row_count = output.count()
    assert row_count == 3

    row_1 = output.filter(output['location_id'] == '0000000-0000-000b-0271-c078e4408806').head(1)[0]

    assert row_1.tenant_id == 1
    assert row_1.income_per_occupant == 7500.0

    row_2 = output.filter(output['location_id'] == '0000000-0000-000b-0271-c078e4408805').head(1)[0]

    assert row_2.tenant_id == 1
    assert row_2.income_per_occupant == 6667.0

    row_3 = output.filter(output['location_id'] == '0000000-0000-000b-0271-c078e4408804').head(1)[0]

    assert row_3.tenant_id == 2
    assert row_3.income_per_occupant == 6071.0
