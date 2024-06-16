import pytest
import uuid
from pyspark.sql import Row
from nydus.core import transform_task


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_transform_ds_clean_bill_fields(spark_session):
    """
    bills_merged (DataFrame): Pyspark dataframe that minimally includes the following fields:
        'bill_start', <date>
        'bill_end', <date>
        'consumption_scaled', <float>
        'tenant_id', <int?>
        'external_account_id', <string>
        'bill_yearmonth', <int>
        'bill_adc', <double>
    """
    test_input_1 = [
        Row(external_account_id=str(uuid.UUID(int=((111<<64) + 1))), bill_end='2018-02-01 00:00:00', bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100, bill_days=31, bill_yearmonth=201801, bill_month=1, bill_adc=100.0),  # noqa
        Row(external_account_id=str(uuid.UUID(int=((111<<64) + 2))), bill_end='2018-02-01 00:00:00', bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=-3100, bill_days=31, bill_yearmonth=201801, bill_month=1, bill_adc=0.0),  # noqa
        Row(external_account_id=str(uuid.UUID(int=((111<<64) + 3))), bill_end='2018-01-01 00:00:00', bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100, bill_days=0, bill_yearmonth=201801, bill_month=1, bill_adc=None),  # noqa
        Row(external_account_id=str(uuid.UUID(int=((111<<64) + 4))), bill_end='2018-01-16 00:00:00', bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=750, bill_days=15, bill_yearmonth=201801, bill_month=1, bill_adc=50.0),  # noqa
        Row(external_account_id=str(uuid.UUID(int=((111<<64) + 4))), bill_end='2018-02-01 00:00:00', bill_start='2018-01-16 00:00:00', fuel_type='ELECTRIC', consumption_scaled=2350, bill_days=16, bill_yearmonth=201801, bill_month=1, bill_adc=146.875),  # noqa
        Row(external_account_id=str(uuid.UUID(int=((222<<64) + 1))), bill_end='2018-02-01 00:00:00', bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100, bill_days=31, bill_yearmonth=201801, bill_month=1, bill_adc=100.0),  # noqa
    ]

    df = spark_session.createDataFrame(test_input_1)
    df.createOrReplaceTempView('bills')

    df.show()

    transform_config = {
        'task': 'tasks.transform_ds_clean_bills.clean_bills.TransformTask',
        'script': 'clean_bills.py',
        'type': 'task'
    }

    output = transform_task(spark_session, {}, transform_config)

    output.show()

    assert output.count() == 3
    assert output.filter(output.external_account_id == '00000000-0000-006f-0000-000000000001').count() == 1
    assert output.filter(output.external_account_id == '00000000-0000-00de-0000-000000000001').count() == 1
    assert output.filter(
        output.external_account_id == '00000000-0000-006f-0000-000000000004').head(1)[0]['bill_adc'] == 100
