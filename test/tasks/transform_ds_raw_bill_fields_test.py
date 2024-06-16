import pytest
import uuid
from pyspark.sql import Row
from nydus.core import transform_task


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_transform_ds_raw_bill_fields(spark_session):
    """
    input_bills (DataFrame): Pyspark dataframe that minimally includes the following fields:
        'external_account_id', <string>
        'bill_start', <date>
        'bill_end', <date>
        'bill_consumption', <float>
    """
    test_input_1 = [
        Row(account_id=str(uuid.UUID(int=((111 << 64) + 1))), bill_end='2018-02-01 00:00:00',
            bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100,
            tenant_id=64, bill_classification_id=1),
        Row(account_id=str(uuid.UUID(int=((111 << 64) + 2))), bill_end='2018-02-01 00:00:00',
            bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=-3100,
            tenant_id=64, bill_classification_id=1),
        Row(account_id=str(uuid.UUID(int=((111 << 64) + 3))), bill_end='2018-01-01 00:00:00',
            bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100,
            tenant_id=64, bill_classification_id=1),
        Row(account_id=str(uuid.UUID(int=((111 << 64) + 4))), bill_end='2018-01-16 00:00:00',
            bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=750,
            tenant_id=64, bill_classification_id=3),
        Row(account_id=str(uuid.UUID(int=((111 << 64) + 4))), bill_end='2018-02-01 00:00:00',
            bill_start='2018-01-16 00:00:00', fuel_type='ELECTRIC', consumption_scaled=2350,
            tenant_id=64, bill_classification_id=3),
        Row(account_id=str(uuid.UUID(int=((222 << 64) + 1))), bill_end='2018-02-01 00:00:00',
            bill_start='2018-01-01 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100,
            tenant_id=64, bill_classification_id=1)
    ]

    test_input_2 = [
        Row(id=str(uuid.UUID(int=((111 << 64) + 1))), external_account_id='1'),
        Row(id=str(uuid.UUID(int=((111 << 64) + 2))), external_account_id='2'),
        Row(id=str(uuid.UUID(int=((111 << 64) + 3))), external_account_id='3'),
        Row(id=str(uuid.UUID(int=((111 << 64) + 4))), external_account_id='4'),
        Row(id=str(uuid.UUID(int=((111 << 64) + 4))), external_account_id='5'),
        Row(id=str(uuid.UUID(int=((222 << 64) + 1))), external_account_id='6')
        ]

    test_input_3 = [
        Row(tenant_id=64, bill_class_id1=1, bill_class_id2=3)
    ]

    spark_session.createDataFrame(test_input_1).createOrReplaceTempView('bills')
    spark_session.createDataFrame(test_input_2).createOrReplaceTempView('accounts')
    spark_session.createDataFrame(test_input_3).createOrReplaceTempView('pie_tenant_configs')

    transform_config = {
        'task': 'tasks.transform_ds_raw_bill_fields.transform_raw_bill_fields.TransformTask',
        'script': 'transform_raw_bill_fields.py',
        'type': 'task'
    }

    output = transform_task(spark_session, {}, transform_config)

    print(output.show())

    rows = output.count()
    assert rows < 10

    acct1 = output.filter(output.external_account_id == 1).head(1)[0]

    assert acct1['bill_days'] == 31
    assert acct1['bill_adc'] == 100
