import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from datetime import datetime

from nydus.core import transform_task


SAMPLE_LOCATIONS = [
    Row(location_id='20000000-0000-000b-0271-c078e4408801',
        account_id='10000000-0000-000b-0271-c078e4408801',
        tenant_id=64,
        postal_code='12345',
        some_other_col=1),
    Row(location_id='20000000-0000-000b-0271-c078e4408802',
        account_id='10000000-0000-000b-0271-c078e4408802',
        tenant_id=64,
        postal_code='12345',
        some_other_col=1),
    Row(location_id='20000000-0000-000b-0271-c078e4408803',
        account_id='10000000-0000-000b-0271-c078e4408802',
        tenant_id=64,
        postal_code='12345',
        some_other_col=1)
]

SAMPLE_DAILY_WEATHER = [
    Row(date_local=datetime.strptime('2019/09/02 00:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/09/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/09/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/09/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=80.0,
        dummy_property='dumb-ditty-dumber'),
    Row(date_local=datetime.strptime('2019/09/03 01:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/09/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/09/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/09/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=70.0,
        dummy_property='dumb-ditty-dumber'),
    Row(date_local=datetime.strptime('2019/10/02 00:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/10/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/10/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/10/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=70.0,
        dummy_property='dumb-ditty-dumber'),
    Row(date_local=datetime.strptime('2019/10/03 01:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/10/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/10/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/10/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=60.0,
        dummy_property='dumb-ditty-dumber'),
    Row(date_local=datetime.strptime('2019/11/02 00:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/11/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/11/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/11/02 00:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=60.0,
        dummy_property='dumb-ditty-dumber'),
    Row(date_local=datetime.strptime('2019/11/03 01:00:00', '%Y/%m/%d %H:%M:%S').date(),
        year=datetime.strptime('2019/11/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().year,
        month=datetime.strptime('2019/11/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().month,
        day=datetime.strptime('2019/11/03 01:00:00', '%Y/%m/%d %H:%M:%S').date().day,
        postal_code='12345',
        temp_f_avg=50.0,
        dummy_property='dumb-ditty-dumber')
]


SAMPLE_BILLS = [
    Row(channel_id='00000000-0000-000b-0271-c078e4408800',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1500,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408800',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=500,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408800',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-12-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408801',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408801',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif2'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408801',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408801',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408801',
        location_id='20000000-0000-000b-0271-c078e4408801',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-12-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=2200,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-12-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=2500,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='GAS',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=2200,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='GAS',
        bill_start=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1000,
        dummy_property='bljdfaif'),
    Row(channel_id=None,
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408802',
        location_id=None,
        fuel_type='GAS',
        bill_start=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-12-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=2500,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408804',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408804',
        location_id='20000000-0000-000b-0271-c078e4408804',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-09-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=2000,
        dummy_property='bljdfaif'),
    Row(channel_id='00000000-0000-000b-0271-c078e4408804',
        tenant_id=64,
        account_id='10000000-0000-000b-0271-c078e4408804',
        location_id='20000000-0000-000b-0271-c078e4408804',
        fuel_type='ELECTRIC',
        bill_start=datetime.strptime('2019-10-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        bill_end=datetime.strptime('2019-11-01 05:00:00', '%Y-%m-%d %H:%M:%S'),
        consumption_scaled=1800,
        dummy_property='bljdfaif')
    ]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_coarse_disagg_ami(spark_session):
    spark_session \
        .createDataFrame(SAMPLE_DAILY_WEATHER) \
        .createOrReplaceTempView('daily_weather')

    spark_session \
        .createDataFrame(SAMPLE_LOCATIONS) \
        .createOrReplaceTempView('locs')

    spark_session \
        .createDataFrame(SAMPLE_BILLS) \
        .createOrReplaceTempView('bills')

    transform_config = {
        'task': 'tasks.model_coarse_disagg_bills.task.ModelCoarseDisaggBills',
        'script': 'task.py',
        'type': 'task',
        'kwargs': {'bill_hist_req': 3,
                   'run_date': '2019-12-02'}
    }

    output = transform_task(spark_session, {}, transform_config)

    row_count = output.count()
    assert row_count == 5

    assert output.filter(col('location_id') == '20000000-0000-000b-0271-c078e4408801').count() == 1
    assert output.filter(col('location_id') == '20000000-0000-000b-0271-c078e4408802').count() == 2
    assert output.filter(col('location_id') == '20000000-0000-000b-0271-c078e4408803').count() == 2

    row_1 = output.filter(output['location_id'] == '20000000-0000-000b-0271-c078e4408801').head(1)[0]

    assert row_1.tenant_id == 64
    assert row_1.account_id == '10000000-0000-000b-0271-c078e4408801'
    assert row_1.location_id == '20000000-0000-000b-0271-c078e4408801'
    assert row_1.fuel_type == 'ELECTRIC'

    assert round(row_1.total_consumption, 3) == 2000.0
    assert round(row_1.heat_consumption, 3) == 500.0
    assert round(row_1.cool_consumption, 3) == 0.0
    assert round(row_1.other_consumption, 3) == 1500.0
    assert round(row_1.heat_percent, 3) == 0.25
    assert round(row_1.cool_percent, 3) == 0.0
    assert round(row_1.other_percent, 3) == 0.75
    assert round(row_1.heat_scaled_percent, 3) == 0.25
    assert round(row_1.cool_scaled_percent, 3) == 0.0
    assert round(row_1.other_scaled_percent, 3) == 0.75
    assert round(row_1.scaling_factor, 3) == 1

    row_2 = output.filter(output['location_id'] == '20000000-0000-000b-0271-c078e4408802').head(1)[0]

    assert row_2.tenant_id == 64
    assert row_2.account_id == '10000000-0000-000b-0271-c078e4408802'
    assert row_2.location_id == '20000000-0000-000b-0271-c078e4408802'
    assert row_2.fuel_type == 'GAS'

    assert round(row_2.total_consumption, 3) == 2500.0
    assert round(row_2.heat_consumption, 3) == 1500.0
    assert round(row_2.cool_consumption, 3) == 0.0
    assert round(row_2.other_consumption, 3) == 1000.0
    assert round(row_2.heat_percent, 3) == 1500/2500
    assert round(row_2.cool_percent, 3) == 0.0
    assert round(row_2.other_percent, 3) == 1000/2500
    assert round(row_2.heat_scaled_percent, 3) == 1500/2500
    assert round(row_2.cool_scaled_percent, 3) == 0.0
    assert round(row_2.other_scaled_percent, 3) == 1000/2500
    assert round(row_2.scaling_factor, 3) == 1

    row_2 = output.filter(output['location_id'] == '20000000-0000-000b-0271-c078e4408802').head(2)[0]

    assert row_2.tenant_id == 64
    assert row_2.account_id == '10000000-0000-000b-0271-c078e4408802'
    assert row_2.location_id == '20000000-0000-000b-0271-c078e4408802'
    assert row_2.fuel_type == 'GAS'

    assert round(row_2.total_consumption, 3) == 2500.0
    assert round(row_2.heat_consumption, 3) == 1500.0
    assert round(row_2.cool_consumption, 3) == 0.0
    assert round(row_2.other_consumption, 3) == 1000.0
    assert round(row_2.heat_percent, 3) == 1500/2500
    assert round(row_2.cool_percent, 3) == 0.0
    assert round(row_2.other_percent, 3) == 1000/2500
    assert round(row_2.heat_scaled_percent, 3) == 1500/2500
    assert round(row_2.cool_scaled_percent, 3) == 0.0
    assert round(row_2.other_scaled_percent, 3) == 1000/2500
    assert round(row_2.scaling_factor, 3) == 1

    row_3 = output.filter(output['location_id'] == '20000000-0000-000b-0271-c078e4408803').head(1)[0]

    assert row_3.tenant_id == 64
    assert row_3.account_id == '10000000-0000-000b-0271-c078e4408802'
    assert row_3.location_id == '20000000-0000-000b-0271-c078e4408803'
    assert row_3.fuel_type == 'GAS'

    assert round(row_3.total_consumption, 3) == 2500.0
    assert round(row_3.heat_consumption, 3) == 1500.0
    assert round(row_3.cool_consumption, 3) == 0.0
    assert round(row_3.other_consumption, 3) == 1000.0
    assert round(row_3.heat_percent, 3) == 1500/2500
    assert round(row_3.cool_percent, 3) == 0.0
    assert round(row_3.other_percent, 3) == 1000/2500
    assert round(row_3.heat_scaled_percent, 3) == 1500/2500
    assert round(row_3.cool_scaled_percent, 3) == 0.0
    assert round(row_3.other_scaled_percent, 3) == 1000/2500
    assert round(row_3.scaling_factor, 3) == 1

    row_3 = output.filter(output['location_id'] == '20000000-0000-000b-0271-c078e4408803').head(2)[0]

    assert row_3.tenant_id == 64
    assert row_3.account_id == '10000000-0000-000b-0271-c078e4408802'
    assert row_3.location_id == '20000000-0000-000b-0271-c078e4408803'
    assert row_3.fuel_type == 'GAS'

    assert round(row_3.total_consumption, 3) == 2500.0
    assert round(row_3.heat_consumption, 3) == 1500.0
    assert round(row_3.cool_consumption, 3) == 0.0
    assert round(row_3.other_consumption, 3) == 1000.0
    assert round(row_3.heat_percent, 3) == 1500/2500
    assert round(row_3.cool_percent, 3) == 0.0
    assert round(row_3.other_percent, 3) == 1000/2500
    assert round(row_3.heat_scaled_percent, 3) == 1500/2500
    assert round(row_3.cool_scaled_percent, 3) == 0.0
    assert round(row_3.other_scaled_percent, 3) == 1000/2500
    assert round(row_3.scaling_factor, 3) == 1
