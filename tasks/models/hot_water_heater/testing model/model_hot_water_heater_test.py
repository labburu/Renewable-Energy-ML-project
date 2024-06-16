import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from nydus.core import transform_task

SAMPLE_CHANNELS = [
    Row(location_id='00000000-0000-000b-0271-c078e4408806',
        channel_id='10000000-0000-000b-0271-c078e4408806',
        some_other_col=1),
    Row(location_id='00000000-0000-000b-0271-c078e4408805',
        channel_id='10000000-0000-000b-0271-c078e4408805',
        some_other_col=2346),
    Row(location_id='00000000-0000-000b-0271-c078e4408804',
        channel_id='10000000-0000-000b-0271-c078e4408804',
        some_other_col=1432),
    Row(location_id='00000000-0000-000b-0271-c078e4408803',
        channel_id='10000000-0000-000b-0271-c078e4408803',
        some_other_col=7654),
]

SAMPLE_HOURLY_AMI = [
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 19:15:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=0.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 19:30:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=1.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 19:45:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=2.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 20:00:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=3.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 20:15:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=4.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 20:30:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=5.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 20:45:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=6.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 21:00:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=7.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 21:15:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=8.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 21:30:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=9.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 21:45:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=4.144,
        month=5,
        year=2019,
        tenant_id=64),
    Row(channel_uuid='10000000-0000-000b-0271-c078e4408806',
        interval_start_utc='2019-05-20 22:00:00',
        seconds_per_interval=900,
        time_zone='US/Eastern',
        consumption=5.144,
        month=5,
        year=2019,
        tenant_id=64),
]

SCHEMA_HOURLY_AMI = StructType([
    StructField('channel_uuid', StringType()),
    StructField('interval_start_utc', StringType()),
    StructField('seconds_per_interval', IntegerType()),
    StructField('time_zone', StringType()),
    StructField('consumption', FloatType()),
    StructField('month', IntegerType()),
    StructField('year', IntegerType()),
    StructField('tenant_id', IntegerType()),
])


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_water_heater_disagg(spark_session):
    spark_session \
        .createDataFrame(SAMPLE_CHANNELS) \
        .createOrReplaceTempView('channels')

    spark_session \
        .createDataFrame(SAMPLE_HOURLY_AMI, SCHEMA_HOURLY_AMI) \
        .createOrReplaceTempView('ami')

    transform_config = {
        'task': 'tasks.hot_water_heater.model.hot_water_heater.HotWHModel',
        'script': 'hot_water_heater.py',
        'type': 'task',
        'kwargs': {'test': True, 'evaluation_date': '2019-05-21'}
    }

    output = transform_task(spark_session, {}, transform_config)

    output.show(20, False)

    row_count = output.count()
    assert row_count == 0
