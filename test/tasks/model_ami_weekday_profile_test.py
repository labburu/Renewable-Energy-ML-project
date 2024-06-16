import pytest
from pyspark.sql import Row
from datetime import datetime
from nydus.core import transform_task

from tasks.model_ami_weekday_profile.model_ami_weekday_profile_test_data import GenerateTestData

CHANNELS = [
    Row(id='00000000-0000-000b-012c-041c5f815c03',
        location_id='00000000-0000-000b-012c-041c5f815c03'),   
]

@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_ami_peak_usage(spark_session):
    ami_data_raw = GenerateTestData().run(spark_session)
    channels_df = spark_session.createDataFrame(CHANNELS)

    ami_data_raw.createOrReplaceTempView('ami')
    channels_df.createOrReplaceTempView('channels')

    transform_config = {
        'task': 'tasks.model_ami_weekday_profile.model_ami_weekday_profile.ModelAMIWeekdayProfile',
        'script': 'model_ami_weekday_profile.py',
        'type': 'task',
        'kwargs': {'window_end_time': datetime.strptime('2017-01-12', '%Y-%m-%d')}
    }

    df_output = transform_task(spark_session, {}, transform_config)

    df_output_rows = df_output.count()
    assert df_output_rows == 1

    df_output_columns = df_output.columns

    assert len(df_output_columns) == 26

    assert 'tenant_id' in df_output_columns
    assert 'location_id' in df_output_columns
    assert 'hod_0' in df_output_columns
    assert 'hod_1' in df_output_columns
    assert 'hod_2' in df_output_columns
    assert 'hod_3' in df_output_columns
    assert 'hod_4' in df_output_columns
    assert 'hod_5' in df_output_columns
    assert 'hod_6' in df_output_columns
    assert 'hod_7' in df_output_columns
    assert 'hod_8' in df_output_columns
    assert 'hod_9' in df_output_columns
    assert 'hod_10' in df_output_columns
    assert 'hod_11' in df_output_columns
    assert 'hod_12' in df_output_columns
    assert 'hod_13' in df_output_columns
    assert 'hod_14' in df_output_columns
    assert 'hod_15' in df_output_columns
    assert 'hod_16' in df_output_columns
    assert 'hod_17' in df_output_columns
    assert 'hod_18' in df_output_columns
    assert 'hod_19' in df_output_columns
    assert 'hod_20' in df_output_columns
    assert 'hod_21' in df_output_columns
    assert 'hod_22' in df_output_columns
    assert 'hod_23' in df_output_columns

    row_1 = df_output.select('*').head(1)[0]
    assert row_1['tenant_id'] == 11
    assert row_1['location_id'] == '00000000-0000-000b-012c-041c5f815c03'

    assert round(row_1['hod_0'], 4) == 1.2316
    assert round(row_1['hod_1'], 4) == 1.0207
    assert round(row_1['hod_2'], 4) == 0.7530
    assert round(row_1['hod_3'], 4) == 0.7330
    assert round(row_1['hod_4'], 4) == 0.7028
    assert round(row_1['hod_5'], 4) == 0.8335
    assert round(row_1['hod_6'], 4) == 1.1162
    assert round(row_1['hod_7'], 4) == 0.9521
    assert round(row_1['hod_8'], 4) == 0.8382
    assert round(row_1['hod_9'], 4) == 0.8404
    assert round(row_1['hod_10'], 4) == 0.8227
    assert round(row_1['hod_11'], 4) == 0.8816
    assert round(row_1['hod_12'], 4) == 0.9575
    assert round(row_1['hod_13'], 4) == 1.2442
    assert round(row_1['hod_14'], 4) == 1.2435
    assert round(row_1['hod_15'], 4) == 1.1488
    assert round(row_1['hod_16'], 4) == 1.5112
    assert round(row_1['hod_17'], 4) == 1.8105
    assert round(row_1['hod_18'], 4) == 2.0234
    assert round(row_1['hod_19'], 4) == 1.8098
    assert round(row_1['hod_20'], 4) == 1.7232
    assert round(row_1['hod_21'], 4) == 1.8046
    assert round(row_1['hod_22'], 4) == 1.7528
    assert round(row_1['hod_23'], 4) == 1.6510
