import pytest
from pyspark.sql import Row

from nydus.core import transform_task

SAMPLE_AMI = [
    Row(tenant_id=11,
        location_id='00000000-0000-000b-012c-043e6ea15402',
        account_id='00000000-0000-000b-012c-043e6ea15401',
        hod_0=0.945366667,
        hod_1=0.820888867,
        hod_2=0.9092222,
        hod_3=0.861111133,
        hod_4=0.8797222,
        hod_5=0.9327778,
        hod_6=0.974555533,
        hod_7=1.063944467,
        hod_8=1.0535,
        hod_9=1.223611133,
        hod_10=1.102055533,
        hod_11=1.1707778,
        hod_12=1.672333333,
        hod_13=2.1757778,
        hod_14=1.761555533,
        hod_15=2.111388867,
        hod_16=2.274944467,
        hod_17=3.4545,
        hod_18=2.720666667,
        hod_19=1.571611133,
        hod_20=1.3952778,
        hod_21=1.163666667,
        hod_22=1.302,
        hod_23=1.3057778),
    Row(tenant_id=11,
        location_id='00000000-0000-000b-012c-044f06615c02',
        account_id='00000000-0000-000b-012c-044f06615c01',
        hod_0=0.64003332,
        hod_1=0.57140668,
        hod_2=0.46353336,
        hod_3=0.58105,
        hod_4=0.63446668,
        hod_5=0.36338332,
        hod_6=0.20958,
        hod_7=0.18966668,
        hod_8=0.22911668,
        hod_9=0.17208668,
        hod_10=0.17199332,
        hod_11=0.15782668,
        hod_12=0.16016336,
        hod_13=0.24819668,
        hod_14=0.19056,
        hod_15=0.35839668,
        hod_16=0.49473,
        hod_17=0.44124,
        hod_18=0.75133,
        hod_19=0.44737668,
        hod_20=0.49695,
        hod_21=0.60605,
        hod_22=0.80409668,
        hod_23=0.53431664),
    Row(tenant_id=11,
        location_id='00000000-0000-000b-012c-049445e16003',
        account_id='00000000-0000-000b-012c-049445e16001',
        hod_0=0.40797776,
        hod_1=0.42983,
        hod_2=0.40672,
        hod_3=0.39329,
        hod_4=0.41828,
        hod_5=0.45917,
        hod_6=0.56151,
        hod_7=0.64968,
        hod_8=0.34495,
        hod_9=0.30521,
        hod_10=0.30966,
        hod_11=0.31097,
        hod_12=0.30891,
        hod_13=0.30767,
        hod_14=0.31011,
        hod_15=0.31249,
        hod_16=0.30691,
        hod_17=0.34406,
        hod_18=0.41274,
        hod_19=0.4872,
        hod_20=0.65953,
        hod_21=0.66231,
        hod_22=0.60991,
        hod_23=0.47988),
    Row(tenant_id=11,
        location_id='00000000-0000-000b-012c-049491515402',
        account_id='00000000-0000-000b-012c-049491515401',
        hod_0=0.8121022,
        hod_1=0.7202,
        hod_2=0.6449,
        hod_3=0.54758,
        hod_4=0.42688,
        hod_5=0.39803,
        hod_6=0.45656,
        hod_7=0.60979,
        hod_8=0.54156,
        hod_9=0.46401,
        hod_10=0.56152,
        hod_11=0.68876,
        hod_12=0.66634,
        hod_13=0.68047,
        hod_14=0.71605,
        hod_15=0.76658,
        hod_16=0.72748,
        hod_17=0.72187,
        hod_18=0.82861144,
        hod_19=0.89725144,
        hod_20=0.88228,
        hod_21=0.80066,
        hod_22=0.86897,
        hod_23=0.88784),
    Row(tenant_id=11,
        location_id='00000000-0000-000b-012c-04cd8ce15402',
        account_id='00000000-0000-000b-012c-04cd8ce15401',
        hod_0=1.07733776,
        hod_1=0.88324,
        hod_2=0.63704,
        hod_3=0.40292,
        hod_4=0.51936,
        hod_5=0.8116,
        hod_6=0.43396,
        hod_7=0.39692,
        hod_8=0.32912,
        hod_9=0.29592,
        hod_10=0.29788,
        hod_11=0.3016,
        hod_12=0.31184,
        hod_13=0.3248,
        hod_14=0.33164,
        hod_15=0.34888,
        hod_16=0.51376,
        hod_17=0.52004,
        hod_18=0.92916,
        hod_19=1.40344,
        hod_20=1.3178,
        hod_21=1.34532,
        hod_22=1.14028,
        hod_23=1.09344),
]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_model_ami_peak_usage(spark_session):

    ami_data_hourly = spark_session.createDataFrame(SAMPLE_AMI)

    ami_data_hourly.createOrReplaceTempView('ami')

    transform_config = {
        'task': 'tasks.model_ami_peak_use.model_ami_peak_use.ModelAMIPeakUse',
        'script': 'model_ami_peak_use.py',
        'type': 'task'
    }

    df_output = transform_task(spark_session, {}, transform_config)

    df_output.show(10, False)

    # Assert
    df_output_rows = df_output.count()
    assert df_output_rows == 5

    df_output_columns = df_output.columns

    assert len(df_output_columns) == 10

    assert 'location_id' in df_output_columns
    assert 'tenant_id' in df_output_columns
    assert 'morning_peak' in df_output_columns
    assert 'afternoon_peak' in df_output_columns
    assert 'evening_peak' in df_output_columns
    assert 'night_peak' in df_output_columns
    assert 'morning_percent_of_day' in df_output_columns
    assert 'afternoon_percent_of_day' in df_output_columns
    assert 'evening_percent_of_day' in df_output_columns
    assert 'night_percent_of_total' in df_output_columns

    row_1 = df_output.select('*').head(1)[0]
    assert row_1['location_id'] == '00000000-0000-000b-012c-043e6ea15402'
    assert row_1['tenant_id'] == 11
    assert row_1['morning_peak'] == 0
    assert row_1['afternoon_peak'] == 0
    assert row_1['evening_peak'] == 1
    assert row_1['night_peak'] == 0
    assert round(row_1['morning_percent_of_day'], 3) == 0.245
    assert round(row_1['afternoon_percent_of_day'], 3) == 0.372
    assert round(row_1['evening_percent_of_day'], 3) == 0.383
    assert round(row_1['night_percent_of_total'], 3) == 0.228
