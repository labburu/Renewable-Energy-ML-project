import uuid
from datetime import date, timedelta

import pytest
from pyspark import Row

from dags.model_baseload.tasks.baseload.baseload_model import BaseLoadModel


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_filter_dataset__one_read_per_day(spark_session):
    evaluation_date = date(2020, 7, 8)
    channel_uuid = str(uuid.uuid4())

    df_rows = []
    for idx in range(720):
        read_date = evaluation_date - timedelta(days=idx)
        year = read_date.year
        month = read_date.month
        day = read_date.day
        df_rows.append(
            Row(channel_uuid=channel_uuid, consumption_total=float(idx), tenant_id=1, num_reads_actual=1,
                num_reads_total=1, min_seconds=86400, date_utc=read_date, year=year, month=month, day=day, hour_utc=0)
        )
    df = spark_session.createDataFrame(df_rows).orderBy('date_utc')

    model = BaseLoadModel(evaluation_date=evaluation_date.isoformat())
    filtered_df = model.filter_dataset(df)

    # the date-filtered dataframe covers start_date (inclusive) to evaluation_date (exclusive)
    assert filtered_df.count() == model.evaluate_previous_days
    assert filtered_df.collect()[0]['read_timestamp'] == evaluation_date - timedelta(days=model.evaluate_previous_days)
    assert filtered_df.collect()[-1]['read_timestamp'] == evaluation_date - timedelta(days=1)
