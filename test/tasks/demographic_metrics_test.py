import pytest
from pyspark.sql import Row
from nydus.core import transform_task

transform_locations = [
        Row(id='1',
            account_id='1',
            tenant_id=1),
        Row(id='2',
            account_id='2',
            tenant_id=1),
        Row(id='3',
            account_id='3',
            tenant_id=2),
        Row(id='4',
            account_id='4',
            tenant_id=2)]

normalized_locations = [Row(location_id='1',
                            requested_address='123 main city st',
                            tenant_id='1'),
                        Row(location_id='2',
                            requested_address='123 main city st',
                            tenant_id='1'),
                        Row(location_id='3',
                            requested_address='123 main city st',
                            tenant_id='2')]

match_demographics = [Row(addr_hash_full='hash1',
                          aid='aid1',
                          location_id='1',
                          tenant_id='1',
                          match_quality='ADDR_HASH_FULL'),
                      Row(addr_hash_full='hash1',
                          aid='aid2',
                          location_id='2',
                          tenant_id='1',
                          match_quality='ADDR_HASH_FULL_UNIQUE'),
                      Row(addr_hash_full='hash1',
                          aid='aid3',
                          location_id='3',
                          tenant_id='2',
                          match_quality='ADDR_HASH_FULL')]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_metrics(spark_session):
    spark_session.createDataFrame(transform_locations).createOrReplaceTempView('transform_locations')
    spark_session.createDataFrame(normalized_locations).createOrReplaceTempView('normalized_locations')
    spark_session.createDataFrame(match_demographics).createOrReplaceTempView('match_demographics')

    transform_config = {
        'task': 'tasks.demographic_metrics.metrics.DemographicMetrics',
        'script': 'metrics.py',
        'type': 'task'
    }

    output = transform_task(spark_session, {}, transform_config)

    output.show()
    assert output.count() == 10

    metric = output.filter(output.metric_name == 'Zeus locations')
    assert metric.head()[1] == 'Total'
    assert metric.head()[2] == 4
    assert metric.head()[3] is None

    metric = output.filter(output.metric_name == 'Normalized locations')
    assert metric.head()[1] == 'Total'
    assert metric.head()[2] == 3
    assert metric.head()[3] == 75.00

    metric = output.filter(output.metric_name == 'Locations matched with Experian')
    assert metric.head()[1] == 'Total'
    assert metric.head()[2] == 3
    assert metric.head()[3] == 75.00
