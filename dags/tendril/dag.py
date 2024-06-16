"""Tendril dag."""

import os.path as op
from datetime import datetime

from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.sensors import ExternalTaskSensor

from airflow.macros.tendril import TendrilDAG, fn_last_execution_date
from airflow.operators.tendril import (
    NydusExtractOperator,
    NydusTransformOperator,
    NydusLoadOperator,
)

PWD = op.abspath(op.join(op.dirname(__file__)))

cron = Variable.get('cron', deserialize_json=True)
spark_defaults = Variable.get('spark_defaults', deserialize_json=True)
s3 = Variable.get('s3', deserialize_json=True)
lp_spark_cluster = Variable.get('lp_spark_cluster', default_var='spark-airflow')

buckets = s3.get('buckets')

dag = TendrilDAG(
    'tendril',
    start_date=datetime(2018, 6, 11),
    default_args={
        'pagerduty_enabled': True,
        'pagerduty_severity': 'warning',
        'pagerduty_connection_id': 'pagerduty-scipi-platform',
        'owner': 'scipi-platform',
    },
    schedule_interval=cron['tendril'],
)
with open(op.join(PWD, 'README.md'), encoding='utf-8') as f:
    dag.doc_md = f.read()

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

wait_for_transform_accounts = ExternalTaskSensor(
    task_id='transform_accounts',
    external_dag_id='soa_daily',
    external_task_id='transform_accounts',
    timeout=82800,
    execution_date_fn=fn_last_execution_date(cron['tendril'], cron['soa_daily']),
    dag=dag
)

private_participation_list = NydusExtractOperator(
    task_id='zeus.private_participation_list',
    task_bucket=buckets.get('nydus-tasks'),
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)

private_participation_list_entry = NydusExtractOperator(
    task_id='zeus.private_participation_list_entry',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.memory': '18g',
    },
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)

private_product = NydusExtractOperator(
    task_id='zeus.private_product',
    task_bucket=buckets.get('nydus-tasks'),
    conn_id='tendril-qubole',
    dag=dag
)

private_tenant_product = NydusExtractOperator(
    task_id='zeus.private_tenant_product',
    task_bucket=buckets.get('nydus-tasks'),
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)


extract_offers = NydusExtractOperator(
    task_id='offer-tracking-service.extract_offers',
    task_bucket=buckets.get('nydus-tasks'),
    conn_id='tendril-qubole',
    dag=dag
)

extract_visits = NydusExtractOperator(
    task_id='offer-tracking-service.extract_visits',
    task_bucket=buckets.get('nydus-tasks'),
    conn_id='tendril-qubole',
    dag=dag
)

extract_merchant_offers = NydusExtractOperator(
    task_id='offer-tracking-service.extract_merchant_offers',
    task_bucket=buckets.get('nydus-tasks'),
    conn_id='tendril-qubole',
    dag=dag
)

latest_only.set_downstream([
    wait_for_transform_accounts,
    private_participation_list,
    private_participation_list_entry,
    private_product,
    private_tenant_product
])


transform_account_participation = NydusTransformOperator(
    task_id='transform_account_participation',
    task_path='tendril/transform_account_participation',
    conn_id='tendril-qubole',
    dag=dag,
    static=True
)
transform_account_participation_flags = NydusTransformOperator(
    task_id='transform_account_participation_flags',
    task_path='tendril/transform_account_participation_flags',
    conn_id='tendril-qubole',
    dag=dag,
    static=True
)

transform_account_participation.set_upstream([
    wait_for_transform_accounts,
    private_participation_list,
    private_participation_list_entry,
    private_product,
    private_tenant_product,
])

transform_account_participation_flags.set_upstream([
    transform_account_participation,
])

load_account_participation = NydusLoadOperator(
    task_id='load_account_participation',
    task_path='tendril/load_account_participation',
    pool='redshift_load_pool',
    conn_id='tendril-qubole',
    dag=dag
)
load_account_participation_flags_redshift_warehouse = NydusLoadOperator(
    task_id='load_account_participation_flags_redshift_warehouse',
    task_path='tendril/load_account_participation_flags',
    load=[{
        'type': 'redshift',
        'database': 'warehouse',
        'table': 'soa.account_participation_flags',
        'mode': 'overwrite',
        'options': {
            'sortkeyspec': 'SORTKEY(tenant_id)',
        }
    }],
    pool='redshift_load_pool',
    conn_id='tendril-qubole',
    dag=dag
)

load_account_participation_flags_redshift_analytics = NydusLoadOperator(
    task_id='load_account_participation_flags_redshift_analytics',
    task_path='tendril/load_account_participation_flags',
    load=[{
        'type': 'redshift',
        'database': 'analytics',
        'table': 'public.account_participation_flags',
        'mode': 'overwrite',
        'options': {
            'sortkeyspec': 'SORTKEY(tenant_id)',
        }
    }],
    pool='redshift_load_pool_analytics',
    conn_id='tendril-qubole',
    dag=dag
)

# Load account_participation_flags into two different Redshift clusters
transform_account_participation_flags >> (
    load_account_participation_flags_redshift_warehouse,
    load_account_participation_flags_redshift_analytics,
)

load_merchant_offers = NydusLoadOperator(
    task_id='load_merchant_offers',
    task_path='tendril/load_merchant_offers',
    pool='redshift_load_pool',
    conn_id='tendril-qubole',
    dag=dag
)
load_offers = NydusLoadOperator(
    task_id='load_offers',
    task_path='tendril/load_offers',
    pool='redshift_load_pool',
    conn_id='tendril-qubole',
    dag=dag
)
load_visits = NydusLoadOperator(
    task_id='load_visits',
    task_path='tendril/load_visits',
    pool='redshift_load_pool',
    conn_id='tendril-qubole',
    dag=dag
)

# Offer tracking pipelines
latest_only >> extract_merchant_offers >> load_merchant_offers
latest_only >> extract_offers >> load_offers
latest_only >> extract_visits >> load_visits

load_account_participation.set_upstream(transform_account_participation)

# location profile flow
lp_extract_conf = {
    'spark.driver.memory': '12g',
    'spark.executor.instances': 4,
    'spark.executor.memory': '29700M',
    'spark.dynamicAllocation.maxExecutors': 4,
    'spark.executor.cores': 4,
}

extract_location_profiles = NydusExtractOperator(
    task_id='zeus.extract_location_profiles',
    task_bucket=buckets.get('nydus-tasks'),
    conf=lp_extract_conf,
    cluster_label=lp_spark_cluster,
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)

extract_source_priorities = NydusExtractOperator(
    task_id='zeus.extract_source_priorities',
    task_bucket=buckets.get('nydus-tasks'),
    cluster_label=lp_spark_cluster,
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)

extract_location_property_names = NydusExtractOperator(
    task_id='zeus.extract_location_property_names',
    task_bucket=buckets.get('nydus-tasks'),
    conf=lp_extract_conf,
    cluster_label=lp_spark_cluster,
    pool='extract_soa_pool',
    conn_id='tendril-qubole',
    dag=dag
)

extract_location_profiles.set_upstream(latest_only)
extract_source_priorities.set_upstream(latest_only)
extract_location_property_names.set_upstream(latest_only)

lp_transform_conf = {
    'spark.driver.memory': '12g',
    'spark.executor.cores': 5,
    'spark.executor.memory': '19800M',
    'spark.dynamicAllocation.initialExecutors': 10,
    'spark.dynamicAllocation.maxExecutors': 20,
}

transform_location_profiles = NydusTransformOperator(
    task_id='transform_location_profiles',
    task_path='tendril/transform_location_profiles',
    conf=lp_transform_conf,
    cluster_label=lp_spark_cluster,
    conn_id='tendril-qubole',
    dag=dag
)
transform_location_profiles.set_upstream([
    extract_location_profiles,
    extract_location_property_names,
    extract_source_priorities
])

enrich_experian_data = NydusTransformOperator(
    task_id='join_location_profiles_to_matched_experian',
    task_path='tendril/join_location_profiles_to_matched_experian',
    conf=lp_transform_conf,
    cluster_label=lp_spark_cluster,
    conn_id='tendril-qubole',
    dag=dag
)

enrich_experian_data.set_upstream(transform_location_profiles)

transform_location_profiles_tabular = NydusTransformOperator(
    task_id='transform_location_profiles_tabular',
    task_path='tendril/transform_location_profiles_tabular',
    cluster_label=lp_spark_cluster,
    conn_id='tendril-qubole',
    dag=dag
)

transform_location_profiles_tabular.set_upstream(transform_location_profiles)

transform_merge_demographics = NydusTransformOperator(
    task_id='transform_merge_demographics',
    task_path='tendril/transform_merge_demographics',
    conf={
        'spark.executor.cores': 4,
        'spark.executor.memory': '12g',
        'spark.driver.memory': '10g',
        'spark.dynamicAllocation.maxExecutors': 24,
        'spark.dynamicAllocation.initialExecutors': 8,
    },
    cluster_label=lp_spark_cluster,
    conn_id='tendril-qubole',
    dag=dag
)
transform_merge_demographics.doc = """
Transform Merge Demographics
----------------------------

This takes address-matched Experian data and joins it to our location
profiles, choosing a winning value based on source priority.
"""

transform_merge_demographics.set_upstream(transform_location_profiles)
