"""Tendril Treatment dag."""

import os.path as op
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.macros.tendril import report_dagrun_to_datadog
from airflow.operators.tendril import (
    NydusExtractOperator,
    NydusTransformOperator,
    NydusLoadOperator
)

PWD = op.abspath(op.join(op.dirname(__file__)))

cron = Variable.get('cron', deserialize_json=True)
s3 = Variable.get('s3', deserialize_json=True)

buckets = s3.get('buckets')


def on_success_callback(context):
    """Execute tasks when DagRun is a success."""
    report_dagrun_to_datadog(context)


def on_failure_callback(context):
    """Execute tasks when DagRun is a failure."""
    report_dagrun_to_datadog(context)


dag = DAG(
    'treatment',
    start_date=datetime(2018, 11, 1),
    default_args={
        'retry_delay': timedelta(seconds=1800),
        'retries': 3,
        'enable_pagerduty': True,
    },
    max_active_runs=1,
    params={
        'env': Variable.get('tendril_environment_name'),
    },
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    schedule_interval=cron['treatment'],
)
with open(op.join(PWD, 'README.md'), encoding='utf-8') as f:
    dag.doc_md = f.read()

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

extract_groups = NydusExtractOperator(
    task_id='treatment-service.extract_groups',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.instances': '1',
        'spark.qubole.max.executors': '1',
        'spark.executor.cores': '3',
        'spark.executor.memory': '1024M',
    },
    pool='treatment_pool',
    dag=dag
)

extract_memberships = NydusExtractOperator(
    task_id='treatment-service.extract_memberships',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.instances': '1',
        'spark.dynamicAllocation.maxExecutors': '1',
        'spark.executor.cores': '5',
        'spark.executor.memory': '32g',
    },
    pool='treatment_pool',
    dag=dag
)

extract_products = NydusExtractOperator(
    task_id='treatment-service.extract_products',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.instances': '1',
        'spark.qubole.max.executors': '1',
        'spark.executor.cores': '3',
        'spark.executor.memory': '1024M',
    },
    pool='treatment_pool',
    dag=dag
)

extract_delivery_channels = NydusExtractOperator(
    task_id='treatment-service.extract_delivery_channels',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.instances': '1',
        'spark.qubole.max.executors': '1',
        'spark.executor.cores': '3',
        'spark.executor.memory': '1024M',
    },
    pool='treatment_pool',
    dag=dag
)

extract_schedules = NydusExtractOperator(
    task_id='treatment-service.extract_schedules',
    task_bucket=buckets.get('nydus-tasks'),
    conf={
        'spark.executor.instances': '1',
        'spark.qubole.max.executors': '1',
        'spark.executor.cores': '3',
        'spark.executor.memory': '2048M',
    },
    pool='treatment_pool',
    dag=dag
)

extract_groups.set_upstream(latest_only)
extract_memberships.set_upstream(latest_only)
extract_products.set_upstream(latest_only)
extract_delivery_channels.set_upstream(latest_only)
extract_schedules.set_upstream(latest_only)

transform_memberships = NydusTransformOperator(
    task_id='transform_treatment_memberships',
    dag=dag
)

transform_memberships.set_upstream(extract_groups)
transform_memberships.set_upstream(extract_memberships)
transform_memberships.set_upstream(extract_products)

transform_schedules = NydusTransformOperator(
    task_id='transform_treatment_schedules',
    dag=dag
)

transform_schedules.set_upstream(transform_memberships)
transform_schedules.set_upstream(extract_delivery_channels)
transform_schedules.set_upstream(extract_schedules)

load_treatment_memberships = NydusLoadOperator(
    task_id='load_treatment_memberships',
    pool='redshift_load_pool',
    dag=dag
)

load_treatment_memberships.set_upstream(transform_memberships)

load_treatment_schedules = NydusLoadOperator(
    task_id='load_treatment_schedules',
    pool='redshift_load_pool',
    dag=dag
)

load_treatment_schedules.set_upstream(transform_schedules)
