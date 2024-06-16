

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.tendril import NydusExtractOperator, NydusTransformOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.macros.tendril import report_dagrun_to_datadog
from aux.aws import move_s3_objects, check_s3_objects

cron = Variable.get('cron', deserialize_json=True)
s3 = Variable.get('s3', deserialize_json=True)

DAG_START_DATE = datetime(2020, 7, 25)

tendril_environment_name = Variable.get('tendril_environment_name')


def on_success_callback(context):
    """Execute tasks when DagRun is a success."""
    report_dagrun_to_datadog(context)


def on_failure_callback(context):
    """Execute tasks when DagRun is a failure."""
    report_dagrun_to_datadog(context)


tni_doc = """
### TNI Ingestion DAG

This DAG ingests TNI files.
TNI files contain demographic information that is utilized for building model simulations.

Prior to running this ensure the following:

- Your input file is in the `s3://tendril-aux-ingestor-common-format-[ENV]/tni/` path in the respective environment.
- __IMPORTANT__ You've set the **tni_tenant** variable in Airflow to the tenant identifier which corresponds to the TNI
    messages contained within the TNI file.  The tni_tenant variable is used during the transform to be included with
    the translated messages.
"""

tni_dag = DAG(
    'tni',
    default_args={
        'start_date': DAG_START_DATE,
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
    schedule_interval=cron['tni'])
tni_dag.doc_md = tni_doc
do_nothing = DummyOperator(task_id='do_nothing', dag=tni_dag)

tni_tenant = Variable.get('tni_tenant', default_var=7357)
tni_aux_bucket = "tendril-aux-ingestor-common-format-%s" % tendril_environment_name

check_for_tni_extract = BranchPythonOperator(
    task_id='check_for_tni_extract',
    python_callable=check_s3_objects,
    op_kwargs=dict(
        bucket=tni_aux_bucket,
        prefix='tni',
        wildcard_key='*.csv',
        task_go='extract_tni_ingest',
        task_stop='do_nothing'
        ),
    provide_context=True,
    trigger_rule='all_success',
    dag=tni_dag
    )


extract_tni_ingest = NydusExtractOperator(
    task_id='extract_tni_ingest',
    task_path='tni/extract_tni_ingest',
    transform_kwargs={
        'tni_tenantid': tni_tenant,
    },
    dag=tni_dag
)

transform_tni_ingest = NydusTransformOperator(
    task_id='transform_tni_to_aux_ingest',
    task_path='tni/transform_tni_to_aux_ingest',
    transform_kwargs={
        'tni_tenantid': tni_tenant,
    },
    dag=tni_dag
)

gather_tni_for_archiving = BranchPythonOperator(
    task_id='gather_tni_for_archiving',
    python_callable=check_s3_objects,
    op_kwargs=dict(
        bucket=tni_aux_bucket,
        prefix='tni',
        wildcard_key='*.csv',
        task_go='archive_tni',
        task_stop='do_nothing'
        ),
    provide_context=True,
    trigger_rule='all_success',
    dag=tni_dag
    )

archive_tni = PythonOperator(
        task_id='archive_tni',
        python_callable=move_s3_objects,
        op_kwargs=dict(
            from_prefix='tni/',
            to_prefix="tni-processed/%s" % tni_tenant,
            sse='AES256',
            manifest_task='gather_tni_for_archiving',
            task_key='archive_tni',
            dag_id="tni"),
        provide_context=True,
        retry_delay=timedelta(seconds=60),
        retries=3,
        dag=tni_dag
)


check_for_tni_extract >> extract_tni_ingest >> transform_tni_ingest >> gather_tni_for_archiving >> archive_tni

gather_tni_for_archiving >> do_nothing
check_for_tni_extract >> do_nothing
