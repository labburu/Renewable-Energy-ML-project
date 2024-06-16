"""AMI-based modeling."""

import os.path as op
from datetime import datetime

from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.macros.tendril import TendrilDAG
from airflow.operators.tendril import NydusModelOperator

PWD = op.abspath(op.join(op.dirname(__file__)))

spark_defaults = Variable.get('spark_defaults', deserialize_json=True)
s3 = Variable.get('s3', deserialize_json=True)
cron = Variable.get('cron', deserialize_json=True)

buckets = s3.get('buckets')

dag = TendrilDAG(
    'models_ami',
    description='Run various analyses on AMI data.',
    start_date=datetime(2019, 9, 1),
    default_args={
        'pagerduty_enabled': True,
        'pagerduty_severity': 'warning',
        'pagerduty_connection_id': 'pagerduty-scipi-models',
        'owner': 'scipi-models',
        'retries': 1,
    },
    max_active_runs=1,
    schedule_interval=cron['ami_insights']
)

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

model_ami_weekday_profile = NydusModelOperator(
    cluster_label='ami-insights-cluster',
    task_id='model_ami_weekday_profile',
    conf={
        'spark.driver.memory': '10g',
    },
    enable_pagerduty=False,
    dag=dag
)

model_ami_weekday_profile.set_upstream(latest_only)


model_ami_peak_use = NydusModelOperator(
    cluster_label='ami-insights-cluster',
    task_id='model_ami_peak_use',
    dag=dag
)

model_ami_peak_use.set_upstream(model_ami_weekday_profile)
