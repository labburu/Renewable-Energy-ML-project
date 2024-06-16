"""Models dag."""

from datetime import datetime

from airflow.models import Variable
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.macros.tendril import TendrilDAG, fn_last_execution_date
from airflow.operators.tendril import NydusModelOperator, NydusTransformOperator

cron = Variable.get('cron', deserialize_json=True)

dag = TendrilDAG(
    'models',
    start_date=datetime(2019, 7, 1),
    default_args={
        'pagerduty_enabled': True,
        'pagerduty_severity': 'warning',
        'pagerduty_connection_id': 'pagerduty-scipi-models',
        'owner': 'scipi-models',
    },
    max_active_runs=1,
    schedule_interval=cron['models'],
)

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

wait_for_location_profiles = ExternalTaskSensor(
    task_id='tendril.transform_location_profiles_tabular',
    external_dag_id='tendril',
    external_task_id='transform_location_profiles_tabular',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['tendril']),
    dag=dag,
)

wait_for_transform_bills = ExternalTaskSensor(
    task_id='soa_daily.transform_bills',
    external_dag_id='soa_daily',
    external_task_id='transform_bills',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['soa_daily']),
    dag=dag,
)

wait_for_weather = ExternalTaskSensor(
    task_id='weather.transform_historical_weather_daily',
    external_dag_id='weather',
    external_task_id='transform_historical_weather_daily',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['weather']),
    dag=dag,
)

wait_for_experian = ExternalTaskSensor(
    task_id='demographics_matching.join_locations_to_matched_experian',
    external_dag_id='demographics_matching',
    external_task_id='join_locations_to_matched_experian',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['demographics_matching']),
    dag=dag,
)

wait_for_experian_normalized = ExternalTaskSensor(
    task_id='demographics_matching.transform_normalize_experian',
    external_dag_id='demographics_matching',
    external_task_id='transform_normalize_experian',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['demographics_matching']),
    dag=dag,
)

wait_for_transform_locations = ExternalTaskSensor(
    task_id='soa_daily.transform_locations',
    external_dag_id='soa_daily',
    external_task_id='transform_locations',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['soa_daily']),
    dag=dag,
)

wait_for_transform_accounts = ExternalTaskSensor(
    task_id='soa_daily.transform_accounts',
    external_dag_id='soa_daily',
    external_task_id='transform_accounts',
    poke_interval=60,
    timeout=43200,
    execution_date_fn=fn_last_execution_date(cron['models'], cron['soa_daily']),
    dag=dag,
)

model_demand_response = NydusModelOperator(
    task_id='model_demand_response',
    cluster_label='spark-models',
    conf={
        'spark.driver.memory': '10g',
    },
    dag=dag
)

transform_ev_propensity = NydusModelOperator(
    task_id='transform_ev_propensity',
    task_path='models/ev_propensity/transform_ev_propensity',
    cluster_label='spark-models',
    conf={
        'spark.dynamicAllocation.maxExecutors': '40',
        'spark.executor.memory': '12g',
    },
    dag=dag
)

model_ev_propensity = NydusModelOperator(
    task_id='model_ev_propensity',
    task_path='models/ev_propensity/model_ev_propensity',
    cluster_label='spark-models',
    conf={
        'spark.dynamicAllocation.maxExecutors': '40',
        'spark.executor.memory': '12g',
    },
    dag=dag
)

model_has_ami = NydusTransformOperator(
    task_id='model_has_ami',
    task_path='models/model_has_ami',
    cluster_label='spark-models',
    transform_kwargs={
        'evaluation_date': '{{ next_ds }}'
    },
    conf={
        'spark.driver.memory': '10g',
    },
    dag=dag
)

model_hvac_upgrade_propensity = NydusModelOperator(
    task_id='model_hvac_upgrade_propensity',
    cluster_label='spark-models',
    conf={
        'spark.dynamicAllocation.maxExecutors': '40',
        'spark.qubole.max.executors': '40',
        'spark.executor.memory': '12g',
    },
    dag=dag
)

model_smart_thermostat_v2 = NydusModelOperator(
    task_id='model_smart_thermostat_v2',
    cluster_label='spark-models',
    conf={
        'spark.driver.memory': '10g',
        'spark.dynamicAllocation.maxExecutors': '40',
        'spark.qubole.max.executors': '40',
        'spark.executor.memory': '12g',
    },
    dag=dag
)

model_income_per_occupant = NydusModelOperator(
    task_id='model_income_per_occupant',
    task_path='model_income_per_occupant',
    cluster_label='spark-models',
    dag=dag
)

model_weather_sensitivity = NydusModelOperator(
    task_id='model_weather_sensitivity',
    cluster_label='spark-models',
    dag=dag
)

transform_weather_sensitivity = NydusTransformOperator(
    task_id='transform_weather_sensitivity',
    cluster_labl='spark_models',
    dag=dag
)

model_seasonal_home = NydusModelOperator(
    task_id='model_seasonal_home',
    cluster_label='spark-models',
    dag=dag
)

model_digital_engagement = NydusModelOperator(
    task_id='model_digital_engagement',
    cluster_label='spark-models',
    dag=dag
)

model_marketplace_propensity = NydusModelOperator(
    task_id='model_marketplace_propensity',
    cluster_label='spark-models',
    dag=dag
)

model_green_energy_propensity = NydusModelOperator(
    task_id='model_green_energy_propensity',
    cluster_label='spark-models',
    dag=dag
)

model_tou_propensity = NydusModelOperator(
    task_id='model_tou_propensity',
    cluster_label='spark-models',
    dag=dag
)


latest_only.set_downstream([
    wait_for_location_profiles,
    wait_for_transform_bills,
    wait_for_weather,
    wait_for_experian,
    wait_for_transform_locations,
    wait_for_transform_accounts,
    model_digital_engagement,
    model_smart_thermostat_v2,
    model_income_per_occupant,
    model_has_ami,
])

wait_for_experian_normalized.set_upstream(wait_for_experian)

model_demand_response.set_upstream(wait_for_experian_normalized)
model_demand_response.set_upstream(wait_for_transform_locations)
model_demand_response.set_upstream(wait_for_transform_accounts)
model_demand_response.set_upstream(transform_weather_sensitivity)

model_hvac_upgrade_propensity.set_upstream(wait_for_experian_normalized)
model_hvac_upgrade_propensity.set_upstream(wait_for_transform_locations)
model_hvac_upgrade_propensity.set_upstream(wait_for_transform_accounts)
model_hvac_upgrade_propensity.set_upstream(transform_weather_sensitivity)

transform_ev_propensity.set_upstream(wait_for_transform_locations)
transform_ev_propensity.set_upstream(wait_for_location_profiles)

model_ev_propensity.set_upstream(transform_ev_propensity)

transform_weather_sensitivity.set_upstream(model_weather_sensitivity)

model_weather_sensitivity.set_upstream(wait_for_weather)
model_weather_sensitivity.set_upstream(wait_for_transform_locations)
model_weather_sensitivity.set_upstream(wait_for_transform_bills)

model_seasonal_home.set_upstream(wait_for_transform_locations)
model_seasonal_home.set_upstream(wait_for_transform_bills)

model_marketplace_propensity.set_upstream(wait_for_experian)
model_green_energy_propensity.set_upstream(wait_for_experian)

model_tou_propensity.set_upstream(wait_for_experian_normalized)
model_tou_propensity.set_upstream(model_ev_propensity)
