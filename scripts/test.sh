#!/bin/bash -xe
# Run all python tests in corresponding environments
# Note: assumed to run from tendrilinc/airflow repository root

export PYTHONPATH=`pwd`:${PYTHONPATH}

# Run python tests (i.e., stuff that runs in the docker-airflow environment)
pyenv local 3.6.7
# Print dependencies and versions into the build log
python --version
pip freeze

pip install -r test/requirements.txt
airflow initdb
pytest -v --junitxml=test_results_dags.xml test/dags/
pytest -v --junitxml=test_results_oe_ineligible_registration_report_utils.xml dags/oe_ineligible_registration_report/test/test_utils.py
pytest -v --junitxml=test_results_oe_enrollment_reports_utils.xml dags/oe_enrollment_reports/test/test_utils.py
pytest -v --junitxml=test_results_dag_engage_messages_reports_utils.xml dags/engage_messages_reports/test/test_utils.py
pytest -v --junitxml=test_results_dag_sync_location_profile_utils.xml dags/sync_location_profile/tests/test_utils.py
pytest -v --junitxml=test_results_weather_migration.xml dags/weather_migration/test/tasks/

# Run PySpark tests (i.e., stuff that runs in the Qubole PySpark environment)
pyenv local 3.5.6
# Print dependencies and versions into the build log
python --version
pip freeze

pip install -r test/requirements.txt
# XXX: Remove this ASAP -- this is very temporary
# This should be resolved by specifying a nydus version in the job folder, which
# will get tested and bundled individually
pip install tendril-nydus==2.0.4 --quiet
pytest -v --junitxml=test_results_tasks.xml test/tasks/

# Run tests that are bundled with DAGs
# For now, run these separately, but eventually split these off into their own
# bundles that are deployed individually.
pytest -v --junitxml=test_results_dag_ami_alert_eligibility_sync.xml dags/ami_alert_eligibility_sync/test/tasks/test_ami_alert_eligibility_sync/sync_iterable_list.py
pytest -v --junitxml=test_results_dag_ami_alert_eligibility_sync.xml dags/ami_alert_eligibility_sync/test/tasks/test_ami_alert_eligibility_sync/transform_duke_alerts_eligible.py
pytest -v --junitxml=test_results_dag_ami_alert_eligibility_sync.xml dags/ami_alert_eligibility_sync/test/tasks/test_ami_alert_eligibility_sync/treatment_service.py
pytest -v --junitxml=test_results_dag_ami_alert_eligibility_sync.xml dags/ami_alert_eligibility_sync/test/tasks/test_ami_alert_eligibility_sync/upload_decision_lists.py
pytest -v --junitxml=test_results_dag_engage_messages_reports.xml dags/engage_messages_reports/test/tasks/
pytest -v --junitxml=test_results_dag_email_performance.xml dags/email_performance/
pytest -v --junitxml=test_results_oe_ineligible_registration_report_tasks.xml dags/oe_ineligible_registration_report/test/tasks/
pytest -v --junitxml=test_results_oe_enrollment_reports_tasks.xml dags/oe_enrollment_reports/test/tasks/
pytest -v --junitxml=test_results_oe.xml dags/oe/test/tasks/
pytest -v --junitxml=test_results_oe_location_day_type.xml dags/oe_location_day_type/test/tasks/
pytest -v --junitxml=test_results_oe_nest_thermostat_history_to_warehouse.xml dags/oe_nest_thermostat_history_to_warehouse/test/tasks/
pytest -v --junitxml=test_results_dag_nba_insight_weather.xml dags/nba_insight_weather/
pytest -v --junitxml=test_results_dag_nba_insight_data_import.xml dags/nba_insight_data_import/
pytest -v --junitxml=test_results_dag_nba_insight_nba_generation.xml dags/nba_insight_nba_generation/
pytest -v --junitxml=test_results_dag_nsync_location_profile.xml dags/sync_location_profile/tests/tasks