import json
from os.path import join, dirname, normpath
from unittest.mock import patch

from dags.oe_location_day_type.runners import dr_event_finder, identify_dr_or_no_dr

DAG_NAME = 'my_dag'
EVENT_DATE = '2020-06-26'
AIRFLOW_VARIABLES = {
    'tenants': ['AMEREN_MO', 'CONSUMERS'],
    'dr_events': [],
}
FIXTURES_DIR = normpath(join(dirname(__file__), '..', '..', 'fixtures'))


def fixture(name: str) -> str:
    """Get the path to a named fixture for this test suite."""
    return join(FIXTURES_DIR, name)


with open(fixture('dr_events_ameren_mo.json')) as f:
    events_ameren_mo = json.load(f)
with open(fixture('dr_events_consumers.json')) as f:
    events_consumers = json.load(f)

EVENT_ECOBEE_AMEREN_MO = [{'tenant': e['tenant'], 'oe_event_id': e['oe_event_id'], 'manufacturer': 'ECOBEE'}
                          for e in events_ameren_mo if e['start_time_utc'].startswith(EVENT_DATE)][0]
EVENT_ECOBEE_CONSUMERS = [{'tenant': e['tenant'], 'oe_event_id': e['oe_event_id'], 'manufacturer': 'ECOBEE'}
                          for e in events_consumers if e['start_time_utc'].startswith(EVENT_DATE)][0]

EVENT_EMERSON_AMEREN_MO = [{'tenant': e['tenant'], 'oe_event_id': e['oe_event_id'], 'manufacturer': 'EMERSON'}
                           for e in events_ameren_mo if e['start_time_utc'].startswith(EVENT_DATE)][0]
EVENT_EMERSON_CONSUMERS = [{'tenant': e['tenant'], 'oe_event_id': e['oe_event_id'], 'manufacturer': 'EMERSON'}
                           for e in events_consumers if e['start_time_utc'].startswith(EVENT_DATE)][0]


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value={'tenants': ['AMEREN_MO', 'CONSUMERS'], 'dr_events': []})
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_ecobee_dr_event_finder_all_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo.json')},
        'some_task_2': {'path': fixture('dr_events_consumers.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='ECOBEE', extract=extract)
    next_step = identify_dr_or_no_dr()
    assert next_step == 'process_dr_events'
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [EVENT_ECOBEE_AMEREN_MO, EVENT_ECOBEE_CONSUMERS],
        },
    )


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value={'tenants': ['AMEREN_MO', 'CONSUMERS'], 'dr_events': []})
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_emerson_dr_event_finder_all_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo.json')},
        'some_task_2': {'path': fixture('dr_events_consumers.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='EMERSON', extract=extract)
    next_step = identify_dr_or_no_dr()
    assert next_step == 'process_dr_events'
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [EVENT_EMERSON_AMEREN_MO, EVENT_EMERSON_CONSUMERS],
        },
    )


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value={'tenants': ['AMEREN_MO', 'CONSUMERS'], 'dr_events': []})
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_ecobee_dr_event_finder_some_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo_no_events.json')},
        'some_task_2': {'path': fixture('dr_events_consumers.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='ECOBEE', extract=extract)
    next_steps = identify_dr_or_no_dr()
    assert next_steps == ['process_dr_events', 'process_no_dr_day']
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [EVENT_ECOBEE_CONSUMERS],
        },
    )


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value={'tenants': ['AMEREN_MO', 'CONSUMERS'], 'dr_events': []})
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_emerson_dr_event_finder_some_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo_no_events.json')},
        'some_task_2': {'path': fixture('dr_events_consumers.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='EMERSON', extract=extract)
    next_steps = identify_dr_or_no_dr()
    assert next_steps == ['process_dr_events', 'process_no_dr_day']
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [EVENT_EMERSON_CONSUMERS],
        },
    )


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value=AIRFLOW_VARIABLES)
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_ecobee_dr_event_finder_no_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo_no_events.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='ECOBEE', extract=extract)
    next_steps = identify_dr_or_no_dr()
    assert next_steps == 'process_no_dr_day'
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [],
        },
    )


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value=AIRFLOW_VARIABLES)
@patch('lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_emerson_dr_event_finder_no_tenants_have_events(_, __, set_mock):
    extract = {
        'some_task_1': {'path': fixture('dr_events_ameren_mo_no_events.json')},
    }
    dr_event_finder(dag_execution_date=EVENT_DATE, manufacturer='EMERSON', extract=extract)
    next_steps = identify_dr_or_no_dr()
    assert next_steps == 'process_no_dr_day'
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [],
        },
    )
