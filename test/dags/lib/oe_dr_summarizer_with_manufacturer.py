import json
from datetime import date
from os.path import join, dirname, normpath
from unittest.mock import patch

from dags.lib.oe_dr_summarizer_with_manufacturer import get_var, set_var, parse_dr_event_date, \
    parse_yyyymmdd_date, dr_event_date_checker, select_dr_events_for_date, load_and_filter_dr_events

DAG_NAME = 'my_dag'
EVENT_DATE = '2020-06-26'
AIRFLOW_VARIABLES = {
    'tenants': ['AMEREN_MO', 'CONSUMERS'],
    'dr_events': []
}
FIXTURES_DIR = normpath(join(dirname(__file__), '..', '..', 'fixtures'))


def fixture(name: str) -> str:
    """Get the path to a named fixture for this test suite."""
    return join(FIXTURES_DIR, name)


with open(fixture('dr_events_ameren_mo.json')) as f:
    events_ameren_mo = json.load(f)
with open(fixture('dr_events_consumers.json')) as f:
    events_consumers = json.load(f)

EVENT_AMEREN_MO = [e for e in events_ameren_mo if e['start_time_utc'].startswith(EVENT_DATE)][0]
EVENT_CONSUMERS = [e for e in events_consumers if e['start_time_utc'].startswith(EVENT_DATE)][0]


@patch('airflow.models.Variable.get', return_value=AIRFLOW_VARIABLES)
@patch('dags.lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_get_var_works(_, get_mock):
    assert get_var() == AIRFLOW_VARIABLES
    get_mock.assert_called_with(deserialize_json=True, key='my_dag')


@patch('airflow.models.Variable.set')
@patch('airflow.models.Variable.get', return_value=AIRFLOW_VARIABLES)
@patch('dags.lib.oe_dr_summarizer_with_manufacturer.dag_name', return_value=DAG_NAME)
def test_set_var_works(_, __, set_mock):
    set_var({'foo': 'bar', 'baz': {'quux': 'plugh'}})
    set_mock.assert_called_with(
        key='my_dag',
        serialize_json=True,
        value={
            'tenants': ['AMEREN_MO', 'CONSUMERS'],
            'dr_events': [],
            'foo': 'bar', 'baz': {'quux': 'plugh'},
        },
    )


def test_parse_dr_event_date_works():
    assert parse_dr_event_date('1992-01-11 07:34:00') == date(1992, 1, 11)
    assert parse_dr_event_date('2020-07-13 11:31:15') == date(2020, 7, 13)


def test_parse_yyyymmdd_date_works():
    assert parse_yyyymmdd_date('1992-01-11') == date(1992, 1, 11)
    assert parse_yyyymmdd_date('2020-07-13') == date(2020, 7, 13)


def test_dr_event_date_checker_works():
    dates_match = dr_event_date_checker('2020-07-13')
    assert dates_match({'start_time_utc': '2020-07-13 11:31:15'})
    assert not dates_match({'start_time_utc': '1992-01-11 07:34:00'})


def test_select_dr_events_for_date_works():
    dr_events = [
        {'start_time_utc': '2020-07-13 11:31:15', 'tenant': 'AMEREN_MO', 'oe_event_id': 12},
        {'start_time_utc': '1992-01-11 07:34:00', 'tenant': 'CONSUMERS', 'oe_event_id': 13},
    ]
    actual_events = select_dr_events_for_date('2020-07-13', dr_events, 'EMERSON')
    assert actual_events == [{'oe_event_id': 12, 'tenant': 'AMEREN_MO', 'manufacturer': 'EMERSON'}]


def test_load_and_filter_dr_events_works():
    paths = [fixture(name) for name in ['dr_events_ameren_mo.json', 'dr_events_consumers.json']]
    selected_events = load_and_filter_dr_events(EVENT_DATE, paths, 'EMERSON')
    assert len(selected_events) == 2
    assert {e['tenant'] for e in selected_events} == {'AMEREN_MO', 'CONSUMERS'}
