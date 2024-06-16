import pytest
import uuid
from pyspark.sql import Row
from tasks.join_raw_bills_weather.task import JoinBillsWeather
from decimal import Decimal
import datetime

test_input_1 = [
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 1))), bill_end='2018-03-01 00:00:00',
    bill_start='2018-02-05 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100, bill_days=31,
    bill_yearmonth=201801, bill_month=1, bill_adc=100.0),  # noqa
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 2))), bill_end='2018-02-01 00:00:00',
    bill_start='2018-01-07 00:00:00', fuel_type='ELECTRIC', consumption_scaled=-3100, bill_days=31,
    bill_yearmonth=201801, bill_month=1, bill_adc=0.0),  # noqa
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 3))), bill_end='2018-01-01 00:00:00',
    bill_start='2018-01-05 00:00:00', fuel_type='ELECTRIC', consumption_scaled=3100, bill_days=31,
    bill_yearmonth=201801, bill_month=1, bill_adc=100.0),  # noqa
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 4))), bill_end='2018-01-16 00:00:00',
    bill_start='2018-01-10 00:00:00', fuel_type='ELECTRIC', consumption_scaled=750, bill_days=15,
    bill_yearmonth=201801, bill_month=1, bill_adc=50.0),  # noqa
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 1))), bill_end='2018-03-01 00:00:00',
    bill_start='2018-02-05 00:00:00', fuel_type='GAS', consumption_scaled=2350, bill_days=16,
    bill_yearmonth=201801, bill_month=1, bill_adc=146.875),  # noqa
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 2))), bill_end='2018-02-01 00:00:00',
    bill_start='2018-01-07 00:00:00', fuel_type='GAS', consumption_scaled=-3100, bill_days=31,
    bill_yearmonth=201801, bill_month=1, bill_adc=0.0),  # noqa
]

test_input_2 = [
    Row(postal_code='80123-0456', date_local=datetime.date(2018, 1, 3), hdd_avg=Decimal('17.52'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('47.48')),
    Row(postal_code='A1H 0A0', date_local=datetime.date(2018, 1, 4), hdd_avg=Decimal('15.91'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('49.09')),
    Row(postal_code='80123-0456', date_local=datetime.date(2018, 1, 5), hdd_avg=Decimal('0.00'),
        cdd_avg=Decimal('12.08'), temp_f_avg=Decimal('77.08')),
    Row(postal_code='80123-0456', date_local=datetime.date(2018, 1, 6), hdd_avg=Decimal('0.00'),
        cdd_avg=Decimal('12.75'), temp_f_avg=Decimal('77.75')),
    Row(postal_code='YYYYY', date_local=datetime.date(2018, 1, 7), hdd_avg=Decimal('12.97'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('52.03')),
    Row(postal_code='80123-0456', date_local=datetime.date(2018, 1, 8), hdd_avg=Decimal('46.42'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('18.58')),
    Row(postal_code='A1H 0A0', date_local=datetime.date(2018, 1, 9), hdd_avg=Decimal('40.28'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('24.72')),
    Row(postal_code='A1H 0A0', date_local=datetime.date(2018, 1, 10), hdd_avg=Decimal('24.91'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('40.09')),
    Row(postal_code='A1H 0A0', date_local=datetime.date(2018, 1, 11), hdd_avg=Decimal('25.52'),
        cdd_avg=Decimal('0.00'), temp_f_avg=Decimal('39.48')),
    Row(postal_code='XXXXX', date_local=datetime.date(2018, 1, 12), hdd_avg=Decimal('4.71'),
        cdd_avg=Decimal('0.13'), temp_f_avg=Decimal('60.41'))
    ]

test_input_3 = [
    Row(postal_code='A1H 0A0', recorded_date='2018-01-09', avg_temp_fahrenheit='75.8',
        hdd=0.0, cdd=10.799999999999997),
    Row(postal_code='A1H 0A0', recorded_date='2018-01-29', avg_temp_fahrenheit='51.6',
        hdd=13.399999999999999, cdd=0.0),
    Row(postal_code='80123', recorded_date='2018-01-09', avg_temp_fahrenheit='83.4',
        hdd=0.0, cdd=18.400000000000006),
    Row(postal_code='80123', recorded_date='2018-01-21', avg_temp_fahrenheit='63.8',
        hdd=1.2000000000000028, cdd=0.0),
    Row(postal_code='80123', recorded_date='2018-01-02', avg_temp_fahrenheit='80.5',
        hdd=0.0, cdd=15.5),
    Row(postal_code='80123', recorded_date='2018-01-18', avg_temp_fahrenheit='71.1',
        hdd=0.0, cdd=6.099999999999994),
    Row(postal_code='80123', recorded_date='2018-01-23', avg_temp_fahrenheit='78.1',
        hdd=0.0, cdd=13.099999999999994),
    Row(postal_code='80123', recorded_date='2019-01-24', avg_temp_fahrenheit='45.4',
        hdd=19.6, cdd=0.0),
    Row(postal_code='80123', recorded_date='2018-01-25', avg_temp_fahrenheit='28.4',
        hdd=36.6, cdd=0.0),
    Row(postal_code='YYYYY', recorded_date='2018-01-26', avg_temp_fahrenheit='75.3',
        hdd=0.0, cdd=10.299999999999997)
    ]

test_input_4 = [
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 1))), postal_code='80123-0456', iso_country_code='US'),
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 2))), postal_code='80123-0456', iso_country_code='US'),
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 3))), postal_code='80123-0456', iso_country_code='US'),
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 4))), postal_code='A1H 0A0', iso_country_code='CA'),
    Row(account_id=str(uuid.UUID(int=((111 << 64) + 5))), postal_code='A1H 0A0', iso_country_code='CA'),
    Row(account_id=str(uuid.UUID(int=((222 << 64) + 6))), postal_code='A1H 0A0', iso_country_code='CA')
    ]


@pytest.mark.usefixtures('spark_session', 'clean_session')
def test_join_transform_bills_weather(spark_session):

    # ARRANGE
    bills = spark_session.createDataFrame(test_input_1)
    weather = spark_session.createDataFrame(test_input_2)
    weather_historical = spark_session.createDataFrame(test_input_3)
    locations = spark_session.createDataFrame(test_input_4)

    test_class_instance = JoinBillsWeather()

    bills1, weather = test_class_instance.data_prep(bills, weather, weather_historical, locations)
    assert weather.count() > 0

    assert bills1.count() > 0
    bills2, bills_unique = test_class_instance.explode_date_range(bills1)
    bills2 = test_class_instance.extract_firxt_x_postal_digits(bills2)
    bills_unique = test_class_instance.extract_firxt_x_postal_digits(bills_unique)
    weather = test_class_instance.extract_firxt_x_postal_digits(weather)
    assert bills2.count() > 0

    bills3 = test_class_instance.join_bills_weather(bills2, bills_unique, weather)

    assert bills1.count() == bills3.count()

    assert 'postal_code' in bills3.columns
    assert 'bill_start' in bills3.columns
    assert 'bill_end' in bills3.columns
    assert 'mean_cdd' in bills3.columns
    assert 'mean_hdd' in bills3.columns
    assert 'account_id' in bills3.columns
    assert 'bill_adc' in bills3.columns
    assert 'bill_days' in bills3.columns
    assert 'bill_month' in bills3.columns
    assert 'bill_yearmonth' in bills3.columns
    assert 'consumption_scaled' in bills3.columns
    assert 'fuel_type' in bills3.columns
    assert bills3.count() > 0
