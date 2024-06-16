"""Weather dag."""

from datetime import datetime

from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.macros.tendril import TendrilDAG
from airflow.operators.tendril import (
    NydusExtractOperator,
    NydusTransformOperator,
)

cron = Variable.get('cron', deserialize_json=True)

dag = TendrilDAG(
    'weather',
    description='Historical and forecasted weather. Runs nightly.',
    start_date=datetime(2018, 7, 17),
    default_args={
        'pagerduty_enabled': True,
        'pagerduty_severity': 'error',
        'pagerduty_connection_id': 'pagerduty-scipi-platform',
        'owner': 'scipi-platform',
    },
    max_active_runs=1,
    schedule_interval=cron['weather']
)
dag.doc_md = __doc__

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

extract_weather_company_api = NydusExtractOperator(
    task_id='extract_weather_company_api',
    task_path='weather/extract_weather_company_api',
    cluster_label='spark-airflow',
    dag=dag
)
extract_weather_company_api.doc_md = """
### Output Schema

    [
        ('postal_code', 'string'),
        ('date', 'date'),
        ('avg_temp', 'int'),
        ('day_hours', 'int'),
        ('cdd', 'int'),
        ('hdd', 'int')
    ]
"""
extract_weather_company_api.set_upstream(latest_only)


extract_historical_weather_hourly = NydusExtractOperator(
    task_id='extract_historical_weather_hourly',
    task_path='weather/extract_historical_weather_hourly',
    conn_id='tendril-qubole',
    cluster_label='spark-airflow',
    dag=dag
)
extract_historical_weather_hourly.doc_md = """
### Output Schema

    [
        ('PostalCode', 'string'),
        ('SiteId', 'string'),
        ('Latitude', 'string'),
        ('Longitude', 'string'),
        ('DateHrGmt', 'string'),
        ('DateHrLwt', 'string'),
        ('Elevation', 'string'),
        ('SurfaceTemperatureCelsius', 'string'),
        ('SurfaceTemperatureFahrenheit', 'string'),
        ('SurfaceDewpointTemperatureCelsius', 'string'),
        ('SurfaceDewpointTemperatureFahrenheit', 'string'),
        ('SurfaceWetBulbTemperatureCelsius', 'string'),
        ('SurfaceWetBulbTemperatureFahrenheit', 'string'),
        ('WindChillTemperatureCelsius', 'string'),
        ('WindChillTemperatureFahrenheit', 'string'),
        ('ApparentTemperatureCelsius', 'string'),
        ('ApparentTemperatureFahrenheit', 'string'),
        ('DirectNormalIrradiance', 'string'),
        ('DiffuseHorizontalRadiation', 'string'),
        ('DownwardSolarRadiation', 'string'),
        ('PrecipitationPreviousHourCentimeters', 'string'),
        ('PrecipitationPreviousHourInches', 'string'),
        ('SnowfallCentimeters', 'string'),
        ('SnowfallInches', 'string'),
        ('WindSpeedKph', 'string'),
        ('WindSpeedMph', 'string'),
        ('WindDirection', 'string'),
        ('SurfaceWindGustsKph', 'string'),
        ('SurfaceWindGustsMph', 'string'),
        ('CloudCoverage', 'string'),
        ('RelativeHumidity', 'string'),
        ('SurfaceAirPressureKilopascals', 'string'),
        ('SurfaceAirPressureMillibars', 'string'),
        ('MslPressure', 'string')
    ]
"""
extract_historical_weather_hourly.set_upstream(latest_only)


transform_historical_weather_hourly = NydusTransformOperator(
    task_id='transform_historical_weather_hourly',
    task_path='weather/transform_historical_weather_hourly',
    conn_id='tendril-qubole',
    cluster_label='spark-airflow',
    dag=dag
)
transform_historical_weather_hourly.doc_md = """
### Output Schema

    [
        ('postal_code', 'string'),
        ('site_id', 'string'),
        ('latitude', 'double'),
        ('longitude', 'double'),
        ('timestamp_utc', 'timestamp'),
        ('timestamp_local', 'timestamp'),
        ('elevation', 'double'),
        ('temp_c', 'double'),
        ('temp_f', 'double'),
        ('dew_point_c', 'double'),
        ('dew_point_f', 'double'),
        ('wet_bulb_c', 'double'),
        ('wet_bulb_f', 'double'),
        ('windchill_c', 'double'),
        ('windchill_f', 'double'),
        ('apparent_temp_c', 'double'),
        ('apparent_temp_f', 'double'),
        ('direct_normal_irradiance', 'int'),
        ('diffuse_horizontal_radiation', 'int'),
        ('downward_solar_radiation', 'int'),
        ('precipitation_cm', 'double'),
        ('precipitation_in', 'double'),
        ('snowfall_cm', 'double'),
        ('snowfall_in', 'double'),
        ('wind_speed_kph', 'double'),
        ('wind_speed_mph', 'double'),
        ('wind_direction', 'int'),
        ('surface_wind_gusts_kph', 'double'),
        ('surface_wind_gusts_mph', 'double'),
        ('cloud_coverage', 'int'),
        ('relative_humidity', 'int'),
        ('air_pressure_kpa', 'double'),
        ('air_pressure_mb', 'double'),
        ('pressure_msl', 'double'),
        ('date_utc', 'date'),
        ('date_local', 'date'),
        ('hdh', 'double'),
        ('cdh', 'double')
    ]

"""

transform_historical_weather_hourly.set_upstream([
    extract_historical_weather_hourly,
])

transform_historical_weather_daily = NydusTransformOperator(
    task_id='transform_historical_weather_daily',
    task_path='weather/transform_historical_weather_daily',
    conn_id='tendril-qubole',
    cluster_label='spark-airflow',
    dag=dag
)
transform_historical_weather_daily.doc_md = """
### Output Schema

    [
        ('postal_code', 'string'),
        ('date_local', 'date'),
        ('hdd_avg', 'decimal(5,2)'),
        ('cdd_avg', 'decimal(5,2)'),
        ('temp_f_avg', 'decimal(5,2)')
    ]
"""

transform_historical_weather_daily.set_upstream([
    transform_historical_weather_hourly,
])
