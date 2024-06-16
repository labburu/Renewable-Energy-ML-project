from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# TODO: this is just for linting, delete when using notebook
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

df_gt = spark.read.csv('s3://tendril-us-prod-sftp/tenant/109/PEV Rate Riders - Active 09.23.2019.csv', header=True)

df_gt = df_gt.select('KY_BA CSS').withColumnRenamed('KY_BA CSS', 'ACCT_NUM')
print(df_gt.count())
df_gt.show()

df = spark.read.parquet('s3://tendril-meter-data-prod/hourly/tenant_id=109')


df_accounts = spark.read.parquet('s3://tendril-airflow-prod/dags/soa_daily/transform_accounts/2019-10-18/'
                                 'soa_daily__transform_accounts__20191018.parquet')
df_accounts = df_accounts.filter(df_accounts.tenant_id == 109)
df_accounts = df_accounts.join(df_gt, df_accounts.external_account_id == df_gt.ACCT_NUM, how='outer')

df_accounts = df_accounts.drop('external_account_id', 'energy_usage_econ_pref', 'energy_usage_environ_pref',
                               'is_benchmark_participant', 'created', 'updated', 'is_default', 'is_inactive',
                               'inactive_date')
df_accounts = df_accounts.withColumnRenamed('id', 'account_id')
df_accounts = df_accounts.where(col('ACCT_NUM').isNull()).limit(500)
df_accounts = df_accounts.drop('ACCT_NUM')
print(df_accounts.count())
df_accounts.show(1000)

df_locations = spark.read.parquet('s3://tendril-airflow-prod/dags/soa_daily/transform_locations'
                                  '/2019-09-30/soa_daily__transform_locations__20190930.parquet')
df_locations = df_locations.join(df_accounts, ['account_id', 'tenant_id'], how='inner')
df_locations = df_locations.drop('external_location_id', 'address', 'city', 'state', 'postal_code', 'time_zone',
                                 'iso_country_code', 'created', 'updated', 'is_default', 'account_id')
df_locations = df_locations.withColumnRenamed('id', 'location_id')

df_channels = spark.read.parquet('s3://tendril-airflow-prod/dags/soa_daily/transform_channels/'
                                 '2019-09-30/soa_daily__transform_channels__20190930.parquet')
df_channels = df_channels.join(df_locations, 'location_id', how='inner')
df_channels = df_channels.drop('type', 'direction', 'name', 'description', 'external_channel_id')
df_channels = df_channels.withColumnRenamed('id', 'channel_uuid')

df = df.join(df_channels, 'channel_uuid', how='inner')
df = df.drop('direction', 'time_zone', 'max_seconds', 'min_seconds', 'num_reads_total', 'num_reads_estimated',
             'num_reads_other', 'num_reads_missing', 'num_reads_actual', 'consumption_estimated', 'consumption_other',
             'consumption_total')


def save_ami(df_ami, has_ev=False):
    """Save AMI data with particular partitioning scheme."""
    df_ami \
        .orderBy(['location_id', 'date_utc', 'hour_utc']) \
        .repartition(1) \
        .write \
        .partitionBy('location_id') \
        .csv('s3a://tendril-ppd-prod/mo/ami/bulk_AP/has_ev={}/'.format(str(has_ev).lower()), mode='overwrite',
             header=True)


save_ami(df)
