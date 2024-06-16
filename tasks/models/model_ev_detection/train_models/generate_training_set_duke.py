from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

# TODO: this is just for linting, delete when using notebook
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

df_ami = spark.read.parquet('s3a://tendril-meter-data-prod/hourly/tenant_id=11')
df_ami = df_ami.withColumn('tenant_id', lit(11))

df_ami = df_ami.withColumnRenamed('channel_uuid', 'channel_id')

channels = spark.read.parquet(
    's3a://tendril-airflow-prod/dags/soa_daily/transform_channels'
    '/2019-10-01/soa_daily__transform_channels__20191001.parquet')

channels = channels \
    .withColumnRenamed('id', 'channel_id') \
    .select('channel_id', 'location_id')

df_ami = df_ami.join(channels, 'channel_id', how='inner')

df = spark.read.parquet(
    's3a://tendril-airflow-prod/dags/tendril/join_location_profiles_to_matched_experian'
    '/2019-09-10/tendril__join_location_profiles_to_matched_experian__20190910.parquet')
df = df.select('tenant_id', 'electric_vehicle', 'location_id', 'match_quality', 'solar_electric')
df = df.filter(df.tenant_id == 11)
df_ewh = df.filter(df.electric_vehicle == 'ELECTRIC_VEHICLE')
df_ewh = df_ewh.filter(df_ewh.solar_electric is False)
df_ewh = df_ewh.limit(9000)

df_gwh = df.filter(df.electric_vehicle == 'OTHER')
df_gwh = df_gwh.limit(9000)

df_ewh = df_ewh.select('location_id').collect()
df_gwh = df_gwh.select('location_id').collect()
# location_ids_ewh = [r['location_id'] for r in df_ewh]


df_ami_locations_ewh = df_ami.where(df_ami.location_id.isin([r['location_id'] for r in df_ewh]))
df_ami_locations_gwh = df_ami.where(df_ami.location_id.isin([r['location_id'] for r in df_gwh]))

df = df_ami_locations_ewh
df = df.drop(*['direction', 'time_zone', 'max_seconds', 'min_seconds',
               'num_reads_total', 'num_reads_estimated', 'num_reads_other', 'num_reads_missing',
               'num_reads_actual', 'consumption_estimated', 'consumption_other', 'consumption_total'])

df.write.parquet('s3a://tendril-ppd-prod/mo/ami/bulk/testing')


def save_ami(df_ami, has_wh=True):
    """Save AMI data with particular partitioning scheme."""
    df_ami \
        .orderBy(['location_id', 'date_utc', 'hour_utc']) \
        .repartition(1) \
        .write \
        .partitionBy('location_id') \
        .csv('s3a://tendril-ppd-prod/mo/ami/bulk/has_wh={}/'.format(str(has_wh).lower()), mode='overwrite', header=True)


save_ami(df)
