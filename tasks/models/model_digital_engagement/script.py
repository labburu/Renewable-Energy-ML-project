from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    first,
    lit,
    sum,
    udf,
)

# Specify output columns
OUTPUT_COLUMNS = [
    'user_uuid',
    'challenge_engagement_level',
    'eher_engagement_level',
    'hba_engagement_level',
    'other_engagement_level',
    'global_engagement_level',
]

# Specify output schema
OUTPUT_SCHEMA = StructType([
        StructField('user_uuid', StringType(), False, metadata={'maxlength': 36}),
        StructField('challenge_engagement_level', StringType(), False, metadata={'maxlength': 6}),
        StructField('eher_engagement_level', StringType(), False, metadata={'maxlength': 6}),
        StructField('hba_engagement_level', StringType(), False, metadata={'maxlength': 6}),
        StructField('other_engagement_level', StringType(), False, metadata={'maxlength': 6}),
        StructField('global_engagement_level', StringType(), False, metadata={'maxlength': 6}),
    ])


# Function to generalize event tags
def generalize_event_tag(x):
    if 'eher' in x.lower():
        return 'EHER'
    elif 'hba' in x.lower():
        return 'HBA'
    elif 'challenge' in x.lower() or 'production' in x.lower():
        return 'CHALLENGE'
    else:
        return 'OTHER'


event_tag_udf = udf(generalize_event_tag, StringType())


# Function to return the engagment level based on the engagement score
def get_engagement_level(simplified_event_tag, unsub, combined_score):
    score_level_dict = {'HBA': (4, 1), 'GLOBAL': (80, 10), 'CHALLENGE': (19, 1), 'EHER': (11, 1), 'OTHER': (3, 1)}
    high, medium = score_level_dict[simplified_event_tag]
    if unsub > 0:
        return 'unsub'
    if combined_score == 0:
        return 'never'
    elif combined_score <= medium:
        return 'low'
    elif combined_score <= high:
        return 'medium'
    else:
        return 'high'


udf_engagement_level = udf(get_engagement_level, StringType())

# SparkSession already exists, so this is getting the existing one
spark = SparkSession.builder.getOrCreate()

df = spark.table('email_events')

# Get simplified event_tags
df = df.withColumn('simplified_event_tag', event_tag_udf('event_tag'))

# Pivot the dataframe to get counts of each event type for each product per user
df_pivot = df.filter(df.event_type.isin('OPEN', 'DELIVERED', 'CLICK', 'UNSUBSCRIBE')) \
    .groupBy('user_uuid', 'simplified_event_tag') \
    .pivot('event_type', ['OPEN', 'DELIVERED', 'CLICK', 'UNSUBSCRIBE']) \
    .agg(countDistinct(col('external_event_id')))

# Get a dataframe to calculate global score for users who are still subscribed to a product
df_sub = df_pivot \
    .filter(col('UNSUBSCRIBE').isNull()) \
    .groupBy('user_uuid') \
    .agg(sum(col('CLICK')).alias('CLICK'),
         sum(col('DELIVERED')).alias('DELIVERED'),
         sum(col('OPEN')).alias('OPEN')) \
    .withColumn('simplified_event_tag', lit('GLOBAL')) \
    .withColumn('UNSUBSCRIBE', lit(0)) \
    .select('user_uuid', 'simplified_event_tag', 'CLICK', 'DELIVERED', 'OPEN', 'UNSUBSCRIBE')

# Get a dataframe to calculate global score for users who have unsubscribed
df_unsub = df_pivot \
    .groupBy('user_uuid') \
    .agg(sum(col('UNSUBSCRIBE')).alias('UNSUBSCRIBE'),
         count(col('DELIVERED')).alias('DELIVERED')) \
    .filter(col('UNSUBSCRIBE') == col('DELIVERED')) \
    .withColumn('simplified_event_tag', lit('GLOBAL')) \
    .withColumn('CLICK', lit(None)) \
    .withColumn('DELIVERED', lit(None)) \
    .withColumn('OPEN', lit(None)) \
    .withColumn('UNSUBSCRIBE', lit(1)) \
    .select('user_uuid', 'simplified_event_tag', 'CLICK', 'DELIVERED', 'OPEN', 'UNSUBSCRIBE')

df_all = df_pivot.union(df_sub).union(df_unsub)

df_results = df_all \
    .fillna(0) \
    .withColumn('combined_score', col('OPEN') + 10*col('CLICK')) \
    .withColumn('engagement_level', udf_engagement_level(col('simplified_event_tag'),
                                                         col('UNSUBSCRIBE'),
                                                         col('combined_score')))

df_results = df_results \
    .select('user_uuid', 'simplified_event_tag', 'engagement_level') \
    .groupBy('user_uuid') \
    .pivot('simplified_event_tag', ['CHALLENGE', 'EHER', 'GLOBAL', 'HBA', 'OTHER']) \
    .agg(first(col('engagement_level'))) \
    .fillna('NA')

event_tags = ['CHALLENGE', 'EHER', 'GLOBAL', 'HBA', 'OTHER']
new_col_names = {'CHALLENGE': 'challenge_engagement_level',
                 'EHER': 'eher_engagement_level',
                 'GLOBAL': 'global_engagement_level',
                 'HBA': 'hba_engagement_level',
                 'OTHER': 'other_engagement_level'}

for event_tag in event_tags:
    if event_tag in df_results.schema.names:
        df_results = df_results.withColumnRenamed(event_tag, new_col_names[event_tag])
    else:
        df_results = df_results.withColumn(new_col_names[event_tag], lit('NA'))


# Create output DataFrame with explicit schema
df_output = spark.createDataFrame([], OUTPUT_SCHEMA)

# Select relevant columns into output schema, yo
df_output = df_output.union(df_results.select(OUTPUT_COLUMNS))
df_output.createOrReplaceTempView('output')
