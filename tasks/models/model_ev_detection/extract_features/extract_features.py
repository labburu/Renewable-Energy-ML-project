import logging
from datetime import datetime, timedelta

from pyspark.sql.functions import (
    col,
    count,
    dayofweek,
    kurtosis,
    lag,
    lit,
    max,
    mean,
    min,
    skewness,
    sum,
    weekofyear,
    when,
    year,
    month,
    concat,
    isnan
)
from pyspark.sql.window import Window
from operator import or_
from functools import reduce
from pyspark.sql.types import FloatType, IntegerType, TimestampType, StringType
from nydus.task import NydusTask
from nydus.utilities import filter_after

log = logging.getLogger(__name__)

not_found_sentinel = -9999

completeness_low_threshold = 0.6
completeness_high_threshold = 1.0
included_months = [3, 4, 5, 9, 10, 11]   # spring + fall
included_days_of_week = [2, 3, 4, 5, 6]  # Mon-Fri

default_history_weight = 0.5
ev_not_detected_week_count_fallback = 0.5

# Values from Mo's original model work (from Pecan Street data, I believe)
initial_threshold = {
    3600: 3,
    1800: .8,
    900: .4
}

minimum_length = {
    3600: 2,
    1800: 2,
    900: 3
}

end_percent_change = -.2

# Maximum 'box' duration, basically the longest possible time we'd expect an EV to charge for (in seconds).
max_window_duration = 14400


class ExtractFeatures(NydusTask):
    def __init__(self, **kwargs):
        # This is populated by Airflow's '{{ next_ds }}` macro in the DAG
        evaluation_date = kwargs.get('evaluation_date')
        if evaluation_date and evaluation_date != 'None':
            date_format = kwargs.get('date_format', '%Y-%m-%d')
            self.evaluation_date = datetime.strptime(evaluation_date, date_format)
        else:
            self.evaluation_date = datetime.now()

        # If None, evaluates all history before evaluation_date
        self.evaluate_previous_days = kwargs.get('evaluate_previous_days', None)

        self.tenant_id = kwargs.get('tenant_id', None)

        self.hourly_rollup_list = kwargs.get('hourly_rollup_list')

        if self.tenant_id in self.hourly_rollup_list:
            self.intervals = [60]
        else:
            self.intervals = kwargs.get('model_interval_minutes_lst', [15, 30, 60])

        if self.intervals is not None:
            self.intervals = [i * 60 for i in self.intervals]

    def get_complete_locations(self, df_ami):
        df_ami = df_ami \
            .dropDuplicates(['channel_id', 'seconds_per_interval', 'interval_start_utc'])

        df_location_filtered = df_ami \
            .groupBy('channel_id', 'seconds_per_interval') \
            .agg(
                count('*').alias('existing_interval_count'),
                min('interval_start_utc').alias('timestamp_min'),
                max('interval_start_utc').alias('timestamp_max')
            ) \
            .withColumn('expected_interval_count',
                        (col('timestamp_max').cast(IntegerType()) - col('timestamp_min').cast(IntegerType())) / col(
                            'seconds_per_interval') + 1) \
            .withColumn('percent_complete', col('existing_interval_count') / col('expected_interval_count')) \
            .where(col('percent_complete') <= completeness_high_threshold) \
            .where(col('percent_complete') > completeness_low_threshold) \
            .select(
                [
                    'channel_id',
                    'seconds_per_interval'
                ]
            )

        df_ami = df_ami \
            .join(
                df_location_filtered,
                on=['channel_id', 'seconds_per_interval'],
                how='inner'
            )

        return df_ami

    def transform_ami_common(self, df_ami):
        df_ami = df_ami \
            .select(
                [
                    'tenant_id',
                    'consumption_code',
                    'consumption',
                    'channel_uuid',
                    'seconds_per_interval',
                    'interval_start_utc',
                    'year',
                    'month'
                ]
            ) \
            .withColumnRenamed('channel_uuid', 'channel_id') \
            .filter(col('tenant_id') == self.tenant_id) \
            .filter(col('consumption_code') == 1) \
            .filter(col('seconds_per_interval').isin(self.intervals)) \
            .drop('tenant_id')

        return df_ami

    def transform_hourly_rollup_ami(self, df_ami):
        df_ami = df_ami \
            .select(
                [
                    'channel_uuid',
                    'tenant_id',
                    'date_utc',
                    'hour_utc',
                    'consumption_actual',
                    'day'
                ]
            ) \
            .filter(col('tenant_id') == self.tenant_id) \
            .withColumnRenamed('channel_uuid', 'channel_id') \
            .withColumnRenamed('consumption_actual', 'consumption') \
            .withColumn('interval_start_utc', concat(col('date_utc').cast(StringType()),
                                                     lit(" "), col('hour_utc').cast(StringType()),
                                                     lit(":00:00")).cast(TimestampType())) \
            .withColumn('seconds_per_interval', lit(3600)) \
            .withColumn('month', month(col('interval_start_utc'))) \
            .withColumn('year', year(col('interval_start_utc'))) \
            .drop('hour_utc', 'tenant_id')
        return df_ami

    def load_filter_transform(self, df_ami):
        """
        Read in all ami common data,
            1. filter down to tenant
            2. filter down to one year of data.
            3. filter down to months of interest (shoulder months).
            4. filter down to business weekdays.
            5. de-dupe as common contains duplicates.
            6. filter to locations with enough data.
        :return df: location and time filtered dataframe
        :rtype DataFrame (pyspark.sql)

        """
        if self.tenant_id in self.hourly_rollup_list:
            df_ami = self.transform_hourly_rollup_ami(df_ami)
        else:
            df_ami = self.transform_ami_common(df_ami)

        # goes back to a set history to extract features from
        if self.evaluate_previous_days is not None:
            window_start = self.evaluation_date - timedelta(days=self.evaluate_previous_days)
            df_ami = filter_after(df_ami, window_start)

        df_ami = self.get_complete_locations(df_ami)

        # narrows to shoulder months, business days
        df_ami = df_ami \
            .where(col('month').isin(included_months)) \
            .withColumn('day_of_week', dayofweek('interval_start_utc')) \
            .withColumn('week_of_year', weekofyear('interval_start_utc')) \
            .where(col('day_of_week').isin(included_days_of_week)) \
            .select(
                [
                    'channel_id',
                    'seconds_per_interval',
                    'interval_start_utc',
                    'year',
                    'week_of_year',
                    'consumption'
                ]
            )

        return df_ami

    def identify_boxes_in_week(self, df_ami):
        """
        Identify boxes in consumption patterns based on configurations and isolate those boxes for stats.
        Do this by:
            1. identifying all box starts marked by threshold jumps.
            2. narrow to last possible ending with a max box length.
            3. find box ends via threshold falls.
            4. narrow data to only boxes.

        :rtype DataFrame (pyspark.sql)

        """
        # partition by keys for efficiency && allow for the lagging
        window_box_starts = Window \
            .partitionBy([
                col('channel_id'),
                col('seconds_per_interval'),
                col('year'),
                col('week_of_year')
            ]) \
            .orderBy(col('interval_start_utc'))

        df_box_starts = df_ami \
            .withColumn(
                'box_start_consumption_difference',
                col('consumption') - lag('consumption', count=1).over(window_box_starts)
            ) \
            .filter(reduce(or_, (
                (col('seconds_per_interval') == x) &
                (col('box_start_consumption_difference') > initial_threshold[x]) for x in self.intervals))
            ) \
            .withColumnRenamed('interval_start_utc', 'box_start_time') \
            .withColumnRenamed('consumption', 'box_start_consumption') \
            .select(
                [
                    'channel_id',
                    'seconds_per_interval',
                    'year',
                    'week_of_year',
                    'box_start_time',
                    'box_start_consumption'
                ]
            )

        # join box starts into ami data, grab all data between boxes with the max_window duration assumption
        window_box_ends = Window.partitionBy(
            ['channel_id', 'seconds_per_interval', 'year', 'week_of_year', 'box_start_time']) \
            .orderBy('interval_start_utc') \
            .rowsBetween(Window.unboundedPreceding, 0)

        df_ami = df_ami.toDF(*df_ami.columns) \
            .join(df_box_starts, on=['channel_id', 'seconds_per_interval', 'year', 'week_of_year'], how='inner') \
            .filter(col('interval_start_utc') >= col('box_start_time')) \
            .filter(
                col('interval_start_utc').cast(IntegerType()) <
                col('box_start_time').cast(IntegerType()) + lit(max_window_duration)
            ) \
            .withColumn('end_box_flag',
                        when((col('consumption') / col('box_start_consumption')) - lit(1) < end_percent_change, 1)
                        .otherwise(0)) \
            .withColumn('end_box_flag', max(col('end_box_flag')).over(window_box_ends)) \
            .filter(col('end_box_flag') == 0) \
            .drop('end_box_flag')

        # summary statistics for the boxes
        df_ami = df_ami \
            .groupBy(
                [
                    'channel_id',
                    'seconds_per_interval',
                    'year',
                    'week_of_year',
                    'box_start_time'
                ]
            ) \
            .agg(
                count('*').alias('box_reading_count'),
                max(col('interval_start_utc')).alias('last_reading'),
                sum('consumption').alias('box_total_consumption')
            ) \
            .filter(reduce(or_, (
                (col('seconds_per_interval') == x) &
                (col('box_reading_count') > minimum_length[x]) for x in self.intervals))
            ) \
            .withColumn(
                'box_interval_length',
                col('last_reading').cast(IntegerType()) - col('box_start_time').cast(IntegerType())
            ) \
            .groupBy(
                [
                    'channel_id',
                    'seconds_per_interval',
                    'year',
                    'week_of_year'
                ]
            ) \
            .agg(
                (mean('box_interval_length').cast(FloatType()) /
                 col('seconds_per_interval')).alias('week_mean_duration'),
                mean('box_total_consumption').alias('week_mean_consumption'),
                count('*').alias('week_box_count')
            )

        return df_ami

    def extract_features(self, df_filtered):
        """
        Create all ami features for EV detection model:
            1. create weekly features
            2. create box features
        :rtype DataFrame (pyspark.sql)
        """

        location_group_columns = [
            'channel_id',
            'seconds_per_interval'
        ]

        location_week_group_columns = [
            'channel_id',
            'seconds_per_interval',
            'year',
            'week_of_year'
        ]

        df_weekly_distribution_stats = df_filtered \
            .groupBy(location_week_group_columns) \
            .agg(
                kurtosis('consumption').alias('kurtosis'),
                skewness('consumption').alias('skewness'),
                max('consumption').alias('peak'),
                min('interval_start_utc').alias('week_start_timestamp')
            ) \
            .withColumn('kurtosis', when(isnan(col('kurtosis')), None).otherwise(col('kurtosis'))) \
            .withColumn('skewness', when(isnan(col('skewness')), None).otherwise(col('skewness')))

        # weekly box stats
        df_boxes = self.identify_boxes_in_week(df_filtered)

        # summary stats on everyone
        df_merged = df_weekly_distribution_stats.toDF(*df_weekly_distribution_stats.columns) \
            .join(
                df_boxes,
                on=location_week_group_columns,
                how='left'
            ) \
            .filter(col('week_box_count').isNotNull())
        # this is to match the original code ^, all zeroed weeks are dropped.
        # if we want people without boxes / weeks without boxes in our final output,
        # we need to remove this and then modify the agg stats below depending on how to calc features.

        df_merged = df_merged\
            .groupBy(location_group_columns) \
            .agg(
                mean('week_mean_duration').alias('mean_duration'),
                mean('week_mean_consumption').alias('mean_consumption'),
                mean('week_box_count').alias('mean_box_count'),
                mean('kurtosis').alias('mean_kurtosis'),
                mean('skewness').alias('mean_skewness'),
                mean('peak').alias('mean_peak'),
                min('week_start_timestamp').alias('earliest_interval_start_utc'),
                sum(when(col('week_box_count').isNotNull(), 1).otherwise(0)).alias('ev_detected_week_count'),
                count('*').alias('week_count')
            ) \
            .withColumn(
                'detection_frequency',
                when(
                    col('week_count') == col('ev_detected_week_count'),
                    col('ev_detected_week_count') / ev_not_detected_week_count_fallback
                )
                .otherwise(col('ev_detected_week_count') / (col('week_count') - col('ev_detected_week_count')))
            ) \
            .withColumn(
                'history_weight',
                when(
                    year('earliest_interval_start_utc') == self.evaluation_date.year,
                    default_history_weight
                )
                .otherwise(self.evaluation_date.year - year('earliest_interval_start_utc'))
            ) \
            .withColumn('heuristic_score', col('detection_frequency') / col('history_weight')) \
            .withColumn('interval_minutes', col('seconds_per_interval') / 60) \
            .select(
                'channel_id',
                'interval_minutes',
                'mean_duration',
                'mean_consumption',
                'mean_box_count',
                'mean_kurtosis',
                'mean_skewness',
                'mean_peak',
                'heuristic_score'
            )

        return df_merged

    def run(self, spark):
        df_results = spark.table('ami')
        df_results = self.load_filter_transform(df_results)
        df_results = self.extract_features(df_results)
        df_results = df_results.withColumn('tenant_id', lit(self.tenant_id))
        df_results.createOrReplaceTempView('output')
