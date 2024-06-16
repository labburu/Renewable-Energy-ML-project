import logging
import os
import sys
import time
from datetime import timedelta

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StructType,
    StructField,
    FloatType,
    LongType,
    StringType,
    ShortType,
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
)
import pyspark.sql.functions as fn


logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# Constants
ONE_DAY = timedelta(days=1)
SEC_PER_HOUR = 3600

# TODO: There are not exactly 24 hours in a *local* day during daylight/standard transitions...
SEC_PER_DAY = SEC_PER_HOUR * 24

ROLL_UP_SCHEMA = StructType([
    StructField('channel_uuid', StringType(), False),
    StructField('direction', StringType(), False),
    StructField('date_local', DateType(), False),
    StructField('time_zone', StringType(), True),
    StructField('pct_complete', FloatType(), False),
    StructField('complete_seconds', IntegerType(), True),
    StructField('num_reads_actual', ShortType(), True),
    StructField('num_reads_estimated', ShortType(), True),
    StructField('num_reads_other', ShortType(), True),
    StructField('num_reads_missing', ShortType(), True),
    StructField('num_reads_total', ShortType(), True),
    StructField('consumption_actual', FloatType(), True),
    StructField('consumption_estimated', FloatType(), True),
    StructField('consumption_other', FloatType(), True),
    StructField('consumption_total', FloatType(), True),
])
OUT_META_SCHEMA = StructType([
    StructField('date_local', DateType(), False),
    StructField('count', IntegerType(), True),
    StructField('error', StringType(), True),
])


class Transform:

    def __init__(self, *args, **kwargs):
        self.tenant_id = int(kwargs['tenant_id'])
        self.lit_tenant_id = lit(self.tenant_id).cast(LongType())
        self.hourly_prefix = kwargs['hourly_prefix']
        self.out_prefix = kwargs['out_prefix']
        self.out_partitions = int(kwargs.get('out_partitions_per_day', '12'))

    def to_hourly_path(self, date_obj):
        """
        Return input s3a://... path of hourly data for a given date
        """
        return os.path.join(
            self.hourly_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'year={}'.format(date_obj.year),
            'month={}'.format(date_obj.month),
            'day={}'.format(date_obj.day)
        ) + '/'

    def to_daily_path(self, date_local):
        """
        Return output s3a://... path of daily data to save for given local date
        """
        return os.path.join(
            self.out_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'local_year={}'.format(date_local.year),
            'local_month={}'.format(date_local.month),
            'local_day={}'.format(date_local.day)
        ) + '/'

    def can_read_path(self, spark, path):
        can_open = False
        try:
            df_chk = spark.read.parquet(path)
            df_chk.unpersist()
            can_open = True
        except Exception as e:
            log.debug('can_open_path failed path=%s: %s', path, str(e))
        return can_open

    def build_paths_to_load(self, spark, date_range):
        """
        Read files starting 1 day before, ending 1 day after to be sure to get local
        timezone start-of-day for first day and end-of-day for local timezone last day
        """
        h_paths = []
        skip_cnt = 0

        # Attempt to extend the end date one day prior and start date one day after
        end_date_utc = date_range.max_date_utc + ONE_DAY
        cur_date_utc = date_range.min_date_utc - ONE_DAY
        while cur_date_utc <= end_date_utc:
            hourly_path = self.to_hourly_path(cur_date_utc)
            if self.can_read_path(spark, hourly_path):
                h_paths.append(self.to_hourly_path(cur_date_utc))
            else:
                log.warning('Skipping unreadable path=%s', hourly_path)
                skip_cnt += 1
            cur_date_utc = cur_date_utc + ONE_DAY
        log.info('Reading %s paths from %s to %s', len(h_paths), h_paths[0], h_paths[-1])
        if skip_cnt > 0:
            log.warning('Skipped %s unreadable paths', skip_cnt)
        return h_paths

    def load_all_hourly_rollups(self, spark, h_paths):
        """
        Given list of s3a://... input paths to load, load into a single huge dataframe
        and add/update columns to have:
        |-- interval_start_utc: timestamp - start time of hour UTC
        |-- interval_start_local: timestamp - above timestamp in local time
        |-- date_local: date - local date at start of interval
        """
        df_h = spark.read.parquet(*h_paths) \
            .withColumn('interval_start_utc', fn.to_timestamp(fn.concat(
                col('date_utc'),
                lit(' '),
                fn.lpad(col('hour_utc'), 2, '0'),
                lit(':00:00')
            ), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('interval_start_local', fn.from_utc_timestamp(
                col('interval_start_utc'),
                col('time_zone')
            )) \
            .withColumn('date_local', fn.to_date('interval_start_local')) \
            .withColumn('num_secs_in_hr', col('num_reads_total') * col('max_seconds')) \
            .withColumn('is_complete',
                        (col('num_secs_in_hr') == lit(SEC_PER_HOUR)) &
                        (col('min_seconds') == col('max_seconds'))) \
            .withColumn('num_secs',
                        (when(col('is_complete'), col('num_secs_in_hr'))
                         .otherwise(lit(None))).cast(IntegerType()))
        log.info('LOAD_ALL_HOURLY_ROLLUPS ok %s paths', len(h_paths))
        return df_h

    def roll_up_to_local_date(self, df_h):
        """Roll up to *local* date level"""
        df_grouped = df_h \
            .groupBy('channel_uuid', 'direction', 'time_zone', 'date_local') \
            .agg(
                fn.sum(col('num_secs')).alias('complete_seconds'),
                fn.sum(col('num_reads_actual')).alias('num_reads_actual'),
                fn.sum(col('num_reads_estimated')).alias('num_reads_estimated'),
                fn.sum(col('num_reads_other')).alias('num_reads_other'),
                fn.sum(col('num_reads_missing')).alias('num_reads_missing'),
                fn.sum(col('num_reads_total')).alias('num_reads_total'),
                fn.sum(col('consumption_actual')).alias('consumption_actual'),
                fn.sum(col('consumption_estimated')).alias('consumption_estimated'),
                fn.sum(col('consumption_other')).alias('consumption_other'),
                fn.sum(col('consumption_total')).alias('consumption_total'),
            ) \
            .withColumn('pct_complete', (
                when(col('complete_seconds').isNull(), lit(0.0))
                .otherwise(100.0 * (col('complete_seconds') / lit(SEC_PER_DAY).cast(FloatType())))
            ).cast(FloatType())) \
            .withColumn('num_reads_actual', col('num_reads_actual').cast(ShortType())) \
            .withColumn('num_reads_estimated', col('num_reads_estimated').cast(ShortType())) \
            .withColumn('num_reads_other', col('num_reads_other').cast(ShortType())) \
            .withColumn('num_reads_missing', col('num_reads_missing').cast(ShortType())) \
            .withColumn('num_reads_total', col('num_reads_total').cast(ShortType())) \
            .withColumn('consumption_actual', col('consumption_actual').cast(FloatType())) \
            .withColumn('consumption_estimated', col('consumption_estimated').cast(FloatType())) \
            .withColumn('consumption_other', col('consumption_other').cast(FloatType())) \
            .withColumn('consumption_total', col('consumption_total').cast(FloatType())) \
            .select(ROLL_UP_SCHEMA.fieldNames()) \
            .repartition(200, 'date_local')
        log.info('ROLL_UP_TO_LOCAL_DATE ok grouped by chan/dir/tz/date_local repartitioned by date_local')
        return df_grouped

    def save_for_date_local(self, df_grouped, date_local):
        """
        Given *all* the grouped data, extract only the slice for given local date
        and save that to its partition folder, returning count of rows saved.
        """
        start_t = time.time()
        out_path = self.to_daily_path(date_local)
        log.info('START_SAVE_FOR_DATE_LOCAL date=%s', date_local)
        df_save = df_grouped \
            .where(col('date_local') == lit(date_local)) \
            .repartition(self.out_partitions) \
            .persist()

        df_save.write.parquet(out_path, mode='overwrite')
        save_count = int(df_save.count())
        df_save.unpersist()
        secs = int(time.time() - start_t)
        log.info('END_SAVE_FOR_DATE_LOCAL secs=%s date=%s count=%s out=%s',
                 secs, date_local, save_count, out_path)
        return save_count

    def run_sequential(self, local_range, df_grouped):
        """Write out each affected local date in sequence to try to not blow the heap"""
        out_meta_rows = []
        max_date_local = local_range.max_date_local
        date_local = local_range.min_date_local
        while date_local <= max_date_local:
            save_count = self.save_for_date_local(df_grouped, date_local)
            out_meta_rows.append((date_local, save_count, None))
            date_local = date_local + ONE_DAY
        return out_meta_rows

    def run(self, spark):
        utc_range = spark.table('hourly_meta') \
            .agg(
                fn.min(col('date_utc')).alias('min_date_utc'),
                fn.max(col('date_utc')).alias('max_date_utc')
            ) \
            .collect()[0]
        log.info('Input dates utc start=%s end=%s', utc_range.min_date_utc, utc_range.max_date_utc)

        # Handle case where input metadata date range is empty
        if utc_range.min_date_utc is None or utc_range.max_date_utc is None:
            log.info('FINISHED_EMPTY date_count=0')
            return spark.createDataFrame([], OUT_META_SCHEMA).createOrReplaceTempView('output')

        # All the hourly rollups for our date ranges with calculation for date_local
        # based on account time zone
        h_paths = self.build_paths_to_load(spark, utc_range)
        df_h = self.load_all_hourly_rollups(spark, h_paths)

        # Determine the minimum/maximum local date from the hourly rollups
        local_range = df_h \
            .where(
                (col('date_utc') == lit(utc_range.min_date_utc)) |
                (col('date_utc') == lit(utc_range.max_date_utc))
            ) \
            .agg(
                fn.min(col('date_local')).alias('min_date_local'),
                fn.max(col('date_local')).alias('max_date_local')
            ) \
            .collect()[0]
        log.info('Input dates LOCAL start=%s end=%s', local_range.min_date_local, local_range.max_date_local)

        # Roll up to grouping by local date then persist to avoid recalculating every date
        df_grouped = self.roll_up_to_local_date(df_h).persist()

        # Save each local date to appropriate partition folder
        out_meta_rows = self.run_sequential(local_range, df_grouped)
        df_out = spark.createDataFrame(out_meta_rows, OUT_META_SCHEMA)

        # Output metadata rows
        log.info('FINISHED date_count=%s', df_out.count())
        return df_out.coalesce(1).createOrReplaceTempView('output')
