import concurrent.futures
import logging
import os
import sys
import time
from datetime import timedelta

from pyspark.sql.functions import (
    col,
    lit,
    when
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    FloatType,
    LongType,
    ShortType,
    StringType,
    StructType,
    StructField,
    TimestampType
)
from pyspark.sql.window import Window
import pyspark.sql.functions as fn

SEC_PER_HOUR = 3600
INITIAL_REPARTITION_SZ = 200
ROLL_UP_SCHEMA = StructType([
    StructField('channel_uuid', StringType(), False),
    StructField('direction', StringType(), False),
    StructField('date_utc', DateType(), False),
    StructField('hour_utc', ShortType(), False),
    StructField('time_zone', StringType(), True),
    StructField('min_seconds', ShortType(), True),
    StructField('max_seconds', ShortType(), True),
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
    StructField('date_utc', DateType(), False),
    StructField('success', BooleanType(), False),
    StructField('error', StringType(), True),
])

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)
log = logging.getLogger(__name__)


class Transform:

    def __init__(self, *args, **kwargs):
        self.tenant_id = int(kwargs['tenant_id'])
        self.lit_tenant_id = lit(self.tenant_id).cast(LongType())
        self.ami_prefix = kwargs['ami_prefix']
        self.out_prefix = kwargs['out_prefix']
        self.max_workers = int(kwargs.get('max_workers', '6'))
        self.out_partitions = int(kwargs.get('out_partitions_per_day', '9'))
        self.consumption_unit = kwargs.get('consumption_unit', 'KWH')

    def to_input_path(self, date_obj):
        """
        Given collect'ed metadata rows with tenant_id and year/month/day return all the
        input AMI paths that have to be passed to spark.read.parquet.
        """
        return os.path.join(
            self.ami_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'year={}'.format(date_obj.year),
            'month={}'.format(date_obj.month),
            'day={}'.format(date_obj.day)) \
            + '/'

    def load_tenant_dates(self, spark, in_ami_meta_name):
        """
        Return list of datetime.date objects of days to be processed
        """
        out_dates = []
        in_dates = spark.table(in_ami_meta_name) \
            .select(['tenant_id', 'date_utc']) \
            .where(col('tenant_id') == self.lit_tenant_id) \
            .dropDuplicates() \
            .orderBy('date_utc') \
            .collect()
        # Loop from the earliest to the last date and fill in all days between, avoiding gaps
        if len(in_dates) > 0:
            end_date = in_dates[-1].date_utc
            cur_date = in_dates[0].date_utc
            while cur_date <= end_date:
                out_dates.append(cur_date)
                cur_date = cur_date + timedelta(days=1)
        return out_dates

    def de_dup_common_ami(df_ami_day):
        """
        Take only the most recent reading for every channel/timestamp within single day dataframe
        """
        return df_ami_day \
            .orderBy(
                col('channel_uuid'),
                col('interval_start_utc'),
                col('updated_time_utc').desc()
            ) \
            .dropDuplicates([
                'channel_uuid',
                'interval_start_utc'
            ])

    def roll_up_ami_by_hour(self, df_a):
        """
        Given transformed AMI data as input, roll up AMI readings into hours.
        One row per location per day where there is data.  Will have potential
        gaps where AMI data is missing for 1 or more hours.
        """
        return df_a \
            .where(col('consumption_unit') == lit(self.consumption_unit)) \
            .select([
                'channel_uuid',
                'direction',
                'interval_start_utc',
                'time_zone',
                'seconds_per_interval',
                'consumption',
                'consumption_code'
            ]) \
            .withColumn('consumption_actual',
                        when(col('consumption_code') == lit(1), col('consumption'))
                        .otherwise(lit(None))) \
            .withColumn('consumption_estimated',
                        when(col('consumption_code') == lit(2), col('consumption'))
                        .otherwise(lit(None))) \
            .withColumn('consumption_other',
                        when(col('consumption_code') >= lit(3), col('consumption'))
                        .otherwise(lit(None))) \
            .withColumn('num_reads_missing',
                        when((col('consumption_code') <= lit(0)) |
                             col('consumption_code').isNull(),
                             lit(1))
                        .otherwise(lit(0))) \
            .drop('consumption', 'consumption_code') \
            .withColumn('date_utc', col('interval_start_utc').cast(DateType())) \
            .withColumn('hour_utc', fn.hour(col('interval_start_utc')).cast(ShortType())) \
            .groupBy([
                'channel_uuid',
                'direction',
                'date_utc',
                'hour_utc',
                'time_zone',
            ]) \
            .agg(
                fn.min(col('seconds_per_interval')).alias('min_seconds'),
                fn.max(col('seconds_per_interval')).alias('max_seconds'),
                fn.count(col('consumption_actual')).alias('num_reads_actual'),
                fn.sum(col('consumption_actual')).alias('consumption_actual'),
                fn.count(col('consumption_estimated')).alias('num_reads_estimated'),
                fn.sum(col('consumption_estimated')).alias('consumption_estimated'),
                fn.count(col('consumption_other')).alias('num_reads_other'),
                fn.sum(col('consumption_other')).alias('consumption_other'),
                fn.sum(col('num_reads_missing')).alias('num_reads_missing')
            ) \
            .withColumn('min_seconds', col('min_seconds').cast(ShortType())) \
            .withColumn('max_seconds', col('max_seconds').cast(ShortType())) \
            .withColumn('num_reads_actual', col('num_reads_actual').cast(ShortType())) \
            .withColumn('num_reads_estimated', col('num_reads_estimated').cast(ShortType())) \
            .withColumn('num_reads_other', col('num_reads_other').cast(ShortType())) \
            .withColumn('num_reads_missing', col('num_reads_missing').cast(ShortType())) \
            .withColumn('consumption_actual', col('consumption_actual').cast(FloatType())) \
            .withColumn('consumption_estimated', col('consumption_estimated').cast(FloatType())) \
            .withColumn('consumption_other', col('consumption_other').cast(FloatType()))

    def roll_up_to_final_schema(self, df_ami_hr_grp):
        return df_ami_hr_grp \
            .withColumn('num_reads_actual', fn.coalesce(
                col('num_reads_actual'), lit(0)).cast(ShortType())) \
            .withColumn('num_reads_estimated', fn.coalesce(
                col('num_reads_estimated'), lit(0)).cast(ShortType())) \
            .withColumn('num_reads_other', fn.coalesce(
                col('num_reads_other'), lit(0)).cast(ShortType())) \
            .withColumn('num_reads_total', col('num_reads_actual') +
                        col('num_reads_estimated') +
                        col('num_reads_other')) \
            .withColumn('consumption_total', (when(
                col('num_reads_total') > 0,
                fn.coalesce(col('consumption_actual'), lit(0)) +
                fn.coalesce(col('consumption_estimated'), lit(0)) +
                fn.coalesce(col('consumption_other'), lit(0)))
                .otherwise(lit(None))).cast(FloatType())
            ) \
            .select(ROLL_UP_SCHEMA.fieldNames())

    def roll_up_and_save_date(self, spark, date_obj):
        """
        Do rollup and write to parquet for a single date
        """
        start_t = time.time()
        date_path = self.to_input_path(date_obj)
        log.info('BEGIN_ROLLUP_AND_SAVE date=%s in=%s', date_obj, date_path)
        df_ami_day = spark.read \
            .option("mergeSchema", "true") \
            .parquet(date_path) \
            .withColumn('updated_time_utc', fn.coalesce(
                col('updated_time_utc'),
                lit('1970-01-01T00:00').cast(TimestampType())))

        # Add row_number window function per channel timestamp to use for de-duping
        window = Window.partitionBy(
            col('channel_uuid'),
            col('interval_start_utc')
        ).orderBy(
            col('updated_time_utc').desc()
        )
        df_ami_dedup = df_ami_day \
            .withColumn('rank_num', fn.row_number().over(window)) \
            .where(col('rank_num') == lit(1))

        # Roll up into (possibly sparse) hours
        df_ami_hr_grp = self.roll_up_ami_by_hour(df_ami_dedup)

        # Left join to fill in rollup hour consumptions for those dates where available,
        # null consumption where orig AMI is missing or incomplete.  Determine number of
        # reads and percent complete for each hour for each channel.
        df_day_rolled = self.roll_up_to_final_schema(df_ami_hr_grp)
        df_day_rolled = df_day_rolled.dropDuplicates()

        out_path = os.path.join(
            self.out_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'year={}'.format(date_obj.year),
            'month={}'.format(date_obj.month),
            'day={}'.format(date_obj.day)) + '/'
        spark.createDataFrame(df_day_rolled.rdd, ROLL_UP_SCHEMA) \
            .repartition(self.out_partitions, 'channel_uuid') \
            .write \
            .parquet(out_path, mode='overwrite')
        # Free up dataframes for day
        for df in [df_day_rolled, df_ami_hr_grp, df_ami_dedup, df_ami_day]:
            df.unpersist()
        secs = int(time.time() - start_t)
        log.info('END_ROLLUP_AND_SAVE secs=%s date=%s out=%s', secs, date_obj, out_path)
        return True

    def run_parallel(self, spark, tenant_dates):
        """
        Use Thread Pool to execute set of dates rollup/saves in parallel
        """
        out_meta_rows = []
        err_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {
                executor.submit(self.roll_up_and_save_date, spark,
                                date_obj): date_obj for date_obj in tenant_dates
            }
            for future in concurrent.futures.as_completed(future_to_date):
                date_utc = future_to_date[future]
                try:
                    success = future.result()
                    log.debug('FUTURE_COMPLETE date_utc=%s', date_utc)
                    out_meta_rows.append((date_utc, success, None))
                except Exception as e:
                    log.error('FUTURE_FAILED date_utc=%s error=%s', date_utc, str(e))
                    log.exception(e)
                    out_meta_rows.append((date_utc, False, str(e)))
                    err_count += 1
        return out_meta_rows, err_count

    def run(self, spark):
        """
        Main Task method entry point
        """
        # Set of distinct dates to process for Today AMI Ingest
        tenant_dates = self.load_tenant_dates(spark, 'ami_meta')

        if len(tenant_dates) > 0:
            log.info('Processing %s dates from %s to %s', len(tenant_dates),
                     tenant_dates[0], tenant_dates[-1])
            out_meta_rows, err_count = self.run_parallel(spark, tenant_dates)
        else:
            log.info('Not Processing %s dates', len(tenant_dates))
            out_meta_rows = []
            err_count = 0

        # Output metadata rows
        df_out = spark.createDataFrame(out_meta_rows, OUT_META_SCHEMA)
        log.info('FINISHED date_count=%s err_count=%s', len(out_meta_rows), err_count)
        return df_out.coalesce(1).createOrReplaceTempView('output')
