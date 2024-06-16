import concurrent.futures
import logging
import os
import sys
import time

from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
    lit,
    when
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    StructField
)
import pyspark.sql.functions as fn


SEC_PER_HOUR = 3600
SEC_PER_DAY = SEC_PER_HOUR * 24

# MDIS Schema definition - one row per channel per day.  Hourly data in a time series within row
# with one array element per hour with optional consumption if 100% complete, null if incomplete.
HOUR_NUMBERS = range(0, 24)
LOAD_SCHEMA_FIELDS = [
    StructField('tenant_id', LongType(), False),
    StructField('year', ShortType(), False),
    StructField('month', ShortType(), False),
    StructField('day', ShortType(), False),
    StructField('chan_part_num', ShortType(), False),
    StructField('channel_uuid', StringType(), False),
    StructField('time_zone', StringType(), False),
    StructField('date_utc', DateType(), False),
    StructField('pct_complete', FloatType(), False),
    StructField('latest_hour_utc', ShortType(), True),
]

# Only process hourly data with this direction
REQUIRED_DIRECTION = 'D'

# Add pivoted-out columns with hourly consumption (nullable if incomplete)
LOAD_SCHEMA_FIELDS.extend([
    StructField('kwh_h%02d' % h, FloatType(), True) for h in HOUR_NUMBERS
])
LOAD_SCHEMA = StructType(LOAD_SCHEMA_FIELDS)
TENANT_META_SCHEMA = StructType([
    StructField('date_utc', StringType(), False),
    StructField('load_folder', StringType(), False),
])
OUT_META_SCHEMA = StructType([
    StructField('date_utc', DateType(), False),
    StructField('success', BooleanType(), False),
    StructField('save_path', StringType(), True),
    StructField('error', StringType(), True),
])

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)
log = logging.getLogger(__name__)


class Load:
    """
    Load AMI Hourly MDIS
    """

    def __init__(self, *args, **kwargs):
        self.start_ts = int(time.time())
        self.hex_ts = '%x' % self.start_ts
        self.max_workers = int(kwargs.get('max_workers', '6'))
        self.tenant_id = kwargs['tenant_id']
        self.hourly_prefix = kwargs['hourly_prefix']
        self.out_prefix = kwargs['out_prefix']
        self.out_run_prefix = os.path.join(self.out_prefix, self.hex_ts)
        self.meta_path = kwargs['meta_path']
        self.num_channel_partitions = int(kwargs['num_channel_partitions'])

    def extract_tenant_dates(self, spark, in_hourly_meta_name):
        """
        Return Rows of: tenant_id, date_utc, year, month, day to be processed
        """
        df_dates = spark.table(in_hourly_meta_name) \
            .select(['date_utc']) \
            .withColumn('year', fn.year(col('date_utc'))) \
            .withColumn('month', fn.month(col('date_utc'))) \
            .withColumn('day', fn.dayofmonth(col('date_utc')))
        return df_dates.orderBy('date_utc').dropDuplicates().collect()

    def extract_hourly_ami(self, spark, date_row):
        """
        Read single date into single dataframe
        """
        path = os.path.join(
            self.hourly_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'year={}'.format(date_row.year),
            'month={}'.format(date_row.month),
            'day={}'.format(date_row.day)
            ) + '/'
        df_a = spark.read.parquet(path)
        # ONLY read channels with indicated direction (no 'R' data in MDIS)
        df_filt = df_a.where(col('direction') == lit(REQUIRED_DIRECTION))
        return df_filt

    def add_part_num_column(self, df_with_ch):
        """
        Add `chan_part_num` (short) column to dataframe that has channel_uuid populated.
        If Load configured with num_channel_partitions > 1, apply a CRC32 hash to the
        hex channel_uuid and then modulo num_channel_partitions to get value.  If
        num_channel_partitions <= 1, set `chan_part_num` to 0.
        """
        if self.num_channel_partitions > 1:
            df_with_ch = df_with_ch \
                .withColumn('chan_part_num', (fn.crc32(col('channel_uuid')) %
                            self.num_channel_partitions).cast(ShortType()))
        else:
            df_with_ch = df_with_ch \
                .withColumn('chan_part_num', lit(0).cast(ShortType()))
        return df_with_ch

    def row_per_channel_hour(self, df_ami_hourly):
        """
        For a single date, produce one row per channel(date) per hour with consumption
        populated (if hour has complete data) and number of seconds of data in hour.
        """
        return df_ami_hourly \
            .withColumn('num_secs_in_hr', col('num_reads_total') * col('max_seconds')) \
            .withColumn('is_complete',
                        (col('num_secs_in_hr') == lit(SEC_PER_HOUR)) &
                        (col('min_seconds') == col('max_seconds'))) \
            .withColumn('num_secs',
                        (when(col('is_complete'), col('num_secs_in_hr'))
                         .otherwise(lit(None))).cast(IntegerType())) \
            .withColumn('consumption',
                        when(col('is_complete'), col('consumption_total'))
                        .otherwise(lit(None).cast(FloatType()))) \
            .select([
                'channel_uuid',
                'time_zone',
                'hour_utc',
                'num_secs',
                'consumption',
            ])

    def row_per_channel_day(self, df_chan_hour):
        """
        Given dataframe of channel/hour data for a single date, pivot out to *one* row per
        channel with each hour consumption in its own kwh_HH column
        """
        df_chan_hour = df_chan_hour \
            .withColumn('latest_hour_utc',
                        when(col('consumption').isNull(), lit(None))
                        .otherwise(col('hour_utc')))
        aggs = [
            fn.max('latest_hour_utc').alias('latest_hour_utc'),
            fn.sum('num_secs').alias('num_secs'),
        ]
        aggs.extend([
            fn.sum(when(col('hour_utc') == lit(h),
                        col('consumption')).otherwise(lit(None))
                   ).alias('kwh_h%02d' % h) for h in HOUR_NUMBERS
        ])
        df_grouped = df_chan_hour \
            .groupBy('channel_uuid', 'time_zone') \
            .agg(*aggs) \
            .withColumn('pct_complete', 100.0 * col('num_secs') / lit(SEC_PER_DAY)) \
            .withColumn('pct_complete', fn.coalesce(col('pct_complete'), lit(0.0)).cast(FloatType())) \
            .withColumn('latest_hour_utc', col('latest_hour_utc').cast(ShortType()))
        df_grouped = self.add_part_num_column(df_grouped)
        select_fields = [
            'chan_part_num',
            'channel_uuid',
            'time_zone',
            'pct_complete',
            'latest_hour_utc',
        ]
        for h in HOUR_NUMBERS:
            col_name = 'kwh_h%02d' % h
            select_fields.append(col_name)
            df_grouped = df_grouped.withColumn(col_name, col(col_name).cast(FloatType()))
        return df_grouped.select(select_fields)

    def add_date_constants(self, spark, date_row, df_chan_day):
        df_chan_day = df_chan_day \
            .withColumn('tenant_id', lit(self.tenant_id).cast(LongType())) \
            .withColumn('year', lit(date_row.year).cast(ShortType())) \
            .withColumn('month', lit(date_row.month).cast(ShortType())) \
            .withColumn('day', lit(date_row.day).cast(ShortType())) \
            .withColumn('date_utc', lit(date_row.date_utc).cast(DateType())) \
            .select(LOAD_SCHEMA.fieldNames())
        return spark.createDataFrame(df_chan_day.rdd, LOAD_SCHEMA)

    def save_partitioned_data(self, date_row, df_chan_day):
        """
        Write partitioned parquet to new s3 folder based on hex timestamp
        """
        save_path = os.path.join(
            self.out_run_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'year={}'.format(date_row.year),
            'month={}'.format(date_row.month),
            'day={}'.format(date_row.day)
            ) + '/'
        # Save output to above path partitioned by channel partition numbering scheme, 1 file per.
        df_chan_day \
            .repartition(1, 'chan_part_num') \
            .write \
            .mode('overwrite') \
            .partitionBy('chan_part_num') \
            .parquet(save_path)
        return save_path

    def etl_single_date(self, spark, date_row):
        """
        Run extract/transform/load for a single date
        """
        start_t = time.time()
        log.info('MDIS_ETL_START date=%s', date_row.date_utc)
        df_hourly = self.extract_hourly_ami(spark, date_row)

        # Intermediate table is one row per channel per hour with hour_utc/consumption
        df_chan_hour = self.row_per_channel_hour(df_hourly)

        # Aggregate/Pivot to one row per channel for this day in form ready to save.
        df_chan_day = self.row_per_channel_day(df_chan_hour)

        # Add date constants to dataframe
        df_save = self.add_date_constants(spark, date_row, df_chan_day)

        # Save output to new mdis folder, release all the dataframes for this day, return save path
        save_path = self.save_partitioned_data(date_row, df_save)
        for df in [df_save, df_chan_day, df_chan_hour, df_hourly]:
            df.unpersist()
        dur_sec = int(time.time() - start_t)
        log.info('MDIS_ETL_END date=%s secs=%s path=%s', date_row.date_utc, dur_sec, save_path)
        return save_path

    def update_tenant_dates_metadata(self, spark, out_meta_rows):
        """
        Save updated path metadata in CSV format to single part-HEX file
        CSV format:
        YYYY-MM-DD,abababab
        YYYY-MM-DD,abcabcab
        Where:
         - YYYY-MM-DD is UTC date of AMI data that was loaded
         - hex (time-based) s3 folder XXXXXXXX beneath `out_prefix`
           where partitioned data for that date was loaded.
        """
        if len(out_meta_rows) < 1:
            log.info('MDIS_META_SKIP date_count=%s', len(out_meta_rows))
            return
        try:
            # Turn csv of date_utc,load_folder strings to dict of date_utc str -> load_folder str
            meta_rows = spark.read \
                .format('csv') \
                .option('header', 'false') \
                .schema(TENANT_META_SCHEMA) \
                .load(self.meta_path) \
                .collect()
            mdis_meta = {row.date_utc: row.load_folder for row in meta_rows}
            log.debug("MDIS_META_READ %s ok prev_count=%s", self.meta_path, len(meta_rows))
        except Exception as e:
            # Handle case where s3 metadata file does not exist
            log.info('MDIS_META_READ %s failed: %s', self.meta_path, str(e))
            mdis_meta = {}
        n_upd = 0
        for row in out_meta_rows:
            if row.success:
                # Success rows from out metadata should update mdis meta
                mdis_meta[row.date_utc.strftime('%Y-%m-%d')] = self.hex_ts
                n_upd += 1
        meta_rows = [Row(date_utc=key, load_folder=mdis_meta[key]) for key in sorted(mdis_meta.keys())]
        df_meta = spark.createDataFrame(meta_rows, TENANT_META_SCHEMA).coalesce(1)
        df_meta.write.csv(self.meta_path, mode='overwrite')
        log.info('MDIS_META_WRITE %s updated=%s total=%s', self.meta_path, n_upd, len(meta_rows))
        return n_upd

    def run_parallel(self, spark, tenant_dates):
        """
        Use Thread Pool to execute set of MDIS loads in parallel
        """
        out_meta_rows = []
        err_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_date = {
                executor.submit(self.etl_single_date, spark,
                                date_row): date_row.date_utc for date_row in tenant_dates
            }
            for future in concurrent.futures.as_completed(future_to_date):
                date_utc = future_to_date[future]
                try:
                    save_path = future.result()
                    log.debug('FUTURE_COMPLETE date_utc=%s save_path=%s', date_utc, save_path)
                    out_meta_rows.append(Row(date_utc=date_utc, success=True,
                                             save_path=save_path, error=None))
                except Exception as e:
                    log.error('FUTURE_FAILED date_utc=%s error=%s', date_utc, str(e))
                    log.exception(e)
                    out_meta_rows.append(Row(date_utc=date_utc, success=False,
                                             save_path=None, error=str(e)))
                    err_count += 1
        return out_meta_rows, err_count

    def run(self, spark):
        # Set of distinct dates to process from Today Hourly Transform
        tenant_dates = self.extract_tenant_dates(spark, 'hourly_meta')
        if len(tenant_dates) > 0:
            log.info('STARTING_DATES count=%s from=%s to=%s', len(tenant_dates),
                     tenant_dates[0].date_utc, tenant_dates[-1].date_utc)
            out_meta_rows, err_count = self.run_parallel(spark, tenant_dates)
        else:
            log.info('NO_DATES_INPUT count=%s', len(tenant_dates))
            out_meta_rows = []
            err_count = 0

        # Update tenant's date metadata with new prefix folder name
        self.update_tenant_dates_metadata(spark, out_meta_rows)

        # Output metadata rows
        log.info('FINISHED date_count=%s err_count=%s', len(out_meta_rows), err_count)
        return spark.createDataFrame(out_meta_rows, OUT_META_SCHEMA).createOrReplaceTempView('output')
