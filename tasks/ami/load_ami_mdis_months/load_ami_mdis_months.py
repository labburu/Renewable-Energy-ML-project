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
    FloatType,
    LongType,
    ShortType,
    StringType,
    StructType,
    StructField
)
import pyspark.sql.functions as fn


# Only process hourly data with this direction
REQUIRED_DIRECTION = 'D'

# Default min_pct_complete
DEFAULT_MIN_PCT_COMPLETE = '95.0'

# MDIS Schema definition - one row per channel per month.
# Daily data in a time series within row with one optional consumption if sufficiently complete,
# null if incomplete.  Note: **local** time used for date rollup data in upstream task
DAY_NUMBERS = range(1, 32)  # 1..31
LOAD_SCHEMA_FIELDS = [
    StructField('tenant_id', LongType(), False),
    StructField('local_year', ShortType(), False),
    StructField('local_month', ShortType(), False),
    StructField('chan_part_num', ShortType(), False),
    StructField('channel_uuid', StringType(), False),
    StructField('time_zone', StringType(), False),
]

# Add pivoted-out columns with
# - daily consumption in kwh (nullable if incomplete)
LOAD_SCHEMA_FIELDS.extend([
    StructField('kwh_d%02d' % h, FloatType(), True) for h in DAY_NUMBERS
])
LOAD_SCHEMA = StructType(LOAD_SCHEMA_FIELDS)
TENANT_META_SCHEMA = StructType([
    StructField('local_month', StringType(), False),
    StructField('load_folder', StringType(), False),
])
OUT_META_SCHEMA = StructType([
    StructField('local_year', ShortType(), False),
    StructField('local_month', ShortType(), False),
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
    Load AMI Monthly MDIS with column-per-day
    """

    def __init__(self, *args, **kwargs):
        self.start_ts = int(time.time())
        self.hex_ts = '%x' % self.start_ts
        self.tenant_id = kwargs['tenant_id']
        self.daily_prefix = kwargs['daily_prefix']
        self.out_prefix = kwargs['out_prefix']
        self.out_run_prefix = os.path.join(self.out_prefix, self.hex_ts)
        self.meta_path = kwargs['meta_path']
        self.num_channel_partitions = int(kwargs['num_channel_partitions'])
        self.min_pct_complete = float(kwargs.get('min_pct_complete', DEFAULT_MIN_PCT_COMPLETE))

    def extract_tenant_months(self, spark, in_daily_meta_name):
        """
        Return Rows of: tenant_id, local_year, local_month, min_date_local, max_date_local
        to be processed
        """
        df_dates = spark.table(in_daily_meta_name) \
            .withColumn('local_year', fn.year(col('date_local'))) \
            .withColumn('local_month', fn.month(col('date_local'))) \
            .groupBy('local_year', 'local_month') \
            .count() \
            .select(['local_year', 'local_month']) \
            .withColumn('min_date_local', fn.to_date(fn.concat_ws(
                '-',
                col('local_year'),
                fn.lpad(col('local_month'), 2, '0'),
                lit('01')
            ))) \
            .withColumn('max_date_local', fn.date_add(fn.add_months(col('min_date_local'), 1), -1))
        return df_dates.orderBy('local_year', 'local_month').collect()

    def extract_month_of_daily_ami(self, spark, month_row):
        """
        Read all local days for month for required direction into dataframe
        """
        path = os.path.join(
            self.daily_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'local_year={}'.format(month_row.local_year),
            'local_month={}'.format(month_row.local_month),
        ) + '/'
        log.info('START_MONTH_OF_DAILY_FILES path=%s', path)
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

    def row_per_channel_day(self, df_ami_daily):
        """
        For input dataframe (month worth of daily rollups), produce one row per channel
        per date_local with consumption populated (if date_local has complete data) and
        percent completeness in date
        """
        return df_ami_daily \
            .withColumn('is_complete', col('pct_complete') >= lit(self.min_pct_complete)) \
            .withColumn(
                'consumption',
                when(col('is_complete'), col('consumption_total'))
                .otherwise(lit(None).cast(FloatType()))
            ) \
            .select([
                'channel_uuid',
                'time_zone',
                'date_local',
                'consumption',
            ])

    def row_per_channel_month(self, df_chan_day):
        """
        Given dataframe of channel/date_local data for a single date, pivot out to *one* row per
        channel with each day consumption in its own kwh_dDD column
        """
        aggs = [
            fn.sum(when(fn.dayofmonth(col('date_local')) == lit(d),
                        col('consumption')).otherwise(lit(None))
                   ).alias('kwh_d%02d' % d) for d in DAY_NUMBERS
        ]
        df_grouped = df_chan_day \
            .groupBy('channel_uuid', 'time_zone') \
            .agg(*aggs)
        df_grouped = self.add_part_num_column(df_grouped)
        select_fields = [
            'chan_part_num',
            'channel_uuid',
            'time_zone',
        ]
        for d in DAY_NUMBERS:
            col_name = 'kwh_d%02d' % d
            select_fields.append(col_name)
            df_grouped = df_grouped.withColumn(col_name, col(col_name).cast(FloatType()))
        return df_grouped.select(select_fields)

    def add_month_constants(self, spark, month_row, df_chan_month):
        df_chan_month = df_chan_month \
            .withColumn('tenant_id', lit(self.tenant_id).cast(LongType())) \
            .withColumn('local_year', lit(month_row.local_year).cast(ShortType())) \
            .withColumn('local_month', lit(month_row.local_month).cast(ShortType())) \
            .select(LOAD_SCHEMA.fieldNames())
        return spark.createDataFrame(df_chan_month.rdd, LOAD_SCHEMA)

    def save_partitioned_data(self, month_row, df_chan_month):
        """
        Write partitioned parquet to new s3 folder based on hex timestamp
        """
        save_path = os.path.join(
            self.out_run_prefix,
            'tenant_id={}'.format(self.tenant_id),
            'local_year={}'.format(month_row.local_year),
            'local_month={}'.format(month_row.local_month)
            ) + '/'
        # Save output to above path partitioned by channel partition numbering scheme, 1 file per.
        df_chan_month \
            .repartition(1, 'chan_part_num') \
            .write \
            .mode('overwrite') \
            .partitionBy('chan_part_num') \
            .parquet(save_path)
        return save_path

    def etl_single_month(self, spark, month_row):
        """
        Run extract/transform/load for a single date
        """
        start_t = time.time()
        log.info('MDIS_ETL_START y%sm%s', month_row.local_year, month_row.local_month)
        df_daily = self.extract_month_of_daily_ami(spark, month_row)

        # Intermediate table is one row per channel per local date with date/consumption
        df_chan_day = self.row_per_channel_day(df_daily)

        # Aggregate/Pivot to one row per channel for this month in form ready to save.
        df_chan_month = self.row_per_channel_month(df_chan_day)

        # Add month constants to dataframe
        df_save = self.add_month_constants(spark, month_row, df_chan_month)

        # Save output to new mdis folder, release all the dataframes for this day, return save path
        save_path = self.save_partitioned_data(month_row, df_save)
        for df in [df_save, df_chan_month, df_chan_day, df_daily]:
            df.unpersist()
        dur_sec = int(time.time() - start_t)
        log.info('MDIS_ETL_END y%sm%s secs=%s path=%s', month_row.local_year,
                 month_row.local_month, dur_sec, save_path)
        return save_path

    def update_tenant_months_metadata(self, spark, out_meta_rows):
        """
        Save updated path metadata in CSV format to single part-HEX file
        CSV format:
        YYYY-MM,abababab
        YYYY-MM,abcabcab
        Where:
         - YYYY-MM: local (to location) year-month of AMI data that was loaded
         - hex (time-based) s3 folder XXXXXXXX beneath `out_prefix`
           where partitioned data for that date was loaded.
        """
        if len(out_meta_rows) < 1:
            log.info('MDIS_META_SKIP date_count=%s', len(out_meta_rows))
            return
        try:
            # Turn csv of date_local,load_folder strings to dict of YYYY-MM str -> load_folder str
            meta_rows = spark.read \
                .format('csv') \
                .option('header', 'false') \
                .schema(TENANT_META_SCHEMA) \
                .load(self.meta_path) \
                .collect()
            mdis_meta = {row.local_month: row.load_folder for row in meta_rows}
            log.debug("MDIS_META_READ %s ok prev_count=%s", self.meta_path, len(meta_rows))
        except Exception as e:
            # Handle case where s3 metadata file does not exist
            log.info('MDIS_META_READ %s failed: %s', self.meta_path, str(e))
            mdis_meta = {}
        n_upd = 0
        for row in out_meta_rows:
            if row.success:
                # Success rows from out metadata should update mdis meta
                mdis_meta['%04d-%02d' % (row.local_year, row.local_month)] = self.hex_ts
                n_upd += 1
        meta_rows = [Row(local_month=key, load_folder=mdis_meta[key]) for key in sorted(mdis_meta.keys())]
        df_meta = spark.createDataFrame(meta_rows, TENANT_META_SCHEMA).coalesce(1)
        df_meta.write.csv(self.meta_path, mode='overwrite')
        log.info('MDIS_META_WRITE %s updated=%s total=%s', self.meta_path, n_upd, len(meta_rows))
        return n_upd

    def run_sequential(self, spark, months):
        """
        Use Thread Pool to execute set of MDIS loads in parallel
        """
        out_meta_rows = []
        for month_row in months:
            save_path = self.etl_single_month(spark, month_row)
            out_meta_rows.append(Row(local_year=month_row.local_year, local_month=month_row.local_month,
                                     success=True, save_path=save_path, error=None))
        return out_meta_rows

    def run(self, spark):
        # Set of distinct *months* to process from Today Daily Transform
        months = self.extract_tenant_months(spark, 'daily_meta')
        if len(months) > 0:
            log.info('STARTING_MONTHS count=%s from=%sm%s to=%sm%s', len(months),
                     months[0].local_year, months[0].local_month,
                     months[-1].local_year, months[-1].local_month)
            out_meta_rows = self.run_sequential(spark, months)
        else:
            log.info('NO_DATES_INPUT count=%s', len(months))
            out_meta_rows = []

        # Update tenant's month metadata with new prefix folder name
        self.update_tenant_months_metadata(spark, out_meta_rows)

        # Output metadata rows
        log.info('FINISHED month_count=%s', len(out_meta_rows))
        return spark.createDataFrame(out_meta_rows, OUT_META_SCHEMA).createOrReplaceTempView('output')
