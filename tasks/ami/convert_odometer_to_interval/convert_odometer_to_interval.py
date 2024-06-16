
import logging
import sys
from datetime import timedelta
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    unix_timestamp,
    asc,
    desc,
    lead,
    to_timestamp,
    lit,
    row_number
)
from pyspark.sql.window import Window

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class ConvertOdometerToInterval:

    def __init__(
        self,
        extraction_date,
        tenant_id,
        odometer_path=None,
        odometer_table='odometer'
    ):
        """
        You have a few options on where the source data comes from:
        - If you want to load the data from a path, use `odometer_path`
        - If you already have the data in a table named `odometer`, and
          don't pass in a path, it will use that. Otherwise you can also
          pass in `odomter_table` to specify another table name.

        `extraction_date` is currently just used to set the updated
        timestamp. This is so whenever we update existing records we
        set a newer value there so we can properly replace outdated data.
        """
        self.extraction_date = extraction_date
        self.tenant_id = tenant_id
        self.odometer_path = odometer_path
        self.odometer_table = odometer_table

    def _get_dates(self, spark):
        """Get date range of newly ingested data.

        In order to calculate the beginning/end interval for a given
        set of odometer data, we need to pull in the data from the days
        before/after. Many days can be presented in a given ingest, so
        here we pull out all of the days that contain new data.

        Schema: "fresh_data"
        |-- tenant_id: long
        |-- date_utc: date
        |-- row_count: integer
        """
        df = spark.table('fresh_data')

        # Grab the list of represented days from the new set of data
        dates = sorted([d.date_utc for d in df.select('date_utc').collect()])

        # For each date in dates, get the day prior to it.
        # If the set of dates is not continuous (very possible), we need
        # to make sure that we get the prior date for all of the dates.
        prior_dates = [d - timedelta(days=1) for d in dates]

        # Return the sorted list of unique dates in the dataset
        return list(sorted(set(dates + prior_dates)))

    def _read_existing_data(self, spark, dates):
        """Read existing common odometer data from a given time range.

        Read each date individually, and union to create the total set.
        """
        log.info('Reading dates: %s', [d.isoformat() for d in dates])
        if self.odometer_path is not None:
            log.info('Reading odometer_path: %s', self.odometer_path)
            spark.read.parquet(self.odometer_path) \
                .createOrReplaceTempView(self.odometer_table)

        dfs = []
        for d in dates:
            try:
                df_date = spark.table(self.odometer_table) \
                    .where(col('tenant_id') == self.tenant_id) \
                    .where(col('year') == d.year) \
                    .where(col('month') == d.month) \
                    .where(col('day') == d.day)

                dfs.append(df_date)
            except Exception as e:
                log.warning('Unable to read data for date: %s (%s)', d, e)

        # Union all DataFrames
        df = reduce(DataFrame.union, dfs)

        # Order by partition columns and drop duplicates
        return df.orderBy(['year', 'month', 'day']).drop_duplicates()

    def _select_most_recent_records(self, spark, df):
        """Get most recent records per channel. """

        # NOTE: year/month/day is added to the partition to improve performance
        most_recent_window = Window \
            .partitionBy(
                'tenant_id',
                'channel_uuid',
                'year',
                'month',
                'day',
                'reading_ts_utc',
                'direction') \
            .orderBy(desc('updated_time_utc')) \
            .rowsBetween(Window.unboundedPreceding, 0)

        return df \
            .withColumn('rn', row_number().over(most_recent_window)) \
            .filter(col('rn') == 1).drop('rn')

    def _convert_to_interval(self, spark, df, max_interval=3600):
        """Convert AMI from odometer to interval format.

        Uses a window to lookahead at each next row.

        `max_interval`: The largest range to return values for.
            i.e. we shouldn't consider ranges longer than an hour. This
            ensures that get missing intervals instead of really ones.

        """

        # Looks one row ahead to check the next odometer value
        lookahead_window = Window \
            .partitionBy('channel_uuid') \
            .orderBy(asc('reading_ts_utc')) \
            .rowsBetween(1, 1)  # 1 refers to the next row

        # NOTE: We filter out records that have a consumption longer
        # than an hour. This will leave gaps in the interval output
        # instead of show really long consumption time periods.
        return df \
            .withColumn(
                'next_consumption',
                lead(col('consumption'), 1).over(lookahead_window)
            ) \
            .withColumn(
                'next_reading_ts_utc',
                lead(col('reading_ts_utc'), 1).over(lookahead_window)
            ) \
            .filter('next_consumption is not null') \
            .withColumn(
                'interval_consumption',
                col('next_consumption') - col('consumption')
            ) \
            .withColumn(
                'interval_start_utc',
                col('reading_ts_utc')
            ) \
            .withColumn(
                'interval_end_utc',
                col('next_reading_ts_utc')
            ) \
            .withColumn(
                'interval_updated_time_utc',
                to_timestamp(lit(self.extraction_date), 'yyyy-MM-dd')
            ) \
            .withColumn(
                'interval_seconds',
                unix_timestamp(col('next_reading_ts_utc')) - unix_timestamp(col('reading_ts_utc'))
            ) \
            .filter(col('interval_seconds') <= max_interval)

    def run(self, spark):
        dates = self._get_dates(spark)

        df = self._read_existing_data(spark, dates=dates)

        df = self._select_most_recent_records(spark, df)

        df = self._convert_to_interval(spark, df)

        output = df.select(
            col('tenant_id'),
            col('channel_uuid'),
            col('interval_start_utc'),
            col('interval_end_utc'),
            col('time_zone'),
            col('interval_seconds').alias('seconds_per_interval'),
            col('interval_consumption').alias('consumption'),
            col('direction'),
            col('consumption_unit'),
            col('consumption_code'),
            col('source_file_name'),
            col('interval_updated_time_utc').alias('updated_time_utc'),
            col('year'),
            col('month'),
            col('day'),
        )

        output.createOrReplaceTempView('output')
