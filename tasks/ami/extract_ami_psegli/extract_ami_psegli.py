
import logging
import math
import sys
from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    explode,
    first,
    input_file_name,
    udf,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    ArrayType,
    TimestampType,
)

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField('service_point_id', StringType()),
    StructField('account_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('consumption', DoubleType()),
    StructField('consumption_unit', StringType()),
    StructField('seconds_per_interval', IntegerType()),
    StructField('time_zone', StringType()),
    StructField('code', StringType()),
    StructField('source_file_name', StringType()),
    StructField('error_type', StringType()),
    StructField('error_record', StringType()),
])


def to_float(x):
    if x is None:
        return None
    x = x.strip()
    if len(x) < 1:
        return None
    if x.startswith('0-.'):
        return float(0)
    return float(x)


class ExtractAmiPsegli:

    def __init__(self, *args, **kwargs):
        """Set up transform."""
        self.udf_process_combined_record = udf(
            f=ExtractAmiPsegli.process_combined_record,
            returnType=ArrayType(SCHEMA),
        )

    @staticmethod
    def clean_file_name(full_name, default='UNKNOWN'):
        """Return source file name.

        Leading path and trailing extension are removed.
        """
        if full_name is None or len(full_name) < 1:
            fname = default
        else:
            fname = full_name.split('/')[-1]
            dotidx = fname.find('.')
            if dotidx > 0:
                fname = fname[0:dotidx]
        return fname

    def add_source_file_name(self, df):
        """Add source file name column stripped of path/extension."""
        clean_udf = udf(ExtractAmiPsegli.clean_file_name, StringType())
        return df.withColumn('source_file_name', clean_udf(input_file_name()))

    @staticmethod
    def process_combined_record(record):
        """Take PSEGLI combined AMI record and return tabular row.

        This function reads a version of the sample data below, where
        all five lines are concatenated into a single string inside a
        column. See the tests for this job for more information.
        """
        try:
            # The window function below concatenates rows with a pipe
            lines = record.split('|')

            # There should be six items after splitting:
            # - The five lines that come with PSEGLI AMI
            # - An additional item that contains the source file name
            assert len(lines) == 6

            # Rows are actually CSVs, so separate each line by comma
            line_1 = lines[0].split(',')
            line_2 = lines[1].split(',')
            line_3 = lines[2].split(',')[0:2]  # The third field may include a comma
            line_4 = lines[3].split(',')
            line_5 = lines[4].split(',')

            # Source file name is concatenated to the condensed AMI data
            source_file_name = lines[5]

            # Ensure that each line has the appropriate number of fields
            assert len(line_1) == 7
            assert len(line_2) == 14
            assert len(line_3) == 2
            assert len(line_4) == 3
            assert len(line_5) > 1

            # Do a sanity check on data headers before continuing
            assert line_1[0] == '00000001'
            assert line_2[0] == '00000002'
            assert line_3[0] == '00000003'
            assert line_4[0] == '00000004'
            assert line_5[0] == '10000000'
        except (AssertionError, IndexError) as e:
            log.error('Malformed AMI record: %s', e)
            return [
                dict(
                    service_point_id=None,
                    account_id=None,
                    timestamp=None,
                    consumption=None,
                    consumption_unit=None,
                    seconds_per_interval=None,
                    time_zone=None,
                    code=None,
                    source_file_name=None,
                    error_type='MALFORMED',
                    error_record=record,
                )
            ]

        # Relevant information from line 1
        service_point_id = line_1[1]  # str
        channel_number = int(line_1[2])  # int
        start_time = datetime.strptime(line_1[3], '%Y%m%d%H%M%S')  # datetime
        end_time = datetime.strptime(line_1[4], '%Y%m%d%H%M%S')  # datetime

        if channel_number != 1:
            # The old ingestor included this warning:
            # NOTE: We used to have this as an exception case, but it's
            # very, very common and not exceptional.
            log.debug('Invalid channel number: %s', channel_number)
            return [
                dict(
                    service_point_id=service_point_id,
                    account_id=None,
                    timestamp=None,
                    consumption=None,
                    consumption_unit=None,
                    seconds_per_interval=None,
                    time_zone=None,
                    code=None,
                    source_file_name=source_file_name,
                    error_type='CHANNEL',
                    error_record=record,
                )
            ]

        # Relevant information from line 2
        seconds_per_interval = int(line_2[7])  # int
        time_zone = line_2[13]  # str

        # Relevant information from line 3
        account_id = line_3[1]  # str

        # Relevant information from line 4
        code = line_4[2]  # str

        # Relevant information from line 5
        consumptions = line_5[1:]  # List

        # Take every third value up to max entries to get the actual meter readings
        max_entries = math.ceil((end_time - start_time).seconds / seconds_per_interval)
        try:
            readings = [to_float(c) for c in consumptions[0::3][:max_entries]]
        except ValueError as e:
            log.debug('Error (%s) parsing consumptions: %s', str(e), consumptions)
            return [
                dict(
                    service_point_id=service_point_id,
                    account_id=account_id,
                    timestamp=None,
                    consumption=None,
                    consumption_unit=None,
                    seconds_per_interval=None,
                    time_zone=None,
                    code=None,
                    source_file_name=source_file_name,
                    error_type='VALUE_ERROR',
                    error_record=record,
                )
            ]

        # Construct list of timestamps using start time and interval
        timestamps = [
            start_time + timedelta(seconds=(seconds_per_interval * i))
            for i, reading in enumerate(readings)
        ]

        # Last timestamp should be prior to end timestamp
        assert timestamps[-1] < end_time

        # Combine timestamps and meter readings
        measurements = list(zip(timestamps, readings))

        output_rows = []  # List[Row]
        for measurement in measurements:
            output_rows.append(
                dict(
                    service_point_id=service_point_id,
                    account_id=account_id,
                    timestamp=measurement[0],
                    consumption=measurement[1],
                    consumption_unit='KWH',
                    seconds_per_interval=seconds_per_interval,
                    time_zone=time_zone,
                    code=code,
                    source_file_name=source_file_name,
                    error_type=None,
                    error_record=None,
                )
            )

        return output_rows

    @staticmethod
    def condense_rows(df, condensed_column='all'):
        """Take five-row PSEGLI data and condense into one row.

        Slide five rows at a time and condense into single row.
        """
        return df \
            .withColumn('1', first('_c0').over(Window.rowsBetween(0, 1))) \
            .withColumn('2', first('_c0').over(Window.rowsBetween(1, 2))) \
            .withColumn('3', first('_c0').over(Window.rowsBetween(2, 3))) \
            .withColumn('4', first('_c0').over(Window.rowsBetween(3, 4))) \
            .withColumn('5', first('_c0').over(Window.rowsBetween(4, 5))) \
            .withColumn(
                condensed_column,
                concat_ws('|', '1', '2', '3', '4', '5', 'source_file_name')
            ) \
            .where(col(condensed_column).startswith('00000001')) \
            .select(condensed_column)

    def run(self, spark):
        """Get PSEGLI AMI data into a normal format.

        The input data structure is strange. A sample:

        00000001,123456,1,20181118000000,20181118235959,Y,N
        00000002,5.788,5.797,80,0,,,900,01,,10,,,EST5EDT
        00000003,12345,CUSTOMER NAME
        00000004,20181119203520,A
        10000000,1.2, ,,1.3, ,,2.1, ,,2.5, ,,1.7, ,,1.0, ,, <ongoing>
        00000001, ... <same as above, for next meter>

        The goal is to get the above data into a format that resembles
        the existing tenant AMI data that we ingest. This will make the
        ingest process simpler, and isolate the logic that operates on
        the raw data.
        """
        log.info('Reading raw AMI data')
        df = spark.table('ami')

        log.info('Adding source file name')
        df = self.add_source_file_name(df)

        log.info('Condensing meter data into single row')
        df = self.condense_rows(df)

        # Get all meter data into one row, build records, then explode
        df = df \
            .withColumn(
                'condensed',
                self.udf_process_combined_record('all')
            ) \
            .withColumn(
                'rows',
                explode('condensed')
            ) \
            .select([
                col('rows').getItem(f).alias(f) for f in SCHEMA.fieldNames()
            ])

        log.info('Register DataFrame as output')
        df.createOrReplaceTempView('output')
