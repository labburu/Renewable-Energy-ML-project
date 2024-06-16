import logging
import os
import pyspark.sql.functions as f
import sys
import json
import collections
import hashlib
from datetime import datetime
from functools import reduce
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType
)
from pyspark.sql.functions import (
    col,
    collect_set,
    concat,
    concat_ws,
    from_unixtime,
    from_utc_timestamp,
    get_json_object,
    hour,
    lit,
    rand,
    to_date,
    to_utc_timestamp,
    unix_timestamp,
    when
)

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField('id', StringType(), False),
    StructField('name', StringType(), False),
    StructField('execution_date', StringType(), False),
    StructField('metrics', StringType(), False, metadata={'maxlength': 2000}),
    StructField('qc_reference', StringType(), False, metadata={'maxlength': 2000}),
    StructField('misc', StringType(), False, metadata={'maxlength': 2000})
])

ROLLUP_QC_CNT = 1000
CONSUMPTION_EPSILON = 0.00001
COMPLETED_QC_STEP_LIST = []

QC_META = {
    1: {
        'name': 'Decrypt',
        'metrics': {
            1: {
                'name': 'file count',
                'left_data':  'manifest file count',
                'right_data': 'audit file count',
                'error_message': 'audit file count does not match manifest',
                'error_df_filter_string': 'file_mismatch = 1'
            },
            2: {
                'name': 'line count',
                'left_data':  'manifest line count',
                'right_data': 'audit line count',
                'error_message': 'audit line count does not match manifest',
                'error_df_filter_string': 'linecount_mismatch = 1'
            },
            3: {
                'name': 'checksums',
                'left_data':  'manifest checksums',
                'right_data': 'audit checksums',
                'error_message': 'checksum of audit checksum list does not match manifest',
                'error_df_filter_string': 'checksum_mismatch = 1'
            },
        }
    },
    2: {
        'name': 'Channel Ingest',
        'metrics': {
            1:  {
                'name': 'all raw ami channels processed',
                'left_data':  'raw ami channel count',
                'right_data': 'channel ingest channel count',
                'error_message': 'raw ami channels missing from channel ingest output',
                'error_df_filter_string': 'no_output = 1'
            },
            2: {
                'name': 'no distinct raw ami channels mapped to multiple channel uuids',
                'left_data':  'multiple mapped channels expected',
                'right_data': 'multiple mapped channels count',
                'error_message': 'distinct raw ami channels mapped to multiple channel uuids',
                'error_df_filter_string': 'external_channel_multiple_channel_uuid = 1'
            },
            3: {
                'name': 'channel uuids in success output are mapped correctly in zeus',
                'left_data':  'channel ingest cuccess count',
                'right_data': 'correctly mapped channels from zeus',
                'error_message': 'channel uuids in success output incorrectly mapped in zeus',
                'error_df_filter_string': 'success = 1 AND channel_uuid_match = 0'
            },
        }
    },
    3: {
        'name': 'Extract Common AMI',
        'metrics': {
            1: {
                'name': 'all raw ami reads processed',
                'left_data':  'raw ami read count',
                'right_data': 'extract common ami total read count',
                'error_message': 'raw ami read count != extract common ami total read count',
                'error_df_filter_string': 'raw_read_cnt != eca_total_cnt'
            }
        }
    },
    4: {
        'name': 'Load Common AMI',
        'metrics': {
            1: {
                'name': 'all ingestable reads loaded',
                'left_data':  'extract common ami success read count',
                'right_data': 'load common ami metadata count',
                'error_message': 'extract common ami success read count != load common ami metadata count',
                'error_df_filter_string': 'eca_success_cnt != lca_success_cnt'
            }
        }
    },
    5: {
        'name': 'Hourly Rollup',
        'metrics': {
            1: {
                'name': 'raw hourly totals match mdis rollups',
                'left_data':  'raw ami hourly rollups',
                'right_data': 'mdis hourly rollups',
                'error_message': 'raw ami hourly rollups != mdis hourly rollups',
                'error_df_filter_string': 'qc_status = 0'
            }
        }
    },
    6: {
        'name': 'Daily Rollup',
        'metrics': {
            1: {
                'name': 'raw daily totals match mdis rollups',
                'left_data':  'raw ami daily rollups',
                'right_data': 'mdis daily rollups',
                'error_message': 'raw ami daily rollups != mdis daily rollups',
                'error_df_filter_string': 'qc_status = 0'
            }
        }
    },
    7: {
        'name': 'Ingest Time',
        'metrics': {
            1: {
                'name': 'ingest time under 24 hours',
                'left_data':  'ingest under 24 hours expected',
                'right_data': 'ingest under 24 hours actual',
                'error_message': 'ingest time is greater than 24 hours',
                'error_df_filter_string': 'qc_status = 0'
            }
        }
    }
}


class Quality():

    def __init__(
        self,
        tenant_id,
        col_external_account_id,
        col_external_channel_id,
        col_external_location_id,
        col_timestamp,
        col_interval,
        col_consumption,
        col_consumption_unit,
        col_consumption_code,
        col_direction,
        col_manifest_filename,
        col_manifest_bytecount,
        col_manifest_checksum,
        col_manifest_linecount,
        string_tz_to_utc,
        errors_save_format,
        execution_date_tenant_format,
        execution_date_y_m_d,
        has_manifest,
        manifest_counts_headers,
        raw_ami_has_headers,
        s3_path_audit,
        s3_path_channel_ingest_error,
        s3_path_common,
        s3_path_decrypt,
        s3_path_encrypt,
        s3_path_extract_common_ami_error,
        s3_path_ingest_time,
        s3_path_load_mdis_daily,
        s3_path_load_mdis_monthly,
        s3_path_manifest,
        s3_path_mdis_daily_meta,
        s3_path_mdis_monthly_meta,
        s3_path_rollup_daily_intermediate,
        s3_path_rollup_hourly_intermediate,
        s3_path_save_ami_summary,
        s3_path_save_errors_base,
        s3_path_save_qc_output,
        s3_path_load_qc_reference,
        *args, **kwargs
    ):
        self.tenant_id = tenant_id
        self.col_external_account_id = col_external_account_id
        self.col_external_channel_id = col_external_channel_id
        self.col_external_location_id = col_external_location_id
        self.col_timestamp = col_timestamp
        self.col_interval = col_interval
        self.col_consumption = col_consumption
        self.col_consumption_unit = col_consumption_unit
        self.col_consumption_code = col_consumption_code
        self.col_direction = col_direction
        self.col_manifest_filename = col_manifest_filename
        self.col_manifest_bytecount = col_manifest_bytecount
        self.col_manifest_checksum = col_manifest_checksum
        self.col_manifest_linecount = col_manifest_linecount
        self.string_tz_to_utc = string_tz_to_utc
        self.errors_save_format = errors_save_format
        self.execution_date_tenant_format = execution_date_tenant_format
        self.execution_date_y_m_d = execution_date_y_m_d
        self.has_manifest = has_manifest
        self.manifest_counts_headers = manifest_counts_headers
        self.raw_ami_has_headers = raw_ami_has_headers
        self.s3_path_audit = s3_path_audit
        self.s3_path_channel_ingest_error = s3_path_channel_ingest_error
        self.s3_path_common = s3_path_common
        self.s3_path_decrypt = s3_path_decrypt
        self.s3_path_encrypt = s3_path_encrypt
        self.s3_path_extract_common_ami_error = s3_path_extract_common_ami_error
        self.s3_path_ingest_time = s3_path_ingest_time
        self.s3_path_load_mdis_daily = s3_path_load_mdis_daily
        self.s3_path_load_mdis_monthly = s3_path_load_mdis_monthly
        self.s3_path_manifest = s3_path_manifest
        self.s3_path_mdis_daily_meta = s3_path_mdis_daily_meta
        self.s3_path_mdis_monthly_meta = s3_path_mdis_monthly_meta
        self.s3_path_rollup_daily_intermediate = s3_path_rollup_daily_intermediate
        self.s3_path_rollup_hourly_intermediate = s3_path_rollup_hourly_intermediate
        self.s3_path_save_ami_summary = s3_path_save_ami_summary
        self.s3_path_save_errors_base = s3_path_save_errors_base
        self.s3_path_save_qc_output = s3_path_save_qc_output
        self.s3_path_load_qc_reference = s3_path_load_qc_reference

    @staticmethod
    def clean_file_name(full_name, default='UNKNOWN'):
        """Return file name with leading path removed and trailing extension removed.
        In many places in QC we need to return filenames, stripped of path or suffixes.
        :param str full_name: file path to be parsed
        :return: string of cleaned file name
        """
        if full_name is None or len(full_name) < 1:
            fname = default
        else:
            fname = full_name.split('/')[-1]
            dotidx = fname.find('.')
            if dotidx > 0:
                fname = fname[0:dotidx]
        return fname

    @staticmethod
    def get_file_path_from_temp_view(spark, table_name):
        """Return file path in s3 for reference from extract id.
        To facilitate investigations into QC issues we need paths to all data used for QC.
        As many data products are defined in the DAG as temp views we need to pull out their actual paths for reference.
        :param str table_name: source table name from extract id passed in by the DAG
        :return: Dataframe with following schema
            |-- file_path: string (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                f.regexp_replace(f.input_file_name(), '(?=/part).*$', '').alias('file_path')
        ) \
            .distinct() \
            .collect()[0][0]

    @staticmethod
    def update_error_dataframe(spark, step_number, metric_number, df_error):
        """Filter dataframe passed with an indiviual step to produce error output, if applicable.
        For error detail output, all pertinent data for each ingest step is joined and passed to the QC functions.
        If a count comparison fails, the approprate filtering is applied to update the error dataframe approriately.
        :param step_number: integer value to define step
        :param metric_number: integer value to define metric
        :param df_errors: joined dataframe for a given ingest step, filter to get errors
        :return: Dataframe with variable schema
        """
        error_message = QC_META[step_number]['metrics'][metric_number]['error_message']
        error_df_filter_string = QC_META[step_number]['metrics'][metric_number]['error_df_filter_string']
        df_error = df_error \
            .filter('{}'.format(error_df_filter_string)) \
            .withColumn('error_message',  lit(error_message))
        return df_error

    def qc_individual_step(self, spark,  step_number, qc_values, df_error):
        """Run QC for n number of metrics in a given ingest step.
        Each step in the AMI ingest process has a number of metrics which define quality for that step.
        This function runs QC on each metric defined for a given step in the QC_META dictionary.
        :param step_number: integer value to define step
        :param qc_values: dictionary which holds qc data, reference urls, and extra data
        :param df_errors: joined dataframe for a given ingest step, filter to get errors
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        step_name = QC_META[step_number]['name']
        log.info('starting qc for step {}: {}'.format(step_number, step_name))
        # set metric output dictionary
        metric_output = {}
        # get metric data and qc individual metric
        metric_keys = QC_META[step_number]['metrics'].keys()
        for key in metric_keys:
            metric_number = key
            left_data = qc_values['metrics'][key]['left_data']
            right_data = qc_values['metrics'][key]['right_data']
            qc_data = self.qc_individual_metric(spark, step_number, metric_number, left_data, right_data, df_error)
            metric_output.update({metric_number: qc_data})
        # set output data
        id = step_number
        name = QC_META[step_number]['name']
        execution_date = self.execution_date_y_m_d
        metrics_json = json.dumps(metric_output)
        reference_json = json.dumps(qc_values['reference'], sort_keys=True)
        misc = json.dumps(qc_values['misc'], sort_keys=True)
        # create output dataframe
        rows = [(
            id,
            name,
            execution_date,
            metrics_json,
            reference_json,
            misc
        )]
        df_output = spark.createDataFrame(rows, SCHEMA)
        return df_output

    def qc_individual_metric(self, spark, step_number, metric_number, left_data, right_data, df_error):
        """Run QC for a single individual metric.
        Each step in the AMI ingest process has a number of metrics which define quality for that step.
        This function performs QC on a single metric at a time and produces QC output and error data if applicable.
        :param step_number: integer value to define step
        :param metric_number: integer value to define metric
        :param left_data: integer value to define left qc data
        :param right_data: integer value to define right qc data
        :param step_number: integer value to define step
        :param df_errors: joined dataframe for a given ingest step, filter to get errors
        :return: ordered dictionary of QC output for indiviual metric
        """
        # set qc data
        metric_name = QC_META[step_number]['metrics'][metric_number]['name']
        qc_time_utc = (datetime.utcnow()).strftime('%Y-%m-%d %H:%M:%S')
        step_name = QC_META[step_number]['name']
        left_data_name = QC_META[step_number]['metrics'][metric_number]['left_data']
        right_data_name = QC_META[step_number]['metrics'][metric_number]['right_data']
        l_r_delta = left_data - right_data
        # do qc
        log.info('running qc for step {}: {}, {}'.format(step_number, step_name, metric_name))
        # qc pass
        if left_data == right_data:
            qc_status = 1
            error_message = ''
        # qc fail and write out errors
        else:
            qc_status = 0
            error_message = QC_META[step_number]['metrics'][metric_number]['error_message']
            df_error = self.update_error_dataframe(spark, step_number, metric_number, df_error)
            log.info('error on step {}: {}, {}'.format(step_number, step_name, metric_name))
            error_save_to_path = self.save_error_rows(df_error, step_number, metric_number)
        # build output
        output = collections.OrderedDict()
        output['metric_name'] = metric_name
        output['left_data_name'] = left_data_name
        output['left_data_value'] = left_data
        output['right_data_name'] = right_data_name
        output['right_data_value'] = right_data
        output['left_right_delta'] = l_r_delta
        output['qc_timestamp'] = qc_time_utc
        output['qc_status'] = qc_status
        if error_message != '':
            output['qc_error_message'] = error_message
            output['qc_error_path'] = error_save_to_path
        return output

    def save_error_rows(self, df_errors, step_number, metric_number):
        """Write any error output to long term storage location.
        Whenever there is a QC issue the error details of the metric in question need to be saved for reference.
        This function saves any error output produced to S3 for further review.
        :param df_errors: error dataframe for a given ingest step
        :param step_number: integer value to define step
        :param metric_number: integer value to define metric
        :return: None
        """
        if step_number == 1:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('decrypt'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 2:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('channel_ingest'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 3:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('extract_common_ami'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 4:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('load_common_ami'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 5:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('raw_to_mdis_hour'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 6:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('raw_to_mdis_day'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        else:
            save_to = None
        cnt = df_errors.count()
        if save_to is None:
            log.info('Skipping save_error_rows for {cnt} rows, no errors_save_to configured'.format(cnt=cnt))
        else:
            df_errors.repartition(1).write.save(
                path=save_to,
                mode='overwrite',
                format=self.errors_save_format)
            log.info('Saved {cnt} rows to {save_to} ok'.format(cnt=cnt, save_to=save_to))

        return save_to

    def save_output(self, df, output_type):
        """Write any non-error output to long term storage location.
        The QC process produces data products which are not errors, but need to be saved long-term.
        This function saves any non-error output produced to S3 for further review.
        :param df: output to save
        :param int: output_type (1:ami_summary, 2:qc_output, 3:mdis_hour, 4:mdis_day)
        :return: None
        """
        if output_type == 1:  # ami summary
            save_to = self.s3_path_save_ami_summary
        elif output_type == 2:  # qc output
            save_to = self.s3_path_save_qc_output
        elif output_type == 3:  # mdis hour check details
            save_to = os.path.join(self.s3_path_load_qc_reference, 'raw_to_mdis_check_details', 'hour')
        elif output_type == 4:  # mdis day check details
            save_to = os.path.join(self.s3_path_load_qc_reference, 'raw_to_mdis_check_details', 'day')
        else:
            save_to = None

        if save_to is None:
            log.info('Skipping save of output, no save path configured')
        else:
            df.repartition(1).write.save(
                path=save_to,
                mode='overwrite',
                format=self.errors_save_format)
            log.info('Saved output to {} ok'.format(save_to))

    def raw_ami_to_common_qc(self, spark, table_name):
        """Translate tenant specific data to Uplight common format for QC.
        Tenant raw AMI data varies in format and to facilitate QC accross multiple tenants a common format is necessary.
        This function takes in tenant data and outputs Uplight common format for QC purposes.
        :param str table_name: source table name from `decrypted` path
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_start_raw: timestamp (nullable = true)
            |-- interval_end_raw: timestamp (nullable = true)
            |-- hour_raw: integer (nullable = true)
            |-- date_raw: date (nullable = true)
            |-- interval_start_utc: timestamp (nullable = true)
            |-- interval_end_utc: timestamp (nullable = true)
            |-- hour_utc: integer (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- interval_seconds: integer (nullable = true)
            |-- consumption_code: integer (nullable = true)
            |-- consumption: decimal(16,3) (nullable = true)
            |-- file_name: string (nullable = true)
        """
        codes_actual = [
            'A',    # AEP
            'ACT',  # Duke
        ]
        codes_estimated = [
            'E',    # AEP
            'EST',  # Duke
        ]
        codes_prorated = [
            'PRO',  # Duke
        ]
        codes_missing = [
            'MIS',  # Duke
        ]
        output_columns = [
            'external_location_id',
            'external_account_id',
            'external_channel_id',
            'direction',
            'interval_start_raw',
            'interval_end_raw',
            'hour_raw',
            'date_raw',
            'interval_start_utc',
            'interval_end_utc',
            'hour_utc',
            'date_utc',
            'interval_seconds',
            'consumption_code',
            'consumption',
            'file_name'
        ]
        clean_udf = f.udf(Quality.clean_file_name, StringType())
        return spark.table(table_name) \
            .select(
                f.trim(col(self.col_external_location_id)).alias('external_location_id'),
                col(self.col_external_account_id).alias('external_account_id'),
                col(self.col_external_channel_id).alias('external_channel_id'),
                col(self.col_direction).alias('direction'),
                col(self.col_timestamp).cast('timestamp').alias('interval_end_raw'),
                col(self.col_consumption).cast('decimal(16,3)').alias('consumption'),
                col(self.col_interval).cast('int').alias('interval_seconds'),
                col(self.col_consumption_code).alias('consumption_code')
        ) \
            .withColumn('file_name', clean_udf(f.input_file_name())) \
            .withColumn('file_name_raw', f.input_file_name()) \
            .withColumn(
                'consumption_code',
                (when(col('consumption_code').isin(codes_actual), lit(1))
                    .when(col('consumption_code').isin(codes_estimated), lit(2))
                    .when(col('consumption_code').isin(codes_prorated), lit(3))
                    .when(col('consumption_code').isin(codes_missing), lit(0))
                    .otherwise(lit(None))).cast('int')
        ) \
            .withColumn(
                'interval_start_raw',
                from_unixtime(
                    unix_timestamp(col('interval_end_raw')) - col('interval_seconds')
                ).cast('timestamp')
        ) \
            .withColumn(
                'date_raw',
                to_date(col('interval_start_raw'))
        ) \
            .withColumn(
                'hour_raw',
                hour(col('interval_start_raw'))
        ) \
            .withColumn(
                'interval_start_utc',
                to_utc_timestamp(
                    timestamp=col('interval_start_raw'),
                    tz=self.string_tz_to_utc
                )) \
            .withColumn(
                'interval_end_utc',
                to_utc_timestamp(
                    timestamp=col('interval_end_raw'),
                    tz=self.string_tz_to_utc
                )) \
            .withColumn(
                'date_utc',
                to_date(col('interval_start_utc'))
        ) \
            .withColumn(
                'hour_utc',
                hour(col('interval_start_utc'))
        ) \
            .select(output_columns)

    def get_and_save_ami_summary(self, spark, df_common):
        """Create and save daily AMI ingest summary.
        Raw AMI data is purged after a short time and to faciliate QC related quesions,
        we require a summary of all ingested data for a given execution date for
        historical analysis, should the need arise. This function takes raw data in
        Uplight common format, creates the ami summary and saves it for long-term storage.
        :param df_common: Dataframe with output from `raw_ami_to_common_qc`
        :return: Dataframe with following schema
        |-- external_location_id: string (nullable = true)
        |-- external_account_id: string (nullable = true)
        |-- external_channel_id: string (nullable = true)
        |-- direction: string (nullable = true)
        |-- date_utc: date (nullable = true)
        |-- file_name: string (nullable = true)
        |-- interval_seconds: integer (nullable = true)
        |-- num_reads_actual: integer (nullable = true)
        |-- day_consumption_actual: decimal(16,3) (nullable = true)
        |-- num_reads_estimated: integer (nullable = true)
        |-- day_consumption_estimated: decimal(16,3) (nullable = true)
        |-- num_reads_prorated: integer (nullable = true)
        |-- day_consumption_prorated: decimal(16,3) (nullable = true)
        |-- num_reads_missed: integer (nullable = true)
        |-- day_consumption_missed: decimal(16,3) (nullable = true)
        |-- num_reads_no_code: integer (nullable = true)
        |-- day_consumption_no_code: decimal(16,3) (nullable = true)
        |-- num_reads_total: integer (nullable = true)
        |-- day_consumption_total: decimal(20,3) (nullable = true)
        """
        df = df_common \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('date_utc'),
                col('file_name'),
                col('interval_seconds'),
                col('consumption_code'),
                col('consumption')
            ) \
            .withColumn('actual_read_flag', when(col('consumption_code') == 1, lit(1))) \
            .withColumn('actual_read_consumption', when(col('consumption_code') == 1, col('consumption'))) \
            .withColumn('estimated_read_flag', when(col('consumption_code') == 2, lit(1))) \
            .withColumn('estimated_read_consumption', when(col('consumption_code') == 2, col('consumption'))) \
            .withColumn('prorated_read_flag', when(col('consumption_code') == 3, lit(1))) \
            .withColumn('prorated_read_consumption', when(col('consumption_code') == 3, col('consumption'))) \
            .withColumn('missed_read_flag', when(col('consumption_code') == 0, lit(1))) \
            .withColumn('missed_read_consumption', when(col('consumption_code') == 0, col('consumption'))) \
            .withColumn('no_code_read_flag', when(~(col('consumption_code').isin([0, 1, 2, 3])) |
                                                  (col('consumption_code').isNull()), lit(1))) \
            .withColumn('no_code_consumption', when(~(col('consumption_code').isin([0, 1, 2, 3])) |
                                                    (col('consumption_code').isNull()), col('consumption'))) \
            .groupBy(
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'date_utc',
                'file_name',
                'interval_seconds'
            ) \
            .agg(
                f.sum('actual_read_flag'),
                f.sum('actual_read_consumption'),
                f.sum('estimated_read_flag'),
                f.sum('estimated_read_consumption'),
                f.sum('prorated_read_flag'),
                f.sum('prorated_read_consumption'),
                f.sum('missed_read_flag'),
                f.sum('missed_read_consumption'),
                f.sum('no_code_read_flag'),
                f.sum('no_code_consumption')) \
            .withColumnRenamed('sum(actual_read_flag)', 'num_reads_actual') \
            .withColumnRenamed('sum(actual_read_consumption)', 'day_consumption_actual') \
            .withColumnRenamed('sum(estimated_read_flag)', 'num_reads_estimated') \
            .withColumnRenamed('sum(estimated_read_consumption)', 'day_consumption_estimated') \
            .withColumnRenamed('sum(prorated_read_flag)', 'num_reads_prorated') \
            .withColumnRenamed('sum(prorated_read_consumption)', 'day_consumption_prorated') \
            .withColumnRenamed('sum(missed_read_flag)', 'num_reads_missed') \
            .withColumnRenamed('sum(missed_read_consumption)', 'day_consumption_missed') \
            .withColumnRenamed('sum(no_code_read_flag)', 'num_reads_no_code') \
            .withColumnRenamed('sum(no_code_consumption)', 'day_consumption_no_code') \
            .withColumn('num_reads_actual', col('num_reads_actual').cast(IntegerType())) \
            .withColumn('day_consumption_actual', col('day_consumption_actual').cast(DecimalType(16, 3))) \
            .withColumn('num_reads_estimated', col('num_reads_estimated').cast(IntegerType())) \
            .withColumn('day_consumption_estimated', col('day_consumption_estimated').cast(DecimalType(16, 3))) \
            .withColumn('num_reads_prorated', col('num_reads_prorated').cast(IntegerType())) \
            .withColumn('day_consumption_prorated', col('day_consumption_prorated').cast(DecimalType(16, 3))) \
            .withColumn('num_reads_missed', col('num_reads_missed').cast(IntegerType())) \
            .withColumn('day_consumption_missed', col('day_consumption_missed').cast(DecimalType(16, 3))) \
            .withColumn('num_reads_no_code', col('num_reads_no_code').cast(IntegerType())) \
            .withColumn('day_consumption_no_code', col('day_consumption_no_code').cast(DecimalType(16, 3))) \
            .orderBy('date_utc') \
            .fillna(0, subset=[
                'num_reads_actual', 'num_reads_estimated', 'num_reads_prorated', 'num_reads_missed', 'num_reads_no_code'
            ]) \
            .withColumn(
                'num_reads_total',
                (
                    col('num_reads_actual') +
                    col('num_reads_estimated') +
                    col('num_reads_prorated') +
                    col('num_reads_missed') +
                    col('num_reads_no_code').cast(IntegerType()))) \
            .withColumn(
                'day_consumption_total',
                when(
                    col('day_consumption_actual').isNull(), lit(0)).otherwise(col('day_consumption_actual')) +
                when(
                    col('day_consumption_estimated').isNull(), lit(0)).otherwise(col('day_consumption_estimated')) +
                when(
                    col('day_consumption_prorated').isNull(), lit(0)).otherwise(col('day_consumption_prorated')) +
                when(
                    col('day_consumption_missed').isNull(), lit(0)).otherwise(col('day_consumption_missed')) +
                when(
                    col('day_consumption_no_code').isNull(), lit(0)).otherwise(col('day_consumption_no_code'))
                .cast(DecimalType(16, 3))) \
            .fillna(0, subset=['num_reads_total']) \
            .orderBy(col('external_location_id'))

        log.info('saving ami summary')
        try:
            Quality.save_output(self, df, 1)
            error = 0
        except Exception as e:
            log.error("!! Error saving ami summary, get summary from ami summary dataframe. Error: {e}".format(e=e))
            error = 1
        if error == 0:
            spark.read.parquet(self.s3_path_save_ami_summary).createOrReplaceTempView('ami_summary')
        else:
            df = df.persist()
            df.createOrReplaceTempView('ami_summary')

    def setup_common_ami_summary_raw_to_rollup(self, spark):
        """ Set up data for QC.
        To facilitate QC we require a few data transformations to happen first. They are:
        Raw AMI into Uplight common format.
        AMI summary created and saved.
        Raw to rollup temp view created.
        :param None
        :return: None
        """
        log.info('Getting common ami')
        df_common = Quality.raw_ami_to_common_qc(self, spark, 'decrypted')
        df_common = df_common.persist()
        log.info('Getting and saving ami summary')
        Quality.get_and_save_ami_summary(self, spark, df_common)
        log.info('Getting and setting raw to rollup ami temp view')
        df_channels = Quality.get_channel_ingest_success(spark, 'channel_ingest_success')
        df_raw_to_rollup_ami = Quality.get_raw_to_rollup_ami(self, spark, df_common, df_channels)
        df_raw_to_rollup_ami = df_raw_to_rollup_ami.persist()
        df_raw_to_rollup_ami.createOrReplaceTempView('raw_to_rollup_ami')

    """ Raw AMI Functions """

    def get_manifest(self, spark, table_name):
        """Get manifest data.
        Some tenants pass us a manifest to describe AMI data delivered.
        This function retrieves pertinent data regarding the manifest.
        :param str table_name: source table name from `manifest_keys` path
        :return: Dataframe with following schema
            |-- manifest_filename: string (nullable = true)
            |-- manifest_checksum: string (nullable = true)
            |-- manifest_linecount: integer (nullable = true)
        """
        clean_udf = f.udf(Quality.clean_file_name, StringType())
        if self.manifest_counts_headers is True:
            manifest_header_offset = 1
        else:
            manifest_header_offset = 0

        return spark.table(table_name) \
            .select(
                clean_udf(col(self.col_manifest_filename)).alias('manifest_filename'),
                col(self.col_manifest_checksum).alias('manifest_checksum'),
                col(self.col_manifest_linecount).alias('manifest_linecount')
        ) \
            .withColumn('manifest_linecount', col('manifest_linecount').cast('integer') - lit(manifest_header_offset)) \
            .orderBy(
                col('manifest_filename')
        )

    @staticmethod
    def get_encrypted(spark, table_name):
        """Get encrypted file data.
        For some workflows it is necessary to examine encrypted data sent to Uplight.
        This function retrieves pertinent data regarding the encrypted files.
        :param str table_name: source table name from `encrypted` path
        :return: Dataframe with following schema
            |-- encrypted_filename: string (nullable = true)
        """
        clean_udf = f.udf(Quality.clean_file_name, StringType())
        return spark.table(table_name) \
            .withColumn(
                'encrypted_filename', clean_udf(f.input_file_name()))  \
            .select(
                'encrypted_filename') \
            .distinct()

    def get_decrypted_audit(self, spark, table_name):
        """Get audit data for decrypt step.
        For some workflows it is necessary to examine audit data generated by the AMI ingest.
        This function retrieves pertinent data regarding the audit data for the decrypt step.
        :param str table_name: source table name from `audit` path
        :param str execution_date_tenant_format: airflow execution date in dmY format
        :return: Dataframe with following schema
            |-- audit_filename: string (nullable = true)
            |-- audit_checksum: string (nullable = true)
            |-- audit_linecount: integer (nullable = true)
        """
        if self.raw_ami_has_headers is True:
            decrypt_header_offset = 1
        else:
            decrypt_header_offset = 0

        # Read in json audit
        df = spark.table(table_name) \
            .filter(
                col('filename').like('%{}%'.format(self.execution_date_tenant_format))
            ) \
            .filter(
                col('event_type') == 'DECRYPT_SUCCESS'
            )
        if ('data', 'string') in df.dtypes:
            log.info('data column is a json string, parse with get_json_object')
            df = df \
                .select(
                    col('filename').alias('audit_filename'),
                    get_json_object(col('data'), '$.inDigest').alias('audit_checksum'),
                    get_json_object(col('data'), '$.linesRead').cast('integer').alias('audit_linecount'),
                    from_unixtime((col('timestamp_utc') / 1000)).cast('date').alias('date_utc')
                )
        else:
            log.info('data column is a struct, parse with select dot notation')
            df = df \
                .select(
                    col('filename').alias('audit_filename'),
                    col('data.inDigest').alias('audit_checksum'),
                    col('data.linesRead').cast('integer').alias('audit_linecount'),
                    from_unixtime((col('timestamp_utc') / 1000)).cast('date').alias('date_utc')
                )
        df = df \
            .withColumn('audit_linecount', col('audit_linecount').cast('integer') - lit(decrypt_header_offset)) \
            .orderBy(
                col('audit_filename')
            ) \
            .dropDuplicates()
        latest_ingest_date = df.select(f.max('date_utc')).collect()[0][0]
        df = df \
            .where(
                col('date_utc') == latest_ingest_date
            ) \
            .drop(
                col('date_utc')
            )
        return df

    @staticmethod
    def get_decrypted_summary(spark, table_name):
        """Get decrypted file data.
        For some workflows it is necessary to examine decrypted data sent to Uplight.
        This function retrieves pertinent data regarding the decrypted files.
        :param str table_name: source table name from `ami_summary` path
        :return: Dataframe with following schema
            |-- decrypted_filename: string (nullable = true)
            |-- decrypted_linecount: integer (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('file_name').alias('decrypted_filename'),
                'num_reads_total'
        ) \
            .groupBy('decrypted_filename') \
            .agg(
                (f.sum('num_reads_total')).cast('integer').alias('decrypted_linecount')
        ) \
            .orderBy('decrypted_filename')

    @staticmethod
    def join_decrypt_no_manifest(spark, df_e, df_a, df_d):
        """Join data for tenants who do not provide a manifest.
        Some tenants provide uplight with a manifest file, while others do not.
        For those tenants with no manifest, this function joins pertienent data for QC.
        :param df_e: Dataframe with output from `get_encrypted`
        :param df_a: DataFrame with output from `get_decrypted_audit`
        :param df_s: Dataframe with output from `get_decrypted_summary`
        :return: Dataframe with following schema
            |-- encrypted_filename: string (nullable = true)
            |-- decrypted_filename: string (nullable = true)
            |-- decrypted_linecount: integer (nullable = true)
            |-- audit_filename: string (nullable = true)
            |-- audit_linecount: integer (nullable = true)
            |-- file_mismatch: integer (nullable = false)
            |-- linecount_mismatch: integer (nullable = false)
        """
        df_a = df_a \
            .select(
                col('audit_filename'),
                col('audit_linecount')
            )
        return df_e \
            .join(
                df_d,
                df_e.encrypted_filename ==
                df_d.decrypted_filename, 'left_outer') \
            .join(
                df_a,
                df_e.encrypted_filename ==
                df_a.audit_filename, 'left_outer') \
            .fillna(
                0,
                subset=['audit_linecount', 'decrypted_linecount']) \
            .withColumn(
                'file_mismatch',
                when(col('decrypted_filename').isNull(),
                     lit(1))
                .otherwise(lit(0))) \
            .withColumn(
                'linecount_mismatch',
                when(col('audit_linecount') != col('decrypted_linecount'),
                     lit(1))
                .otherwise(lit(0))) \
            .orderBy('encrypted_filename')

    @staticmethod
    def join_decrypt_manifest(spark, df_m, df_a):
        """Join data for tenants who do provide a manifest.
        Some tenants provide uplight with a manifest file, while others do not.
        For those tenants who provide a manifest, this function joins pertienent data for QC.
        :param df_m: Dataframe with output from `get_manifest`
        :param df_a: DataFrame with output from `get_decrypted_audit`
        :return: Dataframe with following schema
            |-- manifest_filename: string (nullable = true)
            |-- manifest_checksum: string (nullable = true)
            |-- manifest_linecount: integer (nullable = true)
            |-- audit_filename: string (nullable = true)
            |-- audit_checksum: string (nullable = true)
            |-- audit_linecount: integer (nullable = true)
            |-- file_mismatch: integer (nullable = false)
            |-- linecount_mismatch: integer (nullable = false)
            |-- checksum_mismatch: integer (nullable = false)
        """
        return df_m \
            .join(
                df_a,
                df_m.manifest_filename ==
                df_a.audit_filename,
                'left_outer') \
            .fillna(0, subset=['manifest_linecount', 'audit_linecount', 'manifest_checksum', 'audit_checksum']) \
            .withColumn(
                'file_mismatch',
                when(col('audit_filename').isNull(), lit(1)).otherwise(lit(0))) \
            .withColumn(
                'linecount_mismatch',
                when(col('manifest_linecount') != col('audit_linecount'), lit(1)).otherwise(lit(0))) \
            .withColumn(
                'checksum_mismatch',
                when(col('manifest_checksum') != col('audit_checksum'), lit(1)).otherwise(lit(0))) \
            .orderBy('manifest_filename')

    """ Channel Ingest Functions """
    @staticmethod
    def get_distinct_raw_channels(spark, table_name):
        """Retrieve data about raw channels sent by a given tenant.
        This function gets all raw channels sent for a given execution date.
        :param str table_name: source table name from `get_and_save_ami_summary` function
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction')
        ) \
            .distinct()

    @staticmethod
    def get_zeus_channel_map(spark, table_name):
        """Retrieve channel mapping data from Zeus for given tenant.
        To ensure that all channels are mapped correctly, we need to go to Zeus and
        get channel mapping for a given tenant and compare to ingest output.
        :param str table_name: source table name from `extract_zeus_channel_map` task
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- account_uuid: string (nullable = true)
            |-- location_uuid: string (nullable = true)
            |-- channel_uuid: string (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('account_uuid'),
                col('location_uuid'),
                col('channel_uuid')
        )

    @staticmethod
    def get_channel_ingest_success(spark, table_name):
        """Retrieve channel ingest success output for a given execution data.
        For channel ingest QC we must examine success and error output from the task.
        This function gets the success output for QC.
        :param str table_name: source table name from `channel_ingest` task
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- tenant_id: long (nullable = true)
            |-- account_uuid: string (nullable = true)
            |-- location_uuid: string (nullable = true)
            |-- channel_uuid: string (nullable = true)
            |-- time_zone: string (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('tenant_id'),
                col('account_uuid'),
                col('location_uuid'),
                col('channel_uuid'),
                col('time_zone')
        )

    @staticmethod
    def get_distinct_channel_ingest_errors(spark, table_name):
        """Retrieve channel ingest error output for a given execution data.
        For channel ingest QC we must examine error and error output from the task.
        This function gets the error output for QC.
        :param str table_name: source table name from `channel_ingest` task, error output
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction')
        ) \
            .distinct()

    def get_new_channels_ingested_count(self, spark, table_name):
        """Retrieve newly ingested channel count from channel ingest audit.
        As a supplement to QC, it's good to have insight into how many new channels
        are created in a given run. This data from the audit exposes that count.
        :param str table_name: source table name from `audit` path
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
        """
        date_string = self.execution_date_tenant_format
        new_channel_count = spark.table(table_name) \
            .select(
                '*'
        ) \
            .filter(
                col('filename').like('%{}%'.format(date_string))
        ) \
            .filter(
                col('event_type') == 'CHANNEL_INGEST'
        ) \
            .dropDuplicates() \
            .select(
                f.sum(get_json_object(col('data'), '$.channel_ingest')).cast('integer')
        ) \
            .collect()[0][0]

        return new_channel_count

    @staticmethod
    def join_channel_ingest(spark, df_zcid, df_cs, df_cis, df_cie):
        """Join all QC data for channel ingest QC.
        In case of a QC error we need all details regarding channel ingest.
        This function joins together all pertinent data to be exposed as error detail if neccessary.
        :param df_zcid: Dataframe with output from `get_zeus_channel_map`
        :param df_cs: DataFrame with output from `get_distinct_raw_channels`
        :param df_cis: Dataframe with output from `get_channel_ingest_success`
        :param df_cie: Dataframe with output from `get_channel_ingeest_error`
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- ci_success_channel_uuid: string (nullable = true)
            |-- channel_uuid_from_zeus: string (nullable = true)
            |-- success: integer (nullable = false)
            |-- error: integer (nullable = false)
            |-- no_output: integer (nullable = false)
            |-- channel_uuid_match: integer (nullable = false)
            |-- external_channel_multiple_channel_uuid: integer (nullable = false)
        """
        w = Window.partitionBy(
            'summary_external_location_id',
            'summary_external_account_id',
            'summary_external_channel_id',
            'summary_direction'
        )

        df = df_cs \
            .join(df_cis, [
                df_cs.external_location_id == df_cis.external_location_id,
                df_cs.external_account_id == df_cis.external_account_id,
                df_cs.external_channel_id == df_cis.external_channel_id,
                df_cs.direction == df_cis.direction
            ],
                'left_outer') \
            .join(df_cie, [
                df_cs.external_location_id == df_cie.external_location_id,
                df_cs.external_account_id == df_cie.external_account_id,
                df_cs.external_channel_id == df_cie.external_channel_id,
                df_cs.direction == df_cie.direction
            ],
                'left_outer') \
            .join(df_zcid, [
                df_cs.external_location_id == df_zcid.external_location_id,
                df_cs.external_account_id == df_zcid.external_account_id,
                df_cs.external_channel_id == df_zcid.external_channel_id,
                df_cs.direction == df_zcid.direction,
                df_cis.channel_uuid == df_zcid.channel_uuid
            ],
                'left_outer') \
            .select([
                df_cs.external_location_id.alias('summary_external_location_id'),
                df_cs.external_account_id.alias('summary_external_account_id'),
                df_cs.external_channel_id.alias('summary_external_channel_id'),
                df_cs.direction.alias('summary_direction'),
                df_cis.external_location_id.alias('ci_success_external_location_id'),
                df_cis.external_account_id.alias('ci_success_external_account_id'),
                df_cis.external_channel_id.alias('ci_success_external_channel_id'),
                df_cis.direction.alias('ci_success_direction'),
                df_cis.channel_uuid.alias('ci_success_channel_uuid'),
                df_cie.external_location_id.alias('ci_error_external_location_id'),
                df_cie.external_account_id.alias('ci_error_external_account_id'),
                df_cie.external_channel_id.alias('ci_error_external_channel_id'),
                df_cie.direction.alias('ci_error_direction'),
                df_zcid.channel_uuid.alias('channel_uuid_from_zeus')
            ]) \
            .withColumn('success', f.when(
                col('ci_error_external_location_id').isNull() &
                col('ci_success_external_location_id').isNotNull(), lit(1))
                .otherwise(lit(0))) \
            .withColumn('error', f.when(
                col('ci_error_external_location_id').isNotNull() &
                col('ci_success_external_location_id').isNull(), lit(1))
                .otherwise(lit(0))) \
            .withColumn('no_output', f.when(
                col('ci_error_external_location_id').isNull() &
                col('ci_success_external_location_id').isNull(), lit(1))
                .otherwise(lit(0))) \
            .withColumn('channel_uuid_match', f.when(
                col('ci_success_channel_uuid') == col('channel_uuid_from_zeus'), lit(1))
                .otherwise(lit(0))) \
            .withColumn('external_channel_multiple_channel_uuid', f.when(
                f.count('summary_external_location_id').over(w) > 1, lit(1))
                .otherwise(lit(0))) \
            .dropDuplicates() \
            .select(
                col('summary_external_location_id').alias('external_location_id'),
                col('summary_external_account_id').alias('external_account_id'),
                col('summary_external_channel_id').alias('external_channel_id'),
                col('summary_direction').alias('direction'),
                col('ci_success_channel_uuid'),
                col('channel_uuid_from_zeus'),
                col('success'),
                col('error'),
                col('no_output'),
                col('channel_uuid_match'),
                col('external_channel_multiple_channel_uuid')
            )
        return df

    """ Extract Common AMI Functions """
    @staticmethod
    def get_distinct_raw_channels_read_count(spark, table_name):
        """Retrieve all distinct raw channels with read counts.
        This function gets all raw channels sent for a given execution date, with read counts.
        :param str table_name: source table name from `get_and_save_ami_summary` function
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- raw_read_cnt: integer (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('num_reads_total')
        ) \
            .groupBy(
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction') \
            .agg(
                (f.sum('num_reads_total')).cast('integer').alias('raw_read_cnt')
        )

    @staticmethod
    def get_extract_common_ami_success(spark, table_name_1, table_name_2):
        """Retrieve extract common ami success output for a given execution data.
        For extract common ami QC we must examine success and error output from the task.
        This function gets the success output for QC.
        :param str table_name_1: source table name from `extract_common_ami_success` task
        :param str table_name_2: source table name from `channel_ingest_success` task
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- channel_uuid: string (nullable = false)
            |-- eca_success_cnt: long (nullable = true)
        """
        df_1 = spark.table(table_name_1) \
            .select(
                col('channel_uuid')) \
            .groupBy(
                col('channel_uuid')) \
            .count()

        df_2 = spark.table(table_name_2) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('channel_uuid')) \
            .distinct()

        df = df_1 \
            .join(
                df_2, ['channel_uuid'], 'left_outer') \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('channel_uuid'),
                col('count')) \
            .groupBy(
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction') \
            .agg(
                concat_ws(', ', collect_set('channel_uuid')).alias('channel_uuid'),
                f.sum('count').alias('eca_success_cnt'))

        return df

    @staticmethod
    def get_extract_common_ami_error(spark, table_name):
        """Retrieve extract common ami error output for a given execution date.
        For extract common ami QC we must examine success and error output from the task.
        This function gets the error output for QC.
        :param str table_name: source table name from `extract_common_ami` task, error output
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- eca_error_cnt: long (nullable = false)
        """
        return spark.table(table_name) \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction')) \
            .groupBy(
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction') \
            .count() \
            .withColumnRenamed('count', 'eca_error_cnt')

    @staticmethod
    def join_extract_common_ami(spark, df_d, df_es, df_ee):
        """Join all QC data for extract common ami QC.
        In case of a QC error we need all details regarding extract common ami.
        This function joins together all pertinent data to be exposed as error detail if neccessary.
        :param df_d: Dataframe with output from `get_distinct_raw_channels_read_count`
        :param df_es: DataFrame with output from `get_extract_common_ami_success`
        :param df_ee: Dataframe with output from `get_extract_common_ami_error`
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- raw_read_cnt: integer (nullable = true)
            |-- channel_uuid: string (nullable = true)
            |-- eca_success_cnt: long (nullable = true)
            |-- eca_error_cnt: long (nullable = true)
            |-- eca_total_cnt: long (nullable = true)
        """
        return df_d \
            .join(
                df_es,
                [
                    'external_location_id',
                    'external_account_id',
                    'external_channel_id',
                    'direction'
                ],
                'left_outer') \
            .join(
                df_ee,
                [
                    'external_location_id',
                    'external_account_id',
                    'external_channel_id',
                    'direction'
                ],
                'left_outer') \
            .fillna(
                0,
                subset=[
                    'eca_success_cnt',
                    'eca_error_cnt'
                ]) \
            .withColumn(
                'eca_total_cnt', col('eca_success_cnt') + col('eca_error_cnt')) \
            .orderBy('external_location_id')

    """ Load Common AMI Step """
    @staticmethod
    def get_extract_common_ami_success_by_utc_date(spark, table_name):
        """Retrieve extract common ami success output for a given UTC date.
        For load common ami QC we must examine success output from extract common ami by UTC date.
        This function gets the success output for QC, by UTC date.
        :param str table_name: source table name from `extract_common_ami_success` task
        :return: Dataframe with following schema
            |-- tenant_id: long (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- eca_success_cnt: integer (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('tenant_id'),
                to_date(concat_ws('-', col('year'), col('month'), col('day'))).alias('date_utc')) \
            .groupBy(
                col('tenant_id'),
                col('date_utc')) \
            .count() \
            .withColumnRenamed('count', 'eca_success_cnt') \
            .select(
                col('tenant_id'),
                col('date_utc'),
                col('eca_success_cnt').cast('integer')
        )

    @staticmethod
    def get_load_common_ami_success(spark, table_name):
        """Retrieve load common ami success output for a given execution data.
        For load common ami QC we must examine success and error output from the task.
        This function gets the success output for QC.
        :param str table_name: source table name from `load_common_ami_success` task
        :return: Dataframe with following schema
            |-- tenant_id: long (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- lca_success_cnt: integer (nullable = true)
        """
        return spark.table(table_name) \
            .select(
                col('tenant_id'),
                col('date_utc'),
                col('row_count').cast('integer').alias('lca_success_cnt')
        )

    @staticmethod
    def join_load_common_ami(spark, df_e, df_l):
        """Join all QC data for load common ami QC.
        In case of a QC error we need all details regarding load common ami.
        This function joins together all pertinent data to be exposed as error detail if neccessary.
        :param df_ecam: Dataframe with output from `get_extract_common_ami_success_by_utc_date`
        :param df_lcam: Dataframe with output from `get_load_common_ami_success`
        :return: Dataframe with following schema
            |-- tenant_id: long (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- eca_success_cnt: integer (nullable = false)
            |-- lca_success_cnt: integer (nullable = true)
        """
        df = \
            df_e \
            .select(
                col('tenant_id'),
                col('date_utc')) \
            .union(
                df_l
                .select(
                    col('tenant_id'),
                    col('date_utc'))
            ) \
            .distinct()

        return df \
            .join(df_e, ['tenant_id', 'date_utc'], 'left_outer') \
            .join(df_l, ['tenant_id', 'date_utc'], 'left_outer') \
            .select(
                col('tenant_id'),
                col('date_utc'),
                col('eca_success_cnt'),
                col('lca_success_cnt')
            ) \
            .fillna(
                0,
                subset=[
                    'eca_success_cnt',
                    'lca_success_cnt'
                ]) \
            .orderBy('date_utc')

    """ Raw to Rollup Functions """
    def get_raw_to_rollup_ami(self, spark, df_common, df_channels):
        """Add channel uuid and local dates to common format for raw to rollup QC.
        To QC AMI data rolled up for use by Energize, we need our common format with the
        addition of channel_uuid and local date information. This function pulls together this data.
        :param df_common: Dataframe with output from `raw_ami_to_common_qc`
        :param df_channels: Dataframe with output from `get_channel_ingest_success`
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_start_raw: timestamp (nullable = true)
            |-- interval_end_raw: timestamp (nullable = true)
            |-- hour_raw: integer (nullable = true)
            |-- date_raw: date (nullable = true)
            |-- interval_start_utc: timestamp (nullable = true)
            |-- interval_end_utc: timestamp (nullable = true)
            |-- hour_utc: integer (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- interval_seconds: integer (nullable = true)
            |-- consumption_code: integer (nullable = true)
            |-- consumption: decimal(16,3) (nullable = true)
            |-- file_name: string (nullable = true)
            |-- time_zone: string (nullable = true)
            |-- channel_uuid: string (nullable = true)
            |-- interval_start_local: timestamp (nullable = true)
            |-- date_local: date (nullable = true)
        """
        df_channels = df_channels \
            .select(
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('time_zone'),
                col('channel_uuid')
            )

        df = df_common \
            .join(
                df_channels, [
                    'external_location_id',
                    'external_account_id',
                    'external_channel_id',
                    'direction'
                ], 'left_outer') \
            .filter(
                col('channel_uuid').isNotNull()
            ) \
            .withColumn(
                'interval_start_local',
                from_utc_timestamp(
                    timestamp=col('interval_start_utc'),
                    tz=col('time_zone')
                )) \
            .withColumn(
                'date_local',
                to_date(col('interval_start_local'))
            ) \
            .filter(
                col('direction') == 'D'
            )
        return df

    @staticmethod
    def get_raw_to_mdis_hour(spark, table_name):
        """Retrieve random channel-hour for QC.
        Roll up raw reads using only 'actual' reads (consumption code == 1).
        Attempt to find channel-hours with > ROLLUP_QC_CNT complete consumption.
        If found, choose random channel-hour for QC.
        If not found, choose channel-hour with the most complete consumption.
        :param df: Dataframe with output from `get_raw_to_rollup_ami`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_seconds: string (nullable = true)
            |-- date_raw: date (nullable = true)
            |-- hour_raw: integer (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- hour_utc: integer (nullable = true)
            |-- hourly_consumption: decimal(16,3) (nullable = true)
            |-- hourly_read_cnt: long (nullable = false)
        """
        # Roll up raw reads to get hourly consumption and read count
        df_chr = spark.table(table_name) \
            .select(
                col('channel_uuid'),
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('interval_seconds'),
                col('date_raw'),
                col('hour_raw'),
                col('date_utc'),
                col('hour_utc'),
                col('consumption')
        ) \
            .filter(col('consumption_code') == 1) \
            .groupBy(
                'channel_uuid',
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'interval_seconds',
                'date_raw',
                'hour_raw',
                'date_utc',
                'hour_utc'
        ) \
            .agg(
                (f.sum('consumption').cast(DecimalType(16, 3))).alias('hourly_consumption'),
                (f.count("*")).alias('hourly_read_cnt')
        ) \
            .withColumn('seconds_reads', concat(col('interval_seconds'), col('hourly_read_cnt'))) \
            .filter(col('seconds_reads').isin(['9004', '18002'])) \
            .drop(col('seconds_reads'))
        # Roll up further by `date_raw` and `hour_raw` to determine complete channel-hours
        df_chp = df_chr \
            .select(
                col('date_raw'),
                col('hour_raw')
            ) \
            .groupBy(
                col('date_raw'),
                col('hour_raw')
            ) \
            .agg(
                (f.count("*").cast('integer')).alias('channel_hour_count')
            ) \
            .orderBy(
                    col('channel_hour_count').desc()
            )
        # If channel-hour max reads < ROLLUP_QC_CNT pick channel-hour with max reads,
        # else return a random channel-hour with > ROLLUP_QC_CNT reads for QC
        max_reads = df_chp \
            .orderBy(
                col('channel_hour_count').desc()
            ) \
            .select(
                col('channel_hour_count')
            ) \
            .limit(1) \
            .collect()[0][0]

        if max_reads >= ROLLUP_QC_CNT:
            df_chp = df_chp \
                .filter(col('channel_hour_count') > ROLLUP_QC_CNT) \
                .select(
                    col('date_raw'),
                    col('hour_raw'),
                    col('channel_hour_count')
                ) \
                .orderBy(rand()) \
                .limit(1)

            qc_date = df_chp.select(col('date_raw')).collect()[0][0]
            qc_hour = df_chp.select(col('hour_raw')).collect()[0][0]
            channel_hour_cnt = df_chp.select(col('channel_hour_count')).collect()[0][0]

            log.info('QC Day for Hourly Rollups: {}'.format(qc_date))
            log.info('QC Hour for Hourly Rollups: {}'.format(qc_hour))
            log.info('Channel Hours for QC: {}'.format(channel_hour_cnt))

        else:
            df_chp = df_chp \
                .select(
                    col('date_raw'),
                    col('hour_raw'),
                    col('channel_hour_count')
                ) \
                .orderBy(
                    col('channel_hour_count').desc()
                ) \
                .limit(1)

            qc_date = df_chp.select(col('date_raw')).collect()[0][0]
            qc_hour = df_chp.select(col('hour_raw')).collect()[0][0]
            channel_hour_cnt = df_chp.select(col('channel_hour_count')).collect()[0][0]

            log.info('QC Day for Hourly Rollups: {}'.format(qc_date))
            log.info('QC Hour for Hourly Rollups: {}'.format(qc_hour))
            log.info('Channel Hours for QC: {}'.format(channel_hour_cnt))
        # Filter by chosen `date_raw` and `hour_raw` to get ROLLUP_QC_CNT channel-hours for QC
        df_raw_hour = df_chr \
            .select(
                col('channel_uuid'),
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('interval_seconds'),
                col('date_raw'),
                col('hour_raw'),
                col('date_utc'),
                col('hour_utc'),
                col('hourly_consumption'),
                col('hourly_read_cnt')
            ) \
            .filter(col('date_raw') == qc_date) \
            .filter(col('hour_raw') == qc_hour)  \
            .orderBy(rand())  \
            .limit(ROLLUP_QC_CNT)

        return df_raw_hour

    def get_mdis_hour(self, spark, df_raw_hour):
        """Retrive MDIS channel-hour data for QC.
        Get hourly rollup from mdis-daily, use meta file to find our randomly chosen channel-hour.
        :param df_raw_hour: Dataframe with output from `get_raw_to_mdis_hour`
        :param str mdis_daily_meta: source table name for `mdis_daily_meta`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- daily_consumption_from_mdis: decimal(16,3) (nullable = true)
        """
        # Get params to pass to mdis-daily
        channel_uuid_list = df_raw_hour.select("channel_uuid").rdd.flatMap(lambda x: x).collect()
        date = df_raw_hour \
            .select(
                col('date_utc')
            ) \
            .distinct() \
            .collect()[0][0]
        hour = df_raw_hour \
            .select(
                col('hour_utc')
            ) \
            .distinct() \
            .collect()[0][0]
        # Create hour string
        if hour < 10:
            hour_column = 'kwh_h0{}'.format(hour)
        else:
            hour_column = 'kwh_h{}'.format(hour)
        # Set mdis-daily meta path
        daily_meta_path = self.s3_path_mdis_daily_meta
        year = date.year
        month = date.month
        day = date.day
        # Get mdis-daily meta bucket
        df_meta = spark.read.csv(daily_meta_path, header=False)
        daily_meta_bucket = df_meta \
            .filter(col('_c0') == '{}'.format(date)) \
            .select('_c1') \
            .collect()[0][0]
        # Set mdis-daily path
        daily_path = os.path.join(
            self.s3_path_load_mdis_daily, '{}/tenant_id={}/year={}/month={}/day={}/*/*.parquet'
            .format(daily_meta_bucket, self.tenant_id, year,  month, day)
        )
        # Get MDIS hourly rollup from mdis-daily
        # for chosen channel_uuid list, date and hour
        df_mdis = spark.read.parquet(daily_path)
        df_mdis = df_mdis \
            .filter(col('date_utc') == '{}'.format(date)) \
            .filter(col('channel_uuid').isin(channel_uuid_list)) \
            .select(
               col('channel_uuid'),
               col(hour_column).alias('hourly_consumption_from_mdis').cast(DecimalType(16, 3))
            )
        return df_mdis

    @staticmethod
    def join_raw_to_mdis_hour(spark, df_raw_hour, df_mdis_hour):
        """Join all QC data for raw to mdis hour QC.
        In case of a QC error we need all details regarding raw to mdis hour QC.
        This function joins together all pertinent data to be exposed as error detail if neccessary.
        :param df_raw_day: Dataframe with output from `get_raw_to_mdis_hour`
        :param df_mdis_hour: Dataframe with output from  `get_mdis_hour`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_seconds: integer (nullable = true)
            |-- date_raw: date (nullable = true)
            |-- hour_raw: integer (nullable = true)
            |-- date_utc: date (nullable = true)
            |-- hour_utc: integer (nullable = true)
            |-- hourly_consumption: decimal(16,3) (nullable = true)
            |-- hourly_read_cnt: long (nullable = false)
            |-- hourly_consumption_from_mdis: decimal(16,3) (nullable = true)
            |-- qc_status: integer (nullable = false)
                    """
        return df_raw_hour \
            .join(df_mdis_hour, [
                'channel_uuid'],
                'left_outer'
            ) \
            .withColumn(
                'qc_status',
                when(
                    (f.abs(col('hourly_consumption') - col('hourly_consumption_from_mdis')) < CONSUMPTION_EPSILON),
                    lit(1)
                ).otherwise(
                        lit(0)
                    )
            )

    @staticmethod
    def get_raw_to_mdis_day(spark, table_name):
        """Retrieve random channel-day for QC.
        Roll up raw reads using only 'actual' reads (consumption code == 1).
        Attempt to find channel-day with > ROLLUP_QC_CNT complete consumption.
        If found, choose random channel-day for QC.
        If not found, choose channel-day with the most complete consumption.
        :param df: Dataframe with output from `get_raw_to_rollup_ami`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_seconds: string (nullable = true)
            |-- date_local: date (nullable = true)
            |-- daily_consumption: decimal(16,3) (nullable = true)
            |-- daily_read_cnt: long (nullable = false)
        """
        # Roll up raw reads to get daily consumption and read count
        df_chr = spark.table(table_name) \
            .select(
                col('channel_uuid'),
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('interval_seconds'),
                col('date_local'),
                col('consumption')
        ) \
            .filter(col('consumption_code') == 1) \
            .groupBy(
                'channel_uuid',
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'interval_seconds',
                'date_local'
        ) \
            .agg(
                (f.sum('consumption').cast(DecimalType(16, 3))).alias('daily_consumption'),
                (f.count("*").alias('count')).alias('daily_read_cnt')
        ) \
            .withColumn('seconds_reads', concat(col('interval_seconds'), col('daily_read_cnt')))  \
            .filter(col('seconds_reads').isin(['90096', '180048'])) \
            .drop(col('seconds_reads'))
        # Roll up further by `date_local` to determine complete channel-days
        df_chp = df_chr \
            .select(
                col('date_local')
            ) \
            .groupBy(
                col('date_local')
            ) \
            .agg(
                (f.count("*").alias('count').cast('integer')).alias('channel_day_count')
            ) \
            .orderBy(
                    col('channel_day_count').desc()
            )
        # If channel-day max reads < ROLLUP_QC_CNT pick channel-day with max reads,
        # else return a random channel-day with > ROLLUP_QC_CNT reads for QC
        max_reads = df_chp \
            .orderBy(
                col('channel_day_count').desc()
            ) \
            .select(
                col('channel_day_count')
            ) \
            .limit(1) \
            .collect()[0][0]

        if max_reads > ROLLUP_QC_CNT:
            df_chp = df_chp \
                .filter(
                    col('channel_day_count') > ROLLUP_QC_CNT
                ) \
                .select(
                    col('date_local'),
                    col('channel_day_count')
                ) \
                .orderBy(rand()) \
                .limit(1)
            qc_date = df_chp.select(col('date_local')).collect()[0][0]
            channel_day_cnt = df_chp.select(col('channel_day_count')).collect()[0][0]
            log.info('QC Day for Daily Rollups: {}'.format(qc_date))
            log.info('Channel Days for QC: {}'.format(channel_day_cnt))
        else:
            df_chp = df_chp \
                .select(
                    col('date_local'),
                    col('channel_day_count')
                ) \
                .orderBy(
                    col('channel_day_count').desc()
                ) \
                .limit(1)
            qc_date = df_chp.select(col('date_local')).collect()[0][0]
            channel_day_cnt = df_chp.select(col('channel_day_count')).collect()[0][0]
            log.info('QC Day for Daily Rollups: {}'.format(qc_date))
            log.info('Channel Days for QC: {}'.format(channel_day_cnt))
        # Filter by chosen `date_local` to get ROLLUP_QC_CNT channel-days for QC
        df_raw_day = df_chr \
            .select(
                col('channel_uuid'),
                col('external_location_id'),
                col('external_account_id'),
                col('external_channel_id'),
                col('direction'),
                col('interval_seconds'),
                col('date_local'),
                col('daily_consumption'),
                col('daily_read_cnt')
            ) \
            .filter(col('date_local') == qc_date) \
            .limit(ROLLUP_QC_CNT)

        return df_raw_day

    def get_mdis_day(self, spark, df_raw_day):
        """Retrive MDIS channel-day data for QC.
        Get daily rollup from mdis-monthly, use meta file to find our randomly chosen channel-day.
        :param df_raw_day: Dataframe with output from `get_raw_to_mdis_day`
        :param str mdis_daily_meta: source table name for `mdis_monthly_meta`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- daily_consumption_from_mdis: decimal(16,3) (nullable = true)
        """
        # Get params to pass to mdis-monthly
        channel_uuid_list = df_raw_day.select("channel_uuid").rdd.flatMap(lambda x: x).collect()
        year = df_raw_day \
            .select(
                f.year(
                    col('date_local')
                )
            ) \
            .distinct() \
            .collect()[0][0]
        month = df_raw_day \
            .select(
                f.month(
                    col('date_local')
                )
            ) \
            .distinct() \
            .collect()[0][0]
        day = df_raw_day \
            .select(
                f.dayofmonth(
                    col('date_local')
                )
            ) \
            .distinct() \
            .collect()[0][0]
        # Create day string
        if day < 10:
            day_column = 'kwh_d0{}'.format(day)
        else:
            day_column = 'kwh_d{}'.format(day)
        # Create month string
        if month < 10:
            month_string = '0{}'.format(month)

        else:
            month_string = '{}'.format(month)
        # Set meta path
        monthly_meta_path = self.s3_path_mdis_monthly_meta
        # Get meta bucket
        df_meta = spark.read.csv(monthly_meta_path, header=False)
        monthly_meta_bucket = df_meta \
            .filter(col('_c0') == '{}-{}'.format(year, month_string)) \
            .select('_c1') \
            .collect()[0][0]
        # Set mdis-monthly path
        monthly_path = os.path.join(
            self.s3_path_load_mdis_monthly, '{}/tenant_id={}/local_year={}/local_month={}/*/*.parquet'
            .format(monthly_meta_bucket, self.tenant_id, year,  month)
        )
        # Get MDIS daily rollup from mdis-monthly
        df_mdis = spark.read.parquet(monthly_path)
        df_mdis = df_mdis \
            .filter(col('local_year') == '{}'.format(year)) \
            .filter(col('local_month') == '{}'.format(month)) \
            .filter(col('channel_uuid').isin(channel_uuid_list)) \
            .select(
                col('channel_uuid'),
                col(day_column).alias('daily_consumption_from_mdis').cast(DecimalType(16, 3))
            )
        return df_mdis

    @staticmethod
    def join_raw_to_mdis_day(spark, df_raw_day, df_mdis_day):
        """Join all QC data for raw to mdis day QC.
        In case of a QC error we need all details regarding raw to mdis day QC.
        This function joins together all pertinent data to be exposed as error detail if neccessary.
        :param df_raw_day: Dataframe with output from `get_raw_to_mdis_day`
        :param df_mdis_day: Dataframe with output from `get_mdis_day`
        :return: Dataframe with following schema
            |-- channel_uuid: string (nullable = true)
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
            |-- direction: string (nullable = true)
            |-- interval_seconds: string (nullable = true)
            |-- date_local: date (nullable = true)
            |-- daily_consumption: decimal(16,3) (nullable = true)
            |-- daily_read_cnt: long (nullable = false)
            |-- daily_consumption_from_mdis: decimal(16,3) (nullable = true)
            |-- qc_status: integer (nullable = false)
        """
        return df_raw_day \
            .join(df_mdis_day, [
                'channel_uuid'],
                'left_outer'
            ) \
            .withColumn(
                'qc_status',
                when(
                    (f.abs(col('daily_consumption') - col('daily_consumption_from_mdis')) < CONSUMPTION_EPSILON),
                    lit(1)
                ).otherwise(
                        lit(0)
                    )
            )

    """ Ingest Time """
    def get_ingest_time(self, spark):
        """For tenants who deliever a manifest, get ingest time data.
        :return: Dataframe with following schema
            |-- manifest_time: string (nullable = false)
            |-- ingest_end_time: string (nullable = false)
            |-- ingest_seconds: int (nullable = false)
        """
        if self.has_manifest is True:
            df_ingest_time = spark.read.json(self.s3_path_ingest_time) \
                    .withColumn('qc_status', when(col('ingest_seconds') > 86400, lit(0)).otherwise(lit(1))) \
                    .select(
                        col('manifest_time'),
                        col('ingest_end_time'),
                        col('ingest_seconds').cast('integer'),
                        col('qc_status')
                    )

            return df_ingest_time

    # step definitions
    def decrypt(self, spark):
        """Run QC for decrypt step.
        This is the decrypt function for the AMI QC task. It runs QC for the decrypt step.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        if self.has_manifest is True:  # run for manifest
            # get dataframes
            log.info('getting manifest')
            df_m = Quality.get_manifest(self, spark, 'manifest')
            log.info('getting audit data')
            df_a = Quality.get_decrypted_audit(self, spark, 'audit')
            log.info('join manifest with decrypted audit')
            df_error = Quality.join_decrypt_manifest(spark, df_m, df_a)
            # set variables
            manifest_checksum_list = df_m.select("manifest_checksum").rdd.flatMap(lambda x: x).collect()
            manifest_checksum_list.sort()
            manifest_checksum_string = ''.join(manifest_checksum_list).encode('utf-8')
            audit_checksum_list = df_a.select("audit_checksum").rdd.flatMap(lambda x: x).collect()
            audit_checksum_list.sort()
            audit_checksum_string = ''.join(audit_checksum_list).encode('utf-8')
            # set left and right data
            left_data_file_cnt = df_m.count()
            right_data_file_cnt = df_a.count()
            left_data_row_cnt = df_m.agg({"manifest_linecount": "sum"}).collect()[0][0]
            right_data_row_cnt = df_a.agg({"audit_linecount": "sum"}).collect()[0][0]
            left_data_checksum = int(hashlib.md5(manifest_checksum_string).hexdigest(), 16)
            right_data_checksum = int(hashlib.md5(audit_checksum_string).hexdigest(), 16)

        else:  # run for no manifest
            # get dataframes
            log.info('getting encrypted data')
            df_e = Quality.get_encrypted(spark, 'encrypted')
            log.info('getting audit data')
            df_a = Quality.get_decrypted_audit(self, spark, 'audit')
            log.info('getting decryped data from summary')
            df_d = Quality.get_decrypted_summary(spark, 'ami_summary')
            log.info('join decrypted without manifest to get error df')
            df_error = Quality.join_decrypt_no_manifest(spark, df_e, df_a, df_d)
            # set left and right data
            left_data_file_cnt = df_e.count()
            right_data_file_cnt = df_d.count()
            left_data_row_cnt = df_d.agg({"decrypted_linecount": "sum"}).collect()[0][0]
            right_data_row_cnt = df_a.agg({"audit_linecount": "sum"}).collect()[0][0]
            left_data_checksum = 0
            right_data_checksum = 0
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': left_data_file_cnt,
                    'right_data': right_data_file_cnt
                },
                2: {
                    'left_data': left_data_row_cnt,
                    'right_data': right_data_row_cnt
                },
                3: {
                    'left_data': left_data_checksum,
                    'right_data': right_data_checksum
                }
            },
            'reference': {
                    'ami_summary': self.s3_path_save_ami_summary,
                    'audit': self.s3_path_audit,
                    'decrypted': self.s3_path_decrypt,
                    'encrypted': self.s3_path_encrypt,
                    'manifest': self.s3_path_manifest
            },
            'misc': {
                1: {
                    'name': 'Has Manifest',
                    'value': self.has_manifest
                },
                2: {
                    'name': 'Manifest Counts Headers',
                    'value': self.manifest_counts_headers
                },
                3: {
                    'name': 'Raw AMI Has Headers',
                    'value': self.raw_ami_has_headers
                }
            }
                }
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 1, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def channel_ingest(self, spark):
        """Run QC for channel ingest step.
        This is the channel ingest function for the AMI QC task. It runs QC for the channel ingest step.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        # get dataframes
        log.info('getting zeus channel map')
        df_zcid = Quality.get_zeus_channel_map(spark, 'zeus_channel_map')
        log.info('getting distinct decrypted channels from ami summmary')
        df_cs = Quality.get_distinct_raw_channels(spark, 'ami_summary')
        log.info('getting channel ingest success')
        df_cis = Quality.get_channel_ingest_success(spark, 'channel_ingest_success')
        log.info('getting distinct channels from channel ingest error')
        df_cie = Quality.get_distinct_channel_ingest_errors(spark, 'channel_ingest_error')
        log.info('join channel ingest qc data to get error df')
        df_error = Quality.join_channel_ingest(spark, df_zcid, df_cs, df_cis, df_cie)
        # set variables
        raw_channel_cnt = df_cs.count()
        channel_ingest_success_cnt = df_cis.count()
        channel_ingest_error_cnt = df_cie.count()
        correctly_mapped_channel_cnt = df_error \
            .filter(col('channel_uuid_match') == 1) \
            .count()
        multiple_channels_mapped_cnt = df_error \
            .filter(col('external_channel_multiple_channel_uuid') == 1) \
            .select(
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction'
            ) \
            .distinct() \
            .count()
        new_channels_ingested_cnt = Quality.get_new_channels_ingested_count(self, spark, 'audit')
        # set left and right data
        left_data_channel_cnt = raw_channel_cnt
        right_data_channel_cnt = (channel_ingest_success_cnt + channel_ingest_error_cnt) - multiple_channels_mapped_cnt
        left_data_multiple_channel_cnt = 0
        right_data_multiple_channel_cnt = multiple_channels_mapped_cnt
        left_data_success_channel_map_cnt = channel_ingest_success_cnt
        right_data_success_channel_map_cnt = correctly_mapped_channel_cnt
        # get airflow success output paths from temp view
        channel_ingest_success_path = Quality.get_file_path_from_temp_view(spark, 'channel_ingest_success')
        zeus_channel_map_path = Quality.get_file_path_from_temp_view(spark, 'zeus_channel_map')
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': left_data_channel_cnt,
                    'right_data': right_data_channel_cnt
                },
                2: {
                    'left_data': left_data_multiple_channel_cnt,
                    'right_data': right_data_multiple_channel_cnt
                },
                3: {
                    'left_data': left_data_success_channel_map_cnt,
                    'right_data': right_data_success_channel_map_cnt
                }
            },
            'reference': {
                    'ami_summary': self.s3_path_save_ami_summary,
                    'channel_ingest_error': self.s3_path_channel_ingest_error,
                    'channel_ingest_success': channel_ingest_success_path,
                    'zeus_channel_mapping': zeus_channel_map_path
                },
            'misc': {
                1: {
                    'name': 'Raw Channel Count',
                    'value': raw_channel_cnt
                },
                2: {
                    'name': 'Channel Ingest Success Count',
                    'value': channel_ingest_success_cnt
                },
                3: {
                    'name': 'Channel Ingest Error Count',
                    'value': channel_ingest_error_cnt
                },
                4: {
                    'name': 'Newly Ingested Channels',
                    'value': new_channels_ingested_cnt
                }
            }
        }
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 2, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def extract_common_ami(self, spark):
        """Run QC for extract common ami step.
        This is the extract common ami function for the AMI QC task. It runs QC for the extract common ami step.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        # get dataframes
        log.info('getting distinct decrypted channels with read count')
        df_d = Quality.get_distinct_raw_channels_read_count(spark, 'ami_summary')
        log.info('getting extract common ami success read count by channel')
        df_es = Quality.get_extract_common_ami_success(spark, 'extract_common_ami_success', 'channel_ingest_success')
        log.info('getting extract common ami error read count by channel')
        df_ee = Quality.get_extract_common_ami_error(spark, 'extract_common_ami_error')
        log.info('join extract common ami qc data to get error df')
        df_error = Quality.join_extract_common_ami(spark, df_d, df_es, df_ee)
        # set variables
        raw_read_cnt = df_d.agg({"raw_read_cnt": "sum"}).collect()[0][0]
        extract_common_ami_success_cnt = df_es.agg({"eca_success_cnt": "sum"}).collect()[0][0]
        extract_common_ami_success_cnt = extract_common_ami_success_cnt if extract_common_ami_success_cnt else 0
        extract_common_ami_error_cnt = df_ee.agg({"eca_error_cnt": "sum"}).collect()[0][0]
        extract_common_ami_error_cnt = extract_common_ami_error_cnt if extract_common_ami_error_cnt else 0
        # set left and right data
        left_data_read_cnt = raw_read_cnt
        right_data_read_cnt = extract_common_ami_success_cnt + extract_common_ami_error_cnt
        # get airflow success output paths from temp view
        channel_ingest_success_path = Quality.get_file_path_from_temp_view(spark, 'channel_ingest_success')
        extract_common_ami_success_path = Quality.get_file_path_from_temp_view(spark, 'extract_common_ami_success')
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': left_data_read_cnt,
                    'right_data': right_data_read_cnt
                },
            },
            'reference': {
                'ami_summary': self.s3_path_save_ami_summary,
                'channel_ingest_success': channel_ingest_success_path,
                'extract_common_ami_error': self.s3_path_extract_common_ami_error,
                'extract_common_ami_success': extract_common_ami_success_path
            },
            'misc': {
                1: {
                    'name': 'Extract Common AMI Success Count',
                    'value': extract_common_ami_success_cnt
                },
                2: {
                    'name': 'Extract Common AMI Error Count',
                    'value': extract_common_ami_error_cnt
                }
            }
        }
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 3, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def load_common_ami(self, spark):
        """Run QC for load common ami step.
        This is the load common ami function for the AMI QC task. It runs QC for the extract load ami step.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        # get dataframes
        log.info('getting extract common ami success')
        df_e = Quality.get_extract_common_ami_success_by_utc_date(spark, 'extract_common_ami_success')
        log.info('getting load common ami success')
        df_l = Quality.get_load_common_ami_success(spark, 'load_common_ami_success')
        log.info('join load common ami qc data to get error df')
        df_error = Quality.join_load_common_ami(spark, df_e, df_l)
        # set left and right data
        extract_common_ami_success_cnt = df_e.agg({"eca_success_cnt": "sum"}).collect()[0][0]
        load_common_ami_success_cnt = df_l.agg({"lca_success_cnt": "sum"}).collect()[0][0]
        # get airflow success output paths from temp view
        extract_common_ami_success_path = Quality.get_file_path_from_temp_view(spark, 'extract_common_ami_success')
        load_common_ami_success_path = Quality.get_file_path_from_temp_view(spark, 'load_common_ami_success')
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': extract_common_ami_success_cnt,
                    'right_data': load_common_ami_success_cnt
                }
            },
            'reference': {
                    'common': self.s3_path_common,
                    'extract_common_ami_success': extract_common_ami_success_path,
                    'load_common_ami_success': load_common_ami_success_path
            },
            'misc': {}
        }
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 4, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def raw_to_mdis_hour(self, spark):
        """Run QC for raw to mdis hour ami step.
        This is the raw to mdis hour function for the AMI QC task. It runs QC for raw to mdis hour.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        # get dataframes
        log.info('getting random channel hour data to qc')
        df_raw_hour = Quality.get_raw_to_mdis_hour(spark, 'raw_to_rollup_ami')
        df_raw_hour = df_raw_hour.persist()
        log.info('getting mdis hour reads for qc')
        df_mdis_hour = Quality.get_mdis_hour(self, spark, df_raw_hour)
        df_error = Quality.join_raw_to_mdis_hour(spark, df_raw_hour, df_mdis_hour)
        # get left and right data
        random_channel_hour_cnt = df_error.count()
        channel_hour_qc_pass_cnt = df_error.filter(col('qc_status') == 1).count()
        # get airflow success output paths from temp view
        channel_ingest_success_path = Quality.get_file_path_from_temp_view(spark, 'channel_ingest_success')
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': random_channel_hour_cnt,
                    'right_data': channel_hour_qc_pass_cnt
                }
            },
            'reference': {
                    'channel_ingest_success': channel_ingest_success_path,
                    'common': self.s3_path_common,
                    'hourly_intermediate': self.s3_path_rollup_hourly_intermediate,
                    'mdis_daily_meta': self.s3_path_mdis_daily_meta,
                    'mdis_daily': self.s3_path_load_mdis_daily
            },
            'misc': {}
        }
        # save QC details to s3
        Quality.save_output(self, df_error, 3)
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 5, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def raw_to_mdis_day(self, spark):
        """Run QC for raw to mdis day ami step.
        This is the raw to mdis day function for the AMI QC task. It runs QC for raw to mdis day.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        # get dataframes
        log.info('getting random channel day data to qc')
        df_raw_day = Quality.get_raw_to_mdis_day(spark, 'raw_to_rollup_ami')
        df_raw_day = df_raw_day.persist()
        log.info('getting mdis day reads for qc')
        df_mdis_day = Quality.get_mdis_day(self, spark, df_raw_day)
        log.info('joining raw to rollup data')
        df_error = Quality.join_raw_to_mdis_day(spark, df_raw_day, df_mdis_day)
        # get left and right data
        random_channel_day_cnt = df_error.count()
        channel_day_qc_pass_cnt = df_error.filter(col('qc_status') == 1).count()
        # get airflow success output paths from temp view
        channel_ingest_success_path = Quality.get_file_path_from_temp_view(spark, 'channel_ingest_success')
        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': random_channel_day_cnt,
                    'right_data': channel_day_qc_pass_cnt
                }
            },
            'reference': {
                    'channel_ingest_success': channel_ingest_success_path,
                    'common': self.s3_path_common,
                    'daily_intermediate': self.s3_path_rollup_daily_intermediate,
                    'mdis_daily_meta': self.s3_path_mdis_monthly_meta,
                    'mdis_daily': self.s3_path_load_mdis_monthly
                },
            'misc': {}
        }
        # save QC details to s3
        Quality.save_output(self, df_error, 4)
        # run qc and return output
        output = Quality.qc_individual_step(self, spark, 6, qc_values, df_error)
        COMPLETED_QC_STEP_LIST.append(output)
        return output

    def ingest_time(self, spark):
        """Run QC for ingest time for those tenants with manifest files.
        This is the ingest time function for the AMI QC task. It runs QC for ingest time.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        if self.has_manifest is not True:
            log.info('has_manifest is FALSE for tenant_id {}. Skipping Ingest Time check'.format(self.tenant_id))
            return
        else:
            # get dataframes
            try:
                df_ingest_time = Quality.get_ingest_time(self, spark)
                # get ingest time data
                manifest_time = df_ingest_time.select(col('manifest_time')).collect()[0][0]
                ingest_end_time = df_ingest_time.select(col('ingest_end_time')).collect()[0][0]
                ingest_seconds = df_ingest_time.select(col('ingest_seconds')).collect()[0][0]
                # get left and right data
                expected_ingest_time_bool = 1
                actual_ingest_time_bool = df_ingest_time.select(col('qc_status')).collect()[0][0]
                df_error = df_ingest_time
                # set qc values dictionary
                qc_values = {
                    'metrics': {
                        1: {
                            'left_data': expected_ingest_time_bool,
                            'right_data': actual_ingest_time_bool
                        }
                    },
                    'reference': {
                            'ingest_time_path': self.s3_path_ingest_time
                        },
                    'misc': {
                        1: {
                            'name': 'Manifest Time',
                            'value': manifest_time
                        },
                        2: {
                            'name': 'Ingest End Time',
                            'value': ingest_end_time
                        },
                        3: {
                            'name': 'Ingest Seconds',
                            'value': ingest_seconds
                        }
                    }
                }
                # run qc and return output
                output = Quality.qc_individual_step(self, spark, 7, qc_values, df_error)
                COMPLETED_QC_STEP_LIST.append(output)
                return output
            except Exception as e:
                log.error("!! Error getting ingest time data. Error: {e}".format(e=e))
                return

    def union_qc_output(self, spark, step_list):
        """Union output from individual steps to create QC output for the task.
        Takes a list of arbitrary length to create and return final output for the QC task.
        :param step_list: List of Dataframes from each QC step run
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        if len(step_list) == 0:  # Nothing here, return empty dataframe
            log.info(
                'No QC output for {} on {}. Returning empty dataframe'.format(self.tenant_id, self.execution_date_y_m_d)
            )
            df = spark.createDataFrame([], SCHEMA)
        else:  # Something here, union them all!
            log.info(
                'We have QC output for {} on {}! Returning QC results'.format(self.tenant_id, self.execution_date_y_m_d)
            )
            df = reduce(DataFrame.union, step_list) \
                .orderBy(col('id'))
        return df

    def run(self, spark):
        """Run AMI QC.
        This is the main function for the AMI QC task. It sets up all data and runs QC for each
        step as defined below.
        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        log.info('Start QC for {}'.format(self.tenant_id))
        Quality.setup_common_ami_summary_raw_to_rollup(self, spark)

        # Run QC for each ingest step
        Quality.decrypt(self, spark)
        Quality.channel_ingest(self, spark)
        Quality.extract_common_ami(self, spark)
        Quality.load_common_ami(self, spark)
        Quality.raw_to_mdis_hour(self, spark)
        Quality.raw_to_mdis_day(self, spark)
        Quality.ingest_time(self, spark)

        # Union all QC output
        df_output = Quality.union_qc_output(self, spark, COMPLETED_QC_STEP_LIST)

        # Save output for long-term storage
        Quality.save_output(self, df_output, 2)

        return df_output.createOrReplaceTempView('output')
