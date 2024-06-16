import collections
import json
import logging
import os
import sys

from datetime import datetime


import pyspark.sql.functions as f
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType
)
from pyspark.sql.functions import (
    col,
    lit,
    when
)

try:
    from ami_qc import Quality
except Exception:
    from .ami_qc import Quality


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

COMPLETED_QC_STEP_LIST_PSEGLI = []

QC_META_PSEGLI = {
    1: {
        'name': 'Extract Raw AMI',
        'metrics': {
            1: {
                'name': 'file count',
                'left_data':  'S3 file count',
                'right_data': 'extract raw file count',
                'error_message': 'extract raw file count does not match S3',
                'error_df_filter_string': 'qc_status = 0'
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
}


class QualityPsegli(Quality):
    """
    QC for PSEGLI will be abbreviated relative to the standard AMI QC process.
    The QC for PSEGLI will consist of a daily summary of the `extract_raw_ami` step to expose relevant data for
    historical analysis and possible future reporting.
    """

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
        error_message = QC_META_PSEGLI[step_number]['metrics'][metric_number]['error_message']
        error_df_filter_string = QC_META_PSEGLI[step_number]['metrics'][metric_number]['error_df_filter_string']
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
        step_name = QC_META_PSEGLI[step_number]['name']
        log.info('starting qc for step {}: {}'.format(step_number, step_name))
        # set metric output dictionary
        metric_output = {}
        # get metric data and qc individual metric
        metric_keys = QC_META_PSEGLI[step_number]['metrics'].keys()
        for key in metric_keys:
            metric_number = key
            left_data = qc_values['metrics'][key]['left_data']
            right_data = qc_values['metrics'][key]['right_data']
            qc_data = self.qc_individual_metric(spark, step_number, metric_number, left_data, right_data, df_error)
            metric_output.update({metric_number: qc_data})
        # set output data
        id = step_number
        name = QC_META_PSEGLI[step_number]['name']
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
        metric_name = QC_META_PSEGLI[step_number]['metrics'][metric_number]['name']
        qc_time_utc = (datetime.utcnow()).strftime('%Y-%m-%d %H:%M:%S')
        step_name = QC_META_PSEGLI[step_number]['name']
        left_data_name = QC_META_PSEGLI[step_number]['metrics'][metric_number]['left_data']
        right_data_name = QC_META_PSEGLI[step_number]['metrics'][metric_number]['right_data']
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
            error_message = QC_META_PSEGLI[step_number]['metrics'][metric_number]['error_message']
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
                '{}'.format('extract_raw_ami'),
                'metric_number={}'.format(metric_number), 'errors.parquet')
        elif step_number == 4:
            save_to = os.path.join(
                self.s3_path_save_errors_base,
                '{}'.format('load_common_ami'),
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

    def get_and_save_ami_summary_psegli(self, spark, table_name):
        """Create and save daily AMI ingest summary.

        Raw AMI data is purged after a short time and to faciliate QC related quesions,
        we require a summary of all ingested data for a given execution date for
        historical analysis, should the need arise. This function takes raw data in
        Uplight common format, creates the ami summary and saves it for long-term storage.

        :param df_common: Dataframe with output from `extract_raw_ami`
        :return: Dataframe with following schema
        |-- account_id: string (nullable = true)
        |-- date: date (nullable = true)
        |-- file_name: string (nullable = true)
        |-- interval_seconds: integer (nullable = true)
        |-- num_reads_actual: integer (nullable = true)
        |-- day_consumption_actual: decimal(16,3) (nullable = true)
        |-- num_reads_no_code: integer (nullable = true)
        |-- day_consumption_no_code: decimal(16,3) (nullable = true)
        |-- num_reads_total: integer (nullable = true)
        |-- day_consumption_total: decimal(20,3) (nullable = true)
        """
        df = spark.table(table_name) \
            .filter(
                col('error_type').isNull()
            ) \
            .select(
                col('account_id'),
                f.to_date(col('timestamp')).alias('date'),
                col('source_file_name').alias('file_name'),
                col('seconds_per_interval').alias('interval_seconds').cast('int'),
                col('code').alias('consumption_code'),
                col('consumption')
            ) \
            .withColumn('actual_read_flag', when(col('consumption_code') == 'M', lit(1))) \
            .withColumn('actual_read_consumption', when(col('consumption_code') == 'M', col('consumption'))) \
            .withColumn('no_code_read_flag', when(~(col('consumption_code').isin(['M'])) |
                                                   (col('consumption_code').isNull()), lit(1))) \
            .withColumn('no_code_consumption', when(~(col('consumption_code').isin(['M'])) |
                                                    (col('consumption_code').isNull()), col('consumption'))) \
            .groupBy(
                'account_id',
                'date',
                'file_name',
                'interval_seconds'
            ) \
            .agg(
                f.sum('actual_read_flag'),
                f.sum('actual_read_consumption'),
                f.sum('no_code_read_flag'),
                f.sum('no_code_consumption')) \
            .withColumnRenamed('sum(actual_read_flag)', 'num_reads_actual') \
            .withColumnRenamed('sum(actual_read_consumption)', 'day_consumption_actual') \
            .withColumnRenamed('sum(no_code_read_flag)', 'num_reads_no_code') \
            .withColumnRenamed('sum(no_code_consumption)', 'day_consumption_no_code') \
            .withColumn('num_reads_actual', col('num_reads_actual').cast(IntegerType())) \
            .withColumn('day_consumption_actual', col('day_consumption_actual').cast(DecimalType(16, 3))) \
            .withColumn('num_reads_no_code', col('num_reads_no_code').cast(IntegerType())) \
            .withColumn('day_consumption_no_code', col('day_consumption_no_code').cast(DecimalType(16, 3))) \
            .orderBy('date') \
            .fillna(0, subset=[
                'num_reads_actual', 'num_reads_no_code'
            ]) \
            .withColumn(
                'num_reads_total',
                (
                    col('num_reads_actual') +
                    col('num_reads_no_code').cast(IntegerType()))) \
            .withColumn(
                'day_consumption_total',
                when(
                    col('day_consumption_actual').isNull(), lit(0)).otherwise(col('day_consumption_actual')) +
                when(
                    col('day_consumption_no_code').isNull(), lit(0)).otherwise(col('day_consumption_no_code'))
                .cast(DecimalType(16, 3))) \
            .fillna(0, subset=['num_reads_total']) \
            .orderBy(col('account_id'))

        log.info('saving ami summary')
        try:
            super(QualityPsegli, self).save_output(df, 1)
            error = 0
        except Exception as e:
            log.error("!! Error saving ami summary, get summary from ami summary dataframe. Error: {e}".format(e=e))
            error = 1
        if error == 0:
            spark.read.parquet(self.s3_path_save_ami_summary).createOrReplaceTempView('ami_summary')
        else:
            df = df.persist()
            df.createOrReplaceTempView('ami_summary')

    @staticmethod
    def get_raw_ami_error_list(spark, table_name):
        """Get a list of extract raw ami errors and row counts.

        When extracting raw ami data for PSEGLI there are a number of
        errors that can occur. This function groups these error types and
        their counts and returns a list for reference.

        :param table_nmae: Spark table with output from `extract_raw_ami`
        :return: List
        """
        df = spark.table(table_name) \
            .filter(
                col('error_type').isNotNull()
        ) \
            .select(
                col('error_type')
        ) \
            .groupBy(
                col('error_type')
        ) \
            .agg(
                (f.count('*')).alias('error_cnt')
        ) \
            .select(
                (f.concat_ws(
                    ':',
                    col('error_type'),
                    col('error_cnt')
                )
                )
                .alias('error_distribution')
        )
        raw_ami_error_list = df \
            .select(
                df.error_distribution
            ) \
            .distinct() \
            .orderBy(
                df.error_distribution
            ) \
            .rdd.flatMap(
                lambda x: x
            ) \
            .collect()

        return raw_ami_error_list

    def get_raw_ami_files(spark, table_name):
        """Get a df of source files for PSEGLI AMI data.

        :param table_nmae: Spark table with output from `extract_raw_ami`
        :return: Dataframe with following schema
        |-- raw_ami_filename: string (nullable = true)
        """
        return spark.table(table_name) \
            .filter(
                col('error_type').isNull()
            ) \
            .select(
                col('source_file_name').alias('raw_ami_filename')
            ) \
            .distinct()

    @staticmethod
    def join_extract_raw_ami(spark, df_s3, df_raw_ami):
        """Join all QC data for extract raw ami step.

        In case of a QC error we need all details regarding extract raw ami for PSEGLI.
        This function joins together all pertinent data to be exposed as error detail if neccessary.


        :param df_s3: Dataframe with output from `get_encrypted`
        :param df_raw_ami: DataFrame with output from `get_raw_ami_files`
        :return: Dataframe with following schema
            |-- external_location_id: string (nullable = true)
            |-- external_account_id: string (nullable = true)
            |-- external_channel_id: string (nullable = true)
        """
        df = df_s3 \
            .join(
                df_raw_ami,
                df_s3.encrypted_filename == df_raw_ami.raw_ami_filename,
                'left_outer'
            ) \
            .withColumn(
                'qc_status',
                when(
                    col('encrypted_filename') == col('raw_ami_filename'),
                    lit(1)).otherwise(
                        lit(0)
                        )
                    )
        return df

    def extract_raw_ami(self, spark):
        """Run QC for the extract raw ami step.

        :return: Dataframe with following schema
            |-- id: string (nullable = false)
            |-- name: string (nullable = false)
            |-- execution_date: string (nullable = false)
            |-- metrics: string (nullable = false)
            |-- qc_reference: string (nullable = false)
            |-- misc: string (nullable = false)
        """
        log.info('Starting QC for Extract Raw AMI Task')
        log.info('Getting S3 file list')
        df_s3_files = Quality.get_encrypted(spark, table_name='raw_ami_files')
        log.info('Getting raw ami file list')
        df_raw_ami_files = QualityPsegli.get_raw_ami_files(spark, 'extract_raw_ami')
        log.info('Join PSEGLI File list')
        df_error = QualityPsegli.join_extract_raw_ami(spark, df_s3_files, df_raw_ami_files)
        log.info('Getting raw ami error list')
        raw_ami_error_list = QualityPsegli.get_raw_ami_error_list(spark, 'extract_raw_ami')
        log.info('Getting Raw AMI DF')
        df_raw_ami = spark.table('ami_summary')

        # set left and right data
        left_data_file_cnt = df_s3_files.count()
        right_data_file_cnt = df_raw_ami_files.count()

        # get airflow success output paths from temp view
        extract_raw_ami_path = Quality.get_file_path_from_temp_view(spark, 'extract_raw_ami')

        # set reference data
        file_list = df_raw_ami \
            .select(col('file_name')) \
            .distinct() \
            .orderBy(col('file_name')) \
            .rdd.flatMap(lambda x: x) \
            .collect()
        summary_rows = df_raw_ami.count()
        distinct_account_ids = df_raw_ami.select(col('account_id')).distinct().count()
        distinct_date_count = df_raw_ami.select(col('date')).distinct().count()
        min_date = df_raw_ami \
            .select(col('date')) \
            .orderBy(col('date')) \
            .filter(col('date').isNotNull()) \
            .limit(1) \
            .collect()[0][0]
        max_date = df_raw_ami \
            .select(col('date')) \
            .orderBy(col('date').desc()) \
            .filter(col('date').isNotNull()) \
            .limit(1) \
            .collect()[0][0]
        total_read_count = df_raw_ami.agg({"num_reads_total": "sum"}).collect()[0][0]
        total_consumption = df_raw_ami.agg({"day_consumption_total": "sum"}).collect()[0][0]

        # set qc values dictionary
        qc_values = {
            'metrics': {
                1: {
                    'left_data': left_data_file_cnt,
                    'right_data': right_data_file_cnt
                }
            },
            'reference': {
                    'ami_summary': self.s3_path_save_ami_summary,
                    'extract_raw_ami': extract_raw_ami_path
            },
            'misc': {
                1: {
                    'name': 'File List',
                    'value': file_list
                },
                2: {
                    'name': 'Summary Rows',
                    'value': summary_rows
                },
                3: {
                    'name': 'Distinct Accounts',
                    'value': distinct_account_ids
                },
                4: {
                    'name': 'Distinct Date Count',
                    'value': distinct_date_count
                },
                5: {
                    'name': 'Min Date',
                    'value': str(min_date)
                },
                6: {
                    'name': 'Max Date',
                    'value': str(max_date)
                },
                7: {
                    'name': 'Total Non-Error Read Count',
                    'value': total_read_count
                },
                8: {
                    'name': 'Total Non-Error Consumption',
                    'value': str(total_consumption)
                },
                9: {
                    'name': 'Raw AMI Error List',
                    'value': raw_ami_error_list
                }

            }
        }
        # run qc and return output
        output = QualityPsegli.qc_individual_step(self, spark, 1, qc_values, df_error)
        return output

    def run(self, spark):
        """Run AMI QC for PSEGLI.

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
        QualityPsegli.get_and_save_ami_summary_psegli(self, spark, 'extract_raw_ami')

        # Run QC for each ingest step
        df_extract_raw_ami = QualityPsegli.extract_raw_ami(self, spark)
        df_load_common = super(QualityPsegli, self).load_common_ami(spark)

        # Build output list and union all QC output
        COMPLETED_QC_STEP_LIST_PSEGLI.append(df_extract_raw_ami)
        COMPLETED_QC_STEP_LIST_PSEGLI.append(df_load_common)
        # df_output = Quality.union_qc_output(self, spark, COMPLETED_QC_STEP_LIST_PSEGLI)
        df_output = super(QualityPsegli, self).union_qc_output(spark, COMPLETED_QC_STEP_LIST_PSEGLI)

        # Save output for long-term storage
        super(QualityPsegli, self).save_output(df_output, 2)

        return df_output.createOrReplaceTempView('output')
