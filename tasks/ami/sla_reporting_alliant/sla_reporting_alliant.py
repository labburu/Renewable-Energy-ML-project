import logging
import json
import requests
import sys

from pyspark.sql.functions import (
    current_timestamp,
    col,
    concat,
    date_format,
    get_json_object,
    lit,
    when
)

from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructField,
    StructType
)

SCHEMA = StructType([
    StructField('execution_date', StringType(), False),
    StructField('sla_name', StringType(), False),
    StructField('metric', StringType(), False),
    StructField('ingested_value', IntegerType(), False),
    StructField('orderby', FloatType(), False)
])

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class AmiSlaReportingAlliant:
    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id')
        self.environment = kwargs.get('environment')
        self.execution_date = kwargs.get('execution_date')
        self.execution_date_tenant_format = kwargs.get('execution_date_tenant_format')
        self.qc_output_path = kwargs.get('qc_output_path')
        self.qc_report_save_path = kwargs.get('qc_report_save_path')
        self.sla_report_save_path = kwargs.get('sla_report_save_path')
        self.sla_report_historical_path = kwargs.get('sla_report_historical_path')
        self.errors_tenant_to_common_path = kwargs.get('errors_tenant_to_common_path')
        self.uningestable_reads_save_path = kwargs.get('uningestable_reads_save_path')
        self.daily_file_quota = kwargs.get('daily_file_quota', 15)
        self.manifest_historical_path = kwargs.get('manifest_historical_path')
        self.manifest_path = kwargs.get('manifest_path')
        self.manifest_save_path = kwargs.get('manifest_save_path')
        self.ami_qc_slack_channel = kwargs.get('ami_qc_slack_channel')
        self.ami_bot_slack_url = kwargs.get('ami_bot_slack_url')

    def save_output(self, df, output_type):
        """Write any non-error output to long term storage location.

        The QC process produces data products which are not errors, but need to be saved long-term.
        This function saves any non-error output produced to S3 for further review.

        :param df: output to save
        :param int: output_type
        :return: None
        """
        if output_type == 1:  # Daily QC report
            save_to = self.qc_report_save_path
            save_format = 'parquet'
        elif output_type == 2:  # Daily SLA report
            save_to = self.sla_report_save_path
            save_format = 'csv'
        elif output_type == 3:  # Historical SLA report
            save_to = self.sla_report_historical_path
            save_format = 'csv'
        elif output_type == 4:  # Uningestable Reads
            save_to = self.uningestable_reads_save_path
            save_format = 'csv'
        elif output_type == 5:  # Manifest
            save_to = self.manifest_save_path
            save_format = 'csv'
        else:
            save_to = None

        if save_to is None:
            log.info('Skipping save of output, no save path configured')
        else:
            if save_format == 'parquet':  # QC report
                df.coalesce(1) \
                    .orderBy(
                        col('execution_date').asc(),
                        col('orderby').asc()
                    ) \
                    .drop(
                        col('orderby')
                    ) \
                    .write.save(
                    path=save_to,
                    mode='overwrite',
                    format=save_format
                )
            elif save_format == 'csv' and output_type in [2, 3]:  # SLA reports
                df.coalesce(1) \
                    .orderBy(
                        col('execution_date').asc(),
                        col('orderby').asc()
                    ) \
                    .drop(
                        col('orderby')
                    ) \
                    .write.save(
                    path=save_to,
                    mode='overwrite',
                    format=save_format,
                    header=True
                )
            elif save_format == 'csv' and output_type == 4:  # # Un-ingestagble reads (tenant-to-common errors)
                df.coalesce(1) \
                    .orderBy(
                        col('source_file_name').asc(),
                        col('external_location_id').asc(),
                        col('timestamp').asc()
                    ) \
                    .write.save(
                    path=save_to,
                    mode='overwrite',
                    format=save_format,
                    header=True
                )
            else:  # Manifest
                df.coalesce(1) \
                    .write.save(
                    path=save_to,
                    mode='overwrite',
                    format=save_format,
                    header=True
                )
            log.info('Saved output to {} ok'.format(save_to))

    def get_qc_output_by_id_step(self, spark, qc_id, step, df_existing=None):
        """"This will create a more readable version of QC output for each QC id and step."""
        try:
            df = spark.read.parquet(self.qc_output_path)
        except Exception as e:
            log.error('Exception: {}'.format(e))
            raise Exception('{}'.format(e))
            return

        df = df \
            .filter(
                col('id') == qc_id
            ) \
            .filter(
                get_json_object(col('metrics'), '$.{}.metric_name'.format(step)).isNotNull()
            ) \
            .select(
                lit(self.tenant_id).alias('tenant_id'),
                col('id'),
                col('name'),
                col('execution_date'),
                get_json_object(col('metrics'), '$.{}.metric_name'.format(step)).alias("metric_name"),
                get_json_object(col('metrics'), '$.{}.left_data_name'.format(step)).alias("left_data_name"),
                get_json_object(col('metrics'), '$.{}.left_data_value'.format(step)).alias("left_data_value"),
                get_json_object(col('metrics'), '$.{}.right_data_name'.format(step)).alias("right_data_name"),
                get_json_object(col('metrics'), '$.{}.right_data_value'.format(step)).alias("right_data_value"),
                get_json_object(col('metrics'), '$.{}.left_right_delta'.format(step)).alias("left_right_delta"),
                get_json_object(col('metrics'), '$.{}.qc_timestamp'.format(step)).alias("qc_timestamp"),
                get_json_object(col('metrics'), '$.{}.qc_status'.format(step)).alias("qc_status"),
                get_json_object(col('metrics'), '$.{}.qc_error_message'.format(step)).alias("qc_error_message"),
                get_json_object(col('metrics'), '$.{}.qc_error_path'.format(step)).alias("qc_error_path"),
                when(step == lit(1), col('qc_reference')).otherwise(lit(None)).alias('qc_reference'),
                when(step == lit(1), col('misc')).otherwise(lit(None)).alias('misc'),
            ) \
            .withColumn(
                'orderby',
                when(col('metric_name') == 'line count', lit(1.1))
                .when(col('metric_name') == 'checksums', lit(1.2))
                .when(col('metric_name') == 'channel uuids in success output are mapped correctly in zeus', lit(2.1))
                .when(col('metric_name') == 'no distinct raw ami channels mapped to multiple channel uuids', lit(2.2))
                .otherwise(col('id'))
            ) \
            .withColumn(
                'sla_name',
                when(col('orderby').isin([1, 1.1, 4]), lit('completeness'))
                .when(col('orderby').isin([5, 6]), lit('accuracy'))
                .when(col('orderby') == 7, lit('currency'))
                .otherwise(lit(None))
            ) \
            .drop(
                col('id')
            )

        if df_existing is None:
            df_output = df
            return df_output
        elif df is None:
            return df_existing
        else:
            df_output = df_existing.union(df)
            return df_output

    def get_and_save_manifest(self, spark):
        """"Get manifest file from ALLIANT SFTP and save it to meter-data bucket for long term storage."""
        try:
            df = spark.read.csv(self.manifest_path)
            df = df \
                .withColumnRenamed('_c0', 'file_name') \
                .withColumnRenamed('_c1', 'byte_count') \
                .withColumnRenamed('_c2', 'checksum') \
                .withColumnRenamed('_c3', 'line_count')

            self.save_output(df, 5)
            return

        except Exception as e:
            log.error('Exception: {}'.format(e))
            return

    def create_and_save_daily_qc_report(self, spark):
        """"This will consume the daily raw QC output and will create a daily QC report."""
        for qc_id in range(1, 8):
            for step in range(1, 4):
                try:
                    if qc_id == 1 and step == 1:
                        df = self.get_qc_output_by_id_step(spark, qc_id, step)
                    else:
                        df = self.get_qc_output_by_id_step(spark, qc_id, step, df)
                except Exception as e:
                    log.error('Exception: {}'.format(e))
                    return
        df = df \
            .filter(
                col('execution_date') == self.execution_date
            )
        self.save_output(df, 1)
        return df

    def create_and_save_daily_sla_report(self, spark, df):
        """"This will consume the daily QC report and will create a daily SLA report for ALLIANT."""
        ui_read_cnt = self.get_distinct_uningestable_read_count(spark)
        df_distinct_uningestable_reads = \
            spark.createDataFrame(
                [
                    (
                        self.execution_date,
                        'completeness',
                        'all uningestable reads to sftp',
                        ui_read_cnt,
                        4.1
                    )
                ],
                SCHEMA
            )

        df = df \
            .where(
                col('sla_name').isNotNull()
            ) \
            .select(
                col('execution_date'),
                col('sla_name'),
                col('metric_name').alias('metric'),
                col('right_data_value').alias('ingested_value').cast('int'),
                col('orderby').cast('float')
            ) \
            .union(
                df_distinct_uningestable_reads
            ) \
            .filter(
                col('execution_date') == self.execution_date
            ) \
            .withColumn(
                'sla_timestamp', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss').cast('timestamp')
            )
        self.save_output(df, 2)
        return df

    def create_and_save_historical_sla_report(self, spark, df):
        """"This will take current and historical SLA reports and upsert any new data into the historical report."""
        try:
            df_historical = spark.read.csv(self.sla_report_historical_path, header=True)
            df_historical.cache()
        except Exception as e:
            log.error(
                'Historical SLA report does not exist, loading current SLA report for {} as historical. Exception: {}'
                .format(self.execution_date, e)
            )
            self.save_output(df, 3)
            return
        # Add order by column
        df_historical = AmiSlaReportingAlliant.add_sla_orderby(spark, df_historical)
        # Remove previous execution date
        if df_historical.where(col('execution_date') == self.execution_date).count() > 0:
            df_historical = df_historical \
                .where(col('execution_date') != self.execution_date)

        df_output = df_historical.union(df)
        df_output.cache()
        self.save_output(df_output, 3)
        return

    @staticmethod
    def add_sla_orderby(spark, df):
        """"Add column to order results"""
        return df \
            .withColumn(
                'orderby',
                when(col('metric') == 'file count', lit(1))
                .when(col('metric') == 'line count', lit(2))
                .when(col('metric') == 'all ingestable reads loaded', lit(3))
                .when(col('metric') == 'all uningestable reads to sftp', lit(4))
                .when(col('metric') == 'raw hourly totals match mdis rollups', lit(5))
                .when(col('metric') == 'raw daily totals match mdis rollups', lit(6))
                .when(col('metric') == 'ingest time under 24 hours', lit(7))
            ) \
            .select(
                'execution_date',
                'sla_name',
                'metric',
                'ingested_value',
                'orderby',
                'sla_timestamp'
            )

    def get_and_save_uningestable_reads(self, spark):
        """This will get all un-ingestable AMI reads for a given execution date, de-dupe and save."""
        try:
            df = spark.read.parquet(self.errors_tenant_to_common_path)
            df = df \
                .drop(
                    'tenant_id',
                    'error_code'
                )
            self.save_output(df, 4)
            return

        except Exception as e:
            log.error('Exception: {}'.format(e))
            return

    def get_distinct_uningestable_read_count(self, spark):
        """This will get a distinct count of un-ingestable AMI reads for a given execution date."""
        try:
            df = spark.read.parquet(self.errors_tenant_to_common_path)
            cnt = df \
                .drop(
                    'tenant_id',
                    'error_code'
                )\
                .count()
            return cnt
        except Exception as e:
            log.error('Exception: {}'.format(e))
            return 0

    def get_final_output(self, spark):
        """This will pull the entire historical SLA report to output from this task."""
        try:
            df_output = spark.read.csv(self.sla_report_historical_path, header=True)
        except Exception as e:
            log.error(
                'Historical SLA report does not exist. Exception: {}'.format(e)
            )
            return
        return df_output

    # Slack Reporting Functions
    @staticmethod
    def create_slack_payload_string_from_list(l):
        """Create string with alternating values and new lines for Slack payload."""
        output = ''
        new_line = '\n'
        for item in l:
            output += '{}{}'.format(item, new_line)
        return output

    def create_qc_fail_payload(self, park, df, payload_type=None):
        """Create Slack payload for any QC fails."""
        if payload_type == 'sla':
            sla_flag = 1
            title = "*ALLIANT AMI Ingest QC Errors for SLA Metrics for {}*".format(self.execution_date)
        else:
            sla_flag = 0
            title = "*ALLIANT AMI Ingest QC Errors for Non-SLA Metrics for {}*".format(self.execution_date)

        df_details = df \
            .withColumn(
                'qc_details',
                concat(
                    lit('{'),
                    col('left_data_value'),
                    lit(' : '),
                    col('right_data_value'),
                    lit('}')
                )
            ) \
            .withColumn(
                'ingest_time_details',
                concat(
                    lit(',{'),
                    lit('ingest_seconds'),
                    lit(' : '),
                    get_json_object(col('misc'), '$.3.value'),
                    lit('}')
                )
            ) \
            .where(
                col('qc_status') == 0
            ) \
            .select(
                col('metric_name'),
                when(
                    col('metric_name') != 'ingest time under 24 hours',
                    col('qc_details')
                )
                .otherwise(
                    concat(
                        col('qc_details'),
                        col('ingest_time_details')
                    )
                ).alias('qc_details'),
                when(col('sla_name').isNull(), lit(0)).otherwise(lit(1)).alias('sla_flag')
            )

        header_list = df_details.where(col('sla_flag') == sla_flag) \
            .select('metric_name').rdd.flatMap(lambda x: x).collect()
        value_list = df_details.where(col('sla_flag') == sla_flag) \
            .select('qc_details').rdd.flatMap(lambda x: x).collect()

        headers = AmiSlaReportingAlliant.create_slack_payload_string_from_list(header_list)
        values = AmiSlaReportingAlliant.create_slack_payload_string_from_list(value_list)
        output = []
        output.append(title)
        output.append(headers)
        output.append(values)

        payload = self.create_slack_table_payload(output)
        return payload

    @staticmethod
    def create_sla_value_string_for_slack_callout(spark, df):
        """Create string with alternating new line characters for SLA values."""
        try:
            value_list = []

            file_count = '{:,}'.format(
                int(
                    df.where(col('metric') == 'file count').select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(file_count)
            line_count = '{:,}'.format(
                int(
                    df.where(col('metric') == 'line count').select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(line_count)
            ingested_reads = '{:,}'.format(
                int(
                    df.where(col('metric') == 'all ingestable reads loaded')
                    .select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(ingested_reads)
            uningested_reads = '{:,}'.format(
                int(
                    df.where(col('metric') == 'all uningestable reads to sftp')
                    .select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(uningested_reads)
            raw_to_hourly = '{:,}'.format(
                int(
                    df.where(col('metric') == 'raw hourly totals match mdis rollups')
                    .select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(raw_to_hourly)
            raw_to_daily = '{:,}'.format(
                int(
                    df.where(col('metric') == 'raw daily totals match mdis rollups')
                    .select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(raw_to_daily)
            ingest_time = '{:,}'.format(
                int(
                    df.where(col('metric') == 'ingest time under 24 hours')
                    .select(col('ingested_value')).collect()[0][0]
                    )
            )
            value_list.append(ingest_time)

            output = AmiSlaReportingAlliant.create_slack_payload_string_from_list(value_list)

            return output

        except Exception as e:
            log.error(
                'Error Building SLA Values string for Slack. Exception: {}'.format(e)
            )
            return ''

    def create_daily_sla_input(self, spark, df):
        """Create payload for Slack reporting on daily SLA metrics."""
        title = "*ALLIANT AMI Ingest SLA Report for {}*".format(self.execution_date)
        metric_name_list = \
            [
                'File Count',
                'Row Count', 'Row Ingested', 'Rows Uningested', 'Raw to Hourly', 'Raw to Daily', 'Ingest Time'
            ]
        headers = AmiSlaReportingAlliant.create_slack_payload_string_from_list(metric_name_list)
        values = AmiSlaReportingAlliant.create_sla_value_string_for_slack_callout(spark, df)
        output = []
        output.append(title)
        output.append(headers)
        output.append(values)

        payload = self.create_slack_table_payload(output)
        return payload

    def create_slack_table_payload(self, input_list):
        """Create payload for Slack reporting that uses table format."""
        if 'ALLIANT AMI Ingest QC Errors for SLA Metrics' in input_list[0]:
            alert = 'danger'
            right_column_title = 'QC Details'
        elif 'ALLIANT AMI Ingest QC Errors for Non-SLA Metrics' in input_list[0]:
            alert = 'warning'
            right_column_title = 'QC Details'
        else:
            alert = 'good'
            right_column_title = 'Ingested Value'

        if 'Errors' not in input_list[0]:
            link = 'https://console.aws.amazon.com/s3/buckets/tendril-meter-data-dev-us/quality/{}/{}/sla_report/sla_report.csv/?region=us-east-1&tab=overview'.format(self.tenant_id, self.execution_date)  # noqa
        else:
            link = 'https://console.aws.amazon.com/s3/buckets/tendril-meter-data-dev-us/quality/{}/{}/qc_report/qc_report.parquet/?region=us-east-1&tab=overview'.format(self.tenant_id, self.execution_date)  # noqa

        text = {
            "channel": self.ami_qc_slack_channel,
            "attachments": [
                {
                    "mrkdwn_in": ["text"],
                    "color": alert,
                    "pretext": input_list[0],
                    "title": link,
                    "title_link": link,
                    "fields": [
                        {
                            "title": "Metric",
                            "short": "true"
                        },
                        {
                            "title": right_column_title,
                            "short": "true"
                        },
                        {
                            "value": input_list[1],
                            "short": "true"

                        },
                        {
                            "value": input_list[2],
                            "short": "true"
                        }
                    ]
                }
            ]
        }
        if len(input_list[1]) == 0:
            payload = None
        else:
            payload = 'payload=' + requests.utils.quote(json.dumps(text))
        return payload

    def create_daily_file_count_slack_payload(self, spark, df):
        """Create payload for Slack reporting for daily file count."""
        title = '*ALLIANT AMI Ingest Daily File Count Sanity Check for {}*'.format(self.execution_date)
        path_blurb = 'https://console.aws.amazon.com/s3/buckets/tendril-meter-data-dev-us/quality/41/{}/manifest/manifest.csv/?region=us-east-1&tab=overview'.format(self.execution_date)  # noqa
        delivered_file_count = df.where(col('metric_name') == 'file count') \
            .select(col('left_data_value')).collect()[0][0]

        if int(delivered_file_count) >= self.daily_file_quota:
            alert = 'good'
            file_count_message = \
                'Manifest Files: {} >= ALLIANT Daily Quota {}.'.format(
                    delivered_file_count,
                    self.daily_file_quota
                )
        else:
            alert = 'danger'
            file_count_message = \
                'Manifest Files: {} < ALLIANT Daily Quota {}.'.format(
                    delivered_file_count,
                    self.daily_file_quota,
                )
        if alert == 'good':
            text = {
                    "channel": self.ami_qc_slack_channel,
                    "attachments": [
                        {
                            "mrkdwn_in": ["text", "pretext"],
                            "color": alert,
                            "pretext": title,
                            "text": file_count_message
                        }
                    ]
            }
        else:
            text = {
                    "channel": self.ami_qc_slack_channel,
                    "attachments": [
                        {
                            "mrkdwn_in": ["text", "pretext"],
                            "color": alert,
                            "pretext": title,
                            "text": file_count_message,
                            "title": path_blurb
                        }
                    ]
            }

        payload = 'payload=' + requests.utils.quote(json.dumps(text))
        return payload

    def create_daily_read_count_payload(self, spark, df):
        """Create payload for Slack reporting for daily read count."""
        title = '*ALLIANT AMI Ingest Daily Read Count Sanity Check for {}*'.format(self.execution_date)
        link = 'https://console.aws.amazon.com/s3/buckets/tendril-meter-data-dev-us/quality/{}/{}/qc_report/qc_report.parquet/?region=us-east-1&tab=overview'.format(self.tenant_id, self.execution_date)  # noqa

        raw_read_count = df.where(col('metric') == 'line count') \
            .select(col('ingested_value')).collect()[0][0]
        ingested_read_count = df.where(col('metric') == 'all ingestable reads loaded') \
            .select(col('ingested_value')).collect()[0][0]
        uningested_read_count = df.where(col('metric') == 'all uningestable reads to sftp') \
            .select(col('ingested_value')).collect()[0][0]

        if int(raw_read_count) != int(ingested_read_count) + int(uningested_read_count):
            alert = 'danger'
            read_count_message = \
                'Raw reads {} != Ingested reads {} + Uningested reads {}'.format(
                    '{:,}'.format(int(raw_read_count)),
                    '{:,}'.format(int(ingested_read_count)),
                    '{:,}'.format(int(uningested_read_count))
                )
        else:
            alert = 'good'
            read_count_message = \
                'Raw reads {} == Ingested reads {} + Uningested reads {}'.format(
                    '{:,}'.format(int(raw_read_count)),
                    '{:,}'.format(int(ingested_read_count)),
                    '{:,}'.format(int(uningested_read_count))
                )
        if alert == 'good':
            text = {
                    "channel": self.ami_qc_slack_channel,
                    "attachments": [
                        {
                            "mrkdwn_in": ["text", "pretext"],
                            "color": alert,
                            "pretext": title,
                            "text": read_count_message
                        }
                    ]
            }
        else:
            text = {
                    "channel": self.ami_qc_slack_channel,
                    "attachments": [
                        {
                            "mrkdwn_in": ["text", "pretext"],
                            "color": alert,
                            "pretext": title,
                            "text": read_count_message,
                            "title": link,
                            "title_link": link,
                        }
                    ]
            }
        payload = 'payload=' + requests.utils.quote(json.dumps(text))
        return payload

    def post_to_slack(self, payload):
        """This will post a message to Slack"""
        url = self.ami_bot_slack_url
        if payload is None:
            log.error('Empty Slack callout payload')
        else:
            try:
                req = requests.post(url, data=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})
            except requests.exceptions.RequestException as err:
                log.error(err)
                return
            if req.status_code != 200:
                error = "Error calling Slack API: " + url + "\n" + req.text
                raise Exception(error)
            else:
                return True

    def report_daily_metrics_to_slack(self, spark, df_qc, df_sla):
        payload_list = []
        # Daily SLA report
        daily_sla_payload = self.create_daily_sla_input(spark, df_sla)
        # Daily QC fail reports
        qc_fail_sla_payload = self.create_qc_fail_payload(spark, df_qc, 'sla')
        qc_fail_non_sla_payload = self.create_qc_fail_payload(spark, df_qc)
        # File and read count checks
        daily_file_count_payload = self.create_daily_file_count_slack_payload(spark, df_qc)
        daily_read_count_payload = self.create_daily_read_count_payload(spark, df_sla)
        # Build payload list
        payload_list.append(daily_sla_payload)
        payload_list.append(qc_fail_sla_payload)
        payload_list.append(qc_fail_non_sla_payload)
        payload_list.append(daily_file_count_payload)
        payload_list.append(daily_read_count_payload)
        # Loop through payload list and post to slack
        for payload in payload_list:
            self.post_to_slack(payload)

    def run(self, spark):
        # Get and save manifest to QC directory
        log.info('get_and_save_manifest')
        self.get_and_save_manifest(spark)
        # Create and save all QC and SLA output to S3
        log.info('create_and_save_daily_qc_report')
        df_qc = self.create_and_save_daily_qc_report(spark)
        log.info('create_and_save_daily_sla_report')
        df_sla = self.create_and_save_daily_sla_report(spark, df_qc)
        log.info('get_and_save_uningestable_reads')
        self.get_and_save_uningestable_reads(spark)
        log.info('create_and_save_historical_sla_report')
        self.create_and_save_historical_sla_report(spark, df_sla)
        # Report daily metrics to slack
        log.info('report to slack')
        self.report_daily_metrics_to_slack(spark, df_qc, df_sla)
        # Get final output
        log.info('get_final_output')
        df_final = self.get_final_output(spark)
        return df_final.createOrReplaceTempView('output')
