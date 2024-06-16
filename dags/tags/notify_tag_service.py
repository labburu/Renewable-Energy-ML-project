"""TODO"""

import json
import logging

from airflow.hooks.http_hook import HttpHook
import boto3


logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)


def notify_tag_service(*args, **kwargs):
    try:
        tag_set_name = kwargs['tag_set_name']
        association_type = kwargs['association_type']
        task_instance = kwargs['ti']
        upstream_task_id = kwargs['upstream_task_id']
        expiration_date = kwargs['expiration_date']
    except KeyError as e:
        log.error('PythonOperator\'s op_kwargs is missing required arg {}'.format(e))
        raise e

    xcom = task_instance.xcom_pull(task_ids=upstream_task_id)
    try:
        s3_bucket = xcom['bucket']
        s3_key = xcom['key']
    except KeyError as e:
        log.error('Failed to parse S3 bucket from upstream XCom: {}'.format(str(xcom)))
        raise e

    file_key = find_saved_csv(s3_bucket, s3_key)
    file_location = 's3://{}/{}'.format(s3_bucket, file_key)

    http = HttpHook(method='POST', http_conn_id='tendril-tag-service-api')
    http.run(endpoint='tag-sets/load', data=json.dumps({
        's3Uri': file_location,
        'tagSetName': tag_set_name,
        'associationType': association_type,
        'expirationDate': expiration_date
    }))


def find_saved_csv(s3_bucket, s3_key):

    log.info('bucket: {}, key: {}'.format(s3_bucket, s3_key))

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=s3_key + '/part'
    )

    log.info('resp: {}'.format(str(response['Contents'])))

    if not len(response['Contents']):
        raise Exception('Nothing found at s3://{}/{}.'.format(s3_bucket, s3_key))

    if response['IsTruncated'] or len(response['Contents']) > 1:
        raise Exception('The response from S3 is too big. Something is not as expected.')

    saved_csv = response['Contents'][0]

    return saved_csv['Key']
