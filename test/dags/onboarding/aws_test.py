import unittest
from unittest import TestCase
from airflow.models import Variable
from unittest.mock import MagicMock
from unittest.mock import patch
import botocore
import boto3
from moto import mock_s3
import pytest

import logging
logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)


class AwsUtilTest(TestCase):

    MOCK_FILES = ['cust_filename_ptn_1.txt', 'usg_filename_ptn_1.txt']

    def prepare_test_bucket(self):
        s3_res = boto3.resource('s3')
        s3_cli = boto3.client('s3')
        s3_res.create_bucket(Bucket='test')
        s3_res.create_bucket(Bucket='airflow-bucket')
        return s3_cli, s3_res

    @mock_s3
    def test_check_s3_objects_bothfilespresent_manifestuploadedtaskgo(self):
        from dags.onboarding.aws import check_s3_objects
        s3_cli, s3_res = s3_cli, s3_res = self.prepare_test_bucket()
        cust_o = s3_res.Object('test', 'tenant/cust_filename_ptn_1.txt')
        cust_o.put(Body=b'customer stuff')
        usage_o = s3_res.Object('test', 'tenant/usg_filename_ptn_1.txt')
        usage_o.put(Body=b'usage stuff')
        tenant_keys = s3_cli.list_objects(Bucket='test', Prefix='tenant')
        log.info('Tenant keys has {}'.format(tenant_keys))
        expected_tasks = ['keep_going', 'keep_going_again']
        w_keys = ['cust_filename_ptn', 'usg_filename_ptn']
        res = check_s3_objects(bucket='test', prefix='tenant',
                               wildcard_keys=w_keys,
                               tasks_go=expected_tasks,
                               task_stop='no!!!',
                               ts='the-ts',
                               task_instance=MagicMock(),
                               airflow_bucket='airflow-bucket')

        self.assertEqual(expected_tasks, res)
        try:
            expected_manifest = s3_res.Object('airflow-bucket', 'static/onboarding/manifests/the-ts/keep_going.json')
            manifest_body = expected_manifest.get()['Body'].read()
            log.info('manifest_body has %s', manifest_body)
            self.assertIsNotNone(manifest_body)
        except botocore.exceptions.ClientError as e:
            log.error('Test encountered an error %s', e)
            self.fail('Unexpected error occurred')


    @mock_s3
    def test_got_check_s3_objects_custfilepresent_manifestuploadedtaskgo(self):
        from dags.onboarding.aws import check_s3_objects
        s3_cli, s3_res = self.prepare_test_bucket()
        cust_o = s3_res.Object('test', 'tenant/cust_filename_ptn_1.txt')
        cust_o.put(Body=b'customer stuff')
        tenant_keys = s3_cli.list_objects(Bucket='test', Prefix='tenant')
        log.info('Tenant keys has {}'.format(tenant_keys))


        res = check_s3_objects(bucket='test',
                               prefix='tenant',
                               wildcard_keys=['cust_filename_ptn', 'usg_filename_ptn'],
                               tasks_go=['cust_keep_going', 'asdfasd'],
                               task_stop='no!!!',
                               ts='the-ts',
                               task_instance=MagicMock(),
                               airflow_bucket='airflow-bucket')

        self.assertEqual(['cust_keep_going'], res)
        try:
            expected_manifest = s3_res.Object('airflow-bucket', 'static/onboarding/manifests/the-ts/cust_keep_going.json')
            manifest_body = expected_manifest.get()['Body'].read()
            log.info('manifest_body has %s', manifest_body)
            self.assertIsNotNone(manifest_body)
        except botocore.exceptions.ClientError as e:
            log.error('Test encountered an error %s', e)
            self.fail('Unexpected error occurred')

    @mock_s3
    def test_check_s3_objects_usagefilepresent_manifestuploadedtaskgo(self):
        from dags.onboarding.aws import check_s3_objects
        s3_cli, s3_res = self.prepare_test_bucket()
        usage_o = s3_res.Object('test', 'tenant/usg_filename_ptn_1.txt')
        usage_o.put(Body=b'usage stuff')

        tenant_keys = s3_cli.list_objects(Bucket='test', Prefix='tenant')
        log.info('Tenant keys has {}'.format(tenant_keys))

        res = check_s3_objects(bucket='test',
                               prefix='tenant',
                               wildcard_keys=['cust_filename_ptn', 'usg_filename_ptn'],
                               tasks_go=['cust_keep_going', 'usg_kg'],
                               task_stop='no!!!',
                               ts='the-ts',
                               task_instance=MagicMock(),
                               airflow_bucket='airflow-bucket')

        self.assertEqual(['usg_kg'], res)
        try:
            expected_manifest = s3_res.Object('airflow-bucket', 'static/onboarding/manifests/the-ts/usg_kg.json')
            manifest_body = expected_manifest.get()['Body'].read()
            log.info('manifest_body has %s', manifest_body)
            self.assertIsNotNone(manifest_body)
        except botocore.exceptions.ClientError as e:
            log.error('Test encountered an error %s', e)
            self.fail('Unexpected error occurred')

    @mock_s3
    def test_check_s3_objects_singlewkey_manifestuploadedtaskgo(self):
        from dags.onboarding.aws import check_s3_objects
        s3_cli, s3_res = self.prepare_test_bucket()
        usage_o = s3_res.Object('test', 'tenant/usg_filename_ptn_1.txt')
        usage_o.put(Body=b'usage stuff')

        tenant_keys = s3_cli.list_objects(Bucket='test', Prefix='tenant')
        log.info('Tenant keys has {}'.format(tenant_keys))

        res = check_s3_objects(bucket='test',
                               prefix='tenant',
                               wildcard_key='usg_filename_ptn',
                               task_go='usg_kg',
                               task_stop='no!!!',
                               ts='the-ts',
                               task_instance=MagicMock(),
                               airflow_bucket='airflow-bucket')

        self.assertEqual(['usg_kg'], res)
        try:
            expected_manifest = s3_res.Object('airflow-bucket', 'static/onboarding/manifests/the-ts/usg_kg.json')
            manifest_body = expected_manifest.get()['Body'].read()
            log.info('manifest_body has %s', manifest_body)
            self.assertIsNotNone(manifest_body)
        except botocore.exceptions.ClientError as e:
            log.error('Test encountered an error %s', e)
            self.fail('Unexpected error occurred')


    @mock_s3
    def test_check_s3_objects_nofilepresent_taskstop(self):
        from dags.onboarding.aws import check_s3_objects
        s3_cli, s3_res = self.prepare_test_bucket()
        tenant_keys = s3_cli.list_objects(Bucket='test', Prefix='tenant')
        log.info('Tenant keys has {}'.format(tenant_keys))

        res = check_s3_objects(bucket='test',
                               prefix='tenant',
                               wildcard_keys=['cust_filename_ptn', 'usg_filename_ptn'],
                               tasks_go=['cust_kg', 'usg_kg'],
                               task_stop='no!!!',
                               ts='the-ts',
                               task_instance=MagicMock())

        self.assertEqual('no!!!', res)
        try:
            unexpected_manifest = s3_res.Object('airflow-bucket', 'static/onboarding/manifests/the-ts/keep_going.json')
            manifest_body = unexpected_manifest.get()['Body'].read()
            self.assertNone(manifest_body)
            self.fail("Exection should not have reached here. No manifest should exist")
        except botocore.exceptions.ClientError as e:
            self.assertIsNotNone(e)
