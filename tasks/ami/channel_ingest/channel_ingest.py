"""Ingest and map SOA channel_uuids."""

import datetime
import json
import logging
import os
import sys
import time
import uuid
from collections import namedtuple
from concurrent import futures
from requests import Session
from requests.adapters import HTTPAdapter

from pyspark.sql import Row, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_list,
    collect_set,
    concat,
    concat_ws,
    create_map,
    dayofmonth,
    lit,
    month,
    size,
    sum,
    udf,
    when,
    year,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

OUTPUT_SCHEMA = StructType([
    StructField('external_location_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_account_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_channel_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('direction', StringType(), False, metadata={'maxlength': 1}),
    StructField('tenant_id', LongType(), False),
    StructField('account_uuid', StringType(), False, metadata={'maxlength': 36}),
    StructField('location_uuid', StringType(), False, metadata={'maxlength': 36}),
    StructField('channel_uuid', StringType(), False, metadata={'maxlength': 36}),
    StructField('time_zone', StringType(), False, metadata={'maxlength': 36})
])

ERR_OUTPUT_SCHEMA = StructType([
    StructField('file_name', StringType(), False, metadata={'maxlength': 255}),
    StructField('external_location_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_account_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('external_channel_id', StringType(), False, metadata={'maxlength': 20}),
    StructField('direction', StringType(), True, metadata={'maxlength': 1}),
    StructField('tenant_id', LongType(), False),
    StructField('account_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('location_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('channel_uuid', StringType(), True, metadata={'maxlength': 36}),
    StructField('error_code', IntegerType(), False),
    StructField('error_message', StringType(), False, metadata={'maxlength': 255})
])

METRICS_SCHEMA = StructType([
    StructField('metric_name', StringType(), False, metadata={'maxlength': 255}),
    StructField('file_name', StringType(), False, metadata={'maxlength': 255}),
    StructField('metric_count', LongType(), False)
])

AUDIT_SCHEMA = StructType([
    StructField('year', IntegerType(), False),
    StructField('month', IntegerType(), False),
    StructField('day', IntegerType(), False),
    StructField('timestamp_utc', TimestampType(), False),
    StructField('event_type', StringType(), False, metadata={'maxlength': 80}),
    StructField('filename', StringType(), False, metadata={'maxlength': 255}),
    StructField('tenant_id', LongType(), False),
    StructField('item_size', LongType(), True),
    StructField('row_count', LongType(), True),
    StructField('data', MapType(StringType(), LongType(), False), True),
    StructField('message', StringType(), True, metadata={'maxlength': 255})
])

TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'

ZeusResponse = namedtuple(
    typename='ZeusResponse',
    field_names=[
        'index',
        'error_code',
        'error_message',
        'account_uuid',
        'location_uuid',
        'channel_uuid',
        'direction',
        'time_zone',
        'location_json',
    ],
)

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class ChannelIngest():

    def __init__(
        self,
        tenant_id,
        zeus_base_url,
        zeus_concurrency=20,
        pool_block=True,
        max_retries=0,
        audit_to=None,
        errors_save_to=None,
        errors_save_format='parquet',
        default_time_zone='US/Eastern',
        column_map=None,
        dd_event=None,
        dd_metric=None,
        *args,
        **kwargs
    ):
        """
        Configurable fields --

        :param int tenant_id: SOA tenant ID. This can be used to tailor
            transformations with tenant-specific logic.
        :param str zeus_base_url: Zeus HTTP endpoint base.
            Default: url in dev-us environment
        :param str zeus_concurrency: Max number of Threads hitting zeus
            at once.
            Default: 20
        :param bool pool_block: Whether or not requests will wait for a
            free connection from the pool. If set to false, requests
            throws an exception and stops the ingest when the pool is
            full and a request is tried.
            Default: True
        :param int max_retries: Number of retries made during Zeus RPC
            calls.
            Default: 0
        :param str audit_to: Full URL destination prefix to save audit
            information to.
            Default: None (no audits saved)
        :param str errors_save_to: Full URL destination to save errors
            file to.
            Default: None (not saved)
        :param str errors_save_format: File format for errors table.
            Default: 'parquet'
        :param str default_time_zone: The time zone that will be used
            when no other time zone data are available.
            Default: 'US/Eastern'
        :param dict column_map: The mapping of the column number and column name
            that will be used when loading the AMI data. Only implemented for
            channel_ingest_duke.
            Default: None
        :param dd.api.Event dd_event: Datadog Event class to use to send
            events to dd in `report` call after ChannelIngest.run.
            Must implement `Event.create`.
        :param dd.api.Metric dd_metric: Datadog Metric class to use to
            send metrics to dd in `report` call after ChannelIngest.run.
            Must implement `Metric.send`.
        """
        if zeus_base_url is None:
            raise Exception('Missing zeus_base_url parameter')

        self.tenant_id = int(tenant_id)
        self.zeus_rpc_url = os.path.join(zeus_base_url, 'rpc')
        self.zeus_concurrency = int(zeus_concurrency)
        self.pool_block = bool(pool_block)
        self.max_retries = int(max_retries)
        self.default_time_zone = str(default_time_zone)
        self.column_map = dict(column_map) if column_map else None
        self.audit_to = audit_to
        self.errors_save_to = errors_save_to
        self.errors_save_format = errors_save_format

        # otherwise create requests Session with settings to be able to talk to Zeus HTTP-RPC:
        self.rs = Session()
        self.rs.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

        # Set up requests connection pool properties
        # Do not retry; adds too much time per request, just add to error file
        adapter = HTTPAdapter(
            pool_connections=self.zeus_concurrency,
            pool_maxsize=self.zeus_concurrency,
            max_retries=self.max_retries,
            pool_block=self.pool_block,
        )

        self.rs.mount('http://', adapter)

        self.validate_zeus_connection()

        self.dd_event = dd_event
        self.dd_metric = dd_metric

        # Only import datadog if needed
        if self.dd_metric is None or self.dd_event is None:
            import datadog
            self.dd_metric = self.dd_metric if self.dd_metric else datadog.api.Metric
            self.dd_event = self.dd_event if self.dd_event else datadog.api.Event

    @staticmethod
    def start_metrics(spark):
        """Start summary statistics DataFrame.

        This will contain metrics and audit rows.
        """
        return spark.createDataFrame([], METRICS_SCHEMA)

    @staticmethod
    def clean_file_name(full_name, default='UNKNOWN'):
        """Return sanitized file name.

        Remove leading path and file extension.
        """
        if full_name is None or len(full_name) < 1:
            fname = default
        else:
            fname = full_name.split('/')[-1]
            dotidx = fname.find('.')
            if dotidx > 0:
                fname = fname[0:dotidx]
        return fname

    def validate_zeus_connection(self):
        """Ensure that Zeus connection has been established."""
        url = os.path.join(self.zeus_rpc_url, 'HealthCheck')
        # Time limit for connection validation is 30 seconds from now
        until = time.time() + 30
        # Keep track of the last exception recieved
        last_exception = None
        # Keep track of the number of attempts to connect
        try_num = 0
        while time.time() < until:
            try_num += 1
            try:
                r = self.rs.post(url, json={})
                r.raise_for_status()
                log.info('Connected to Zeus ok %s on try#%s', url, try_num)
                return
            except Exception as e:
                log.warning('Failed to connect to Zeus at %s try#%s', url, try_num)
                last_exception = e
                time.sleep(2)

        # If we got this far without connecting, fail with an exception
        log.exception(last_exception)
        raise last_exception

    @staticmethod
    def load_zeus_channels(spark, table_name='channels', type='ELECTRIC_METER'):
        """Load transform_channels data.

        Load Dataframe with output from zeus transform_channels table.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name from `transform_channels` task
        :return: Dataframe with following schema
         |-- channel_uuid: string (nullable = true)
         |-- location_uuid: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction_char: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        return spark \
            .table(table_name) \
            .where(col('type') == lit(type)) \
            .withColumnRenamed('id', 'channel_uuid') \
            .withColumnRenamed('location_id', 'location_uuid') \
            .withColumn(
                'direction_char',
                when(
                    condition=col('direction') == 'DELIVERED',
                    value='D'
                )
                .when(
                    condition=col('direction') == 'RECEIVED',
                    value='R'
                )
                .when(
                    condition=col('direction') == 'NET',
                    value='N'
                )
                .otherwise(lit(None).cast('string'))
            ) \
            .select([
                'channel_uuid',
                'location_uuid',
                'external_channel_id',
                'direction_char',
            ])

    @staticmethod
    def load_zeus_locations(spark, tenant_id, default_time_zone, table_name='locations'):
        """Load transform_locations data.

        Load Dataframe with output from zeus transform_locations table.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name from `transform_locations` task
        :return: DataFrame with following schema
         |-- location_uuid: string (nullable = true)
         |-- account_uuid: string (nullable = true)
         |-- external_location_id: string (nullable = true)
         |-- time_zone: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        return spark \
            .table(table_name) \
            .where(col('tenant_id') == lit(tenant_id).cast(LongType())) \
            .withColumnRenamed('id', 'location_uuid') \
            .withColumnRenamed('account_id', 'account_uuid') \
            .withColumn(
                'time_zone',
                coalesce(col('time_zone'), lit(default_time_zone))
            ) \
            .select([
                'location_uuid',
                'account_uuid',
                'external_location_id',
                'time_zone'
            ])

    @staticmethod
    def load_zeus_accounts(spark, tenant_id, table_name='accounts'):
        """Load transform_accounts data.

        Load Dataframe with columns needed from zeus transform_accounts
        table.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name from `transform_accounts` task
        :return: DataFrame with following schema
         |-- account_uuid: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- is_inactive: boolean (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        return spark \
            .table(table_name) \
            .where(col('tenant_id') == lit(tenant_id).cast(LongType())) \
            .withColumnRenamed('id', 'account_uuid') \
            .withColumn(
                'is_inactive',
                coalesce(col('is_inactive'), lit(False))
            ) \
            .select([
                'account_uuid',
                'external_account_id',
                'is_inactive',
            ])

    @staticmethod
    def load_zeus_account_hub_ids(spark, tenant_id, table_name='hub_ids'):
        """Read Zeus hub_id data.

        Load Dataframe with columns needed from zeus
        transform_account_hub_ids table.

        :param pyspark.sql.SparkSession spark: Spark context
        :param str table_name: source table name from `transform_account_hub_id` task
        :return: DataFrame with following schema
         |-- account_uuid: string (nullable = true)
         |-- hub_id: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        return spark \
            .table(table_name) \
            .where(col('tenant_id') == lit(tenant_id).cast(LongType())) \
            .withColumnRenamed('account_id', 'account_uuid') \
            .select([
                'account_uuid',
                'hub_id',
            ])

    @staticmethod
    def join_tenant_ami_to_zeus_ids(
        df_ami_ids,
        df_zeus_accounts,
        df_zeus_locations,
        df_zeus_channels,
        tenant_id,
    ):
        """Left-Join the AMI IDS with matching Zeus external IDs.

        This is the standard method of joining Tenant AMI data with Zeus
        data.

        :param df_ami_ids: DataFrame from load_ami
        :param df_zeus_accounts: Dataframe with output from `load_zeus_accounts`
        :param df_zeus_locations: DataFrame with output from `load_zeus_locations`
        :param df_zeus_channels: Dataframe with output from `load_zeus_channels`
        :param int tenant_id: Tendril tenant identifier to add to result as tenant_id
        :return: DataFrame with following schema
         |-- file_name: string (nullable = true)
         |-- external_account_id: string (nullable = true)
         |-- external_location_id: string (nullable = true)
         |-- external_channel_id: string (nullable = true)
         |-- direction: string (nullable = true)
         |-- channel_uuid: string (nullable = true)
         |-- location_uuid: string (nullable = true)
         |-- account_uuid: string (nullable = true)
         |-- time_zone: string (nullable = true)
         |-- tenant_id: long (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        window = Window.partitionBy(
            'ami.external_location_id',
            'ami.external_account_id',
            'ami.external_channel_id',
            'ami.direction'
        )
        return df_ami_ids.alias('ami') \
            .join(
                df_zeus_accounts.alias('za'),
                on=[
                    col('ami.external_account_id') == col('za.external_account_id'),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_locations.alias('zl'),
                on=[
                    (col('ami.external_location_id') == col('zl.external_location_id')) &
                    (col('za.account_uuid') == col('zl.account_uuid')),
                ],
                how='left_outer',
            ) \
            .join(
                df_zeus_channels.alias('zc'),
                on=[
                    (col('ami.external_channel_id') == col('zc.external_channel_id')) &
                    (col('ami.direction') == col('zc.direction_char')) &
                    (col('zl.location_uuid') == col('zc.location_uuid')),
                ],
                how='left_outer',
            ) \
            .withColumn(
                'tenant_id',
                lit(tenant_id).cast(LongType())
            ) \
            .withColumn(
                'n_account_uuids',  # number distinct zeus account matches
                size(collect_set('za.account_uuid').over(window))
            ) \
            .withColumn(
                'n_location_uuids',  # number distinct zeus location matches
                size(collect_set('zl.location_uuid').over(window))
            ) \
            .select([
                'ami.file_name',
                'ami.external_location_id',
                'ami.external_account_id',
                'ami.external_channel_id',
                'ami.direction',
                'zc.channel_uuid',
                'zl.location_uuid',
                'za.account_uuid',
                'za.is_inactive',
                'zl.time_zone',
                'tenant_id',
                'n_account_uuids',
                'n_location_uuids',
            ])

    @staticmethod
    def extract_locations(user_graph, external_location_id):
        """Get location from RPC response.

        Given output from `GetUsersByAccountExtendedProperty` or
        `GetUsersByExternalAccount` Zeus HTTP-RPC call, traverse through
        the user/account/location portions of the graph to find the
        Location object(s) that have a matching `external_residence_id`.

        :param dict user_graph: Parsed JSON output from Zeus RPC
        :param str external_location_id: Zeus external_location_id
            Note: Duke values are zero-padded to 20 characters
        :return: Location object if a match is found, None otherwise
        :rtype: dict
        """
        locations = []
        if 'user' in user_graph:
            for user in user_graph['user']:
                if 'account' in user:
                    for account in user['account']:
                        if 'location' in account:
                            for location in account['location']:
                                if 'external_residence_id' in location:
                                    if location['external_residence_id'] == external_location_id:
                                        locations.append(location)
        return locations

    @staticmethod
    def direction_char_to_zeus(direction_char):
        """Translate consumption direction character to Zeus values.

        AMI data usually provide the direction as a single character.
        """
        if direction_char == 'D':
            direction_zstr = 'DELIVERED'
        elif direction_char == 'R':
            direction_zstr = 'RECEIVED'
        elif direction_char == 'N':
            direction_zstr = 'NET'
        else:
            direction_zstr = None
        return direction_zstr

    @staticmethod
    def extract_channel(location, external_channel_id, direction_char):
        """Get channel from RPC response.

        Given a Location object from Zeus HTTP-RPC, find the Channel
        that has a matching external_id.

        :param dict location: Parsed Zeus Location object
        :param str external_channel_id: Zeus external_channel_id value
            Note: Duke values are zero-padded to 20 characters
        :param str direction_char: single-character direction, D or R.
        :return: Channel object if a match is found, None otherwise
        :rtype: dict
        """
        direction_zstr = ChannelIngest.direction_char_to_zeus(direction_char)
        if location is not None:
            if 'channel' in location:
                for channel in location['channel']:
                    if 'external_id' in channel:
                        if channel['external_id'] == external_channel_id and \
                                channel['direction'] == direction_zstr:
                            return channel

    @staticmethod
    def common_to_uuid(common_id):
        """Return UUID hex string given Zeus Common.Id.

        Given a common_id in `{'legacy_id_prefix':9,'id':9999}` format,
        convert to UUID hex string.

        :param dict common_id: Common.Id object from Zeus
        :return: hex UUID if input object was provided in valid format,
            None otherwise.
        :rtype: str
        """
        if common_id is not None:
            if 'legacy_id_prefix' in common_id and 'id' in common_id:
                long_id = common_id['id']
                tenant_id = common_id['legacy_id_prefix']
                return str(uuid.UUID(int=int(long_id) + (int(tenant_id) << 64)))

    @staticmethod
    def uuid_to_common(u):
        """Return Zeus Common.Id from string uuid.

        Given a uuid hex string or UUID object, return as a Zeus
        Common.Id dict.

        :param str u: uuid hex string or UUID object
        :return: Common.Id in `{'legacy_id_prefix':9,'id':9999}` form
        :rtype: dict
        """
        if type(u) is str:
            u = uuid.UUID(u)
        if type(u) is uuid.UUID:
            uint = u.int
            msb = uint >> 64
            lsb = uint & ((1 << 64)-1)
            return {'legacy_id_prefix': msb, 'id': lsb}
        else:
            raise Exception('Unknown type: {}'.format(type(u)))

    @staticmethod
    def get_uuid_from_object_id(zeus_obj, attr_name='id'):
        """Return Zeus identifier in string uuid hex format.

        Given a Zeus parsed JSON object with an `id` (or other
        indicated) attribute, return the identifier in string
        uuid hex format.

        :param dict zeus_obj: Zeus object to check/parse identifier.
        :param str attr_name: identifier attribute name, default `id`
        :return: hex UUID if input object was provided, identifier
            attribute found/parsable
        :rtype: str
        """
        if zeus_obj is not None:
            if attr_name in zeus_obj:
                return ChannelIngest.common_to_uuid(zeus_obj[attr_name])

    @staticmethod
    def get_time_zone(location_obj, default_time_zone):
        """Return time zone from location object.

        Given a zeus location object, return the time_zone attribute if
        populated or class default_time_zone if null/empty.

        :param dict location_obj: Zeus Location object.
        :return: time zone String like 'US/Eastern', 'US/Pacific'
        :rtype: str
        """
        tz = None
        if location_obj is not None:
            if location_obj.get('time_zone') is not None:
                tz = location_obj['time_zone'].strip()
            if tz is None or len(tz) == 0:
                tz = default_time_zone
        return tz

    def http_lookup(self, index, external_account_id, external_location_id, external_channel_id, direction):
        """Make Zeus HTTP-RPC call to retrieve user.

        For Duke, use /GetUsersByAccountExtendedProperty.
            - external_account_id here is actually the Duke hub_id
            - Duke external IDs are zero-padded to 20 characters

        For AEP, use /GetUsersByExternalAccount.

        :param int index: Row index for concurrent result/response handling
        :param str external_account_id: tenant account id
        :param str external_location_id: tenant premise id
        :param str external_channel_id: tenant meter id
        :param str direction: direction character D (delivered) R (received)
        :return: nine-item tuple containing the following:
            - index: row index that was passed in
            - error_code: integer error code, None for success
            - error_message: string error message, None for success
            - account_uuid: string hex Tendril account UUID, None if not found
            - location_uuid: string hex Tendril location UUID, None if not found
            - channel_uuid: string hex Tendril channel UUID, None if not found
            - direction: direction character, D (delivered) or R (received)
            - time_zone: string time zone, None if location not found
            - location_json: Location JSON for use in HTTP-RPC channel, created if needed
        :rtype: tuple
        """
        try:
            if self.tenant_id == 11:
                # DukeInternalId (hub_id) is in account extended properties
                response = self.rs.post(
                    url=os.path.join(self.zeus_rpc_url, 'GetUsersByAccountExtendedProperty'),
                    json={
                        'context': {
                            'tenant_id': self.tenant_id
                        },
                        'key': 'DukeInternalId',
                        'value': external_account_id,
                        'materialize_graph': True,
                    }
                )
            else:
                # Every other tenant uses external account id
                response = self.rs.post(
                    url=os.path.join(self.zeus_rpc_url, 'GetUsersByExternalAccount'),
                    json={
                        'context': {
                            'tenant_id': self.tenant_id
                        },
                        'externalAccount': external_account_id,
                        'materialize_graph': True,
                    },
                )

            response.raise_for_status()
            user_graph = response.json()
            location_objs = ChannelIngest.extract_locations(user_graph, external_location_id)

            if len(location_objs) < 1:
                return ZeusResponse(
                    index=index,
                    error_code=4042,
                    error_message='Unmatched external location/premise id (rpc)',
                    account_uuid=None,
                    location_uuid=None,
                    channel_uuid=None,
                    direction=direction,
                    time_zone=None,
                    location_json=None,
                )
            elif len(location_objs) > 1:
                return ZeusResponse(
                    index=index,
                    error_code=5242,
                    error_message='Multiple ({}) external location/premise id matches'.format(len(location_objs)),
                    account_uuid=None,
                    location_uuid=None,
                    channel_uuid=None,
                    direction=direction,
                    time_zone=None,
                    location_json=None,
                )

            # If we get this far, must have a *single* matching location object/id
            location_obj = location_objs[0]
            account_uuid = ChannelIngest.get_uuid_from_object_id(location_obj, 'account_id')
            location_uuid = ChannelIngest.get_uuid_from_object_id(location_obj)
            time_zone = ChannelIngest.get_time_zone(location_obj, default_time_zone=self.default_time_zone)
            channel_obj = ChannelIngest.extract_channel(location_obj, external_channel_id, direction)
            channel_uuid = ChannelIngest.get_uuid_from_object_id(channel_obj)
            # Populate location_json if channel create will be needed
            location_json = None
            if channel_uuid is None and location_obj is not None:
                location_json = json.dumps(location_obj)

            return ZeusResponse(
                index=index,
                error_code=None,
                error_message=None,
                account_uuid=account_uuid,
                location_uuid=location_uuid,
                channel_uuid=channel_uuid,
                direction=direction,
                time_zone=time_zone,
                location_json=location_json,
            )
        except Exception as e:
            log.debug('Failed to retrieve User by Id=%s: %s', external_account_id, e)
            return ZeusResponse(
                index=index,
                error_code=4041,
                error_message='External account id not found',
                account_uuid=None,
                location_uuid=None,
                channel_uuid=None,
                direction=direction,
                time_zone=None,
                location_json=None,
            )

    @staticmethod
    def update_vals_in_row(in_row, in_cols, row_factory, vals_to_update):
        new_vals = []
        for col_name in in_cols:
            if col_name in vals_to_update:
                new_vals.append(vals_to_update[col_name])
            else:
                new_vals.append(in_row[col_name])
        return row_factory(*new_vals)

    @staticmethod
    def dur_rate(start_t, row_count):
        """Calculate duration and rate.

        Uses input start time and row count-so-far, handling zeros.
        """
        dur_s = time.time() - start_t
        rate = row_count
        if dur_s > 0:
            rate = int(row_count / dur_s)
        return int(dur_s), rate

    def zeus_rpc_find_channels(self, spark, df_ids):
        """Find channels using Zeus RPC.

        For each dataframe row, use external account id to look up user
        graph by account extended property or external account id, and
        fill in Zeus UUID columns where a match can be found. This needs
        to be done with a *collected* set of rows to ensure a single
        HTTP-RPC call per row to be looked up.

        :param df_ids: input DataFrame with incomplete rows
            (missing account_, location_, or channel_uuid's) from join
            of ami to zeus IDs
        :return: synthesized DataFrame with these columns populated if
            able to match from Zeus HTTP-RPC lookup, or remaining null
            if unable to find a match with error_code and error_message
            populated on any lookup errors/reasons
         |-- channel_uuid: string (nullable = true)
         |-- location_uuid: string (nullable = true)
         |-- account_uuid: string (nullable = true)
         |-- time_zone: string (nullable = true)
         |-- location_json: string (nullable = true)
         |-- error_code: int (nullable = true)
         |-- error_message: string (nullable = true)
        :rtype: pyspark.sql.DataFrame
        """
        # Add location_json to output schema
        df_ids = df_ids.withColumn('location_json', lit(None).cast(StringType()))
        start_t = time.time()
        log.info('Collecting for Zeus HTTP-RPC lookup of accounts...')
        in_rows = df_ids.collect()
        tgt_count = len(in_rows)
        log.info('Collected Zeus HTTP-RPC lookup of accounts, secs=%s count=%s',
                 int(time.time() - start_t), tgt_count)
        in_cols = df_ids.columns
        row_factory = Row(*in_cols)
        out_rows = []
        row_count = 0
        fnd_count = 0

        def futures_to_out_rows(future_to_tuple):
            count_completed = 0
            count_success = 0
            for future in futures.as_completed(future_to_tuple):
                count_completed += 1

                # Construct ZeusResponse tuple
                response = future.result()

                row = in_rows[response.index]
                if response.account_uuid is not None:
                    count_success += 1

                row = ChannelIngest.update_vals_in_row(
                    in_row=row,
                    in_cols=in_cols,
                    row_factory=row_factory,
                    vals_to_update={
                        'account_uuid': response.account_uuid,
                        'location_uuid': response.location_uuid,
                        'channel_uuid': response.channel_uuid,
                        'direction': response.direction,
                        'time_zone': response.time_zone,
                        'location_json': response.location_json,
                        'error_code': response.error_code,
                        'error_message': response.error_message,
                    }
                )

                out_rows.append(row)

            return count_completed, count_success

        with futures.ThreadPoolExecutor(max_workers=self.zeus_concurrency) as executor:
            future_to_tuple = []
            for idx, row in enumerate(in_rows):
                future_to_tuple.append(
                    executor.submit(
                        self.http_lookup,
                        index=idx,
                        external_account_id=row.external_account_id,
                        external_location_id=row.external_location_id,
                        external_channel_id=row.external_channel_id,
                        direction=row.direction,
                    )
                )

                # let each thread run 10x requests before collecting
                if (len(future_to_tuple) % (self.zeus_concurrency * 10)) == 0:
                    count_completed, count_success = futures_to_out_rows(future_to_tuple)
                    row_count += count_completed
                    fnd_count += count_success
                    future_to_tuple.clear()
                    if (row_count % 1000) == 0:
                        dur_s, rate = ChannelIngest.dur_rate(start_t, row_count)
                        log.info('HTTP-RPC lookup progress %s/%s %s/s found=%s nomatch=%s',
                                 row_count, tgt_count, rate, fnd_count, (row_count-fnd_count))

            # Collect final futures
            if len(future_to_tuple) > 0:
                count_completed, count_success = futures_to_out_rows(future_to_tuple)
                row_count += count_completed
                fnd_count += count_success
                future_to_tuple.clear()

        dur_s, rate = ChannelIngest.dur_rate(start_t, row_count)

        log.info('HTTP-RPC lookup complete %s/%s secs=%s rate=%s/s found=%s nomatch=%s',
                 row_count, tgt_count, int(dur_s), rate, fnd_count, (row_count-fnd_count))
        return spark.createDataFrame(out_rows, df_ids.schema)

    @staticmethod
    def add_channel(location_obj, external_channel_id, direction_char):
        """Adds a channel to the location object.

        Mutates location object.

        :param dict location_obj: the location object to be updated
        :param str external_channel_id: the external channel ID to add
        :param str direction_char: direction character like D or R
        :return: The location object we added the channel to
        """
        location_obj.setdefault('channel', []).append({
            'location_id': location_obj['id'],
            'type': 'ELECTRIC_METER',
            'direction': ChannelIngest.direction_char_to_zeus(direction_char),
            'name': external_channel_id,
            'external_id': external_channel_id
        })
        return location_obj

    def http_get_location(self, location_uuid):
        """Make Zeus HTTP-RPC call to look up Location by Tendril UUID.

        :param str location_uuid: Tendril location UUID
        :raises: Exception in case of HTTP error or Location missing in response
        :return: Location object from Zeus if successfully looked up
        :rtype: dict
        """
        r = self.rs.post(
            url=os.path.join(self.zeus_rpc_url, 'GetLocationById'),
            json={
                'context': {
                    'tenant_id': self.tenant_id
                },
                'id': ChannelIngest.uuid_to_common(location_uuid)
            }
        )
        r.raise_for_status()
        location_resp = r.json()
        if 'location' in location_resp:
            return location_resp['location']
        else:
            raise Exception('Failed GetLocationById - no location in response for request id {}: {}'
                            .format(location_uuid, r.text))

    def http_update_location(self, location_uuid, location_obj):
        """Make Zeus HTTP-RPC call to update a Location object.

        :param str location_uuid: Tendril location UUID
        :param dict location_obj: Zeus Location object to be updated
        :raises: Exception in case of HTTP error
        :return: None
        """
        r = self.rs.post(
            os.path.join(self.zeus_rpc_url, 'UpdateLocation'),
            json={
                'context': {
                    'tenant_id': self.tenant_id
                },
                'location': location_obj
            }
        )
        r.raise_for_status()
        log.debug('UpdateLocation uuid=%s response=%s', location_uuid, r.text)
        resp_obj = r.json()
        if 'error' in resp_obj:
            raise Exception(resp_obj['error'])

    def http_create_channel(self, idx, location_uuid, external_channel_id, direction_char, location_json):
        """Create channel using Zeus HTTP-RPC.

        Make the necessary sequence of Zeus HTTP-RPC calls to add a
        Channel to a Location but only if *not* already present in the
        Location.

        :param int idx: Row index for concurrent result/response handling
        :param str location_uuid: Tendril location UUID
        :param str external_channel_id: Tenant meter id
        :param str direction_char: Direction character (e.g., D or R)
        :param str location_json: Zeus Location object in json to have Channel added
        :return: tuple of idx, channel_uuid, error_message where:
            - If successful, channel_uuid will be populated and error_message will not
            - If failed, channel_uuid will be None and error_message will be populated
        :rtype: tuple
        """
        try_num = 0
        location_obj = json.loads(location_json)
        channel_uuid = None
        error_message = None
        while try_num < 2:
            try_num += 1
            try:
                if location_obj is None:
                    location_obj = self.http_get_location(location_uuid)
                channel_obj = ChannelIngest.extract_channel(location_obj, external_channel_id, direction_char)
                if channel_obj is None:
                    location_obj = ChannelIngest.add_channel(location_obj, external_channel_id, direction_char)
                    self.http_update_location(location_uuid, location_obj)
                    location_obj = self.http_get_location(location_uuid)
                    channel_obj = ChannelIngest.extract_channel(location_obj, external_channel_id, direction_char)
                channel_uuid = ChannelIngest.get_uuid_from_object_id(channel_obj)
                if channel_uuid is None:
                    error_message = 'Failed Create Channel location={} external_channel_id={} dir={}' \
                            .format(location_uuid, external_channel_id, direction_char)
                else:
                    error_message = None
                    break
            except Exception as e:
                log.warning('Error try#%s create_channel for external_channel_id=%s dir=%s: %s',
                            try_num, external_channel_id, direction_char, e)
                location_obj = None
                error_message = str(e)
        return idx, channel_uuid, error_message

    def zeus_rpc_create_channels(self, spark, df_ingestable):
        """Make Zeus HTTP-RPC calls to create channels from DataFrame.

        For each dataframe row that is *missing* channel_uuid but has
        valid location_uuid, and location_json, make appropriate Zeus
        HTTP-RPC calls to update the Location with a new Channel. This
        is done with a *collected* data set to ensure each HTTP-RPC
        sequence is only done *once*.

        :param df_w_location_uuid: input DataFrame to check for and
            handle Channel creation
        :return: DataFrame with channel_uuid populated if successful
            and error_code,error_message added and populated if
            unsuccessful.
        Affected columns:
            |-- channel_uuid: string (nullable = true)
            |-- error_code: integer (nullable = false)
            |-- error_message: string (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        start_t = time.time()
        tgt_count = df_ingestable.count()
        log.info('Collecting for Zeus HTTP-RPC Create Channel, count=%s', tgt_count)
        df_w_err_cols = df_ingestable \
            .withColumn('error_code', lit(None).cast(IntegerType())) \
            .withColumn('error_message', lit(None).cast(StringType()))
        in_rows = df_w_err_cols.collect()
        in_cols = df_w_err_cols.columns
        row_factory = Row(*in_cols)
        out_rows = []
        row_count = 0
        add_count = 0

        def futures_to_out_rows(future_to_tuple):
            count_completed = 0
            count_success = 0
            for future in futures.as_completed(future_to_tuple):
                count_completed += 1
                idx, channel_uuid, error_message = future.result()
                row = in_rows[idx]
                error_code = 5001
                if error_message is None:
                    count_success += 1
                    error_code = None

                row = ChannelIngest.update_vals_in_row(
                    in_row=row,
                    in_cols=in_cols,
                    row_factory=row_factory,
                    vals_to_update={
                        'channel_uuid': channel_uuid,
                        'error_code': error_code,
                        'error_message': error_message,
                    }
                )

                out_rows.append(row)
            return count_completed, count_success

        with futures.ThreadPoolExecutor(max_workers=self.zeus_concurrency) as executor:
            future_to_tuple = []
            for idx, row in enumerate(in_rows):
                future_to_tuple.append(
                    executor.submit(
                        self.http_create_channel,
                        idx,
                        row.location_uuid,
                        row.external_channel_id,
                        row.direction,
                        row.location_json,
                    )
                )
                # let each thread run 10x requests before collecting
                if (len(future_to_tuple) % (self.zeus_concurrency * 10)) == 0:
                    count_completed, count_success = futures_to_out_rows(future_to_tuple)
                    row_count += count_completed
                    add_count += count_success
                    future_to_tuple.clear()
                    if (row_count % 1000) == 0:
                        dur_s, rate = ChannelIngest.dur_rate(start_t, row_count)
                        log.info('HTTP-RPC channel progress %s/%s secs=%s rate=%s/s create=%s fail=%s',
                                 row_count, tgt_count, dur_s, rate, add_count, (row_count-add_count))
            # Collect final futures
            if len(future_to_tuple) > 0:
                count_completed, count_success = futures_to_out_rows(future_to_tuple)
                row_count += count_completed
                add_count += count_success
                future_to_tuple.clear()
        dur_s, rate = ChannelIngest.dur_rate(start_t, row_count)
        log.info('HTTP-RPC channel complete %s/%s secs=%s rate=%s/s created=%s failed=%s',
                 row_count, tgt_count, dur_s, rate, add_count, (row_count-add_count))
        return spark.createDataFrame(out_rows, df_w_err_cols.schema)

    @staticmethod
    def start_complete_rows(df_join):
        """Start DataFrame of complete ID rows.

        Start a DataFrame that is a subset of the ami/zeus join that
        has fully-populated Zeus ids and a subset of the columns in
        appropriate output order.

        :param df_join: output of the `join_ami_zeus_ids` call that
            left-joins zeus to ami IDs
        :return: Dataframe subset where channel_uuid, location_uuid,
            and account_uuid are all successfully populated from the
            left join with following schema:
            |-- file_name: string (nullable = false)
            |-- external_location_id: string (nullable = false)
            |-- external_account_id: string (nullable = false)
            |-- external_channel_id: string (nullable = false)
            |-- direction: string (nullable = false)
            |-- tenant_id: long (nullable = false)
            |-- account_uuid: string (nullable = false)
            |-- location_uuid: string (nullable = false)
            |-- channel_uuid: string (nullable = false)
            |-- time_zone: string (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        return df_join \
            .where(
                col('channel_uuid').isNotNull() &
                # ONLY allow output where there is *exactly* one
                # matching zeus account and location
                # Bugfix: https://app.clubhouse.io/sustaining/story/2887
                (col('n_account_uuids') == lit(1)) &
                (col('n_location_uuids') == lit(1)) &
                # Disallow *inactive* zeus accounts
                # Bugfix: https://app.clubhouse.io/sustaining/story/3250
                (col('is_inactive').isNull() | ~col('is_inactive'))
            ) \
            .select([
                'file_name',
                'external_location_id',
                'external_account_id',
                'external_channel_id',
                'direction',
                'tenant_id',
                'account_uuid',
                'location_uuid',
                'channel_uuid',
                'time_zone',
            ])

    @staticmethod
    def select_incomplete_rows(df_join):
        """Start DataFrame of incomplete ID rows.

        Start a DataFrame that is the subset of output from the tenant
        ID join step that is missing channel ids.

        :param df_join: output of the `join_ami_zeus_ids` call that
            left-joins zeus to ami IDs
        :return: DataFrame subset where channel_uuid is null, same
            schema as `join_ami_zeus_ids`.
        :rtype: pyspark.sql.DataFrame
        """
        return df_join \
            .where(
                col('channel_uuid').isNull() &
                (col('n_account_uuids') <= lit(1)) &
                (col('n_location_uuids') <= lit(1)) &
                # Do not bother looking up zeus inactive accounts
                (col('is_inactive').isNull() | ~col('is_inactive'))
            ) \
            .withColumn('error_code', lit(None).cast(IntegerType())) \
            .withColumn('error_message', lit(None).cast(StringType()))

    @staticmethod
    def select_ingestable_rows(df_w_location_uuid):
        """Start DataFrame of ingestable ID rows.

        Select subset of Dataframe output from join_ami_zeus_ids that
        is ingestable as a new channel for an existing Location and
        Account with uuids populated.

        :param df_w_location_uuid: output of `join_ami_zeus_ids` and/or
            `zeus_rpc_find_channels`
        :return: DataFrame subset where channel_uuid_is null but
            location_uuid and account_uuid are populated
        :rtype: pyspark.sql.DataFrame
        """
        return df_w_location_uuid.where(
            col('channel_uuid').isNull() &
            col('location_uuid').isNotNull() &
            col('account_uuid').isNotNull() &
            col('location_json').isNotNull() &
            col('error_code').isNull()
        )

    @staticmethod
    def add_complete_rows(df_dest, df_to_add_if_complete):
        """Add rows to complete IDs DataFrame.

        Union on to destination DataFrame where the rows from the
        Dataframe to add have complete Zeus channel_uuid, location_uuid,
        and account_uuid values populated.

        :param df_dest: Dataframe to add to
        :param df_to_add_if_complete: Dataframe rows/IDs to add to dest
            if Zeus uuids populated.
        :return: Dataframe with union where Zeus uuids are complete.
        :rtype: pyspark.sql.DataFrame
        """
        df_to_add = df_to_add_if_complete.where(
            col('channel_uuid').isNotNull() &
            col('location_uuid').isNotNull() &
            col('account_uuid').isNotNull() &
            col('error_code').isNull() &
            col('error_message').isNull()
        )
        log.info('Adding more complete rows, count=%s', df_to_add.count())
        return df_dest.union(df_to_add.select(df_dest.columns))

    @staticmethod
    def start_err_rows(spark, df_join, df_after_rpc_find):
        """Start DataFrame of error rows.

        Start DataFrame of Error row results, adding error_code and
        error_message columns and populating them where either the Zeus
        account_uuid or location_uuid are missing.

        :param array base_columns: starting (success) columns to begin the output
        :param df_after_rpc_find: Dataframe results of the call to
            `zeus_rpc_find_channels` to look for rows where account_uuid
            or location_uuid are missing.
        :return: Dataframe to be output as "error" rows with schema:
            |-- external_location_id: string (nullable = false)
            |-- external_account_id: string (nullable = false)
            |-- external_channel_id: string (nullable = false)
            |-- direction: string (nullable = false)
            |-- tenant_id: long (nullable = false)
            |-- account_uuid: string (nullable = true)
            |-- location_uuid: string (nullable = true)
            |-- channel_uuid: string (nullable = true)
            |-- error_code: integer (nullable = false)
            |-- error_message: string (nullable = false)
        :rtype: pyspark.sql.DataFrame
        """
        df_errors = spark.createDataFrame([], ERR_OUTPUT_SCHEMA)

        df_errors_rpc_find = df_after_rpc_find \
            .where(
                col('account_uuid').isNull() |
                col('location_uuid').isNull() |
                col('error_code').isNotNull()
            ) \
            .withColumn(
                'error_code',
                (
                    when(
                        condition=col('error_code').isNotNull(),
                        value=col('error_code')
                    )
                    .when(
                        condition=col('account_uuid').isNull(),
                        value=lit(4041)
                    )
                    .otherwise(
                        value=lit(4042)
                    )
                ).cast(IntegerType())
            ) \
            .withColumn(
                'error_message',
                (
                    when(
                        condition=col('error_message').isNotNull(),
                        value=col('error_message')
                    )
                    .when(
                        condition=col('account_uuid').isNull(),
                        value=lit('Unmatched account hub id')
                    )
                    .otherwise(
                        value=lit('Unmatched external location/premise id')
                    )
                ).cast(StringType())
            ) \
            .select(ERR_OUTPUT_SCHEMA.fieldNames())

        df_errors = df_errors.union(df_errors_rpc_find)

        # Error rows where external IDs match multiple zeus accounts/locations
        # Bugfix: https://app.clubhouse.io/sustaining/story/2887
        # Error rows where latest zeus export indicated account was marked inactive
        # Bugfix: https://app.clubhouse.io/sustaining/story/3250
        df_errors_join = df_join \
            .where(
                (col('n_account_uuids') > lit(1)) |
                (col('n_location_uuids') > lit(1)) |
                (col('is_inactive').isNotNull() & col('is_inactive'))
            ) \
            .withColumn(
                'error_code',
                (
                    when(
                        condition=col('n_account_uuids') > lit(1),
                        value=lit(5241)
                    )
                    .when(
                        condition=col('is_inactive') == lit(True),
                        value=lit(5141)
                    )
                    .otherwise(
                        value=lit(5242)
                    )
                ).cast(IntegerType())
            ) \
            .withColumn(
                'error_message',
                (
                    when(
                        condition=col('n_account_uuids') > lit(1),
                        value=concat(col('n_account_uuids'), lit(' account matches'))
                    )
                    .when(
                        condition=col('is_inactive') == lit(True),
                        value=lit('Inactive account')
                    )
                    .otherwise(
                        value=concat(col('n_location_uuids'), lit(' location matches'))
                    )
                ).cast(StringType())
            ) \
            .select(ERR_OUTPUT_SCHEMA.fieldNames())

        df_errors = df_errors.union(df_errors_join)
        log.info('Start error rows, count=%s', df_errors.count())
        return df_errors

    @staticmethod
    def add_err_rows_from_rpc_create(df_dest, df_to_add_if_err):
        """
        Append unsuccessful rows from `zeus_rpc_create_channels` to the error output rows
        where channel_uuid remained empty.
        :param df_dest: error rows Dataframe to be appended to with a union
        :param df_to_add_if_err: output dataframe from `zeus_rpc_create_channels` where
          channel_uuid is missing
        :return: Dataframe result of the union with same schema as `start_err_rows`
        :rtype: pyspark.sql.DataFrame
        """
        df_to_add = df_to_add_if_err.where(col('channel_uuid').isNull())
        log.info(
            'Adding error rows from rpc create channel, count=%s',
            df_to_add.count()
        )
        return df_dest.union(df_to_add.select(df_dest.columns))

    def save_error_rows(self, df_err_rows):
        """
        Save error rows DataFrame if configured to do so (if errors_save_to given to ctor)
        :param df_err_rows: Error rows dataframe with schema from `start_err_rows`
        :return: None
        """
        cnt = df_err_rows.count()
        if self.errors_save_to is None:
            log.info(
                'Skipping save_error_rows for {} rows, '
                'no errors_save_to configured'.format(cnt)
            )
        else:
            df_err_rows \
                .coalesce(1) \
                .write \
                .save(
                    path=self.errors_save_to,
                    mode='overwrite',
                    format=self.errors_save_format
                )
            log.info('Saved {} rows to {} ok'.format(cnt, self.errors_save_to))

    @staticmethod
    def add_metric_count(df_metrics, metric_name, df_to_count):
        """Add a summary count row onto the metrics dataframe.

        :param df_metrics: destination metrics DataFrame
        :param str metric_name: name of new metric to add
        :param df_to_count: dataframe to count by `file_name`
        :return: new df_metrics DataFrame with new metrics row(s) added
        """
        df_to_add = df_to_count \
            .groupBy(col('file_name')) \
            .count() \
            .withColumnRenamed('count', 'metric_count') \
            .withColumn('metric_name', lit(metric_name)) \
            .select([
                'metric_name',
                'file_name',
                'metric_count',
            ])

        return df_metrics.union(df_to_add)

    @staticmethod
    def add_error_code_metrics(df_metrics, df_err_rows):
        """Add count rows onto metrics dataframe with one row per error_code.

        :param df_metrics: destination metrics DataFrame
        :param df_err_rows: error rows dataframe with file_name and error_code to be counted
        :return: new df_metrics DataFrame with new metrics row(s) added
        """
        df_to_add = df_err_rows \
            .groupBy(col('file_name'), col('error_code')) \
            .count() \
            .withColumnRenamed('count', 'metric_count') \
            .withColumn(
                'metric_name',
                concat(lit('error_'), col('error_code'))
            ) \
            .select(['metric_name', 'file_name', 'metric_count'])

        return df_metrics.union(df_to_add)

    @staticmethod
    def combine_map(list_of_maps):
        res = {}
        for m in list_of_maps:
            for k in m:
                if k in res:
                    res[k] += m[k]
                else:
                    res[k] = m[k]
        return res

    def report_audit(self, spark, dtnow, df_metrics_by_file):
        """
        Append audit table entries to S3, one per file, if `audit_to` is configured.
        :param datetime.datetime dtnow: "now" timestamp
        :param df_metrics_by_file: metrics dataframe
        :return: None
        """
        combine_map_udf = udf(ChannelIngest.combine_map, MapType(StringType(), LongType(), False))
        df_audit_tmp = df_metrics_by_file \
            .withColumn(
                'metric_map',
                create_map(col('metric_name'), col('metric_count'))
            ) \
            .withColumn(
                'row_count',
                when(
                    condition=col('metric_name').isin(['channel_ok', 'channel_error']),
                    value=col('metric_count')
                )
                .otherwise(lit(None))
            ) \
            .groupBy('file_name') \
            .agg(
                collect_list(col('metric_map')).alias('metric_maps'),
                sum(col('row_count')).alias('row_count')
            ) \
            .withColumn('data', combine_map_udf(col('metric_maps'))) \
            .withColumnRenamed('file_name', 'filename') \
            .withColumn('tenant_id', lit(self.tenant_id).cast(LongType())) \
            .withColumn('item_size', lit(None).cast(LongType())) \
            .withColumn('timestamp_utc', lit(dtnow).cast(TimestampType())) \
            .withColumn('year', year(col('timestamp_utc'))) \
            .withColumn('month', month(col('timestamp_utc'))) \
            .withColumn('day', dayofmonth(col('timestamp_utc'))) \
            .withColumn('event_type', lit('CHANNEL_INGEST')) \
            .withColumn('message', lit('Ingest completed'))

        df_audit = spark.createDataFrame([], AUDIT_SCHEMA)
        df_audit = df_audit.union(df_audit_tmp.select(AUDIT_SCHEMA.fieldNames()))

        if self.audit_to is None:
            log.info(
                'AUDIT skipping save of %s rows, no audit_to configured',
                df_audit.count()
            )
        else:
            df_audit \
                .coalesce(1) \
                .write \
                .option('timeZone', 'UTC') \
                .partitionBy('tenant_id', 'year', 'month', 'day') \
                .json(
                    self.audit_to,
                    mode='append',
                    timestampFormat=TIMESTAMP_FORMAT
                )

    def report_dd_events(self, dtnow, tags, df_metrics_by_file):
        """Report a Datadog Event for each file_name that we ingested"""
        df_metrics = df_metrics_by_file \
            .withColumn(
                'm_eq_v',
                concat_ws('=', col('metric_name'), col('metric_count'))
            ) \
            .groupBy(col('file_name')) \
            .agg(collect_list('m_eq_v').alias('metrics'))

        for row in df_metrics.orderBy('file_name').collect():
            log.info('FILE %s: %s', row.file_name, row.metrics)
            title = 'Channel Ingest: {}'.format(row.file_name)
            ok_cnt = 0
            err_cnt = 0
            for kv in row.metrics:
                if kv.startswith('channel_ok='):
                    ok_cnt += int(kv.split('=')[-1])
                elif kv.startswith('channel_error'):
                    err_cnt += int(kv.split('=')[-1])

            pct_ok = 0.0
            if ok_cnt + err_cnt > 0:
                pct_ok = 100.0 * (ok_cnt * 1.0 / (ok_cnt + err_cnt))
            if pct_ok >= 100.0:
                alert_type = 'info'
            elif pct_ok >= 95.0:
                alert_type = 'warning'
            else:
                alert_type = 'error'

            text = 'Channel ingest completed, {:.4f}% complete. ' \
                'Results: {}'.format(pct_ok, ', '.join(row.metrics))

            self.dd_event.create(
                title=title,
                text=text,
                alert_type=alert_type,
                tags=tags,
            )

    def report_dd_metrics(self, dtnow, tags, df_metrics_by_file):
        """Aggregate overall metrics to report to Datadog"""
        now = dtnow.timestamp()
        metrics = []
        df_metrics_ov = df_metrics_by_file \
            .groupBy(col('metric_name')) \
            .agg(sum(col('metric_count')).alias('metric_count'))

        for row in df_metrics_ov.orderBy('metric_name').collect():
            log.info('METRIC %s=%s', row.metric_name, row.metric_count)
            metrics.append({
                'metric': 'duke_channel_ingest.{}'.format(row.metric_name),
                'points': (now, row.metric_count),
                'tags': tags,
                'type': 'counter'
            })

        if len(metrics) > 0:
            self.dd_metric.send(metrics)

    def register_output(self, spark, df):
        """Register output DataFrame.

        Final output schema drops file_name column and duplicate ID
        mappings across files.

        :return: None
        """
        spark \
            .createDataFrame([], OUTPUT_SCHEMA) \
            .union(df.select(OUTPUT_SCHEMA.fieldNames()).dropDuplicates()) \
            .createOrReplaceTempView('output')

    def run(self, spark):
        """Main Entry Point for Channel ingest.

        This must be implemented per-tenant.
        """
        raise NotImplementedError

    def report(self, spark, default_tags):
        """Report ingest metrics."""
        # only import pytz if needed
        from pytz import UTC
        df_metrics_by_file = spark.table('metrics')
        dtnow = datetime.datetime.now(tz=UTC)
        tags = default_tags

        # Add tenant_id tag for tenant filtering in DataDog
        tags += ['tenant_id:{}'.format(self.tenant_id)]

        # Create audit rows and save to partitioned json files
        self.report_audit(spark, dtnow, df_metrics_by_file)

        # Datadog events and metrics
        self.report_dd_events(dtnow, tags, df_metrics_by_file)
        self.report_dd_metrics(dtnow, tags, df_metrics_by_file)
