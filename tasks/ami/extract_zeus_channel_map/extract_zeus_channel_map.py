import logging
import sys

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] {%(filename)s:%(lineno)d}: %(message)s',
    stream=sys.stdout,
    level=logging.INFO,
)

log = logging.getLogger(__name__)


class ZeusChannelMap():

    def __init__(
        self,
        tenant_id,
        extract_query_name,
        *args, **kwargs
            ):
        self.tenant_id = tenant_id
        self.extract_query_name = extract_query_name

    @staticmethod
    def get_zeus_channel_map(spark, table_name):
        return spark.table(table_name)

    def run(self, spark):
        log.info('getting zeus channel map for tenant {}'.format(self.tenant_id))
        log.info('query name = {}'.format(self.extract_query_name))
        df_output = ZeusChannelMap.get_zeus_channel_map(spark, '{}'.format(self.extract_query_name))
        df_output.createOrReplaceTempView('output')
