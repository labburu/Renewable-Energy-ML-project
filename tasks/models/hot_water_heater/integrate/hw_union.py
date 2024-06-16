from functools import reduce

from pyspark.sql import DataFrame

from nydus.task import NydusTask


class UnionModel(NydusTask):
    def register_pickles(self, pickles):
        pass

    def __init__(self, **kwargs):
        self.tenants_with_ami = kwargs.get('tenants_with_ami', [])

    def run(self, spark):

        feature_dataframes = []

        for tenant_id in self.tenants_with_ami:
            feature_dataframes.append(spark.table('wh_results_{}'.format(tenant_id)))

        df_features = reduce(DataFrame.union, feature_dataframes)

        df_features.createOrReplaceTempView('output')
