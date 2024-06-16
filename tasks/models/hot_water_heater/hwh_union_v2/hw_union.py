from nydus.task import NydusTask


class UnionModel(NydusTask):
    def __init__(self):
        self.tenant_other = 'other'
        self.tenant_APC = '109'

    def run(self, spark):
        df_other = spark.table('wh_results_'+self.tenant_other)
        df_109 = spark.table('wh_results_'+self.tenant_APC)

        df_output = df_other.union(df_109)

        df_output.createOrReplaceTempView('output')
