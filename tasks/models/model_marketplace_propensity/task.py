"""Marketplace purchase propensity model/heuristic"""

from datadog import api
import time
from pyspark.sql.functions import (
     lit,
     when,
     col,
 )


class GetMarketplaceProp:

    @staticmethod
    def report(spark, default_tags):

        now = time.time()

        df_output = spark.table('output')

        # get % high/med/low count
        khigh = (df_output.groupBy('marketplace_purchase_propensity').count().collect())[1][1]
        kmed = (df_output.groupBy('marketplace_purchase_propensity').count().collect())[2][1]
        klow = (df_output.groupBy('marketplace_purchase_propensity').count().collect())[0][1]
        tot = df_output.count()

        phigh = khigh / tot
        pmed = kmed / tot
        plow = klow / tot

        # submit point with a timestamp (i.e. the metric_value calculated in last step)
        metrics = [{
             'metric': 'model_marketplace_prop.percent_high_prop',
             'points': (now, phigh),
             'tags': default_tags,
         }, {
             'metric': 'model_marketplace_prop.percent_med_prop',
             'points': (now, pmed),
             'tags': default_tags,
         }, {
             'metric': 'model_marketplace_prop.percent_low_prop',
             'points': (now, plow),
             'tags': default_tags,
         }]

        api.Metric.send(metrics)

    def run(self, spark):
        # Input files
        experian = spark.table('joined_experian')

        # Select relevant Experian fields: GreenAware and Marital Status
        df = experian \
            .select([
                'location_id',
                'green_aware',
                'mrtl_model_1',
                ])

        # Exclude nulls/0s/unknowns
        df = df.filter(
            (df['green_aware'].isin([1, 2, 3, 4])) &
            (df['mrtl_model_1'].isin(['1M', '5M', '5S']))
            )

        # Bin into High/Med/Low propensity
        # Experian codes in parentheses
        # High: Behavioral Green (1), Married (1M, 5M)
        # Medium: Behavioral Green (1), Single (5S), and
        #   Think Green (2), Married/Single (1M, 5M)
        # Low: Potential Green (3) and True Brown (4), Married/Single (1M, 5M, 5S)
        df = df \
            .withColumn('marketplace_purchase_propensity', when(
                (col('green_aware') == 1) &
                ((col('mrtl_model_1') == '5M') |
                    (col('mrtl_model_1') == '1M')),
                lit('high')
                )
                .when(
                    (col('green_aware') == 1) & (col('mrtl_model_1') == '5S'),
                    lit('medium')
                    )
                .when(
                    (col('green_aware') == 2) &
                    ((col('mrtl_model_1') == '5M') |
                        (col('mrtl_model_1') == '1M') |
                        (col('mrtl_model_1') == '5S')),
                    lit('medium')
                    )
                .when(
                    ((col('green_aware') == 3) | (col('green_aware') == 4)) &
                    ((col('mrtl_model_1') == '5M') |
                        (col('mrtl_model_1') == '1M') |
                        (col('mrtl_model_1') == '5S')),
                    lit('low')
                    )
                .otherwise(lit('unknown')))

        # Select fields to output
        df_output = df.select('location_id', 'marketplace_purchase_propensity')

        df_output.groupBy('marketplace_purchase_propensity').count().show()

        assert df_output.dtypes == [
            ('location_id', 'string'),
            ('marketplace_purchase_propensity', 'string'),
        ]

        df_output.createOrReplaceTempView('output')
