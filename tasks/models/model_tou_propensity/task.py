"""TOU propensity model/heuristic"""

from datadog import api
import time
from pyspark.sql.functions import (
     lit,
     when,
     col,
 )


class GetTOUProp:

    @staticmethod
    def report(spark, default_tags):

        now = time.time()

        df_output = spark.table('output')

        # get % high/med/low count
        khigh = df_output.where(df_output.propensity_tou == 'high').count()
        kmed = df_output.where(df_output.propensity_tou == 'med').count()
        klow = df_output.where(df_output.propensity_tou == 'low').count()

        tot = df_output.count()

        phigh = khigh / tot
        pmed = kmed / tot
        plow = klow / tot

        # submit point with a timestamp (i.e. metric_value from last step)
        metrics = [{
             'metric': 'model_tou_prop.percent_high_prop',
             'points': (now, phigh),
             'tags': default_tags,
         }, {
             'metric': 'model_tou_prop.percent_med_prop',
             'points': (now, pmed),
             'tags': default_tags,
         }, {
             'metric': 'model_tou_prop.percent_low_prop',
             'points': (now, plow),
             'tags': default_tags,
         }]

        api.Metric.send(metrics)

    def run(self, spark):
        # Input files
        experian_normalized = spark.table('experian_normalized')
        ev = spark.table('ev')
        ev = ev\
            .select('location_id', 'has_ev_bucket')

        df = experian_normalized.join(ev, 'location_id', how='inner')

        # Dummy code Experian and EV fields
        df = df.withColumn('ev_high_med', when(
            (
                (df.has_ev_bucket <= 2) & (df.has_ev_bucket >= 0)
            ), 1).otherwise(0))
        df = df.withColumn('green_think_act', when(
            (
                (df.green_aware == 3) |
                (df.green_aware == 2)
            ), 1).otherwise(0))
        df = df.withColumn('married', when(
            (
                (df.marital_model_1 == 1) |
                (df.marital_model_1 == 2)
            ), 1).otherwise(0))
        df = df.withColumn('no_kids', when(
            (df.total_children == 0), 1).otherwise(0))
        df = df.withColumn('residence_le_5y', when(
            (df.length_residence <= 5), 1).otherwise(0))
        df = df.withColumn('age_40_to_70', when(
            (
                (df.age_1 >= 40) &
                (df.age_1 <= 70)
            ), 1).otherwise(0))
        df = df.withColumn('sq_ft_1000_to_1800', when(
            (
                (df.home_sq_footage >= 1000) &
                (df.home_sq_footage <= 1800)
            ), 1).otherwise(0))

        df = df.withColumn(
            'tou_score',
            df.ev_high_med*10 +
            df.green_think_act*6 +
            df.married*5 +
            df.no_kids*5 +
            df.residence_le_5y*4 +
            df.age_40_to_70*3 +
            df.sq_ft_1000_to_1800*2
            )

        # Bin into High/Med/Low propensity
        df = df.withColumn('propensity_tou', when(
            (col('tou_score') >= 18), lit('high'))
            .when(
                (col('tou_score') >= 10) &
                (col('tou_score') <= 17), lit('medium'))
            .otherwise(lit('low')))

        # Select fields to output
        df_output = df.select(
            'location_id',
            'tenant_id',
            'tou_score',
            'propensity_tou',
            )

        df_output.createOrReplaceTempView('output')
