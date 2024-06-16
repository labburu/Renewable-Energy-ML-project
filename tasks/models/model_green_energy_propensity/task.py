"""Green Energy propensity model/heuristic"""

from datadog import api
import time
from pyspark.sql.functions import (
     lit,
     when,
     col,
 )


class GetGreenEnergyProp:

    @staticmethod
    def report(spark, default_tags):

        now = time.time()

        df_output = spark.table('output')

        # get % high/med/low count
        khigh = df_output.filter(df_output.propensity_green_energy == 'high').count()
        kmed = df_output.filter(df_output.propensity_green_energy == 'medium').count()
        klow = df_output.filter(df_output.propensity_green_energy == 'low').count()

        tot = df_output.count()

        phigh = khigh / tot
        pmed = kmed / tot
        plow = klow / tot

        # submit point with a timestamp (i.e. metric_value from last step)
        metrics = [{
             'metric': 'model_green_energy_prop.percent_high_prop',
             'points': (now, phigh),
             'tags': default_tags,
         }, {
             'metric': 'model_green_energy_prop.percent_med_prop',
             'points': (now, pmed),
             'tags': default_tags,
         }, {
             'metric': 'model_green_energy_prop.percent_low_prop',
             'points': (now, plow),
             'tags': default_tags,
         }]

        api.Metric.send(metrics)

    def run(self, spark):
        # Input files
        experian = spark.table('joined_experian')

        # Select relevant Experian fields: GreenAware, Education, Income
        df = experian \
            .select([
                'location_id',
                'green_aware',
                'educ_model_1',
                'lu_ehi_v5_amt',
                ])

        df = df.withColumn('hhincome', df.lu_ehi_v5_amt.cast("int"))

        # Bin into High/Med/Low propensity (+ No Info, Unknown for reference)
        # Using Shelton Group/SEPA segment: True Believers + Concerned Parents
        # High: Behavioral/Think Green (1-2), Income $40K-85K,
        #      BA or Grad (or unknown educ)
        # Medium: Behavioral/Think Green or unknown (1-2), Income $30K-100K,
        #      and Some college/BA/Grad (or unknown educ)
        # Low: Potential Green or True Brown, Income <$30K or >= $100K,
        #      HS or less (or unknown)
        # None: If GreenAware, Income, and Education are all unknown
        # None: If Not filtered into any of the above categories
        #      (e.g., if Potential Green but missing Income and Education
        df = df \
            .withColumn('propensity_green_energy', when(
                (
                    (col('hhincome') >= 40) &
                    (col('hhincome') < 85)
                    ) &
                (
                    (col('educ_model_1') == 13) |
                    (col('educ_model_1') == 14) |
                    (col('educ_model_1') == 53) |
                    (col('educ_model_1') == 54) |
                    (col('educ_model_1').isNull()) |
                    (col('educ_model_1') == 0)
                    ) &
                (
                    (col('green_aware') == 1) |
                    (col('green_aware') == 2)
                ),
                lit('high')
                )
                .when(
                (
                    (col('hhincome') >= 30) &
                    (col('hhincome') < 100)
                    ) &
                (
                    (col('green_aware') == 1) |
                    (col('green_aware') == 2) |
                    (col('green_aware') == 0) |
                    (col('green_aware').isNull())
                    ) &
                (
                    (col('educ_model_1') == 13) |
                    (col('educ_model_1') == 14) |
                    (col('educ_model_1') == 12) |
                    (col('educ_model_1') == 53) |
                    (col('educ_model_1') == 54) |
                    (col('educ_model_1') == 52) |
                    (col('educ_model_1').isNull()) |
                    (col('educ_model_1') == 0)
                    ),
                lit('medium')
                )
                .when(
                (
                    (col('hhincome') >= 100) |
                    (col('hhincome') < 30)
                    ) &
                (
                    (col('green_aware') == 3) |
                    (col('green_aware') == 4)
                    ) &
                (
                    (col('educ_model_1') == 11) |
                    (col('educ_model_1') == 15) |
                    (col('educ_model_1') == 51) |
                    (col('educ_model_1') == 55) |
                    (col('educ_model_1').isNull()) |
                    (col('educ_model_1') == 0)
                    ),
                lit('low')
                )
                .when(
                    (col('hhincome').isNull()) &
                (
                    (col('educ_model_1') == 0) |
                    (col('educ_model_1').isNull())
                    ) &
                (
                    (col('green_aware') == 0) |
                    (col('green_aware').isNull())
                    ),
                lit(None)
                    )
                .otherwise(lit(None)))

        # Filter out No Info/Unknown
        df = df.filter(
            (df.propensity_green_energy == 'high') |
            (df.propensity_green_energy == 'medium') |
            (df.propensity_green_energy == 'low')
            )

        # Select fields to output
        df_output = df.select('location_id', 'propensity_green_energy')

        assert df_output.dtypes == [
            ('location_id', 'string'),
            ('propensity_green_energy', 'string'),
        ]

        df_output.createOrReplaceTempView('output')
