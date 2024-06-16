from pyspark.sql.functions import (
    col,
    rank,
)
from pyspark.sql.window import Window


class TransformWeatherSensitivity:

    def run(self, spark):
        df_ws_bills_elec = spark \
            .table('bill_weather_sensitivity')

        window_spec = Window \
            .partitionBy([
                'tenant_id',
                'account_id',
                'location_id',
                'fuel_type',
            ]) \
            .orderBy(
                col('total_consumption').desc()
            )

        df_tall = df_ws_bills_elec \
            .withColumn('rank_temp', rank().over(window_spec)) \
            .filter(col('rank_temp') == 1) \
            .select([
                'tenant_id',
                'account_id',
                'location_id',
                'fuel_type',
                'channel_id',
                'rsquare',
                'intercept',
                'cdd_coef',
                'hdd_coef',
            ])

        df_gas = df_tall \
            .filter(col('fuel_type') == 'GAS') \
            .withColumnRenamed('channel_id', 'gas_channel_id') \
            .withColumnRenamed('rsquare', 'gas_rsquare') \
            .withColumnRenamed('intercept', 'gas_intercept') \
            .withColumnRenamed('cdd_coef', 'gas_cdd_coef') \
            .withColumnRenamed('hdd_coef', 'gas_hdd_coef')

        df_output = df_tall \
            .filter(col('fuel_type') == 'ELECTRIC') \
            .drop('fuel_type') \
            .join(
                df_gas.drop('fuel_type'),
                [
                    'tenant_id',
                    'account_id',
                    'location_id',
                ],
                how='outer'
            )

        df_output.createOrReplaceTempView('output')
