from pyspark.sql.functions import col, lit, round, when


class ModelIncomePerOccupant:

    def run(self, spark):
        df_demogs = spark \
            .table('demogs') \
            .withColumnRenamed('lu_ehi_v5', 'income_range') \
            .select([
                'tenant_id',
                'location_id',
                'number_of_adults',
                'number_of_children',
                'income_range',
            ])

        # Note that I've used the mid-point of the salary range given by Experian.
        df_output = df_demogs \
            .withColumn(
                'income_range',
                when(col('income_range') == 'A', 7500)
                .when(col('income_range') == 'B', 20000)
                .when(col('income_range') == 'C', 30000)
                .when(col('income_range') == 'D', 42500)
                .when(col('income_range') == 'E', 62500)
                .when(col('income_range') == 'F', 87500)
                .when(col('income_range') == 'G', 112500)
                .when(col('income_range') == 'H', 137500)
                .when(col('income_range') == 'I', 162500)
                .when(col('income_range') == 'J', 187500)
                .when(col('income_range') == 'K', 225000)
                .when(col('income_range') == 'L', 500000)
                .when(col('income_range') == 'U', lit(None))
                .otherwise(lit(None))
            ) \
            .filter(col('number_of_adults') > 0) \
            .dropna() \
            .withColumn(
                'income_per_occupant',
                round(col('income_range') / (col('number_of_adults') + col('number_of_children')), 0)
            ) \
            .select([
                'tenant_id',
                'location_id',
                'income_per_occupant',
            ])

        df_output.createOrReplaceTempView('output')
