from pyspark.sql.functions import col, when


class TransformEVPropensity:

    def __init__(self):
        self.label_col = 'has_ev'
        self.id_cols = ['location_id', 'account_id', 'tenant_id']

        self.feature_cols = [
            'experian_person_one_combined_age',
            'experian_person_one_education_model',
            'experian_green_aware',
            'experian_income_lower_bound',
            'experian_income_upper_bound',
            'experian_home_land_value',
            'experian_home_bedrooms',
            'experian_person_one_political_party_affiliation',
            'experian_length_of_residence',
            'experian_dwelling_type',
            'experian_home_heat_indicator',
            'experian_person_one_marital_status',
            'experian_home_air_conditioning',
            'experian_home_building_square_footage',
            'experian_year_built',
            'experian_number_of_adults',
            'experian_number_of_children'
        ]

    def join_data(self, df_locs, df_experian, df_lpt):
        return df_locs \
            .join(df_experian, ['tenant_id', 'location_id'], how='left') \
            .join(df_lpt, ['location_id'], how='left') \
            .select(self.id_cols + self.feature_cols + [self.label_col]) \
            .dropDuplicates(subset=['tenant_id', 'account_id', 'location_id'])

    def run(self, spark):

        locations_df = spark.table('locations')\
            .withColumnRenamed('id', 'location_id') \
            .select([
                'tenant_id',
                'location_id',
                'account_id',
            ])

        experian_df = spark.table('experian')\
            .select(['location_id', 'tenant_id'] + self.feature_cols)

        lpt_df = spark.table('location_profiles') \
            .filter(col('electric_vehicle__priority') < 3) \
            .select(['location_id'] + [when(col('electric_vehicle__value') == 'true', 1)
                                       .otherwise(0).alias(self.label_col)])

        output_df = self.join_data(locations_df, experian_df, lpt_df)

        output_df.createOrReplaceTempView('output')

        return output_df
