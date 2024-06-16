from pyspark.sql.functions import avg, col, lower

ELECTRIC_HWH_KEY_WORDS = ['electric', 'heat_pump']
NULL_ID = '000000000000000000'
EXPERIAN_COLS = ['experian_home_building_square_footage', 'experian_home_bathrooms',
                 'experian_home_estimated_current_value', 'experian_new_parent_indicator_last_three_years']


class TransformHWH:

    def __init__(self):
        self.electric_hwh_key_words = ELECTRIC_HWH_KEY_WORDS
        self.null_id = NULL_ID
        self.experian_cols = EXPERIAN_COLS

    @staticmethod
    def _derive_has_electric_hwh_indicator(df_lpt, electric_hwh_key_words):
        return df_lpt.filter(col('water_heater_type__priority') < 2) \
            .select(['location_id'] + [(lower(col('water_heater_type__value'))
                                        .rlike('(.*)(' + '|'.join(electric_hwh_key_words) + ')(.*)'))
                    .alias('wh_gt').cast('integer')])

    def run(self, spark):
        channels_df = spark \
            .table('channels') \
            .withColumnRenamed('id', 'channel_id') \
            .select(['channel_id', 'location_id'])

        locations_df = spark \
            .table('locations') \
            .withColumnRenamed('id', 'location_id') \
            .select(['tenant_id', 'location_id'])

        experian_df = spark \
            .table('experian') \
            .select(['tenant_id', 'location_id'] + self.experian_cols)

        wh_score = spark \
            .table('wh_score') \
            .select(['location_id', 'tenant_id', 'wh_avg_likelihood_score'])

        weather_sensitivity_df = spark \
            .table('ws_ami') \
            .drop('location_id')

        features_11 = spark \
            .table('features_11') \

        features_12 = spark \
            .table('features_12') \

        features_79 = spark \
            .table('features_79') \

        features_df = features_11 \
            .union(features_12) \
            .union(features_79)

        features_df = features_df \
            .join(channels_df, on=['channel_id'], how='inner') \
            .join(weather_sensitivity_df, on=['channel_id'], how='inner') \
            .select('location_id', 'mean_kurtosis', 'mean_skewness', 'mean_peak', 'intercept', 'electric_hdd_coef_ami',
                    'electric_cdd_coef_ami')

        features_df = features_df.groupBy('location_id').agg(avg('mean_kurtosis'), avg('mean_skewness'),
                                                             avg('mean_peak'),
                                                             avg('intercept'), avg('electric_hdd_coef_ami'),
                                                             avg('electric_cdd_coef_ami'))

        features_df = features_df \
            .withColumnRenamed('avg(mean_kurtosis)', 'mean_kurtosis') \
            .withColumnRenamed('avg(mean_skewness)', 'mean_skewness') \
            .withColumnRenamed('avg(mean_peak)', 'mean_peak') \
            .withColumnRenamed('avg(intercept)', 'intercept') \
            .withColumnRenamed('avg(electric_hdd_coef_ami)', 'electric_hdd_coef_ami') \
            .withColumnRenamed('avg(electric_cdd_coef_ami)', 'electric_cdd_coef_ami')

        df_lpt = TransformHWH._derive_has_electric_hwh_indicator(spark.table('location_profiles_tabular'),
                                                                 self.electric_hwh_key_words)

        wh_other_df = locations_df \
            .join(df_lpt, on=['location_id'], how='left_outer') \
            .filter(locations_df.tenant_id != 109) \
            .select('tenant_id', 'location_id', 'wh_gt')

        training_df = wh_score.join(wh_other_df, on=['location_id', 'tenant_id'], how='inner') \
            .join(features_df, on=['location_id'], how='inner') \
            .select('tenant_id', 'location_id', 'wh_gt', 'wh_avg_likelihood_score', 'mean_kurtosis',
                    'mean_skewness', 'mean_peak', 'intercept', 'electric_hdd_coef_ami', 'electric_cdd_coef_ami')

        df_output = training_df \
            .join(experian_df, on=['location_id', 'tenant_id'], how='inner')

        df_output.createOrReplaceTempView('output')
        return df_output
