# HVAC Upgrade Propensity
## Methodology
# Random Forest Classifier Model
- Builds multiple decision trees based on feature inputs and merges them together to get a more accurate and stable prediction

# Pros
- does not overfit which allows for as many trees to be specified given time and business constraints
- the larger the number of trees, the more accurate the result

# Cons
- slower

## Inputs
- see canal file
- Output from: `demographics_matching/transform_normalize_experian`
- Output from: `kcpl_participation_hvac`
- Output from: `soa_daily/transform_locations`
- Output from: `soa_daily/transform_accounts`
- Output from: `models/weather_sensitivity`

### Random Forest Hyperparameter Inputs
- num_trees: `Number of decision trees in the forest`
- max_depth: `Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes). Suggested value is 4 to get a better result`
- max_bins:  `Signifies the maximum number of bins used for splitting the features; where the suggested value is 100 to get better results.  max_bins should be greater or equal to the maximum number of categories for categorical features.`
- feat_subset_strategy:  `featureSubsetStrategy signifies the number of features to be considered for splits at each node.  If auto, the algorithm chooses the best feature subset strategy automatically.`


### Label Column:  dependent variable we want to predict
- Binary:  Whether participated in HVAC system upgrade or not

## Features:  independent variables or predictors

### Features in order of relative influence
```
                         feature  relative_influence
25                    year_built            0.199025
13            political_spectrum            0.115948
24               home_sq_footage            0.111840
2                       hdd_coef            0.071970
19               is_singlefamily            0.062827
23   has_central_air_conditioner            0.048923
0                        rsquare            0.041165
7             income_lower_bound            0.034932
6                    green_aware            0.034461
22               marital_model_1            0.033277
3                      intercept            0.030370
18              length_residence            0.029727
21               has_gas_furnace            0.028453
12              is_nonregistered            0.024976
1                       cdd_coef            0.023200
4                          age_1            0.020561
10                 is_republican            0.018863
8             income_upper_bound            0.013400
28               total_occupancy            0.012389
14  unregistered_and_not_engaged            0.011156
26                  total_adults            0.010312
5              education_model_1            0.006739
9                    is_democrat            0.005187
27                total_children            0.002897
11                is_independent            0.002487
16              on_the_fence_lib            0.001652
15      unregistered_and_engaged            0.001652
20                 has_heat_pump            0.001383
17          green_traditionalist            0.000227
FINISHED    
Took 0
```
## Outputs

### Columns
- str: account_id
- str: location_id
- str: tenant_id
- str: prediction_binary
- float: propensity_hvac_score
- int: label
- str: propensity_hvac

![Screenshot](roc_hvac.png)

### Sample

```
+------------------------------------+------------------------------------+---------+-----------------+---------------------+-----+---------------+
|account_id                          |location_id                         |tenant_id|prediction_binary|propensity_hvac_score|label|propensity_hvac|
+------------------------------------+------------------------------------+---------+-----------------+---------------------+-----+---------------+
|00000000-0000-005c-02f7-5cb204911801|00000000-0000-005c-02f7-5cb204911802|92       |0                |0.3744129            |0    |medium         |
|00000000-0000-005c-02f7-5cb8bbc1a001|00000000-0000-005c-02f7-5cb8bbc1a002|92       |0                |0.2415148            |0    |low            |
|00000000-0000-005c-02f7-5cba4b51ac01|00000000-0000-005c-02f7-5cba4b51ac02|92       |0                |0.42953667           |0    |medium         |
|00000000-0000-005c-02f7-5cbc2d711801|00000000-0000-005c-02f7-5cbc2d711802|92       |0                |0.095124744          |0    |low            |
|00000000-0000-005c-02f7-5cbdc3a1a001|00000000-0000-005c-02f7-5cbdc3a1a002|92       |1                |0.62482077           |0    |high           |
+------------------------------------+------------------------------------+---------+-----------------+---------------------+-----+---------------+
```

### At the time of this PR entry,
KCPL customers:
```
+---------------+------+
|propensity_hvac| count|
+---------------+------+
|            low|463442|
|           high|201815|
|         medium|276389|
+---------------+------+
```

### Sample Summary Tables
- Sensitivity is the True positive rate: Rate of correctly predicting locations that truly have elected into a DR program(sensitivity = TP/(TP+FN)).
- Specificity is the True negative rate: Rate of correctly predicting locations that truly have NOT elected into a DR program (specificity = TN/(TN+FP)).
```
sensitivity (true positive rate)= 0.78
specificity (true negative rate) = 0.73
area_under_roc = 0.84
```
