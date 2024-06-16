# TOU Propensity Model/Heuristic

## Inputs
- Output from: `demographics_matching/join_locations_to_matched_experian`

## Features/Predictors
```
'propensity_ev',
'age_1',
'green_aware',
'length_residence',
'marital_model_1',
'home_sq_footage',
'total_children'
```

## Output

### Columns
- string: location_id
- integer: tenant_id
- integer: tou_score
- string: propensity_tou


### Sample Summary Tables
```
+--------------+------+
|propensity_tou| count|
+--------------+------+
|           low|575891|
|          high|170046|
|        medium|289649|
+--------------+------+
```
- Propensity for KCPL (note: 348K have null Experian values and are put in the Low propensity bin)

```
+------------------------------------+---------+---------+--------------+
|location_id                         |tenant_id|tou_score|propensity_tou|
+------------------------------------+---------+---------+--------------+
|00000000-0000-005c-02f7-5cb1bb61ac02|92       |20       |high          |
|00000000-0000-005c-02f7-5cb59b21a002|92       |0        |low           |
|00000000-0000-005c-02f7-5cb9ed713002|92       |4        |low           |
|00000000-0000-005c-02f7-5cba49711802|92       |0        |low           |
|00000000-0000-005c-02f7-5cbabff13002|92       |6        |low           |
|00000000-0000-005c-02f7-5cbd4bb1a402|92       |14       |medium        |
|00000000-0000-005c-02f7-5cc22b51a402|92       |20       |high          |
|00000000-0000-005c-02f7-5cc520013002|92       |6        |low           |
|00000000-0000-005c-02f7-5cc71281a402|92       |9        |low           |
|00000000-0000-005c-02f7-5cca33313002|92       |15       |medium        |
+------------------------------------+---------+---------+--------------+
```


