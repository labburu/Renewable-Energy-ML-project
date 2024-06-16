# Marketplace Purchase Propensity Model

## Inputs
- Output from: `demographics_matching/join_locations_to_matched_experian`


## Output

### Columns
- string: location_id
- string: marketplace_purchase_propensity


### Sample Summary Tables
```
+-------------------------------+--------+
|marketplace_purchase_propensity|   count|
+-------------------------------+--------+
|                            low| 7595670|
|                           high| 4169695|
|                         medium| 4963818|
+-------------------------------+--------+
```

```
+------------------------------------+-------------------------------+
|location_id                         |marketplace_purchase_propensity|
+------------------------------------+-------------------------------+
|00000000-0000-0005-011f-cdac9b413c02|medium                         |
|00000000-0000-0005-011f-cdd616913802|medium                         |
|00000000-0000-0005-011f-ce141b213802|medium                         |
|00000000-0000-0005-011f-ce483be13c02|high                           |
|00000000-0000-0005-011f-cec4bc214002|low                            |
|00000000-0000-0005-011f-cfc140713802|medium                         |
|00000000-0000-0005-011f-d08c50913802|medium                         |
+------------------------------------+-------------------------------+
```


