# Green Energy Propensity Model/Heuristic

## Inputs
- Output from: `demographics_matching/join_locations_to_matched_experian`


## Output

### Columns
- string: location_id
- string: propensity_green_energy


### Sample Summary Tables
```
+-----------------------+-------+
|propensity_green_energy|  count|
+-----------------------+-------+
|                    low|2407828|
|                   high|1162901|
|                 medium|2204101|
+-----------------------+-------+
```

```
+------------------------------------+-----------------------+
|location_id                         |propensity_green_energy|
+------------------------------------+-----------------------+
|00000000-0000-0005-011f-cdd616913802|medium                 |
|00000000-0000-0005-011f-cfc140713802|medium                 |
|00000000-0000-0005-011f-d77fe3e13802|high                   |
|00000000-0000-0005-011f-d7a355d14002|high                   |
|00000000-0000-0005-011f-d7e9e7e13802|medium                 |
|00000000-0000-0005-011f-d80527314002|high                   |
|00000000-0000-0005-011f-d818a4d13802|low                    |
|00000000-0000-0005-011f-d8495a213c02|high                   |
|00000000-0000-0005-011f-d85244713c02|medium                 |
|00000000-0000-0005-011f-d85eb3e14002|low                    |
+------------------------------------+-----------------------+
```


