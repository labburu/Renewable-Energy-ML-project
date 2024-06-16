## TNI 

This DAG ingests TNI files. TNI files contain demographic information that is utilized for building model simulations.

Prior to running this ensure the following:

  * Your input file is in the s3://tendril-aux-ingestor-common-format-[ENV]/tni/ path in the respective environment.
  * IMPORTANT You've set the tni_tenant variable in Airflow to the tenant identifier which corresponds to the TNI messages contained within the TNI file. The tni_tenant variable is used during the transform to be included with the translated messages.


## How does it work?

It ETLs the TNI CSV format into the Aux CSV format and posts the file to be processed. 

## How long will it take for my messages to be processed?

It depends on how many messages first of all. After this DAG completes, there are subsequent automation steps that may take up to a half hour to **begin** consuming these messages. Once the Aux ingestor is auto-scaled, the processing of the messages can take several hours depending on the size of the file and the number of updates being made. 

## Is there anything that needs to be done after the DAG has successfully completed?

Aside from monitoring the `zeus-aux-iungestor-<ENV>` SQS queue to determine how many more messages need to be processed, there sholdn't be any manual intervention necessary.

### TNI CSV Format

The header of the TNI CSV format consists of these columns:
```
  source -> 0
  account_id -> 1
  name -> 2
  address -> 3
  city -> 4
  state -> 5
  postal_code -> 6
  country_code -> 7
  income_bracket -> 8
  latitude -> 9
  longitude -> 10
  geo2 -> 11
  date_of_birth -> 12
  combined_age -> 13
  homeowner -> 14
  dwelling_type -> 15
  number_of_children -> 16
  number_of_adults -> 17
  home_business -> 18
  home_stories_in_tenths -> 19
  square_footage_in_hundreds -> 20
  swimming_pool -> 21
  heat_type -> 22
  ac_type -> 23
  building_construction -> 24
  year_built -> 25
  exterior_wall -> 26
  total_rooms -> 27
  prizm_code -> 28
  sort_index -> 29
```

### Field Requirements

The fields specified below have special formatting requirements. Any field not mentioned below is treated as a varchar/int with no specific requirements beyond that. 

## income_bracket (Offset 8)

The income estimation is determined using multiple statistical methodologies to predict which of 12 income ranges a living unit is most likely to be
assigned.

| Value | Description | 
| ----- | :---------: |
| A | $1,000 - $14,999|
| B | $15,000 - $24,999|
| C | $25,000 - $34,999|
| D | $35,000 - $49,999|
| E | $50,000 - $74,999|
| F | $75,000 - $99,999|
| G | $100,000 - $124,999|
| H | $125,000 - $149,999|
| I | $150,000 - $174,999|
| J | $175,000 - $199,999|
| K | $200,000 - $249,999|
| L | $250,000+|
| U | Unknown|

## latitude (Offset 9)

9-digit number followed by a 1-character directional. The 9-digit number is in degrees and calculated to six decimal places (decimal is implied, not shown). The 1-character directional is one of the following. 

## longitude (Offset 10)

9-digit number followed by a 1-character directional. The 9-digit number is in degrees and calculated to six decimal places (decimal is implied, not shown). The 1-character directional is one of the following. 

## homeowner (Offset 14)

Indicates whether the account holder owns the home. Omitted if unknown. Specified as a (Y or N) varchar.

## dwelling_type (Offset 15)

Specifies the type of home that will be simulated. Single family homes have all exterior walls exposed to the climate. 

| Value | Description | 
| ----- | ----------- |
| A | Multi-Family |
| M | Multi-Family | 
| P | Single Family | 
| S | Single Family |
| U | Single Family |


## home_business (Offset 18)

Indicates whether home business is conducted. Specified as a (Y or N) varchar.

## home_stories_in_tenths (Offset 19)

Indicates whether the account holder owns the home. Omitted if unknown.

| Value | Description | 
| ----- | ----------- |
| Homeowner | Owns the property | 
| Rent | Leases property | 

## square_footage_in_hundreds (Offset 20)

Size of living space in square footage divided by 100 (e.g. 20 = 2000 square footage)

## swimming_pool (Offset 21)
Indicative of a pool. Specified as a (Y or N) varchar.

## heat_type (Offset 22)

Heat type for the property

| Value | Description | 
| ----- | ----------- |
| 1 | Gas Furnace | 
| 2 | Heat Pump |
| 3 | Oil baseboard radiator | 
| 4 | None |
| 5 | Electric baseboard radiator |
| 7 | Wood stove |

## ac_type (Offset 23)

Air conditioning type for the property

| Value | Description | 
| ----- | ----------- |
| 0 | None |
| 1 | Central A/C |
| 2 | Evaporative Cooler |
| 3 | Window Wall A/C |

## building_construction (Offset 24)

The Home Building Construction is determined from Grant/Warranty Deed information recorded or other legal documents filed at the county recorder's office in the county where the property is located. 

| Value | 
| ----- | 
| brick | 
| frame | 
| steel | 
| stone | 
| stucco | 
| block | 
| metal | 
| shingle | 
| siding | 
| other | 

## exterior_wall (Offset 26)

The Exterior Wall is determined from Grant/Warranty Deed information recorded or other legal documents filed at the county recorder's office in the county where the property is located. 


| Value | 
| ----- | 
| aluminum | 
| asbestos | 
| block | 
| brick | 
| metal | 
| siding-wood | 
| stucco | 
| stone | 
| wallboard | 
| log | 
| other | 

