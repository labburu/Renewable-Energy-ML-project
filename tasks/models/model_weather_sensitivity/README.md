# Weather-sensitive Homes

## Inputs
- Output from: `soa_daily/transform_bills`
- Output from: `soa_daily/transform_locations`
- Output from: `weather/transform_historical_weather_daily`

## Output

### Columns
- string: account_id
- float: rsquare
- float: intercept
- float: cdd_coef
- float: hdd_coef
- string: home_sensitivity_to_hot_weather
- string: home_sensitivity_to_cold_weather

## The cutoffs for each level of weather sensitivity were re-calibrated on 2018-08-03, following the introduction of properly calculated HDD and CDD values

old
cold
low=3300000, 55.8%
med=1366000, 21.3%
high=1250000, 21.1%

new
In [18]: df['cdd_coef'].describe(percentiles=[.01, .05, .1,.2,.3,.4,.5,.6,.7,.8,.9,.95,.99])
Out[18]:
count    1.144269e+07
mean     3.622257e-01
std      1.351461e+03
min     -2.473214e+06
1%      -6.690380e+00
5%      -3.564382e-01
10%     -5.188769e-02
20%      9.562196e-02
30%      2.226967e-01
40%      3.793385e-01
50%      6.117632e-01
60%      9.817067e-01
70%      1.517958e+00
80%      2.219744e+00
90%      3.304686e+00
95%      4.445943e+00
99%      9.246880e+00
max      2.811714e+06
Name: cdd_coef, dtype: float64916

In [20]: df['cdd_coef'].describe(percentiles=[.558, .771])
Out[20]:
count    1.144269e+07
mean     3.622257e-01
std      1.351461e+03
min     -2.473214e+06
50%      6.117632e-01
55.8%    8.061274e-01
77.1%    1.994761e+00
max      2.811714e+06
Name: cdd_coef, dtype: float64

old
hot
low=1668000, 28.2%
med=2721000, 46%
high=1528000, 25.8%

new
In [17]: df['hdd_coef'].describe(percentiles=[.01, .05, .1,.2,.3,.4,.5,.6,.7,.8,.9,.95,.99])
Out[17]:
count    1.144269e+07
mean     2.198228e+00
std      1.907564e+03
min     -2.037898e+06
1%      -4.758353e+00
5%      -3.497246e-01
10%      1.448943e-01
20%      7.593415e-01
30%      1.231634e+00
40%      1.655567e+00
50%      2.077518e+00
60%      2.537442e+00
70%      3.088252e+00
80%      3.840073e+00
90%      5.166238e+00
95%      6.663027e+00
99%      1.239035e+01
max      5.565096e+06

In [21]: df['hdd_coef'].describe(percentiles=[.282, .742])
Out[21]:
count    1.144269e+07
mean     2.198228e+00
std      1.907564e+03
min     -2.037898e+06
28.2%    1.152160e+00
50%      2.077518e+00
74.2%    3.368589e+00
max      5.565096e+06
Name: hdd_coef, dtype: float64a
