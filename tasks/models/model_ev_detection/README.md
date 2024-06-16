## Electic Vehicle (EV) Detection

This DAG uses a UDF to extract features for each location (described below).
The features are passed to trained models, which make a prediction about whether
the location has an electric vehicle or not.

The models are trained on a labeled data set (picking the data out of the
 Location Profile). The trained models are saved as Joblib pickled objects and
 checked into the repo (see `run_models/*.joblib` files). The pickled models are
 loaded into the task at runtime and used to evaluate the features.

##### Extract Features schema
```
|-- location_id: string (nullable = true)
|-- interval_minutes: integer (nullable = true)
|-- mean_duration: double (nullable = true)
|-- mean_consumption: double (nullable = true)
|-- mean_box_count: double (nullable = true)
|-- mean_kurtosis: double (nullable = true)
|-- mean_skewness: double (nullable = true)
|-- mean_peak: decimal(20,7) (nullable = true)
|-- heuristic_score: double (nullable = true)
```

##### Run Models schema
TODO use `df.printSchema()`
```
'location_id',
'interval_minutes',
'supervised_score',
'heuristic_score',
'ev_yes',
```

## How to retrain the models
scikit-learn something. TODO how? document. the starter code is in
 `train_models/training_15mdn_EV.ipynb`.

## How to evaluate model performance
scikit-learn something else. TODO how? document.

## Things it would be nice to do

### Ideas for improving model accuracy
- Try filter out crazy consumption values before feature extraction. What is
 "crazy consumption?"
- For `mean_box_duration`, we are taking the average of averages and losing the
relative weighting in the process. Try just a plain average.
- For `mean_box_consumption`, we are taking the average of averages of averages. Ditto.
- Harden the box detection algorithm.
- How to handle locations with multiple intervals?


### Ideas for improving model speed
- Skip the UDAF. Rewrite this in vector operations.

### Other
- Write regression tests
- Have a notebook (or task?) which gathers the most recent training set
- Have a notebook to retrain the models on updated feature algorithms
- There was an SVM model once. Should we use it?