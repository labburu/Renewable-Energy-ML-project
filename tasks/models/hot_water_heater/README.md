# Electric Hot Water Heater Detection

Creates 0/1 flags of electric hot water heater for households with hourly (for now) AMI data.

## Methodology

### General Concept
Hot water heater is a multi-state appliance, often it can be categorized into 3 states (draw water, heating, idle) for simplicity purpose. A multi-state appliance, if given AMI time series data, the best way to approach the disagg/detection problem is to apply HMM (Hidden Markov Model). 
Different states of the appliance can be treated as hidden states in HMM, and the consumption (here we get the consumption by using `diff` because we can capture the rise/fall of signals) is the observations we have.
 
For detection, we use pre-trained HMM (Hidden Markov Model) parameters to calculate log maximum-likelihood for each fixed size of sliding windows in hourly AMI data. We don't need the actual inference stage from HMM because we only care about whether the appliance is present or not, not the actual consumption.

Raw AMI data will be divided into sliding windows of equal sizes (72 reads, which is 3 days for hourly data), then we use pre-trained parameters (trasition matrix, covariance and means) to obtain the log maximum-likelihood on the sliding windows. 
Finally we calculate the average log maximum-likelihood scores[^1] on all windows, and obtain the number of eligible windows that are smaller than the preset average maximum-likelihood threshold and standard deviations. Based on the eligible windows characteristics (average score, average scaled standard deviation), we can aceept/reject 
if this house has a electric hot water heater or not. 

[^1]: Log maximum-likelihood can be used as model benchmark on data with fixed sample sizes

### Pre-trained parameters 
Pre-trained parameters are obtained from Pecan Street user data 2018. Following steps were applied when we use the data:

1. Do a hourly roll-up and integration on Pecan Street data (1-min interval), given the Pecan data is demand (kW) instead of consumption (kWh).
2. Filter out incomplete data, filter out houses with water heater sub-metered but no readings were shown (mal-functioned houses).
3. Extract water heater operation cycles ground truth from eligible houses, smooth out their consumption to fit the HMM model (consumption reads needs to smooth out to fit the 3-state, for example reads `0.55` and `0.6` will all be categorized as state `drawing water` and the consumption will be `0.5`.
4. Use the fixed size of sliding windows (fix all sample sizes) and perform HMM fit for all smoothed out sequences, obtain the generic parameters (transition matrix, means and covriance)*[]: 
5. Export parameters for detection usage

## Inputs
- Output from: `soa_daily/transform_channels`
- Static file: `ami_common (tenant_id=109)`

## Outputs

|  location_id |  tenant_id | interval_minutes  | wh_yes  |
|---|---|---|---|
| 123456789  |  109 | 60  |  1 |

## Testing the Model
Testing the hot water heater model by using Pytest can be a little complex. Since the library version and requirements are a lot
different from the pre-built Jenkins environment. You need to have your own Jenkins pyspark docker container set up in local and run the tests.
The testing script is located in `testing model` folder. You need to follow steps shown below:

1. Clone docker-images to local:
```
git clone tendrilinc/docker-images
```
2. Navigate to `tendril/jenkins/jenkins-pyspark` and change the `requirement.txt` to the following:
```
awscli==1.16.191
boto3==1.9.181
boto==2.49.0
datadog==0.29.3
findspark==1.3.0
flake8==3.5.0
Jinja2==2.10.1
joblib==0.13.2
numpy==1.11.3
pandas==0.20.1
patsy==0.5.1
pyarrow==0.8.0
pytest==5.0.0
python-slugify==3.0.2
requests==2.22.0
scikit-learn==0.18.1
scipy==1.2.1
statsmodels==0.9.0
testing.redis==1.1.1
yamllint==1.16.0
hmmlearn==0.2.2
```
3. Change `Dockerfile` in the same directory to:
```
FROM registro.tendrilinc.com/pyspark:20190702

ARG DOCKER_CONFIG=/root/.docker/config.json
ARG PYTHON_REQUIREMENTS=/tmp/requirements.txt

COPY config.json ${DOCKER_CONFIG}
COPY requirements.txt ${PYTHON_REQUIREMENTS}

RUN set -xe \
    && \
    echo 'Give docker permissions to jenkins user...' \
    && useradd jenkins && usermod -a -G docker jenkins \
    && \
    echo 'Installing additional Python requirements...' \
    && pyenv global 3.5.6 \
    && pip install --upgrade pip==19.2.3 \
    && pip install setuptools==41.2.0 \
    && pip install -r ${PYTHON_REQUIREMENTS} \
    && pyenv global 3.6.7 \
    && pip install --upgrade pip==19.2.3 \
    && pip install setuptools==41.2.0 \
    && pip install -r ${PYTHON_REQUIREMENTS} \
    && rm ${PYTHON_REQUIREMENTS}
```
4. Build the Jenkins-pyspark container locally and give it a new tag:
```
docker build . -t mchen-test
```
5. Run `docker-image` and see the most recent name of your docker image
6. Navigate to `~/airflow` and modify the `docker-compose.yaml` to:
```
---

version: '3'

services:
  local-jenkins-airflow-slave:
    container_name: local-jenkins-airflow-slave
    image: mchen-test:latest
    volumes:
      - .:/usr/local/airflow/
    entrypoint: /usr/local/airflow/scripts/build.sh
    environment:
      - AIRFLOW_HOME=/usr/local/airflow/
```
7. Copy `model_hot_water_heater_test.py` (or write your own tests) to the folder `test/tasks` and run the tests by running: 
```
docker-compose up
```

8. If you are doing local testing with docker-airflow by using `Pyenv`, don't forget to switch back to your original local version
## Future Work (WIP)
1) Fine-tuning the model by using Alabama Power's ground truth
2) 15-min and 30-min AMI electric water heater detection capability
3) Add another ensemble learning layer to the model by leveraging Experian data and HMM model outputs


