# airflow
Tendril DAGs and tasks that run in Apache Airflow

## Table of Contents
- [Resources](#resources)
- [Development](#development)
- [Deployment](#deployment)



### Dependencies
- Docker
- Service code (docker): Python 3.6.x
- PySpark code (qubole): Python 3.5.x


## Development
It is highly recommended to leverage Python management tools to setup your
development environment.
  - See [How to pyenv](https://wiki.tendrilinc.com/display/ENG/How-To+pyenv)

To run airflow locally, see
[local development](https://github.com/tendrilinc/docker-airflow#development).

### Style Conventions

See https://www.python.org/dev/peps/pep-0008/#naming-conventions

- Use `PascalCase` for class names
  - nydus task classes
- Use `snake_case` for almost everything else
  - python variables
  - column names in data products
  - file names

These style conventions are enforced by `flake8`, and will break the PR build.


### Testing
Testing of dags can be difficult. The dag itself just needs to be parsable to
pass a test. If you're using the nydus operator, you can and should write unit
tests for your behavior.

We do testing of tasks through pytest on every PR commit:
 - https://jenkins.useast.tni01.com:8443/job/airflow/job/pull-requests/

However, you can run through the same process locally by using the included
`docker-compose.yaml`:

```sh
docker-compose up --build
```
This uses the same docker container that the PR build uses:
  - https://github.com/tendrilinc/docker-images/tree/master/tendril/jenkins/jenkins-pyspark

To test a nydus task in pytest, you will need to write a test file that:

1. Imports the spark sql context from the framework
1. Creates a dataframe and view with the name defined in your canal file
1. Executes behavior defined in the script you're authoring
  - i.e. via and `exec`s of a script, or by importing the script and testing
  individual methods
1. Uses the view `output` as the file output and tests that for correct
  behavior

Your unit tests should go in `test/tasks/` and be named something that makes it
easy to know what task is being tested. E.g., `test_transform_data.py`.


## Deployment
1. Merge feature branch to master
  - This automatically creates a Jenkins build and deploys to `dev-us`
2. Navigate to `jenkins` > `airflow` > `deploy-airflow-dags`
3. Find your build number (it should be the latest) and click into it to make
  verify the commit message matches the feature branch you just merged.
4. Click on `promotion status` > there are `Approve` buttons to manually deploy
  to `stg-us` and `prod` when ready.


## Alerting
The TendrilOperator is set up to send PagerDuty alerts on task failure through
the `enable_pagerduty` flag. To enable this for all tasks in a DAG, add it to
the `default_args` dict for that DAG.


## FAQ
> _What is all this "nydus" stuff?_

All `NydusOperator` tasks submit spark applications to spark clusters that are
managed by Qubole.

> _Should I add to an existing dag or create a new one?_

If a task is not excessively complicated and does not need a custom cadence,
it is preferrable to add to an existing DAG rather than create a new one. You
get to avoid boilerplate, and the list of DAGs stays clean. If you do need a
different cadence, have a very complicated set of transforms to do, or just
want to start in an isolated environment before merging your tasks into a
master dag, consider using remote sensors to access data from other DAGs to
avoid repeating extract work.

> _What if I want to reference a task in a different DAG?_

You can use the "External Task Sensor." Remote sensors are Airflow operators
that block and retry until a timeout is reached or a task has been complete.
The `ExternalTaskSensor` can be used to wait for a task in another dag to
complete before moving on:

```python
# This function is used to compare the schedules and arrive at the correct date
execution_date_fn = fn_last_execution_date(cron['this_dag'], cron['other_dag'])

# The external task sensor itself
wait_for_extract_locations = ExternalTaskSensor(
    task_id='soa_daily.zeus.extract_locations',
    external_dag_id='soa_daily',
    external_task_id='zeus.extract_locations',
    timeout=82800,
    execution_date_fn=execution_date_fn,
    dag=dag
)
```

When using a sensor in this manner that is feeding into a Nydus task, we also
need to make sure our Nydus task definitions know to look at the appropriate
dag to gather source data:

```yaml
extract:
  - id: locations
    type: airflow
    dag_id: soa_daily
    task_id: zeus.extract_locations
    format: parquet
```

> _Why are some jobs in this repo while other jobs are in other repos?_

We set it up so that services can own their own tasks (i.e., interfaces). This
is usually done with the "extract_" tasks that pull data out of service-owned
databases.

For the stuff that lives on S3, we namespace DAGs using S3 prefixes in order
to prevent collisions. E.g., 
  - `zeus/extract_locations`
  - `bill-service/extract_bills`

We'd like to use the namespaced path to identify DAGs but airflow restricts
DAG IDs to alphanumeric characters plus `.`, `_`, and `-`.  Therefore a
convention has been adopted that uses `.` in place of `/` in DAG IDs for DAGs
that live in S3. Our NydusOperator code handles this translation. If your DAG
lives in S3 with prefix `some-service/some-task-name` then specify a DAG ID of
`some-service.some-task-name`.
