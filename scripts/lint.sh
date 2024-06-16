#!/bin/bash -xe
# Run python and yaml file linters
# Note: assumed to run from tendrilinc/airflow repository root

# Airflow docker environment uses Python 3.6
pyenv local 3.6.7
flake8 dags/

# Qubole environment uses Python 3.5
pyenv local 3.5.6
flake8 tasks/
yamllint tasks/
