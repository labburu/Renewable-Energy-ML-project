#!/bin/bash -e

# This is for running docker-compose locally; this doesn't affect PR builds
AIRFLOW_HOME=${AIRFLOW_HOME:-.}

# Go to tendrilinc/airflow repository root
cd ${AIRFLOW_HOME}

# oe_hdr tests use this Postgres DB hosted in RDS
export DB_PROTOCOL=redshift+psycopg2
export DB_HOST=dev-us-oe-analytics-service-testing.colqsn0l21ic.us-east-1.rds.amazonaws.com
export DB_PORT=5432
export DB_NAME=postgres
export WAREHOUSE_USERNAME=postgres
export WAREHOUSE_PASSWORD=moose-penumbra-anion-creation
# configures report delivery location for integration tests
export DELIVERY_BUCKET_NAME=oe_hourly_device_report

# Run linters
./scripts/lint.sh

# Run tests
./scripts/test.sh
