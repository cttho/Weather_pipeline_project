#!/bin/bash
set -e

airflow db upgrade

if [ "$AIRFLOW_ADMIN_USER" ]; then
    airflow users create \
        --username "$AIRFLOW_ADMIN_USER" \
        --password "$AIRFLOW_ADMIN_PASSWORD" \
        --firstname "Airflow" \
        --lastname "Admin" \
        --role Admin \
        --email "admin@example.com"
fi

exec "$@"