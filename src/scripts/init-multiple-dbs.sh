#!/bin/bash
set -e

MLFLOW_DB_NAME="${MLFLOW_DB_NAME:-mlflow}"
MLFLOW_DB_USER="${MLFLOW_DB_USER:-mlflow}"
MLFLOW_DB_PASSWORD="${MLFLOW_DB_PASSWORD:-mlflow123}"

psql \
  --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=mlflow_db_name="$MLFLOW_DB_NAME" \
  --set=mlflow_db_user="$MLFLOW_DB_USER" \
  --set=mlflow_db_password="$MLFLOW_DB_PASSWORD" <<'EOSQL'
    SELECT format(
        'CREATE USER %I WITH PASSWORD %L',
        :'mlflow_db_user',
        :'mlflow_db_password'
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = :'mlflow_db_user'
    ) \gexec

    SELECT format(
        'CREATE DATABASE %I OWNER %I',
        :'mlflow_db_name',
        :'mlflow_db_user'
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_database
        WHERE datname = :'mlflow_db_name'
    ) \gexec

    SELECT format(
        'GRANT ALL PRIVILEGES ON DATABASE %I TO %I',
        :'mlflow_db_name',
        :'mlflow_db_user'
    ) \gexec
EOSQL