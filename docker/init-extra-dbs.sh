#!/bin/bash
# Creates additional databases on the shared postgres instance.
# Mounted into /docker-entrypoint-initdb.d/ — runs once on first init.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE bugsink;
EOSQL
