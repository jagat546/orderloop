#!/usr/bin/env bash
# Runs once, on an empty data volume. Creates the roles and schemas in docs/DATA_MODEL.md.
# Tables, policies and grants come later, from each service's Flyway migrations run as orderloop_owner.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v owner_pw="$POSTGRES_OWNER_PASSWORD" \
  -v app_pw="$POSTGRES_APP_PASSWORD" \
  -v ops_pw="$POSTGRES_OPS_PASSWORD" <<'SQL'
-- None of these may be a superuser or have BYPASSRLS, or row-level security stops applying to them.
CREATE ROLE orderloop_owner LOGIN PASSWORD :'owner_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE ROLE orderloop_app   LOGIN PASSWORD :'app_pw'   NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE ROLE orderloop_ops   LOGIN PASSWORD :'ops_pw'   NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO orderloop_owner, orderloop_app, orderloop_ops;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- One schema per service, owned by the migration role. Migrations grant USAGE and table rights.
CREATE SCHEMA integration  AUTHORIZATION orderloop_owner;
CREATE SCHEMA orchestrator AUTHORIZATION orderloop_owner;
CREATE SCHEMA inventory    AUTHORIZATION orderloop_owner;
CREATE SCHEMA payment      AUTHORIZATION orderloop_owner;
CREATE SCHEMA fulfilment   AUTHORIZATION orderloop_owner;
SQL
