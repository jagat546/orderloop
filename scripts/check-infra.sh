#!/usr/bin/env bash
# Checks a running development stack: health, roles and schemas from docs/DATA_MODEL.md.
# Start it first: docker compose -f deploy/docker-compose.yml up -d --wait
# Set COMPOSE_PROJECT_NAME to check a stack started under another project name.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose=(docker compose -f "$root/deploy/docker-compose.yml")
failures=0

check() { # label expected actual
  if [ "$2" = "$3" ]; then
    echo "ok: $1"
  else
    echo "FAIL: $1 (expected '$2', got '$3')"
    failures=$((failures + 1))
  fi
}
sql() { "${compose[@]}" exec -T postgres psql -U postgres -d orderloop -tAc "$1" 2>&1 | tr -d '\r'; }

for svc in postgres kafka redis; do
  check "$svc is healthy" healthy "$("${compose[@]}" ps --format '{{.Health}}' "$svc" 2>&1 | tr -d '\r')"
done

# Superusers and BYPASSRLS roles skip row-level security, so none of these may have either.
for role in orderloop_owner orderloop_app orderloop_ops; do
  check "$role logs in without SUPERUSER or BYPASSRLS" "t|f|f" \
    "$(sql "SELECT rolcanlogin, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = '$role'")"
done

check "service schemas exist and belong to orderloop_owner" \
  "fulfilment:orderloop_owner,integration:orderloop_owner,inventory:orderloop_owner,orchestrator:orderloop_owner,payment:orderloop_owner" \
  "$(sql "SELECT string_agg(nspname || ':' || pg_get_userbyid(nspowner), ',' ORDER BY nspname)
          FROM pg_namespace
          WHERE nspname IN ('integration', 'orchestrator', 'inventory', 'payment', 'fulfilment')")"

check "orderloop_app connects over TCP with its password" "orderloop_app" \
  "$("${compose[@]}" exec -T postgres sh -c \
    'PGPASSWORD="$POSTGRES_APP_PASSWORD" psql -h localhost -U orderloop_app -d orderloop -tAc "SELECT current_user"' 2>&1 | tr -d '\r')"

check "orderloop_ops abandons a transaction left idle for a minute" "1min" \
  "$(sql "SELECT split_part(unnest(setconfig), '=', 2) FROM pg_db_role_setting s
          JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = 'orderloop_ops'")"

check "orderloop_app cannot create objects in public" "f" \
  "$(sql "SELECT has_schema_privilege('orderloop_app', 'public', 'CREATE')")"

check "Kafka runs in KRaft mode with a quorum leader" "LeaderId: 1" \
  "$(MSYS2_ARG_CONV_EXCL="/opt" "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status 2>&1 | tr -d '\r' | grep -oE 'LeaderId:[[:space:]]+1' | head -1 | tr -s ' \t' ' ')"

check "Redis answers" "PONG" "$("${compose[@]}" exec -T redis redis-cli ping 2>&1 | tr -d '\r')"

if [ "$failures" -gt 0 ]; then
  echo "Infrastructure check failed with $failures problem(s)."
  exit 1
fi
echo "Infrastructure check passed."
