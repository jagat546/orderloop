#!/usr/bin/env bash
# Shared checks used by the Claude Code hooks and scripts/check-invariants.sh.
# Each function reads file content on stdin and prints one line per violation.
# These checks catch common mistakes. They are not a security boundary.

# Drops comment lines so a comment explaining a ban does not trigger the ban.
strip_java_comments() {
  grep -vE '^[[:space:]]*(//|\*|/\*)' || true
}

# $1 is the file path. Applies only to production Java.
java_violations() {
  local path="$1" code
  case "$path" in
    */src/main/java/*|src/main/java/*) ;;
    *) cat >/dev/null; return 0 ;;
  esac
  code="$(strip_java_comments)"

  grep -qE '\bThreadLocal\b|InheritableThreadLocal' <<<"$code" &&
    echo "ThreadLocal is banned; tenant context lives in ScopedValue (TenantContext)"
  grep -qE '^import[[:space:]]+lombok\.' <<<"$code" &&
    echo "Lombok is banned; use records, or a plain class for JPA entities"
  grep -qE '@Async\b' <<<"$code" &&
    echo "@Async is banned; scoped values do not cross into its threads"
  grep -qE 'System\.(out|err)\.print' <<<"$code" &&
    echo "Use SLF4J, not System.out or System.err"
  grep -qE '\.unwrap\([A-Za-z]*DataSource\.class\)' <<<"$code" &&
    echo "Do not unwrap the DataSource: raw pool connections still carry the last tenant's setting"
  grep -qE '@(Filter|FilterDef)\b' <<<"$code" &&
    echo "Hibernate @Filter is banned for tenancy; use @TenantId and RLS"
  grep -qiE '\b(double|float|BigDecimal)[[:space:]]+[a-z_]*(amount|price|total|minor|cost|refund|money)[a-z_]*' <<<"$code" &&
    echo "Money is long minor units plus an ISO 4217 currency (Money), not double, float or BigDecimal"
  grep -qiE 'paise' <<<"$code" &&
    echo "Paise is retired; use Money, with minor units and a currency code (ADR 0007)"

  case "$path" in
    */outbox/*) ;;
    *) grep -qE '\bKafkaTemplate\b' <<<"$code" &&
         echo "Only the outbox relay may use KafkaTemplate; write an outbox row instead" ;;
  esac
  case "$path" in
    */tenant/*) ;;
    *) grep -qE '"X-Tenant-ID"' <<<"$code" &&
         echo "Read X-Tenant-ID only in the tenant package, and only to compare it with the verified tenant" ;;
  esac
  return 0
}

# $1 is the file path. Applies to Flyway SQL migrations.
migration_violations() {
  local path="$1" sql tables forced enabled tenant_cols
  case "$path" in
    */db/migration/*.sql|db/migration/*.sql) ;;
    *) cat >/dev/null; return 0 ;;
  esac
  case "$(basename "$path")" in
    V[0-9]*__*.sql) ;;
    *) echo "Migration name must look like V12__add_refund_intent.sql" ;;
  esac
  sql="$(cat)"

  # Policy checks apply even to tenant-exempt files. Flatten whitespace and the spaces around "("
  # so formatting cannot hide a pattern.
  local flat settings guarded
  flat="$(tr -s '[:space:]' ' ' <<<"$sql" | sed -E 's/ *\( */(/g')"
  # An unbound scope sets ''. Without NULLIF, a row with tenant_id = '' would match it.
  settings="$(grep -oiE "current_setting\('app\.tenant_id'" <<<"$flat" | wc -l | tr -d ' ')"
  guarded="$(grep -oiE "nullif\(current_setting\('app\.tenant_id'" <<<"$flat" | wc -l | tr -d ' ')"
  [ "$settings" -gt "$guarded" ] &&
    echo "Compare tenant_id with NULLIF(current_setting('app.tenant_id', true), ''), so an unbound scope matches nothing"
  grep -qiE "create policy[^;]* to [^;]*orderloop_app[^;]*(using|with check)\(true\)" <<<"$flat" &&
    echo "A policy for orderloop_app must not be USING (true) or WITH CHECK (true); that reads every tenant"

  grep -qiE 'tenant-exempt:' <<<"$sql" && return 0

  tables="$(grep -ciE '^[[:space:]]*CREATE[[:space:]]+TABLE' <<<"$sql" || true)"
  [ "${tables:-0}" -eq 0 ] && return 0
  tenant_cols="$(grep -ciE '^[[:space:]]*tenant_id[[:space:]]+text[[:space:]]+NOT[[:space:]]+NULL' <<<"$sql" || true)"
  enabled="$(grep -ciE 'ENABLE[[:space:]]+ROW[[:space:]]+LEVEL[[:space:]]+SECURITY' <<<"$sql" || true)"
  forced="$(grep -ciE 'FORCE[[:space:]]+ROW[[:space:]]+LEVEL[[:space:]]+SECURITY' <<<"$sql" || true)"

  [ "${tenant_cols:-0}" -lt "$tables" ] &&
    echo "Each CREATE TABLE needs a 'tenant_id text NOT NULL' column on its own line ($tenant_cols of $tables)"
  [ "${enabled:-0}" -lt "$tables" ] &&
    echo "Each table needs ENABLE ROW LEVEL SECURITY ($enabled of $tables)"
  [ "${forced:-0}" -lt "$tables" ] &&
    echo "Each table needs FORCE ROW LEVEL SECURITY ($forced of $tables)"
  return 0
}
