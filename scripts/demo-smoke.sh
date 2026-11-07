#!/usr/bin/env bash
# Checks the Docker demo end to end (roadmap M1.16): every service starts from the root
# compose.yaml, an order created through the order API is handled by the services, the demo
# generator's orders reach Closed, ClosedRto and an open exception on their own, and those
# transitions reach Prometheus, which the Grafana board reads.
#
#   bash scripts/demo-smoke.sh          # builds and starts the demo first
#   bash scripts/demo-smoke.sh --no-up  # checks a demo that is already running
#
# Needs only Docker and curl. The demo key below is a development key with no secret value.
set -euo pipefail

cd "$(dirname "$0")/.."

API="http://localhost:${ORCHESTRATOR_HOST_PORT:-8080}"
PROMETHEUS="http://localhost:${PROMETHEUS_HOST_PORT:-9090}"
KEY="${DEMO_API_KEY:-demo-key-not-a-secret}"
SERVICES=(order-orchestrator inventory-service payment-service fulfilment-service)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Polls a command until it succeeds, for at most $1 seconds.
wait_for() {
  local seconds=$1
  shift
  local deadline=$((SECONDS + seconds))
  until "$@"; do
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

if [[ "${1:-}" != "--no-up" ]]; then
  [[ -f compose.yaml ]] || fail "no compose.yaml at the repository root"
  docker compose up --build --detach --wait --wait-timeout 600
fi

echo "Services running:"
for service in "${SERVICES[@]}"; do
  status="$(docker compose ps --format '{{.Service}} {{.State}} {{.Health}}' "$service" 2>/dev/null || true)"
  [[ "$status" == "$service running"* ]] || fail "$service is not running: '${status}'"
  echo "  $status"
done

# No payment is recorded for this order, so payment verification fails, inventory releases the
# stock, and the saga closes as cancelled: inventory, payment and the orchestrator all act on it.
order="smoke-$(date +%s)"
created="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/orders" \
  -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
  -d "{\"orderId\":\"$order\",\"total\":{\"amountMinor\":12345,\"currency\":\"KWD\"}}")"
[[ "$created" == 201 ]] || fail "creating $order returned HTTP $created"
echo "Created order $order"

state() {
  curl -s "$API/api/v1/orders/$1" -H "X-Api-Key: $KEY" |
    sed -n 's/.*"state":"\([A-Za-z]*\)".*/\1/p'
}
in_state() { [[ "$(state "$1")" == "$2" ]]; }
wait_for 120 in_state "$order" ClosedCancelled ||
  fail "$order is in '$(state "$order")', not ClosedCancelled"
echo "Order $order reached ClosedCancelled"

# The generator's own orders, with nothing driving the simulators (roadmap M1.16b). The first one
# of each kind it created, from its log.
demo_order() {
  docker compose logs --no-log-prefix demo-orders 2>/dev/null |
    sed -n "s/^201 \(demo-[0-9]*-[0-9]*-$1\)\r\{0,1\}$/\1/p" | sed -n 1p
}
generated() { [[ -n "$(demo_order "$1")" ]]; }
for expected in delivered-INR-149900:Closed rto-JPY-4500:ClosedRto out-of-stock:NeedsAttention; do
  kind="${expected%%:*}" end="${expected##*:}"
  wait_for 60 generated "$kind" || fail "the generator created no $kind order"
  demo="$(demo_order "$kind")"
  wait_for 120 in_state "$demo" "$end" || fail "$demo is in '$(state "$demo")', not $end"
  echo "Demo order $demo reached $end"
done

# Metrics are pushed every 15 seconds, labelled with each service's name as job.
query() {
  curl -s --get "$PROMETHEUS/api/v1/query" --data-urlencode "query=$1"
}
counted() {
  query "sum(orderloop_saga_transitions_total{to=\"$1\"})" | grep -q '"value":\[[^]]*,"[1-9]'
}
for end in ClosedCancelled Closed ClosedRto NeedsAttention; do
  wait_for 120 counted "$end" || fail "Prometheus has no transition to $end"
  echo "Prometheus counted transitions to $end"
done

every_service_reports() {
  local jobs
  jobs="$(query 'count by (job) (orderloop_outbox_backlog)')"
  for service in "${SERVICES[@]}"; do
    grep -q "\"job\":\"$service\"" <<<"$jobs" || return 1
  done
}
wait_for 120 every_service_reports || fail "not every service reports its outbox to Prometheus"
echo "Prometheus has outbox metrics from every service"

# The board itself, as an anonymous viewer sees it.
GRAFANA="http://localhost:${GRAFANA_HOST_PORT:-3000}"
curl -s "$GRAFANA/api/dashboards/uid/orderloop" | grep -q '"uid":"orderloop"' ||
  fail "Grafana does not serve the orderloop board"
echo "Grafana serves the orderloop board at $GRAFANA/d/orderloop"

echo "Demo smoke check passed."
