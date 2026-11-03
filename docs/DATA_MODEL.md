# Data model

Each service has its own schema. Money is a `bigint` column in minor units with a `_minor` suffix, beside a `currency` column holding an ISO 4217 code (ADR 0007). Time is `timestamptz`.

## Roles

| Role | Purpose | Limits |
|---|---|---|
| `orderloop_owner` | Runs Flyway, owns tables | Never used by running request code |
| `orderloop_app` | Normal service traffic | `NOSUPERUSER NOBYPASSRLS`, not an owner |
| `orderloop_ops` | Outbox relay, deadline and detector scans | `NOSUPERUSER NOBYPASSRLS`, explicit policies on specific tables only, `idle_in_transaction_session_timeout = 1min` |

In development, `deploy/postgres/init/01-roles-and-schemas.sh` creates these roles and the five service schemas, owned by `orderloop_owner`. `bash scripts/check-infra.sh` checks them.

## Table template

```sql
CREATE TABLE orchestrator.order_saga (
  tenant_id text NOT NULL,
  order_id text NOT NULL,
  state text NOT NULL,
  mode text NOT NULL DEFAULT 'OBSERVE',
  version bigint NOT NULL DEFAULT 0,
  order_total_minor bigint NOT NULL CHECK (order_total_minor >= 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, order_id)
);

ALTER TABLE orchestrator.order_saga ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.order_saga FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.order_saga
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT, UPDATE ON orchestrator.order_saga TO orderloop_app;
```

`TenantAwareDataSource` sets `app.tenant_id` at every connection checkout, to an empty string when no tenant is bound. `current_setting('app.tenant_id', true)` returns NULL when the setting was never made, and `NULLIF(..., '')` turns the empty string into NULL too. NULL equals nothing, so an unbound scope reads no rows and the `WITH CHECK` clause refuses every write, even a row with `tenant_id = ''`. `scripts/check-invariants.sh` rejects a policy that compares `current_setting('app.tenant_id'...)` without `NULLIF`. `platform-starter`'s `TenantRlsIT` proves this as `orderloop_app`.

Tables that `orderloop_ops` must scan get one more policy, on that table only:

```sql
CREATE POLICY ops_scan ON orchestrator.saga_deadline
  TO orderloop_ops
  USING (true)
  WITH CHECK (true);

GRANT SELECT, UPDATE ON orchestrator.saga_deadline TO orderloop_ops;
```

`tenant_id` is an opaque text ID such as `t_01J...`, not the shop domain. It matches `t_[A-Za-z0-9]{1,64}`, which `TenantContext` enforces.

Each schema migration also needs `GRANT USAGE ON SCHEMA <schema> TO orderloop_app, orderloop_ops`.

**Data backfills:** because RLS is forced, `orderloop_owner` sees no rows either. A migration that changes existing data must create a temporary policy for the owner and drop it in the same migration:

```sql
CREATE POLICY owner_backfill ON orchestrator.order_saga
  TO orderloop_owner USING (true) WITH CHECK (true);
-- UPDATE ...
DROP POLICY owner_backfill ON orchestrator.order_saga;
```

## Tables

### `integration` schema (gateway)

| Table | Key columns |
|---|---|
| `tenant` | `tenant_id`, `shop_domain`, `status`, `plan`, `installed_at`, `uninstalled_at` |
| `provider_connection` | `tenant_id`, `provider`, `credential_ciphertext`, `key_version`, `status` |
| `raw_webhook_event` | `tenant_id`, `provider`, `provider_event_id`, `payload`, `received_at`, `purge_after` |
| `outbox`, `processed_event` | See below |

The gateway resolves a webhook's tenant by looking up `shop_domain` as `orderloop_ops`, through an `ops_scan` policy on `tenant`, after signature verification.

### `orchestrator` schema

| Table | Key columns |
|---|---|
| `order_saga` | As above, plus `state_data`: the engine's full `SagaState` as JSON. `state` repeats its name for queries |
| `saga_step_log` | `tenant_id`, `order_id`, `seq`, `from_state`, `to_state`, `cause_event_id`, `early`, `at`; `seq` is the saga's `version` after the step |
| `saga_deadline` | `tenant_id`, `order_id`, `kind`, `due_at`, `status` |
| `exception_case` | `tenant_id`, `case_id`, `order_id`, `reason`, `amount_at_risk_minor`, `currency`, `status` (`OPEN` or `RESOLVED`), `opened_at`, `resolved_at`, `resolved_by`; at most one `OPEN` per order |
| `leak_finding` | `tenant_id`, `detector`, `order_id`, `amount_at_risk_minor`, `recovered_minor`, `currency`, `is_estimate`, `status`; unique on the first three |
| `approval` | `tenant_id`, `approval_id`, `order_id`, `kind`, `amount_minor`, `currency`, `approved_by`, `approved_at` |
| `tenant_settings` | `tenant_id`, `automation_mode`, deadline overrides |

### `inventory`, `payment`, `fulfilment` schemas

| Table | Key columns |
|---|---|
| `inventory.allocation` | `tenant_id`, `order_id`, `sku`, `quantity`, `location_id`, `status`. Not created yet: the simulator keeps no state, so it arrives with the first real adapter |
| `payment.payment_record` | `tenant_id`, `order_id`, `provider_payment_id`, `method`, `captured_minor`, `refunded_minor`, `currency`, `status`. Not created yet: verification (M1.11a) reports the provider's record and keeps none; refunds need it to cap amounts (M1.11b) |
| `payment.refund_intent` | `tenant_id`, `refund_id`, `order_id`, `attempt`, `amount_minor`, `currency`, `idempotency_key` (`<refund_id>:<attempt>`), `provider_refund_id`, `status`, `failure_reason`, `cause_event_id`, `updated_at`. `orderloop_ops` may only read it, through `ops_scan` |
| `payment.settlement` | `tenant_id`, `order_id`, `expected_minor`, `settled_minor`, `currency`, `expected_at`, `settled_at` |
| `fulfilment.shipment` | `tenant_id`, `order_id`, `awb`, `carrier`, `status`, `ndr_count`, `last_event_at`. Not created yet: the carrier simulator keeps its bookings in memory, so it arrives with the first real adapter |

`refund_intent.status` is one of `INTENT`, `SENT`, `SUCCEEDED`, `FAILED`, `UNKNOWN`.

### In every schema

```sql
CREATE TABLE <schema>.outbox (
  id bigserial PRIMARY KEY,
  tenant_id text NOT NULL,
  event_id uuid NOT NULL UNIQUE,
  event_type text NOT NULL,
  schema_version int NOT NULL,
  aggregate_id text NOT NULL,
  topic text NOT NULL,
  payload json NOT NULL,
  headers jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON <schema>.outbox (id) WHERE published_at IS NULL;

CREATE TABLE <schema>.processed_event (
  tenant_id text NOT NULL,
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, consumer, event_id)
);
```

Both also get forced RLS and the tenant policy. `orderloop_app` may insert into `outbox` (plus `USAGE` on `outbox_id_seq`), and `orderloop_ops` may read and mark it through an `ops_scan` policy. `orderloop_app` may select and insert `processed_event`; `V3__processed_event.sql` in `platform-starter`'s test migrations is a complete example. A nightly job purges published rows after 7 days and processed events after 30 days.

`payload` is `json`, not `jsonb`. `jsonb` reformats the text and reorders keys, so the relay would send different bytes from the form pinned by `common-dto`'s golden file. `json` validates the text and keeps it exactly. `platform-starter/src/test/resources/db/migration/V2__outbox.sql` is a complete example with policies and grants.

## PII

Observe mode stores no customer names, phones, emails or addresses. Raw webhook payloads may contain them, so the gateway strips fields it does not need before storing and purges raw events after 14 days.
