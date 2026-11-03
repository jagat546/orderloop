-- The payment service's outbox and inbox, from the templates in docs/DATA_MODEL.md.
CREATE TABLE payment.outbox (
  id bigserial PRIMARY KEY,
  tenant_id text NOT NULL,
  event_id uuid NOT NULL UNIQUE,
  event_type text NOT NULL,
  schema_version int NOT NULL,
  aggregate_id text NOT NULL,
  topic text NOT NULL,
  payload json NOT NULL, -- json keeps the exact text; jsonb would reformat it and reorder keys
  headers jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON payment.outbox (id) WHERE published_at IS NULL;

ALTER TABLE payment.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment.outbox FORCE ROW LEVEL SECURITY;

-- The app writes rows only for the bound tenant.
CREATE POLICY tenant_isolation ON payment.outbox
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

-- The relay reads and marks every tenant's rows, on this table only.
CREATE POLICY ops_scan ON payment.outbox
  TO orderloop_ops
  USING (true)
  WITH CHECK (true);

GRANT INSERT ON payment.outbox TO orderloop_app;
GRANT USAGE ON SEQUENCE payment.outbox_id_seq TO orderloop_app;
GRANT SELECT, UPDATE ON payment.outbox TO orderloop_ops;

CREATE TABLE payment.processed_event (
  tenant_id text NOT NULL,
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, consumer, event_id)
);

ALTER TABLE payment.processed_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment.processed_event FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON payment.processed_event
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT ON payment.processed_event TO orderloop_app;

GRANT USAGE ON SCHEMA payment TO orderloop_app, orderloop_ops;
