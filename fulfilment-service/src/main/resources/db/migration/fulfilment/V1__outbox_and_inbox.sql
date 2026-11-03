-- The fulfilment service's outbox and inbox, from the templates in docs/DATA_MODEL.md.
CREATE TABLE fulfilment.outbox (
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
CREATE INDEX outbox_unpublished ON fulfilment.outbox (id) WHERE published_at IS NULL;

ALTER TABLE fulfilment.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE fulfilment.outbox FORCE ROW LEVEL SECURITY;

-- The app writes rows only for the bound tenant.
CREATE POLICY tenant_isolation ON fulfilment.outbox
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

-- The relay reads and marks every tenant's rows, on this table only.
CREATE POLICY ops_scan ON fulfilment.outbox
  TO orderloop_ops
  USING (true)
  WITH CHECK (true);

GRANT INSERT ON fulfilment.outbox TO orderloop_app;
GRANT USAGE ON SEQUENCE fulfilment.outbox_id_seq TO orderloop_app;
GRANT SELECT, UPDATE ON fulfilment.outbox TO orderloop_ops;

CREATE TABLE fulfilment.processed_event (
  tenant_id text NOT NULL,
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, consumer, event_id)
);

ALTER TABLE fulfilment.processed_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE fulfilment.processed_event FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON fulfilment.processed_event
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT ON fulfilment.processed_event TO orderloop_app;

GRANT USAGE ON SCHEMA fulfilment TO orderloop_app, orderloop_ops;
