-- Test-only outbox, copied from the template in docs/DATA_MODEL.md. Each service adds its own.
CREATE TABLE orchestrator.outbox (
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
CREATE INDEX outbox_unpublished ON orchestrator.outbox (id) WHERE published_at IS NULL;

ALTER TABLE orchestrator.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.outbox FORCE ROW LEVEL SECURITY;

-- The app writes rows only for the bound tenant.
CREATE POLICY tenant_isolation ON orchestrator.outbox
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

-- The relay reads and marks every tenant's rows, on this table only.
CREATE POLICY ops_scan ON orchestrator.outbox
  TO orderloop_ops
  USING (true)
  WITH CHECK (true);

GRANT INSERT ON orchestrator.outbox TO orderloop_app;
GRANT USAGE ON SEQUENCE orchestrator.outbox_id_seq TO orderloop_app;
GRANT SELECT, UPDATE ON orchestrator.outbox TO orderloop_ops;
