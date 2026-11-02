-- Test-only inbox table, copied from the template in docs/DATA_MODEL.md. Each service adds its own.
CREATE TABLE orchestrator.processed_event (
  tenant_id text NOT NULL,
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, consumer, event_id)
);

ALTER TABLE orchestrator.processed_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.processed_event FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.processed_event
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT ON orchestrator.processed_event TO orderloop_app;
