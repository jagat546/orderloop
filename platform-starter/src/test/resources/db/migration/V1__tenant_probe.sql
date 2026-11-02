-- Test-only sample table that proves row-level security. Copied from the template in docs/DATA_MODEL.md.
CREATE TABLE orchestrator.tenant_probe (
  tenant_id text NOT NULL,
  probe_id uuid NOT NULL,
  label text NOT NULL,
  PRIMARY KEY (tenant_id, probe_id)
);

ALTER TABLE orchestrator.tenant_probe ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.tenant_probe FORCE ROW LEVEL SECURITY;

-- NULLIF turns the empty setting of an unbound scope into NULL, which matches no row and allows no write.
CREATE POLICY tenant_isolation ON orchestrator.tenant_probe
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT USAGE ON SCHEMA orchestrator TO orderloop_app, orderloop_ops;
GRANT SELECT, INSERT, UPDATE, DELETE ON orchestrator.tenant_probe TO orderloop_app;
