-- One saga per order, from the template in docs/DATA_MODEL.md. state_data is the engine's full
-- SagaState as JSON; state repeats its name for queries. version is the optimistic lock.
CREATE TABLE orchestrator.order_saga (
  tenant_id text NOT NULL,
  order_id text NOT NULL,
  state text NOT NULL,
  state_data text NOT NULL,
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

-- Every transition the saga took. seq is the saga's version after it, so two writers that both
-- read the same version cannot both log a step.
CREATE TABLE orchestrator.saga_step_log (
  tenant_id text NOT NULL,
  order_id text NOT NULL,
  seq bigint NOT NULL,
  from_state text NOT NULL,
  to_state text NOT NULL,
  cause_event_id uuid,
  early boolean NOT NULL,
  at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, order_id, seq),
  FOREIGN KEY (tenant_id, order_id) REFERENCES orchestrator.order_saga (tenant_id, order_id)
);

ALTER TABLE orchestrator.saga_step_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.saga_step_log FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.saga_step_log
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT USAGE ON SCHEMA orchestrator TO orderloop_app, orderloop_ops;
GRANT SELECT, INSERT, UPDATE ON orchestrator.order_saga TO orderloop_app;
GRANT SELECT, INSERT ON orchestrator.saga_step_log TO orderloop_app;
