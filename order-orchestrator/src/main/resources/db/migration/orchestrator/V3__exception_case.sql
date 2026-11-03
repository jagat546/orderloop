-- An exception is open while an order's saga is parked in NeedsAttention, and resolved when the
-- saga moves on. At most one is open per order. resolved_by names what resolved it: the input that
-- resumed the saga, until operators act through the API.
CREATE TABLE orchestrator.exception_case (
  tenant_id text NOT NULL,
  case_id uuid NOT NULL,
  order_id text NOT NULL,
  reason text NOT NULL,
  amount_at_risk_minor bigint NOT NULL CHECK (amount_at_risk_minor >= 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  status text NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
  opened_at timestamptz NOT NULL,
  resolved_at timestamptz,
  resolved_by text,
  PRIMARY KEY (tenant_id, case_id),
  FOREIGN KEY (tenant_id, order_id) REFERENCES orchestrator.order_saga (tenant_id, order_id)
);

CREATE UNIQUE INDEX exception_case_one_open
  ON orchestrator.exception_case (tenant_id, order_id) WHERE status = 'OPEN';

ALTER TABLE orchestrator.exception_case ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.exception_case FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.exception_case
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT, UPDATE ON orchestrator.exception_case TO orderloop_app;
