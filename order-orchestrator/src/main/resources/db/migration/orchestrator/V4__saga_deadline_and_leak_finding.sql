-- The one deadline in force for a saga, replaced on every transition that enters a new step and
-- removed when the saga reaches a state without one. Stored, not scheduled in memory, so a restart
-- loses nothing. orderloop_ops may only read it, to find due deadlines across tenants; each one is
-- fired as orderloop_app in its own tenant's scope.
CREATE TABLE orchestrator.saga_deadline (
  tenant_id text NOT NULL,
  order_id text NOT NULL,
  kind text NOT NULL,
  due_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, order_id),
  FOREIGN KEY (tenant_id, order_id) REFERENCES orchestrator.order_saga (tenant_id, order_id)
);

-- shortcut: no index on due_at, because tenant indexes start with tenant_id (tenancy rules); the
-- scan reads one row per open saga. Add a reviewed exemption if the M4.4 load test shows it hurts.

ALTER TABLE orchestrator.saga_deadline ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.saga_deadline FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.saga_deadline
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

CREATE POLICY ops_scan ON orchestrator.saga_deadline
  FOR SELECT
  TO orderloop_ops
  USING (true);

GRANT SELECT, INSERT, UPDATE, DELETE ON orchestrator.saga_deadline TO orderloop_app;
GRANT SELECT ON orchestrator.saga_deadline TO orderloop_ops;

-- Money at risk that a missed deadline found. One row per detector and order: finding the same leak
-- again reopens it. Amounts are in the order's own currency and never summed across currencies.
CREATE TABLE orchestrator.leak_finding (
  tenant_id text NOT NULL,
  detector text NOT NULL,
  order_id text NOT NULL,
  amount_at_risk_minor bigint NOT NULL CHECK (amount_at_risk_minor >= 0),
  recovered_minor bigint NOT NULL DEFAULT 0 CHECK (recovered_minor >= 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  is_estimate boolean NOT NULL,
  status text NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
  detected_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, detector, order_id),
  FOREIGN KEY (tenant_id, order_id) REFERENCES orchestrator.order_saga (tenant_id, order_id)
);

ALTER TABLE orchestrator.leak_finding ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator.leak_finding FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON orchestrator.leak_finding
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

GRANT SELECT, INSERT, UPDATE ON orchestrator.leak_finding TO orderloop_app;
