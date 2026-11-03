-- One refund per refund ID, following intent, key, result, reconcile (docs/ARCHITECTURE.md).
-- INTENT: recorded, nothing sent. SENT: the call may be in flight. UNKNOWN: the call failed in a way
-- that hides whether the refund happened. SUCCEEDED and FAILED are final for that attempt. An
-- operator's retry after FAILED starts a new attempt under a new idempotency key.
CREATE TABLE payment.refund_intent (
  tenant_id text NOT NULL,
  refund_id text NOT NULL,
  order_id text NOT NULL,
  attempt int NOT NULL CHECK (attempt >= 1),
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  idempotency_key text NOT NULL,
  provider_refund_id text,
  status text NOT NULL CHECK (status IN ('INTENT', 'SENT', 'SUCCEEDED', 'FAILED', 'UNKNOWN')),
  failure_reason text,
  cause_event_id uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, refund_id)
);

ALTER TABLE payment.refund_intent ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment.refund_intent FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON payment.refund_intent
  TO orderloop_app
  USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
  WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

-- The reconciler finds due refunds across tenants, then handles each one as orderloop_app inside
-- that refund's tenant scope. It may only read.
CREATE POLICY ops_scan ON payment.refund_intent
  TO orderloop_ops
  USING (true);

GRANT SELECT, INSERT, UPDATE ON payment.refund_intent TO orderloop_app;
GRANT SELECT ON payment.refund_intent TO orderloop_ops;
