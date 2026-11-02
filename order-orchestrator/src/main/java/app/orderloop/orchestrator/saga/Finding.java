package app.orderloop.orchestrator.saga;

/** Money-at-risk findings a missed deadline opens, from the detectors in docs/PRODUCT.md. */
public enum Finding {
  PAID_NOT_FULFILLED,
  NDR_PAST_SLA,
  RTO_NOT_RESTOCKED,
  COD_REMITTANCE_GAP,
  STUCK_ORDER
}
