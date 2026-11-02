---
paths:
  - "order-orchestrator/**/saga/**"
  - "docs/SAGA.md"
---

# Saga rules

**Why:** the saga decides what happens to a merchant's money. Its logic must be testable with no database, broker or clock.

- The core is a pure function: `Transition SagaEngine.decide(SagaState state, SagaInput input, Instant now)`. It performs no I/O and reads no clock of its own. `SagaInput` is `Reply(SagaEvent)` for Kafka messages, plus internal inputs that never go on the wire: approvals, `OrderAccepted` and `DeadlinePassed`.
- A `Transition` holds the next state, commands to emit, the deadline in force (derived from the state entered), and a finding to open.
- Use pattern matching over sealed types with no default branch. A per-state catch-all type pattern is the only fallback:

```java
return switch (new Step(state, input)) {
  case Step(Allocating s, Reply(InventoryReservedEvt e)) ->
      enter(new VerifyingPayment(s.orderId(), 1), now).emit(new ProcessPaymentCmd(s.orderId()));
  case Step(Allocating s, Reply(InventoryFailedEvt e)) -> attention(s, INVENTORY_FAILED, now);
  case Step(Allocating s, SagaInput other) -> Transition.ignore(s);
  // ...
};
```

- A late or duplicate reply is ignored and counted in a metric. It never throws.
- The application layer applies a transition in one transaction: update the saga row with optimistic locking, write `saga_step_log`, write outbox rows, change deadlines.
- Every state with work outstanding has a persisted deadline: all non-terminal states except `Received` (left at once) and `NeedsAttention` (an exception is already open). A missed deadline opens an exception and a finding, and never refunds automatically.
- Only `RefundApproved` or an operator's `RetryStep` may lead to `RefundPaymentCmd`. Both are human actions.
- An ignored `Transition` changes nothing, not even the deadline. Check `ignored()` first; `deadline()` throws on it.
- A compensation undoes only completed steps, in reverse order.
- In observe mode, handlers verify state with the provider and never write to it. See `docs/SAGA.md`.
- Downstream services never choose the next step.
- Change `docs/SAGA.md` and the table-driven transition test in the same commit as the logic. The `saga-reviewer` agent checks all three.
