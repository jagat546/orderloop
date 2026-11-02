---
paths:
  - "order-orchestrator/**/saga/**"
  - "docs/SAGA.md"
---

# Saga rules

**Why:** the saga decides what happens to a merchant's money. Its logic must be testable with no database, broker or clock.

- The core is a pure function: `Transition decide(SagaState state, SagaEvent event, Instant now)`. It performs no I/O and reads no clock of its own.
- A `Transition` holds the next state, commands to emit, deadlines to set or cancel, and findings to open.
- Use pattern matching over sealed types with no default branch on the sealed state or event type itself:

```java
return switch (new Step(state, event)) {
  case Step(Allocating s, InventoryReservedEvt e) -> Transition.to(new VerifyingPayment(s.orderId()))
      .emit(new ProcessPaymentCmd(s.orderId())).deadline(Deadlines.PAYMENT);
  case Step(Allocating s, InventoryFailedEvt e) -> Transition.to(new NeedsAttention(s.orderId(), e.reason()));
  case Step(Closed s, SagaEvent e) -> Transition.ignoreLate(s, e);
  // ...
};
```

- A late or duplicate reply is ignored and counted in a metric. It never throws.
- The application layer applies a transition in one transaction: update the saga row with optimistic locking, write `saga_step_log`, write outbox rows, change deadlines.
- Every non-terminal state has a persisted deadline. A missed deadline opens an exception and never refunds automatically.
- A compensation undoes only completed steps, in reverse order.
- In observe mode, handlers verify state with the provider and never write to it. See `docs/SAGA.md`.
- Downstream services never choose the next step.
- Change `docs/SAGA.md` and the table-driven transition test in the same commit as the logic. The `saga-reviewer` agent checks all three.
