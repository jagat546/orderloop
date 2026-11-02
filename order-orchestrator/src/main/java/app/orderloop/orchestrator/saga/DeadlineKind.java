package app.orderloop.orchestrator.saga;

import java.time.Duration;
import java.util.Optional;

/**
 * One deadline per non-terminal state, with the defaults in docs/SAGA.md.
 *
 * <p>shortcut: the durations are fixed defaults; per-tenant overrides arrive with tenant settings.
 */
public enum DeadlineKind {
  ALLOCATING(Duration.ofMinutes(5)),
  VERIFYING_PAYMENT(Duration.ofMinutes(15)),
  BOOKING(Duration.ofHours(2)),
  RELEASING(Duration.ofMinutes(15)),
  IN_TRANSIT(Duration.ofDays(10)),
  NDR_PENDING(Duration.ofHours(24)),
  RETURNING(Duration.ofDays(15)),
  RESTOCKING(Duration.ofHours(48)),
  REFUND_PROPOSED(Duration.ofHours(24)),
  REFUNDING(Duration.ofMinutes(30)),
  AWAITING_SETTLEMENT(Duration.ofDays(10));

  private final Duration duration;

  DeadlineKind(Duration duration) {
    this.duration = duration;
  }

  public Duration duration() {
    return duration;
  }

  /** The deadline a state runs under, or empty for a state that waits for nothing. */
  public static Optional<DeadlineKind> of(SagaState state) {
    return Optional.ofNullable(
        switch (state) {
          case SagaState.Allocating s -> ALLOCATING;
          case SagaState.VerifyingPayment s -> VERIFYING_PAYMENT;
          case SagaState.Booking s -> BOOKING;
          case SagaState.Releasing s -> RELEASING;
          case SagaState.InTransit s -> IN_TRANSIT;
          case SagaState.NdrPending s -> NDR_PENDING;
          case SagaState.Returning s -> RETURNING;
          case SagaState.Restocking s -> RESTOCKING;
          case SagaState.RefundProposed s -> REFUND_PROPOSED;
          case SagaState.Refunding s -> REFUNDING;
          case SagaState.AwaitingSettlement s -> AWAITING_SETTLEMENT;
          // Left at once, or parked with an exception open, or finished.
          case SagaState.Received s -> null;
          case SagaState.NeedsAttention s -> null;
          case SagaState.Closed s -> null;
          case SagaState.ClosedCancelled s -> null;
          case SagaState.ClosedRto s -> null;
        });
  }
}
