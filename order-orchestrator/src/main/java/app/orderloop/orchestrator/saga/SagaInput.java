package app.orderloop.orchestrator.saga;

import app.orderloop.dto.SagaEvent;
import java.util.Objects;

/**
 * Everything that can move a saga. {@link Reply} wraps a Kafka message; the rest are internal and
 * never go on the wire. Who approved what, and when, is recorded by the application layer.
 */
public sealed interface SagaInput {

  record Reply(SagaEvent event) implements SagaInput {
    public Reply {
      Objects.requireNonNull(event, "event");
    }
  }

  /** Shopify accepted the order; starts the saga. */
  record OrderAccepted() implements SagaInput {}

  record RefundApproved() implements SagaInput {}

  record ReattemptApproved() implements SagaInput {}

  record ReturnDecided() implements SagaInput {}

  record RestockApproved() implements SagaInput {}

  /**
   * An operator re-runs the step a parked saga stopped at, sending its command again. A human
   * action, so retrying a refund counts as approving it again (invariant 8).
   */
  record RetryStep() implements SagaInput {}

  record DeadlinePassed(DeadlineKind kind) implements SagaInput {
    public DeadlinePassed {
      Objects.requireNonNull(kind, "kind");
    }
  }
}
