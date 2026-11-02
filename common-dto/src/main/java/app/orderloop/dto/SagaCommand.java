package app.orderloop.dto;

import java.util.Objects;

/** Commands the orchestrator sends to downstream services. See docs/SAGA.md. */
public sealed interface SagaCommand extends Message {

  record ReserveInventoryCmd(String orderId) implements SagaCommand {}

  /** Verifies an existing payment. It never charges. */
  record ProcessPaymentCmd(String orderId) implements SagaCommand {}

  record BookShipmentCmd(String orderId) implements SagaCommand {}

  record ReleaseInventoryCmd(String orderId) implements SagaCommand {}

  record ReattemptDeliveryCmd(String orderId) implements SagaCommand {}

  record RestockInventoryCmd(String orderId) implements SagaCommand {}

  /** Sent only after a human approves. {@code refundId} is the basis of the idempotency key. */
  record RefundPaymentCmd(String orderId, String refundId, Money amount) implements SagaCommand {
    public RefundPaymentCmd {
      Objects.requireNonNull(refundId, "refundId");
      Objects.requireNonNull(amount, "amount");
    }
  }
}
