package app.orderloop.dto;

import java.util.Objects;

/**
 * Replies and provider events the orchestrator reacts to. See docs/SAGA.md. A {@code reason} is a
 * short code such as {@code OUT_OF_STOCK}, never free text that could hold customer data.
 */
public sealed interface SagaEvent extends Message {

  record InventoryReservedEvt(String orderId) implements SagaEvent {}

  record InventoryFailedEvt(String orderId, String reason) implements SagaEvent {}

  record InventoryReleasedEvt(String orderId) implements SagaEvent {}

  record InventoryRestockedEvt(String orderId) implements SagaEvent {}

  /** A COD order replies with {@code method=COD, captured=false}; its money is tracked later. */
  record PaymentProcessedEvt(String orderId, PaymentMethod method, boolean captured, Money amount)
      implements SagaEvent {
    public PaymentProcessedEvt {
      Objects.requireNonNull(method, "method");
      Objects.requireNonNull(amount, "amount");
    }
  }

  record PaymentFailedEvt(String orderId, String reason) implements SagaEvent {}

  record ShipmentBookedEvt(String orderId) implements SagaEvent {}

  record ShipmentFailedEvt(String orderId, String reason) implements SagaEvent {}

  record DeliveredEvt(String orderId) implements SagaEvent {}

  record DeliveryFailedEvt(String orderId, String reason) implements SagaEvent {}

  record RtoStartedEvt(String orderId) implements SagaEvent {}

  record RtoReceivedEvt(String orderId) implements SagaEvent {}

  record RefundCompletedEvt(String orderId, String refundId, Money amount) implements SagaEvent {
    public RefundCompletedEvt {
      Objects.requireNonNull(refundId, "refundId");
      Objects.requireNonNull(amount, "amount");
    }
  }

  record RefundFailedEvt(String orderId, String refundId, String reason) implements SagaEvent {}

  record SettlementObservedEvt(String orderId, Money amount) implements SagaEvent {
    public SettlementObservedEvt {
      Objects.requireNonNull(amount, "amount");
    }
  }
}
