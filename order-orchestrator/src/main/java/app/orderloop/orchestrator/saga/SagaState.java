package app.orderloop.orchestrator.saga;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import java.util.Objects;

/** Where one order's saga stands. See docs/SAGA.md. */
public sealed interface SagaState {

  String orderId();

  /**
   * What the payment check found; decides between refunding and releasing.
   *
   * @param settled the money already reached the merchant, reported before delivery, so delivery
   *     closes the order (docs/SAGA.md, early settlement)
   */
  record Payment(PaymentMethod method, boolean captured, Money amount, boolean settled) {
    public Payment {
      Objects.requireNonNull(method, "method");
      Objects.requireNonNull(amount, "amount");
    }

    public Payment(PaymentMethod method, boolean captured, Money amount) {
      this(method, captured, amount, false);
    }

    Payment asSettled() {
      return new Payment(method, captured, amount, true);
    }
  }

  /** Where the saga goes once a refund completes. */
  enum AfterRefund {
    /** Stock is still held: release it, then the order is cancelled. */
    RELEASE_STOCK,
    /** The order came back through a return: it is closed as returned. */
    CLOSE_RTO,
    /** Stock was already released after a failed payment that turned out captured. */
    CLOSE_CANCELLED
  }

  /** Why a saga needs a human. */
  enum AttentionReason {
    INVENTORY_FAILED,
    /** A prepaid order whose payment is not captured: never ship it. */
    PAYMENT_NOT_CAPTURED,
    RELEASE_FAILED,
    RESTOCK_FAILED,
    REFUND_FAILED,
    DEADLINE_MISSED
  }

  record Received(String orderId) implements SagaState {}

  record Allocating(String orderId) implements SagaState {}

  /** {@code attempt} counts payment checks; the second missed deadline needs attention. */
  record VerifyingPayment(String orderId, int attempt) implements SagaState {}

  record Booking(String orderId, Payment payment) implements SagaState {}

  record InTransit(String orderId, Payment payment) implements SagaState {}

  record NdrPending(String orderId, Payment payment) implements SagaState {}

  record Returning(String orderId, Payment payment) implements SagaState {}

  /** {@code requested} is true once a restock was approved and its command sent. */
  record Restocking(String orderId, Payment payment, boolean requested) implements SagaState {}

  record RefundProposed(String orderId, Payment payment, AfterRefund then) implements SagaState {}

  record Refunding(String orderId, Payment payment, AfterRefund then) implements SagaState {}

  /**
   * Gives back reserved stock.
   *
   * @param paymentFailed released because the payment check failed, so a capture reported later is
   *     money the customer paid for nothing; on other paths a payment reply is a duplicate
   * @param lateCapture that late capture, to refund once the stock is back; null when none
   */
  record Releasing(String orderId, boolean paymentFailed, Payment lateCapture)
      implements SagaState {}

  record AwaitingSettlement(String orderId, Payment payment) implements SagaState {}

  /** Parked with an open exception. Keeps the prior state so the saga can resume from it. */
  record NeedsAttention(String orderId, SagaState prior, AttentionReason reason)
      implements SagaState {}

  record Closed(String orderId) implements SagaState {}

  /** {@code paymentFailed}: a capture reported after this point reopens the saga for a refund. */
  record ClosedCancelled(String orderId, boolean paymentFailed) implements SagaState {}

  record ClosedRto(String orderId) implements SagaState {}
}
