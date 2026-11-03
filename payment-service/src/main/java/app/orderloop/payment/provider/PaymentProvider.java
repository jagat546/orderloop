package app.orderloop.payment.provider;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import java.util.Objects;
import java.util.Optional;

/**
 * The payment port. The simulator is the default; real adapters are opt-in through {@code
 * orderloop.payment.provider} (ADR 0007). Never call it inside a database transaction (invariant
 * 6), except the simulator, which does no I/O.
 */
public interface PaymentProvider {

  /** What the provider holds for the order. It never charges. */
  PaymentStatus lookup(String tenantId, String orderId);

  /**
   * Refunds under an idempotency key: the same key never refunds twice. Only adapters for providers
   * that honour such keys may implement it.
   *
   * @throws RuntimeException on a timeout or a lost connection; the refund may have happened, so
   *     the caller records the outcome as unknown and asks {@link #findRefund} later
   */
  RefundResult refund(String tenantId, RefundRequest request);

  /** The refund made under this key, if the provider made one. */
  Optional<RefundResult> findRefund(String tenantId, String idempotencyKey);

  sealed interface PaymentStatus {

    /** The provider's own amount and currency, never the order's. COD is never captured. */
    record Found(PaymentMethod method, boolean captured, Money amount) implements PaymentStatus {
      public Found {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(amount, "amount");
      }
    }

    /** {@code reason} is a code such as {@code NO_PAYMENT_FOUND}, never free text. */
    record Failed(String reason) implements PaymentStatus {}
  }

  record RefundRequest(String orderId, String idempotencyKey, Money amount) {
    public RefundRequest {
      Objects.requireNonNull(orderId, "orderId");
      Objects.requireNonNull(idempotencyKey, "idempotencyKey");
      Objects.requireNonNull(amount, "amount");
    }
  }

  sealed interface RefundResult {

    record Refunded(String providerRefundId, Money amount) implements RefundResult {}

    /** The provider refused; no money moved. {@code reason} is a code, never free text. */
    record Rejected(String reason) implements RefundResult {}
  }
}
