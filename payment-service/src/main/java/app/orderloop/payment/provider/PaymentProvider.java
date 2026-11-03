package app.orderloop.payment.provider;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import java.util.Objects;

/**
 * The payment port. The simulator is the default; real adapters are opt-in through {@code
 * orderloop.payment.provider} (ADR 0007). Refunds arrive in M1.11b.
 */
public interface PaymentProvider {

  /** What the provider holds for the order. It never charges. */
  PaymentStatus lookup(String tenantId, String orderId);

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
}
