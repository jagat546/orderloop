package app.orderloop.payment.provider;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * The default payments: an in-memory ledger per tenant. Tests and the demo {@link #record} what
 * checkout charged; an order with nothing recorded has no payment, as at a real provider. It does
 * no I/O, which is why it may run inside the inbox transaction (invariant 6).
 *
 * <p>shortcut: the ledger is lost on restart; persist it if the demo needs payments to survive one.
 */
@Component
@ConditionalOnProperty(
    name = "orderloop.payment.provider",
    havingValue = "simulator",
    matchIfMissing = true)
public class SimulatedPayments implements PaymentProvider {

  private record Key(String tenantId, String orderId) {}

  private final Map<Key, PaymentStatus.Found> ledger = new ConcurrentHashMap<>();

  /** Records what checkout did for an order: captured, authorised only, or cash on delivery. */
  public void record(
      String tenantId, String orderId, PaymentMethod method, boolean captured, Money amount) {
    if (method == PaymentMethod.COD && captured) {
      throw new IllegalArgumentException("Cash on delivery is never captured at checkout");
    }
    ledger.put(new Key(tenantId, orderId), new PaymentStatus.Found(method, captured, amount));
  }

  @Override
  public PaymentStatus lookup(String tenantId, String orderId) {
    PaymentStatus found = ledger.get(new Key(tenantId, orderId));
    return found != null ? found : new PaymentStatus.Failed("NO_PAYMENT_FOUND");
  }
}
