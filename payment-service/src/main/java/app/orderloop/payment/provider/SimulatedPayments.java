package app.orderloop.payment.provider;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaEvent.SettlementObservedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The default payments: an in-memory ledger per tenant. Tests and the demo {@link #record} what
 * checkout charged; an order with nothing recorded has no payment, as at a real provider. Refunds
 * honour idempotency keys and never exceed what is left of the capture, in its currency. {@link
 * #onNextRefund} makes the next refund for an order fail, or time out after it succeeded. It does
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

  /** What the next refund call for an order does. */
  public enum RefundBehaviour {
    SUCCEED,
    REJECT,
    /** The refund is made, then the call fails as a timeout would, so the caller cannot know. */
    TIME_OUT_AFTER_SUCCESS
  }

  private record Key(String tenantId, String orderId) {}

  private record RefundKey(String tenantId, String idempotencyKey) {}

  // Guarded by this: a refund checks and changes several of them together.
  private final Map<Key, PaymentStatus.Found> ledger = new HashMap<>();
  private final Map<Key, RefundBehaviour> nextRefund = new HashMap<>();
  private final Map<RefundKey, RefundResult.Refunded> refundsByKey = new HashMap<>();
  private final Map<Key, List<Money>> refunds = new HashMap<>();
  private final Map<Key, Integer> refundCalls = new HashMap<>();

  private final OutboxWriter outbox;
  private final TransactionTemplate tx;

  SimulatedPayments(OutboxWriter outbox, TransactionTemplate tx) {
    this.outbox = outbox;
    this.tx = tx;
  }

  /**
   * Reports that the order's money reached the merchant: the capture, or for COD the cash the
   * courier remitted. It goes out through the service's outbox in the tenant's scope, as a
   * provider's settlement report would, and only for a payment that tenant has.
   */
  public void settle(String tenantId, String orderId) {
    PaymentStatus.Found payment;
    synchronized (this) {
      payment = ledger.get(new Key(tenantId, orderId));
    }
    if (payment == null) {
      throw new IllegalStateException("No payment recorded for order " + orderId);
    }
    var event = new SettlementObservedEvt(orderId, payment.amount());
    TenantContext.callAs(
        tenantId,
        () ->
            tx.execute(
                status -> {
                  outbox.append(
                      Topics.PAYMENT_EVENTS,
                      new Envelope(UUID.randomUUID(), tenantId, null, event));
                  return null;
                }));
  }

  /** Records what checkout did for an order: captured, authorised only, or cash on delivery. */
  public synchronized void record(
      String tenantId, String orderId, PaymentMethod method, boolean captured, Money amount) {
    if (method == PaymentMethod.COD && captured) {
      throw new IllegalArgumentException("Cash on delivery is never captured at checkout");
    }
    ledger.put(new Key(tenantId, orderId), new PaymentStatus.Found(method, captured, amount));
  }

  /** Sets what the next refund call for the order does; afterwards refunds succeed again. */
  public synchronized void onNextRefund(String tenantId, String orderId, RefundBehaviour then) {
    nextRefund.put(new Key(tenantId, orderId), then);
  }

  /** Every refund made for the order, in the order made. */
  public synchronized List<Money> refunds(String tenantId, String orderId) {
    return List.copyOf(refunds.getOrDefault(new Key(tenantId, orderId), List.of()));
  }

  /** How many times {@link #refund} was called for the order, whatever the outcome. */
  public synchronized int refundCalls(String tenantId, String orderId) {
    return refundCalls.getOrDefault(new Key(tenantId, orderId), 0);
  }

  @Override
  public synchronized PaymentStatus lookup(String tenantId, String orderId) {
    PaymentStatus found = ledger.get(new Key(tenantId, orderId));
    return found != null ? found : new PaymentStatus.Failed("NO_PAYMENT_FOUND");
  }

  @Override
  public synchronized RefundResult refund(String tenantId, RefundRequest request) {
    Key order = new Key(tenantId, request.orderId());
    refundCalls.merge(order, 1, Integer::sum);
    RefundResult.Refunded earlier =
        refundsByKey.get(new RefundKey(tenantId, request.idempotencyKey()));
    if (earlier != null) {
      return earlier; // The same key never refunds twice.
    }
    RefundBehaviour behaviour = nextRefund.getOrDefault(order, RefundBehaviour.SUCCEED);
    nextRefund.remove(order);
    if (behaviour == RefundBehaviour.REJECT) {
      return new RefundResult.Rejected("PROVIDER_DECLINED");
    }
    PaymentStatus.Found payment = ledger.get(order);
    if (payment == null || !payment.captured()) {
      return new RefundResult.Rejected("NOTHING_CAPTURED");
    }
    if (!payment.amount().currency().equals(request.amount().currency())) {
      return new RefundResult.Rejected("CURRENCY_MISMATCH");
    }
    Money left = payment.amount();
    for (Money done : refunds.getOrDefault(order, List.of())) {
      left = left.minus(done);
    }
    if (request.amount().amountMinor() > left.amountMinor()) {
      return new RefundResult.Rejected("EXCEEDS_CAPTURED");
    }

    var made = new RefundResult.Refunded("re_sim_" + (refundsByKey.size() + 1), request.amount());
    refundsByKey.put(new RefundKey(tenantId, request.idempotencyKey()), made);
    refunds.computeIfAbsent(order, k -> new ArrayList<>()).add(request.amount());
    if (behaviour == RefundBehaviour.TIME_OUT_AFTER_SUCCESS) {
      throw new UncheckedIOException(
          new SocketTimeoutException("Simulated timeout after the refund was made"));
    }
    return made;
  }

  @Override
  public synchronized Optional<RefundResult> findRefund(String tenantId, String idempotencyKey) {
    return Optional.ofNullable(refundsByKey.get(new RefundKey(tenantId, idempotencyKey)));
  }
}
