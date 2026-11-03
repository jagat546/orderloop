package app.orderloop.payment;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Money;
import app.orderloop.dto.SagaCommand.RefundPaymentCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.RefundCompletedEvt;
import app.orderloop.dto.SagaEvent.RefundFailedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.payment.provider.PaymentProvider;
import app.orderloop.payment.provider.PaymentProvider.PaymentStatus;
import app.orderloop.payment.provider.PaymentProvider.RefundRequest;
import app.orderloop.payment.provider.PaymentProvider.RefundResult;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Refunds by intent, key, result, reconcile (invariant 6, docs/ARCHITECTURE.md). {@link #request}
 * only records the intent, in the inbox transaction. {@link #process} moves the money, outside any
 * transaction: it settles the amount from the provider's capture, marks the intent SENT, calls the
 * provider under the intent's idempotency key, and saves the result with its reply in one
 * transaction. A call that fails without an answer leaves it UNKNOWN, and the next pass asks the
 * provider what happened before sending anything again.
 */
@Component
class Refunds {

  private static final Logger log = LoggerFactory.getLogger(Refunds.class);

  private static final String SELECT =
      "SELECT order_id, attempt, amount_minor, currency, idempotency_key, status, cause_event_id"
          + " FROM payment.refund_intent WHERE refund_id = ?";
  // Only an open attempt can be settled, so a result is saved and answered once.
  private static final String SETTLE =
      "UPDATE payment.refund_intent SET status = ?, provider_refund_id = ?, failure_reason = ?,"
          + " amount_minor = ?, updated_at = now() WHERE refund_id = ? AND attempt = ?"
          + " AND status IN ('INTENT', 'SENT', 'UNKNOWN')";

  private record Intent(
      String refundId,
      String orderId,
      int attempt,
      Money amount,
      String key,
      String status,
      UUID causeEventId) {}

  private final PaymentProvider provider;
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final OutboxWriter outbox;

  Refunds(
      PaymentProvider provider, JdbcTemplate jdbc, TransactionTemplate tx, OutboxWriter outbox) {
    this.provider = provider;
    this.jdbc = jdbc;
    this.tx = tx;
    this.outbox = outbox;
  }

  /**
   * In the inbox transaction: records the refund to make, and calls nothing. A repeat while it is
   * open does nothing; after success it answers again, and after a failure it is an operator's
   * retry, so it starts a new attempt under a new key.
   */
  void request(RefundPaymentCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    Optional<Intent> existing = load(command.refundId());
    if (existing.isEmpty()) {
      jdbc.update(
          "INSERT INTO payment.refund_intent (tenant_id, refund_id, order_id, attempt,"
              + " amount_minor, currency, idempotency_key, status, cause_event_id)"
              + " VALUES (?, ?, ?, 1, ?, ?, ?, 'INTENT', ?)",
          tenant,
          command.refundId(),
          command.orderId(),
          command.amount().amountMinor(),
          command.amount().currency(),
          key(command.refundId(), 1),
          commandEventId);
      log.info("Refund {} for order {} recorded", command.refundId(), command.orderId());
      return;
    }
    Intent intent = existing.get();
    switch (intent.status()) {
      case "SUCCEEDED" ->
          reply(
              tenant,
              commandEventId,
              new RefundCompletedEvt(intent.orderId(), intent.refundId(), intent.amount()));
      case "FAILED" -> {
        int attempt = intent.attempt() + 1;
        jdbc.update(
            "UPDATE payment.refund_intent SET attempt = ?, idempotency_key = ?, status = 'INTENT',"
                + " amount_minor = ?, currency = ?, provider_refund_id = NULL,"
                + " failure_reason = NULL, cause_event_id = ?, updated_at = now()"
                + " WHERE refund_id = ? AND status = 'FAILED'",
            attempt,
            key(intent.refundId(), attempt),
            command.amount().amountMinor(),
            command.amount().currency(),
            commandEventId,
            intent.refundId());
        log.info("Refund {} retried as attempt {}", intent.refundId(), attempt);
      }
      default -> log.info("Refund {} is already {}", intent.refundId(), intent.status());
    }
  }

  /** Outside any transaction, in the refund's tenant scope: moves it on as far as it can go. */
  void process(String refundId) {
    if (TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException("A provider call must not run inside a transaction");
    }
    Optional<Intent> found = load(refundId);
    if (found.isEmpty()) {
      return;
    }
    Intent intent = found.get();
    switch (intent.status()) {
      case "INTENT" -> send(intent);
      case "SENT", "UNKNOWN" -> reconcile(intent);
      default -> {} // Settled since it was found due.
    }
  }

  private void send(Intent intent) {
    String tenant = TenantContext.require();
    // The provider's record of the capture decides the amount, in its currency (docs/SAGA.md).
    Money amount;
    switch (provider.lookup(tenant, intent.orderId())) {
      case PaymentStatus.Failed failed -> {
        settle(intent, new RefundResult.Rejected(failed.reason()));
        return;
      }
      case PaymentStatus.Found payment when !payment.captured() -> {
        settle(intent, new RefundResult.Rejected("NOTHING_CAPTURED"));
        return;
      }
      case PaymentStatus.Found payment
          when !payment.amount().currency().equals(intent.amount().currency()) -> {
        settle(intent, new RefundResult.Rejected("CURRENCY_MISMATCH"));
        return;
      }
      case PaymentStatus.Found payment ->
          amount =
              intent.amount().amountMinor() > payment.amount().amountMinor()
                  ? payment.amount()
                  : intent.amount();
    }
    // Claim the attempt with its final amount. Only the claimant calls; a key never changes amount.
    int claimed =
        jdbc.update(
            "UPDATE payment.refund_intent SET status = 'SENT', amount_minor = ?, updated_at = now()"
                + " WHERE refund_id = ? AND attempt = ? AND status = 'INTENT'",
            amount.amountMinor(),
            intent.refundId(),
            intent.attempt());
    if (claimed == 1) {
      call(
          new Intent(
              intent.refundId(),
              intent.orderId(),
              intent.attempt(),
              amount,
              intent.key(),
              "SENT",
              intent.causeEventId()));
    }
  }

  /** The outcome is not known: ask before sending again, and send again only under the same key. */
  private void reconcile(Intent intent) {
    Optional<RefundResult> made = provider.findRefund(TenantContext.require(), intent.key());
    if (made.isPresent()) {
      log.info("Refund {} found at the provider; settling it", intent.refundId());
      settle(intent, made.get());
    } else {
      call(intent);
    }
  }

  private void call(Intent intent) {
    RefundResult result;
    try {
      result =
          provider.refund(
              TenantContext.require(),
              new RefundRequest(intent.orderId(), intent.key(), intent.amount()));
    } catch (RuntimeException e) {
      // A timeout is unknown, not failed: the refund may have been made.
      jdbc.update(
          "UPDATE payment.refund_intent SET status = 'UNKNOWN', updated_at = now()"
              + " WHERE refund_id = ? AND attempt = ? AND status IN ('SENT', 'UNKNOWN')",
          intent.refundId(),
          intent.attempt());
      log.warn(
          "Refund {} outcome unknown after {}; will reconcile",
          intent.refundId(),
          e.getClass().getSimpleName());
      return;
    }
    settle(intent, result);
  }

  private void settle(Intent intent, RefundResult result) {
    String tenant = TenantContext.require();
    tx.executeWithoutResult(
        status -> {
          SagaEvent reply;
          int settled;
          switch (result) {
            case RefundResult.Refunded refunded -> {
              settled =
                  jdbc.update(
                      SETTLE,
                      "SUCCEEDED",
                      refunded.providerRefundId(),
                      null,
                      refunded.amount().amountMinor(),
                      intent.refundId(),
                      intent.attempt());
              reply =
                  new RefundCompletedEvt(intent.orderId(), intent.refundId(), refunded.amount());
            }
            case RefundResult.Rejected rejected -> {
              settled =
                  jdbc.update(
                      SETTLE,
                      "FAILED",
                      null,
                      rejected.reason(),
                      intent.amount().amountMinor(),
                      intent.refundId(),
                      intent.attempt());
              reply = new RefundFailedEvt(intent.orderId(), intent.refundId(), rejected.reason());
            }
          }
          if (settled == 1) {
            reply(tenant, intent.causeEventId(), reply);
            log.info(
                "Refund {} for order {} settled: {}",
                intent.refundId(),
                intent.orderId(),
                reply.getClass().getSimpleName());
          }
        });
  }

  private void reply(String tenant, UUID causeEventId, SagaEvent reply) {
    outbox.append(
        Topics.PAYMENT_EVENTS, new Envelope(UUID.randomUUID(), tenant, causeEventId, reply));
  }

  private Optional<Intent> load(String refundId) {
    List<Intent> rows =
        jdbc.query(
            SELECT,
            (row, n) ->
                new Intent(
                    refundId,
                    row.getString("order_id"),
                    row.getInt("attempt"),
                    new Money(row.getLong("amount_minor"), row.getString("currency")),
                    row.getString("idempotency_key"),
                    row.getString("status"),
                    row.getObject("cause_event_id", UUID.class)),
            refundId);
    return rows.stream().findFirst();
  }

  private static String key(String refundId, int attempt) {
    return refundId + ":" + attempt;
  }
}
