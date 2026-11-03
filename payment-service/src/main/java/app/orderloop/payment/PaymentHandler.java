package app.orderloop.payment;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaCommand.ProcessPaymentCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.PaymentFailedEvt;
import app.orderloop.dto.SagaEvent.PaymentProcessedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.payment.provider.PaymentProvider;
import app.orderloop.payment.provider.PaymentProvider.PaymentStatus;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Answers payment commands. Runs inside the inbox transaction, so the reply in the outbox commits
 * with the record that the command was handled. It reports what the provider holds and never
 * chooses the saga's next step.
 */
@Component
class PaymentHandler {

  private static final Logger log = LoggerFactory.getLogger(PaymentHandler.class);

  private final PaymentProvider provider;
  private final OutboxWriter outbox;

  PaymentHandler(PaymentProvider provider, OutboxWriter outbox) {
    this.provider = provider;
    this.outbox = outbox;
  }

  // shortcut: the lookup runs inside the inbox transaction, which is safe only because the
  // simulator does no I/O. A real adapter reads over HTTP, so it must run after commit (invariant
  // 6); M2.1 adds the Stripe adapter.
  void verify(ProcessPaymentCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    SagaEvent reply =
        switch (provider.lookup(tenant, command.orderId())) {
          case PaymentStatus.Found found ->
              new PaymentProcessedEvt(
                  command.orderId(), found.method(), found.captured(), found.amount());
          case PaymentStatus.Failed failed ->
              new PaymentFailedEvt(command.orderId(), failed.reason());
        };
    outbox.append(
        Topics.PAYMENT_EVENTS, new Envelope(UUID.randomUUID(), tenant, commandEventId, reply));
    log.info(
        "Answered payment check for order {} with {}",
        command.orderId(),
        reply.getClass().getSimpleName());
  }
}
