package app.orderloop.fulfilment;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaCommand.BookShipmentCmd;
import app.orderloop.dto.SagaCommand.ReattemptDeliveryCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.ShipmentBookedEvt;
import app.orderloop.dto.SagaEvent.ShipmentFailedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.fulfilment.provider.FulfilmentProvider;
import app.orderloop.fulfilment.provider.FulfilmentProvider.BookingResult;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Answers shipment commands. Runs inside the inbox transaction, so the reply in the outbox commits
 * with the record that the command was handled. It never chooses the saga's next step.
 */
@Component
class FulfilmentHandler {

  private static final Logger log = LoggerFactory.getLogger(FulfilmentHandler.class);

  private final FulfilmentProvider provider;
  private final OutboxWriter outbox;

  FulfilmentHandler(FulfilmentProvider provider, OutboxWriter outbox) {
    this.provider = provider;
    this.outbox = outbox;
  }

  // shortcut: the provider is called inside the inbox transaction, which is safe only because the
  // simulator does no I/O. A real adapter needs intent, call after commit, result (invariant 6).
  void book(BookShipmentCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    SagaEvent reply =
        switch (provider.book(tenant, command.orderId())) {
          case BookingResult.Booked b -> new ShipmentBookedEvt(command.orderId());
          case BookingResult.Failed f -> new ShipmentFailedEvt(command.orderId(), f.reason());
        };
    outbox.append(
        Topics.FULFILMENT_EVENTS, new Envelope(UUID.randomUUID(), tenant, commandEventId, reply));
    log.info(
        "Answered booking for order {} with {}",
        command.orderId(),
        reply.getClass().getSimpleName());
  }

  /** No reply: the carrier reports the outcome later, as a delivery or a failed delivery. */
  void reattempt(ReattemptDeliveryCmd command) {
    provider.requestReattempt(TenantContext.require(), command.orderId());
    log.info("Re-attempt requested for order {}", command.orderId());
  }
}
