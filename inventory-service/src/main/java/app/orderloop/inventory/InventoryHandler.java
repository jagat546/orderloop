package app.orderloop.inventory;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.InventoryFailedEvt;
import app.orderloop.dto.SagaEvent.InventoryReservedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.inventory.provider.InventoryProvider;
import app.orderloop.inventory.provider.InventoryProvider.Reservation;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Answers inventory commands. Runs inside the inbox transaction, so the reply in the outbox commits
 * with the record that the command was handled. It never chooses the saga's next step.
 */
@Component
class InventoryHandler {

  private static final Logger log = LoggerFactory.getLogger(InventoryHandler.class);

  private final InventoryProvider provider;
  private final OutboxWriter outbox;

  InventoryHandler(InventoryProvider provider, OutboxWriter outbox) {
    this.provider = provider;
    this.outbox = outbox;
  }

  // shortcut: the provider is called inside the inbox transaction, which is safe only because the
  // simulator does no I/O. A real adapter needs intent, call after commit, result (invariant 6).
  void reserve(ReserveInventoryCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    SagaEvent reply =
        switch (provider.reserve(tenant, command.orderId())) {
          case Reservation.Reserved r -> new InventoryReservedEvt(command.orderId());
          case Reservation.Failed f -> new InventoryFailedEvt(command.orderId(), f.reason());
        };
    outbox.append(
        Topics.INVENTORY_EVENTS, new Envelope(UUID.randomUUID(), tenant, commandEventId, reply));
    log.info(
        "Answered reservation for order {} with {}",
        command.orderId(),
        reply.getClass().getSimpleName());
  }
}
