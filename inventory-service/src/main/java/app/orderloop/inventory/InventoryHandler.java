package app.orderloop.inventory;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaCommand.ReleaseInventoryCmd;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaCommand.RestockInventoryCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.InventoryFailedEvt;
import app.orderloop.dto.SagaEvent.InventoryReleasedEvt;
import app.orderloop.dto.SagaEvent.InventoryReservedEvt;
import app.orderloop.dto.SagaEvent.InventoryRestockedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.inventory.provider.InventoryProvider;
import app.orderloop.inventory.provider.InventoryProvider.StockResult;
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
    reply(
        tenant,
        commandEventId,
        switch (provider.reserve(tenant, command.orderId())) {
          case StockResult.Done d -> new InventoryReservedEvt(command.orderId());
          case StockResult.Failed f -> new InventoryFailedEvt(command.orderId(), f.reason());
        });
  }

  /** A compensation: a failure goes back as InventoryFailedEvt, and the saga parks for a human. */
  void release(ReleaseInventoryCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    reply(
        tenant,
        commandEventId,
        switch (provider.release(tenant, command.orderId())) {
          case StockResult.Done d -> new InventoryReleasedEvt(command.orderId());
          case StockResult.Failed f -> new InventoryFailedEvt(command.orderId(), f.reason());
        });
  }

  /** Sent only after a human approved the restock; a failure parks the saga for a human. */
  void restock(RestockInventoryCmd command, UUID commandEventId) {
    String tenant = TenantContext.require();
    reply(
        tenant,
        commandEventId,
        switch (provider.restock(tenant, command.orderId())) {
          case StockResult.Done d -> new InventoryRestockedEvt(command.orderId());
          case StockResult.Failed f -> new InventoryFailedEvt(command.orderId(), f.reason());
        });
  }

  private void reply(String tenant, UUID commandEventId, SagaEvent reply) {
    outbox.append(
        Topics.INVENTORY_EVENTS, new Envelope(UUID.randomUUID(), tenant, commandEventId, reply));
    log.info("Answered order {} with {}", reply.orderId(), reply.getClass().getSimpleName());
  }
}
