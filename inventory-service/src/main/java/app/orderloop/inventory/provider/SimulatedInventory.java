package app.orderloop.inventory.provider;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaEvent.InventoryRestockedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The default inventory: every order is in stock, except one whose ID ends in {@value
 * #OUT_OF_STOCK_SUFFIX}, so tests and the demo can choose a failure the way a test card number
 * does. Release and restock always succeed. It keeps no state, so a repeated command gets the same
 * answer. It does no I/O, which is why it may run inside the inbox transaction (invariant 6).
 */
@Component
@ConditionalOnProperty(
    name = "orderloop.inventory.provider",
    havingValue = "simulator",
    matchIfMissing = true)
public class SimulatedInventory implements InventoryProvider {

  static final String OUT_OF_STOCK_SUFFIX = "-out-of-stock";

  private final OutboxWriter outbox;
  private final TransactionTemplate tx;

  SimulatedInventory(OutboxWriter outbox, TransactionTemplate tx) {
    this.outbox = outbox;
    this.tx = tx;
  }

  @Override
  public StockResult reserve(String tenantId, String orderId) {
    return orderId.endsWith(OUT_OF_STOCK_SUFFIX)
        ? new StockResult.Failed("OUT_OF_STOCK")
        : new StockResult.Done();
  }

  /** Always gives the stock back; a repeat is harmless because nothing is held. */
  @Override
  public StockResult release(String tenantId, String orderId) {
    return new StockResult.Done();
  }

  /** Always puts returned stock back on sale. */
  @Override
  public StockResult restock(String tenantId, String orderId) {
    return new StockResult.Done();
  }

  /**
   * Reports that the merchant put an order's returned stock back by hand, with no command from us,
   * as the store would. It goes out through the service's outbox in the tenant's scope.
   */
  public void restockedByHand(String tenantId, String orderId) {
    var event = new InventoryRestockedEvt(orderId);
    TenantContext.callAs(
        tenantId,
        () ->
            tx.execute(
                status -> {
                  outbox.append(
                      Topics.INVENTORY_EVENTS,
                      new Envelope(UUID.randomUUID(), tenantId, null, event));
                  return null;
                }));
  }
}
