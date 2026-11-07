package app.orderloop.inventory.provider;

import app.orderloop.dto.DemoOrder;
import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaEvent.InventoryRestockedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The default inventory: every order is in stock, except one whose ID ends in {@value
 * #OUT_OF_STOCK_SUFFIX}, so tests and the demo can choose a failure the way a test card number
 * does. Release and restock always succeed, and a repeated command gets the same answer. It does no
 * I/O, which is why it may run inside the inbox transaction (invariant 6).
 *
 * <p>An RTO {@link DemoOrder} is restocked by the merchant's hand six {@code
 * orderloop.simulator.demo-step}s after it is reserved, once the carrier has brought it back.
 *
 * <p>shortcut: the restock is timed, not triggered by the return, because this service never sees
 * carrier events; under heavy load it can land before the saga is restocking and be ignored, and
 * the order then waits for its deadline. Pending restocks are lost on restart.
 */
@Component
@ConditionalOnProperty(
    name = "orderloop.inventory.provider",
    havingValue = "simulator",
    matchIfMissing = true)
public class SimulatedInventory implements InventoryProvider {

  static final String OUT_OF_STOCK_SUFFIX = "-out-of-stock";

  private record Key(String tenantId, String orderId) {}

  private final Set<Key> restocksPending = new HashSet<>();
  private final OutboxWriter outbox;
  private final TransactionTemplate tx;
  private final TaskScheduler scheduler;
  private final Duration demoStep;

  SimulatedInventory(
      OutboxWriter outbox,
      TransactionTemplate tx,
      TaskScheduler scheduler,
      @Value("${orderloop.simulator.demo-step:5s}") Duration demoStep) {
    this.outbox = outbox;
    this.tx = tx;
    this.scheduler = scheduler;
    this.demoStep = demoStep;
  }

  @Override
  public StockResult reserve(String tenantId, String orderId) {
    if (orderId.endsWith(OUT_OF_STOCK_SUFFIX)) {
      return new StockResult.Failed("OUT_OF_STOCK");
    }
    boolean returns =
        DemoOrder.of(orderId).filter(demo -> demo.outcome() == DemoOrder.Outcome.RTO).isPresent();
    if (returns && firstReservation(tenantId, orderId)) {
      scheduler.schedule(
          () -> restockedByHand(tenantId, orderId),
          scheduler.getClock().instant().plus(demoStep.multipliedBy(6)));
    }
    return new StockResult.Done();
  }

  private synchronized boolean firstReservation(String tenantId, String orderId) {
    return restocksPending.add(new Key(tenantId, orderId));
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
