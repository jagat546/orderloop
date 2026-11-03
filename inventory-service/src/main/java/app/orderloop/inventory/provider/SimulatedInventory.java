package app.orderloop.inventory.provider;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * The default inventory: every order is in stock, except one whose ID ends in {@value
 * #OUT_OF_STOCK_SUFFIX}, so tests and the demo can choose a failure the way a test card number
 * does. It keeps no state, so a repeated command gets the same answer. It does no I/O, which is why
 * it may run inside the inbox transaction (invariant 6).
 */
@Component
@ConditionalOnProperty(
    name = "orderloop.inventory.provider",
    havingValue = "simulator",
    matchIfMissing = true)
class SimulatedInventory implements InventoryProvider {

  static final String OUT_OF_STOCK_SUFFIX = "-out-of-stock";

  @Override
  public Reservation reserve(String tenantId, String orderId) {
    return orderId.endsWith(OUT_OF_STOCK_SUFFIX)
        ? new Reservation.Failed("OUT_OF_STOCK")
        : new Reservation.Reserved();
  }
}
