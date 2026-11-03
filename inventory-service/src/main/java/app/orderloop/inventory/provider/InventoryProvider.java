package app.orderloop.inventory.provider;

/**
 * The inventory port. The simulator is the default; real adapters are opt-in through {@code
 * orderloop.inventory.provider} (ADR 0007).
 */
public interface InventoryProvider {

  /** Holds the order's stock, or confirms it is held in observe mode. */
  Reservation reserve(String tenantId, String orderId);

  sealed interface Reservation {

    record Reserved() implements Reservation {}

    /** {@code reason} is a code such as {@code OUT_OF_STOCK}, never free text from a customer. */
    record Failed(String reason) implements Reservation {}
  }
}
