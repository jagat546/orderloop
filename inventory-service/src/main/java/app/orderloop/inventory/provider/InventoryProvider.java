package app.orderloop.inventory.provider;

/**
 * The inventory port. The simulator is the default; real adapters are opt-in through {@code
 * orderloop.inventory.provider} (ADR 0007).
 *
 * <p>shortcut: no automation mode reaches the port yet, so it cannot tell observe mode (confirm
 * only) from action mode (change stock). A real adapter needs that first, and must key its writes
 * by order ID: a retried step resends the command under a new event ID, which the inbox does not
 * catch.
 */
public interface InventoryProvider {

  /** Holds the order's stock. */
  StockResult reserve(String tenantId, String orderId);

  /** Gives back the order's held stock, as a compensation. */
  StockResult release(String tenantId, String orderId);

  /** Puts a returned order's stock back on sale, after a human approved it. */
  StockResult restock(String tenantId, String orderId);

  sealed interface StockResult {

    record Done() implements StockResult {}

    /** {@code reason} is a code such as {@code OUT_OF_STOCK}, never free text from a customer. */
    record Failed(String reason) implements StockResult {}
  }
}
