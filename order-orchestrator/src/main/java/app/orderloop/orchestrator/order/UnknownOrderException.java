package app.orderloop.orchestrator.order;

/**
 * The bound tenant has no saga for this order. For a reply, that means it names another tenant's
 * order or none; it is retried, then dead-lettered.
 */
public class UnknownOrderException extends RuntimeException {

  UnknownOrderException(String orderId) {
    super("No saga for order " + orderId);
  }
}
