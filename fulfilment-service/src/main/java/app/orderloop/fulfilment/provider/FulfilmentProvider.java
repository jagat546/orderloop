package app.orderloop.fulfilment.provider;

/**
 * The fulfilment port. The simulator is the default; real adapters are opt-in through {@code
 * orderloop.fulfilment.provider} (ADR 0007). Carrier events (delivered, failed delivery, RTO) are
 * not answers to a call: the carrier reports them later, and the service publishes each one.
 *
 * <p>shortcut: no automation mode reaches the port yet, so it cannot tell observe mode (wait for
 * the shipment to appear) from action mode (book it). A real adapter needs that first, and must key
 * bookings by order ID: a retried step resends the command under a new event ID.
 */
public interface FulfilmentProvider {

  /** Books the order's shipment with a carrier. */
  BookingResult book(String tenantId, String orderId);

  /** Asks the carrier to try delivering again; the carrier reports the outcome as an event. */
  void requestReattempt(String tenantId, String orderId);

  sealed interface BookingResult {

    record Booked() implements BookingResult {}

    /** {@code reason} is a code such as {@code NO_COURIER}, never free text. */
    record Failed(String reason) implements BookingResult {}
  }
}
