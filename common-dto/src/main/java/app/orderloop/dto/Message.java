package app.orderloop.dto;

/**
 * Every Kafka payload. Payloads carry IDs, codes and {@link Money}, never names, phones, emails or
 * addresses. The tenant travels in the {@code x-tenant-id} header, not the payload.
 */
public sealed interface Message permits SagaCommand, SagaEvent {

  /** Topics are keyed by order, so one order's messages stay in order. */
  String orderId();
}
