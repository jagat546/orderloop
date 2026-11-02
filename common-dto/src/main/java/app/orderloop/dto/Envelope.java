package app.orderloop.dto;

import java.util.Objects;
import java.util.UUID;

/**
 * A message with the metadata that travels in Kafka headers (see {@link Headers}). The relay maps
 * it to a record keyed by {@link #key()}; the inbox maps it back.
 *
 * @param causationId the event that caused this one, or null for the first
 */
public record Envelope(UUID eventId, String tenantId, UUID causationId, Message payload) {

  public Envelope {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(payload, "payload");
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId is required");
    }
    if (payload.orderId() == null || payload.orderId().isBlank()) {
      throw new IllegalArgumentException("orderId is required");
    }
  }

  public String key() {
    return payload.orderId();
  }

  public String type() {
    return MessageTypes.nameOf(payload.getClass());
  }

  public int schemaVersion() {
    return MessageTypes.SCHEMA_VERSION;
  }
}
