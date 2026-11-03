package app.orderloop.platform.outbox;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Headers;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

/**
 * Appends an event to the outbox in the caller's transaction, so it commits or rolls back with the
 * domain change (invariant 4). Nothing else may publish; the relay sends committed rows. The
 * current span's W3C {@code traceparent} goes with the row, so the trace continues in whichever
 * service handles it, however late the relay sends it.
 */
public final class OutboxWriter {

  // A fixed mapper, the same as common-dto's golden test, so no spring.jackson.* setting can
  // change what goes on the wire.
  private static final JsonMapper JSON = JsonMapper.builder().build();

  private final JdbcTemplate jdbc;
  private final String insert;
  private final Tracer tracer;
  private final Propagator propagator;

  OutboxWriter(JdbcTemplate jdbc, String schema, Tracer tracer, Propagator propagator) {
    this.tracer = tracer;
    this.propagator = propagator;
    this.jdbc = jdbc;
    this.insert =
        "INSERT INTO "
            + schema
            + ".outbox (tenant_id, event_id, event_type, schema_version, aggregate_id, topic,"
            + " payload, headers) VALUES (?, ?, ?, ?, ?, ?, ?::json, ?::jsonb)";
  }

  public void append(String topic, Envelope envelope) {
    Objects.requireNonNull(topic, "topic");
    Objects.requireNonNull(envelope, "envelope");
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "An outbox row must commit with its domain change; append inside that transaction");
    }
    String tenant = TenantContext.require();
    if (!tenant.equals(envelope.tenantId())) {
      throw new IllegalStateException(
          "The envelope's tenant is not the tenant bound to this scope");
    }
    Map<String, String> extraHeaders = new HashMap<>();
    if (envelope.causationId() != null) {
      extraHeaders.put(Headers.CAUSATION_ID, envelope.causationId().toString());
    }
    traceparent().ifPresent(value -> extraHeaders.put(Headers.TRACEPARENT, value));
    jdbc.update(
        insert,
        tenant,
        envelope.eventId(),
        envelope.type(),
        envelope.schemaVersion(),
        envelope.key(),
        topic,
        JSON.writeValueAsString(envelope.payload()),
        JSON.writeValueAsString(extraHeaders));
  }

  /** Only traceparent: no baggage, which could carry customer data onto the wire (invariant 9). */
  private Optional<String> traceparent() {
    Span span = tracer.currentSpan();
    if (span == null) {
      return Optional.empty();
    }
    Map<String, String> carrier = new HashMap<>();
    propagator.inject(span.context(), carrier, Map::put);
    return Optional.ofNullable(carrier.get(Headers.TRACEPARENT));
  }
}
