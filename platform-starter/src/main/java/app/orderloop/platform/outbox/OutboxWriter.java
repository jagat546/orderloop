package app.orderloop.platform.outbox;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Headers;
import app.orderloop.platform.tenant.TenantContext;
import java.util.Map;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

/**
 * Appends an event to the outbox in the caller's transaction, so it commits or rolls back with the
 * domain change (invariant 4). Nothing else may publish; the relay sends committed rows.
 */
public final class OutboxWriter {

  // A fixed mapper, the same as common-dto's golden test, so no spring.jackson.* setting can
  // change what goes on the wire.
  private static final JsonMapper JSON = JsonMapper.builder().build();

  private final JdbcTemplate jdbc;
  private final String insert;

  OutboxWriter(JdbcTemplate jdbc, String schema) {
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
    Map<String, String> extraHeaders =
        envelope.causationId() == null
            ? Map.of()
            : Map.of(Headers.CAUSATION_ID, envelope.causationId().toString());
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
}
