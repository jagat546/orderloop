package app.orderloop.platform.inbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import app.orderloop.dto.Headers;
import app.orderloop.dto.Message;
import app.orderloop.dto.MessageTypes;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.LogicalType;

/**
 * Every {@code @KafkaListener} delegates here (invariant 5). In order: check the headers, bind the
 * tenant from {@code x-tenant-id}, then in one transaction record the event in {@code
 * processed_event} and run the handler. A redelivered event finds its row and does nothing. If the
 * handler fails, the inbox row rolls back with its changes, so a retry runs it again.
 */
public final class InboxConsumer {

  private static final Logger log = LoggerFactory.getLogger(InboxConsumer.class);

  /** The order a handler works on, copied to MDC for logs; the Kafka key is the order ID too. */
  static final String ORDER_ID = "orderId";

  /**
   * Ignores unknown fields, so a producer may add an optional field before consumers know it. Never
   * bends a number: 12.9, "500" or 1e3 for an integer is refused, not truncated or converted, so a
   * malformed amount cannot become a different amount (invariant 7).
   */
  private static final JsonMapper JSON =
      JsonMapper.builder()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
          .withCoercionConfig(
              LogicalType.Integer,
              config ->
                  config
                      .setCoercion(CoercionInputShape.String, CoercionAction.Fail)
                      .setCoercion(CoercionInputShape.Float, CoercionAction.Fail))
          .build();

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final String insert;
  private final Tracer tracer;
  private final Propagator propagator;

  InboxConsumer(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      String schema,
      Tracer tracer,
      Propagator propagator) {
    this.tracer = tracer;
    this.propagator = propagator;
    this.jdbc = jdbc;
    this.tx = tx;
    this.insert =
        "INSERT INTO "
            + schema
            + ".processed_event (tenant_id, consumer, event_id) VALUES (?, ?, ?)"
            + " ON CONFLICT DO NOTHING";
  }

  /**
   * Handles a record once per tenant, consumer group and event ID. {@code type} may be a sealed
   * parent such as {@code SagaCommand}: the concrete type comes from {@code x-event-type}, and the
   * handler pattern-matches over it. Call it from the listener thread, outside any transaction.
   *
   * @throws InvalidMessageException if the record can never be handled; it is dead-lettered
   */
  public <T extends Message> void handle(
      ConsumerRecord<String, String> record, Class<T> type, Consumer<? super T> handler) {
    if (TransactionSynchronizationManager.isActualTransactionActive()) {
      // Its connection was checked out with no tenant bound, so RLS would refuse every write.
      throw new IllegalStateException("InboxConsumer.handle must not run inside a transaction");
    }
    String consumer = KafkaUtils.getConsumerGroupId();
    if (consumer == null) {
      throw new IllegalStateException("InboxConsumer.handle must run on a listener thread");
    }
    String tenant = header(record, Headers.TENANT_ID);
    if (!TenantContext.isValidTenantId(tenant)) {
      // The format only: an unknown but well-formed tenant is caught once tenants are stored (M2).
      throw new InvalidMessageException("Missing or malformed " + Headers.TENANT_ID);
    }
    UUID eventId = eventId(record);
    T message = read(record, type);

    // Continues the producer's trace, so one trace follows an order across services.
    Span span =
        propagator
            .extract(record, InboxConsumer::header)
            .name("handle " + header(record, Headers.EVENT_TYPE))
            .kind(Span.Kind.CONSUMER)
            // IDs only (invariant 9); they let Grafana find every trace for an order.
            .tag("order.id", message.orderId())
            .tag("tenant.id", tenant)
            .start();
    MDC.put(ORDER_ID, message.orderId());
    try (Tracer.SpanInScope inScope = tracer.withSpan(span)) {
      TenantContext.callAs(
          tenant,
          () ->
              tx.execute(
                  status -> {
                    if (jdbc.update(insert, tenant, consumer, eventId) == 0) {
                      log.debug("Skipped duplicate event {} for {}", eventId, consumer);
                      return null;
                    }
                    handler.accept(message);
                    return null;
                  }));
    } catch (RuntimeException e) {
      span.error(e);
      throw e;
    } finally {
      span.end();
      MDC.remove(ORDER_ID);
    }
  }

  private static UUID eventId(ConsumerRecord<String, String> record) {
    String value = header(record, Headers.EVENT_ID);
    if (value == null) {
      throw new InvalidMessageException("Missing " + Headers.EVENT_ID);
    }
    try {
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new InvalidMessageException(Headers.EVENT_ID + " is not a UUID", e);
    }
  }

  private static <T extends Message> T read(ConsumerRecord<String, String> record, Class<T> type) {
    String name = header(record, Headers.EVENT_TYPE);
    Class<? extends Message> concrete;
    try {
      concrete = MessageTypes.classOf(name);
    } catch (IllegalArgumentException e) {
      throw new InvalidMessageException("Unknown " + Headers.EVENT_TYPE + " on " + record.topic());
    }
    if (!type.isAssignableFrom(concrete)) {
      throw new InvalidMessageException(
          name + " is not a " + type.getSimpleName() + " on " + record.topic());
    }
    if (record.value() == null) {
      throw new InvalidMessageException("Empty payload on " + record.topic());
    }
    Message message;
    try {
      message = JSON.readValue(record.value(), concrete);
    } catch (JacksonException e) {
      // The exception text can quote the payload, so only the type is reported.
      throw new InvalidMessageException("Unreadable " + name + " payload", e);
    }
    if (message.orderId() == null || message.orderId().isBlank()) {
      throw new InvalidMessageException(name + " has no orderId");
    }
    return type.cast(message);
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), UTF_8);
  }
}
