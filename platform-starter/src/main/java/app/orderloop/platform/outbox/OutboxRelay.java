package app.orderloop.platform.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import app.orderloop.dto.Headers;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes committed outbox rows. Each batch is one transaction on the ops connection: take the
 * advisory lock, read in ID order, send and wait for every result, mark what Kafka acknowledged.
 * Another relay that finds the lock taken stays idle. A crash before the commit republishes the
 * batch; consumers' inboxes absorb the duplicates. The only class that sends to Kafka.
 *
 * <p>A row Kafka rejects stays unpublished and is retried, and later rows for the same order wait
 * behind it so that order's events stay in sequence. Rows for other orders still go out.
 */
public final class OutboxRelay implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
  private static final TypeReference<Map<String, String>> HEADER_MAP = new TypeReference<>() {};
  private static final JsonMapper JSON = JsonMapper.builder().build();
  // Every send has a final result by the delivery timeout; this margin only guards against hangs.
  private static final Duration WAIT_MARGIN = Duration.ofSeconds(5);

  /** Dead letters for a topic go to the topic's name plus this suffix. */
  public static final String DEAD_LETTER_SUFFIX = ".dlt";

  private final HikariDataSource opsDataSource;
  private final DefaultKafkaProducerFactory<String, String> producerFactory;
  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;
  private final String lockName;
  private final String select;
  private final String markPublished;
  private final String backlog;
  private final String oldestAge;
  private final int batchSize;
  private final Duration wait;

  OutboxRelay(
      HikariDataSource opsDataSource,
      DefaultKafkaProducerFactory<String, String> producerFactory,
      OutboxProperties properties) {
    this.opsDataSource = opsDataSource;
    this.producerFactory = producerFactory;
    this.jdbc = new JdbcTemplate(opsDataSource);
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(opsDataSource));
    this.kafka = new KafkaTemplate<>(producerFactory);
    this.lockName = lockName(properties.schema());
    this.select =
        "SELECT id, tenant_id, event_id, event_type, schema_version, aggregate_id, topic,"
            + " payload::text AS payload, headers::text AS headers FROM "
            + properties.schema()
            + ".outbox WHERE published_at IS NULL ORDER BY id LIMIT ?";
    this.markPublished =
        "UPDATE " + properties.schema() + ".outbox SET published_at = now() WHERE id = ?";
    this.backlog =
        "SELECT count(*) FROM " + properties.schema() + ".outbox WHERE published_at IS NULL";
    this.oldestAge =
        "SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0) FROM "
            + properties.schema()
            + ".outbox WHERE published_at IS NULL";
    this.batchSize = properties.batchSize();
    this.wait = properties.deliveryTimeout().plus(WAIT_MARGIN);
  }

  /** Rows not yet published, for the backlog gauge. */
  public long backlog() {
    return jdbc.queryForObject(backlog, Long.class);
  }

  /** Seconds the oldest unpublished row has waited, or 0 when none waits; alert past 5 minutes. */
  public double oldestAgeSeconds() {
    return jdbc.queryForObject(oldestAge, Double.class);
  }

  /** The advisory lock name for a schema's relay, shared by every instance of that service. */
  static String lockName(String schema) {
    return "orderloop.outbox." + schema;
  }

  /**
   * Publishes one batch.
   *
   * @return how many rows were published, or empty if another relay holds the lock
   */
  public OptionalInt publishBatch() {
    return tx.execute(
        status -> {
          Boolean locked =
              jdbc.queryForObject(
                  "SELECT pg_try_advisory_xact_lock(hashtext(?))", Boolean.class, lockName);
          if (!Boolean.TRUE.equals(locked)) {
            return OptionalInt.empty();
          }
          List<Row> rows = jdbc.query(select, Row.MAPPER, batchSize);
          List<Long> published = sendAndWait(rows);
          jdbc.batchUpdate(markPublished, published.stream().map(id -> new Object[] {id}).toList());
          return OptionalInt.of(published.size());
        });
  }

  /**
   * Sends a record that could not be handled to {@code <topic>.dlt}, with every original header,
   * including the tenant, plus where it came from and the failure's class. The failure's message is
   * not copied, because it can quote the payload. Throws if Kafka does not acknowledge, so the
   * record is not committed as handled and is tried again.
   */
  public void deadLetter(ConsumerRecord<?, ?> record, Exception failure) {
    var dead =
        new ProducerRecord<String, String>(
            record.topic() + DEAD_LETTER_SUFFIX, text(record.key()), text(record.value()));
    record.headers().forEach(dead.headers()::add);
    dead.headers().add(header("x-dlt-original-topic", record.topic()));
    dead.headers().add(header("x-dlt-original-partition", Integer.toString(record.partition())));
    dead.headers().add(header("x-dlt-original-offset", Long.toString(record.offset())));
    dead.headers().add(header("x-dlt-exception", unwrap(failure).getClass().getName()));
    try {
      kafka.send(dead).get(wait.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while dead-lettering", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Could not dead-letter a record from " + record.topic(), e);
    }
    Header eventId = record.headers().lastHeader(Headers.EVENT_ID);
    log.warn(
        "Dead-lettered event {} from {} after {}",
        eventId == null ? "without an ID" : new String(eventId.value(), UTF_8),
        record.topic(),
        unwrap(failure).getClass().getSimpleName());
  }

  /** Keeps a byte-array key or value as its text instead of a "[B@..." reference. */
  private static String text(Object value) {
    return value instanceof byte[] bytes ? new String(bytes, UTF_8) : Objects.toString(value, null);
  }

  /** Spring Kafka wraps listener failures; the class that matters is the one the listener threw. */
  private static Throwable unwrap(Throwable failure) {
    Throwable cause = failure;
    while (cause instanceof ListenerExecutionFailedException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  int batchSize() {
    return batchSize;
  }

  Map<String, Object> producerConfig() {
    return producerFactory.getConfigurationProperties();
  }

  /** Sends rows in ID order and returns the IDs Kafka acknowledged. */
  private List<Long> sendAndWait(List<Row> rows) {
    Set<String> heldBack = new HashSet<>();
    List<Row> sent = new ArrayList<>();
    List<CompletableFuture<?>> results = new ArrayList<>();
    for (Row row : rows) {
      if (heldBack.contains(row.aggregateId())) {
        continue;
      }
      CompletableFuture<?> result;
      try {
        result = kafka.send(toRecord(row));
      } catch (RuntimeException e) {
        result = CompletableFuture.failedFuture(e);
      }
      if (result.isCompletedExceptionally()) {
        heldBack.add(row.aggregateId());
        log.warn(
            "Kafka rejected outbox event {}; it and its order's later events wait", row.eventId());
        continue;
      }
      sent.add(row);
      results.add(result);
    }

    long deadline = System.nanoTime() + wait.toNanos();
    List<Long> acknowledged = new ArrayList<>();
    for (int i = 0; i < sent.size(); i++) {
      Row row = sent.get(i);
      try {
        results.get(i).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        acknowledged.add(row.id());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for Kafka", e);
      } catch (Exception e) {
        // Left unpublished; the next batch sends it again.
        log.warn("Kafka did not acknowledge outbox event {}; it will be retried", row.eventId());
      }
    }
    return acknowledged;
  }

  private ProducerRecord<String, String> toRecord(Row row) {
    var record = new ProducerRecord<String, String>(row.topic(), row.aggregateId(), row.payload());
    record.headers().add(header(Headers.TENANT_ID, row.tenantId()));
    record.headers().add(header(Headers.EVENT_ID, row.eventId()));
    record.headers().add(header(Headers.EVENT_TYPE, row.eventType()));
    record.headers().add(header(Headers.SCHEMA_VERSION, Integer.toString(row.schemaVersion())));
    JSON.readValue(row.headers(), HEADER_MAP)
        .forEach((name, value) -> record.headers().add(header(name, value)));
    return record;
  }

  private static RecordHeader header(String name, String value) {
    return new RecordHeader(name, value.getBytes(UTF_8));
  }

  @Override
  public void close() {
    producerFactory.destroy();
    opsDataSource.close();
  }

  private record Row(
      long id,
      String tenantId,
      String eventId,
      String eventType,
      int schemaVersion,
      String aggregateId,
      String topic,
      String payload,
      String headers) {

    static final RowMapper<Row> MAPPER =
        (rs, n) ->
            new Row(
                rs.getLong("id"),
                rs.getString("tenant_id"),
                rs.getString("event_id"),
                rs.getString("event_type"),
                rs.getInt("schema_version"),
                rs.getString("aggregate_id"),
                rs.getString("topic"),
                rs.getString("payload"),
                rs.getString("headers"));
  }
}
