package app.orderloop.platform.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaEvent.InventoryFailedEvt;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The outbox and relay against real PostgreSQL and Kafka. The scheduled relay is off, so each test
 * drives batches itself and nothing races it.
 */
@Testcontainers
@SpringBootTest(
    classes = OutboxIT.App.class,
    webEnvironment = WebEnvironment.NONE,
    properties = {
      "orderloop.outbox.schema=orchestrator",
      "orderloop.outbox.ops-username=orderloop_ops",
      "orderloop.outbox.relay-enabled=false"
    })
class OutboxIT {

  private static final String TOPIC = "orderloop.test.commands";

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES);
    registry.add("orderloop.outbox.ops-password", () -> TestDatabase.OPS_PASSWORD);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @BeforeAll
  static void createTopic() throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
    }
  }

  @Autowired private OutboxWriter outbox;
  @Autowired private OutboxRelay relay;
  @Autowired private TransactionTemplate tx;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private Tracer tracer;

  @Test
  void aRollbackPublishesNothing() {
    Envelope rolledBack = envelope("t_a", "o-rolled-back");
    UUID probe = UUID.randomUUID();
    TenantContext.callAs(
        "t_a",
        () ->
            tx.execute(
                status -> {
                  jdbc.update(
                      "INSERT INTO orchestrator.tenant_probe (tenant_id, probe_id, label)"
                          + " VALUES ('t_a', ?, 'domain change')",
                      probe);
                  outbox.append(TOPIC, rolledBack);
                  status.setRollbackOnly();
                  return null;
                }));
    // A committed event after it: once this one arrives, the rolled-back one had its chance.
    Envelope marker = envelope("t_a", "o-marker");
    commit(marker);

    drainRelay();
    List<String> published = eventIdsOnTopicUntil(marker.eventId());

    assertThat(published).contains(marker.eventId().toString());
    assertThat(published).doesNotContain(rolledBack.eventId().toString());
    assertThat(allRows(rolledBack.eventId())).isZero();
    Integer domainRows =
        TenantContext.callAs(
            "t_a",
            () ->
                tx.execute(
                    status ->
                        jdbc.queryForObject(
                            "SELECT count(*) FROM orchestrator.tenant_probe WHERE probe_id = ?",
                            Integer.class,
                            probe)));
    assertThat(domainRows).isZero();
  }

  @Test
  void aCommittedEventIsPublishedEvenIfTheWriterStopsBeforeTheRelayRuns() {
    Envelope event = envelope("t_a", "o-1");
    commit(event);
    // The writer's work ends at commit. Nothing publishes yet; the row alone carries the event.
    assertThat(unpublishedRows(event.eventId())).isEqualTo(1);

    drainRelay();
    ConsumerRecord<String, String> record = recordOnTopic(event.eventId());

    assertThat(record.key()).isEqualTo("o-1");
    assertThat(record.value()).isEqualTo("{\"orderId\":\"o-1\"}");
    assertThat(header(record, "x-tenant-id")).isEqualTo("t_a");
    assertThat(header(record, "x-event-id")).isEqualTo(event.eventId().toString());
    assertThat(header(record, "x-event-type")).isEqualTo("ReserveInventoryCmd");
    assertThat(header(record, "x-schema-version")).isEqualTo("1");
    assertThat(unpublishedRows(event.eventId())).isZero();
  }

  @Test
  void aSecondRelayStaysIdleWhileTheFirstHoldsTheLock() throws Exception {
    Envelope event = envelope("t_a", "o-locked");
    commit(event);

    OptionalInt whileHeld;
    try (Connection first = opsDataSource().getConnection()) {
      first.setAutoCommit(false);
      // Stand in for a relay part-way through its batch: it holds the transaction-scoped lock.
      try (PreparedStatement lock =
          first.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
        lock.setString(1, OutboxRelay.lockName("orchestrator"));
        lock.execute();
      }
      whileHeld = relay.publishBatch();
      assertThat(unpublishedRows(event.eventId())).isEqualTo(1);
      first.commit();
    }
    OptionalInt afterRelease = relay.publishBatch();

    assertThat(whileHeld).isEmpty();
    assertThat(afterRelease).isPresent();
    assertThat(afterRelease.getAsInt()).isPositive();
    assertThat(recordOnTopic(event.eventId())).isNotNull();
  }

  @Test
  void aRowKafkaRejectsDoesNotHoldBackOtherOrders() {
    // A payload larger than the producer's 1 MB request limit, so Kafka rejects it on every
    // attempt. The key stays short, so the same order's next event would be accepted if sent.
    String order = "o-poison-" + UUID.randomUUID();
    Envelope poison =
        new Envelope(
            UUID.randomUUID(), "t_a", null, new InventoryFailedEvt(order, "x".repeat(2_000_000)));
    Envelope behindPoisonOnTheSameOrder =
        new Envelope(UUID.randomUUID(), "t_a", null, new ReserveInventoryCmd(order));
    Envelope otherOrder = envelope("t_a", "o-healthy");
    commit(poison);
    commit(behindPoisonOnTheSameOrder);
    commit(otherOrder);

    OptionalInt published = relay.publishBatch();

    assertThat(published).isPresent();
    assertThat(recordOnTopic(otherOrder.eventId())).isNotNull();
    assertThat(unpublishedRows(otherOrder.eventId())).isZero();
    assertThat(unpublishedRows(poison.eventId())).isEqualTo(1);
    // Held back, not sent: sending it would put the order's events out of sequence.
    assertThat(unpublishedRows(behindPoisonOnTheSameOrder.eventId())).isEqualTo(1);
  }

  @Test
  void theRelayProducerIsIdempotentWithBoundedTimeouts() {
    assertThat(relay.producerConfig())
        .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        .containsEntry(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE)
        .containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
        .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000)
        .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
        .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000L);
  }

  @Test
  void opsSessionsAbandonATransactionLeftIdle() {
    // A frozen relay must not hold the advisory lock until TCP notices, which can take hours.
    String timeout =
        new JdbcTemplate(opsDataSource())
            .queryForObject("SHOW idle_in_transaction_session_timeout", String.class);

    assertThat(timeout).isEqualTo("1min");
  }

  @Test
  void theWriterRefusesToWriteOutsideATransaction() {
    assertThatThrownBy(
            () ->
                TenantContext.callAs(
                    "t_a",
                    () -> {
                      outbox.append(TOPIC, envelope("t_a", "o-no-tx"));
                      return null;
                    }))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void theWriterRefusesAnEnvelopeForAnotherTenant() {
    assertThatThrownBy(
            () ->
                TenantContext.callAs(
                    "t_a",
                    () ->
                        tx.execute(
                            status -> {
                              outbox.append(TOPIC, envelope("t_b", "o-other"));
                              return null;
                            })))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void anEventAppendedInsideASpanCarriesItsTraceToKafka() {
    Envelope event = envelope("t_a", "o-traced");
    Span span = tracer.nextSpan().name("create order").start();
    try (Tracer.SpanInScope inScope = tracer.withSpan(span)) {
      commit(event);
    } finally {
      span.end();
    }

    drainRelay();

    assertThat(header(recordOnTopic(event.eventId()), "traceparent"))
        .matches("00-" + span.context().traceId() + "-" + span.context().spanId() + "-[0-9a-f]{2}");
  }

  @Test
  void anEventAppendedOutsideAnySpanCarriesNoTrace() {
    Envelope event = envelope("t_a", "o-untraced");
    commit(event);

    drainRelay();

    assertThat(recordOnTopic(event.eventId()).headers().lastHeader("traceparent")).isNull();
  }

  private void commit(Envelope envelope) {
    TenantContext.callAs(
        envelope.tenantId(),
        () ->
            tx.execute(
                status -> {
                  outbox.append(TOPIC, envelope);
                  return null;
                }));
  }

  private void drainRelay() {
    while (relay.publishBatch().orElseThrow() > 0) {
      // Keep going until a batch finds nothing to publish.
    }
  }

  private static Envelope envelope(String tenant, String orderId) {
    return new Envelope(UUID.randomUUID(), tenant, null, new ReserveInventoryCmd(orderId));
  }

  private static PGSimpleDataSource opsDataSource() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(POSTGRES.getJdbcUrl());
    dataSource.setUser("orderloop_ops");
    dataSource.setPassword(TestDatabase.OPS_PASSWORD);
    return dataSource;
  }

  private static int unpublishedRows(UUID eventId) {
    return new JdbcTemplate(opsDataSource())
        .queryForObject(
            "SELECT count(*) FROM orchestrator.outbox WHERE event_id = ? AND published_at IS NULL",
            Integer.class,
            eventId);
  }

  private static int allRows(UUID eventId) {
    return new JdbcTemplate(opsDataSource())
        .queryForObject(
            "SELECT count(*) FROM orchestrator.outbox WHERE event_id = ?", Integer.class, eventId);
  }

  private static ConsumerRecord<String, String> recordOnTopic(UUID eventId) {
    List<ConsumerRecord<String, String>> seen = recordsUntil(eventId);
    return seen.stream()
        .filter(r -> eventId.toString().equals(header(r, "x-event-id")))
        .findFirst()
        .orElseThrow();
  }

  private static List<String> eventIdsOnTopicUntil(UUID eventId) {
    return recordsUntil(eventId).stream().map(r -> header(r, "x-event-id")).toList();
  }

  /** Reads the topic from the start until the given event appears. */
  private static List<ConsumerRecord<String, String>> recordsUntil(UUID eventId) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(TOPIC).stream()
              .map(p -> new TopicPartition(TOPIC, p.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> {
                consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
                return seen.stream()
                    .anyMatch(r -> eventId.toString().equals(header(r, "x-event-id")));
              });
    }
    return seen;
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), UTF_8);
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class App {}
}
