package app.orderloop.platform.inbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.SagaCommand;
import app.orderloop.dto.SagaCommand.ReleaseInventoryCmd;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The inbox and dead-letter handling against real PostgreSQL and Kafka. */
@Testcontainers
@SpringBootTest(
    classes = InboxIT.App.class,
    webEnvironment = WebEnvironment.NONE,
    properties = {
      "orderloop.outbox.schema=orchestrator",
      "orderloop.outbox.relay-enabled=false",
      "orderloop.inbox.max-retries=2",
      "orderloop.inbox.initial-interval=50ms",
      "spring.kafka.consumer.auto-offset-reset=earliest"
    })
class InboxIT {

  static final String TOPIC = "orderloop.test.inventory-commands";
  static final String DEAD_LETTERS = TOPIC + ".dlt";

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES);
    registry.add("orderloop.outbox.ops-password", () -> TestDatabase.OPS_PASSWORD);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @BeforeAll
  static void createTopics() throws Exception {
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(
              List.of(new NewTopic(TOPIC, 3, (short) 1), new NewTopic(DEAD_LETTERS, 3, (short) 1)))
          .all()
          .get();
    }
  }

  @Autowired private SagaHandler saga;
  @Autowired private ProjectionHandler projection;
  @Autowired private InboxConsumer inbox;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private TransactionTemplate tx;
  @Autowired private MeterRegistry meters;

  @Test
  void aDuplicateChangesStateOnce() {
    String order = "o-dup-" + UUID.randomUUID();
    UUID eventId = UUID.randomUUID();

    send(reserve("t_a", eventId, order));
    send(reserve("t_a", eventId, order));
    // A later event on the same order and partition: once it is handled, both copies were seen.
    UUID markerId = UUID.randomUUID();
    send(reserve("t_a", markerId, order));

    await().atMost(Duration.ofSeconds(30)).until(() -> saga.calls(markerId) > 0);
    assertThat(saga.calls(eventId)).isEqualTo(1);
    assertThat(stateRows("t_a", order)).isEqualTo(2); // the event once, plus the marker
  }

  @Test
  void theHandlerRunsInTheProducersTraceWithTenantAndOrderInItsLogs() {
    String order = "o-traced-" + UUID.randomUUID();
    UUID eventId = UUID.randomUUID();
    String traceId = UUID.randomUUID().toString().replace("-", "");
    ProducerRecord<String, String> traced = reserve("t_a", eventId, order);
    traced.headers().add("traceparent", ("00-" + traceId + "-00f067aa0ba902b7-01").getBytes(UTF_8));

    send(traced);

    await().atMost(Duration.ofSeconds(30)).until(() -> saga.calls(eventId) > 0);
    assertThat(saga.logContext(eventId)).isEqualTo(traceId + "|t_a|" + order);
  }

  @Test
  void eachConsumerGroupHandlesAnEventOnce() {
    UUID eventId = UUID.randomUUID();

    send(reserve("t_a", eventId, "o-two-groups-" + UUID.randomUUID()));

    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> saga.calls(eventId) > 0 && projection.calls(eventId) > 0);
    assertThat(saga.calls(eventId)).isEqualTo(1);
    assertThat(projection.calls(eventId)).isEqualTo(1);
  }

  @Test
  void anotherCommandOnTheSameTopicIsHandledToo() {
    String order = "o-release-" + UUID.randomUUID();
    UUID eventId = UUID.randomUUID();

    send(record("t_a", eventId, order, "ReleaseInventoryCmd", "{\"orderId\":\"" + order + "\"}"));

    await().atMost(Duration.ofSeconds(30)).until(() -> saga.calls(eventId) > 0);
    assertThat(stateRows("t_a", "released " + order)).isEqualTo(1);
  }

  @Test
  void aPoisonMessageReachesTheDeadLetterTopicWithItsTenantAfterBoundedRetries() {
    String order = "poison-" + UUID.randomUUID();
    UUID eventId = UUID.randomUUID();

    send(reserve("t_a", eventId, order));

    ConsumerRecord<String, String> dead = deadLetter(eventId);
    assertThat(header(dead, "x-tenant-id")).isEqualTo("t_a");
    assertThat(header(dead, "x-event-type")).isEqualTo("ReserveInventoryCmd");
    assertThat(header(dead, "x-dlt-original-topic")).isEqualTo(TOPIC);
    assertThat(header(dead, "x-dlt-exception")).isEqualTo(IllegalStateException.class.getName());
    assertThat(dead.key()).isEqualTo(order);
    assertThat(dead.value()).isEqualTo("{\"orderId\":\"" + order + "\"}");
    // One attempt plus two retries, each rolled back with the inbox row it wrote.
    assertThat(saga.calls(eventId)).isEqualTo(3);
    assertThat(stateRows("t_a", order)).isZero();
  }

  @Test
  void aDeadLetterIsCountedForItsTopic() {
    double before = deadLetters();
    UUID eventId = UUID.randomUUID();

    send(reserve("t_a", eventId, "poison-" + UUID.randomUUID()));

    deadLetter(eventId);
    await().atMost(Duration.ofSeconds(10)).until(() -> deadLetters() == before + 1);
  }

  private double deadLetters() {
    var counter = meters.find("orderloop.inbox.dead.letters").tag("topic", TOPIC).counter();
    return counter == null ? 0 : counter.count();
  }

  @Test
  void aMissingTenantHeaderIsRejectedWithoutReachingTheHandler() {
    UUID eventId = UUID.randomUUID();

    send(reserve(null, eventId, "o-no-tenant-" + UUID.randomUUID()));

    assertInvalid(eventId);
    assertThat(header(deadLetter(eventId), "x-tenant-id")).isNull();
  }

  @Test
  void aMalformedTenantIdIsRejected() {
    UUID eventId = UUID.randomUUID();

    send(reserve(" t_a", eventId, "o-spaced-tenant-" + UUID.randomUUID()));

    assertInvalid(eventId);
  }

  @Test
  void anEventOfAnotherKindIsRejected() {
    UUID eventId = UUID.randomUUID();

    send(record("t_a", eventId, "o-event", "PaymentFailedEvt", "{\"orderId\":\"o-event\"}"));

    assertInvalid(eventId);
  }

  @Test
  void anUnknownTypeIsRejected() {
    UUID eventId = UUID.randomUUID();

    send(record("t_a", eventId, "o-unknown", "NoSuchCmd", "{\"orderId\":\"o-unknown\"}"));

    assertInvalid(eventId);
  }

  @Test
  void aFractionalAmountIsRejectedRatherThanTruncated() {
    UUID eventId = UUID.randomUUID();
    String refund =
        "{\"orderId\":\"o-refund\",\"refundId\":\"r-1\","
            + "\"amount\":{\"amountMinor\":12.9,\"currency\":\"USD\"}}";

    send(record("t_a", eventId, "o-refund", "RefundPaymentCmd", refund));

    assertInvalid(eventId);
  }

  @Test
  void aQuotedAmountIsRejected() {
    UUID eventId = UUID.randomUUID();
    String refund =
        "{\"orderId\":\"o-refund\",\"refundId\":\"r-1\","
            + "\"amount\":{\"amountMinor\":\"500\",\"currency\":\"USD\"}}";

    send(record("t_a", eventId, "o-refund", "RefundPaymentCmd", refund));

    assertInvalid(eventId);
  }

  @Test
  void aMissingOrderIdIsRejected() {
    UUID eventId = UUID.randomUUID();

    send(record("t_a", eventId, "o-missing", "ReserveInventoryCmd", "{}"));

    assertInvalid(eventId);
  }

  @Test
  void anUnknownFieldIsIgnoredSoAddingAFieldStaysCompatible() {
    String order = "o-newer-" + UUID.randomUUID();
    UUID eventId = UUID.randomUUID();

    send(
        record(
            "t_a",
            eventId,
            order,
            "ReserveInventoryCmd",
            "{\"orderId\":\"" + order + "\",\"addedLater\":1}"));

    await().atMost(Duration.ofSeconds(30)).until(() -> saga.calls(eventId) > 0);
    assertThat(stateRows("t_a", order)).isEqualTo(1);
  }

  @Test
  void handlingInsideAnOpenTransactionIsRefused() {
    // The transaction's connection was checked out with no tenant, so RLS would refuse every write.
    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(TOPIC, 0, 0, "o-1", "{\"orderId\":\"o-1\"}");
    reserve("t_a", UUID.randomUUID(), "o-1").headers().forEach(record.headers()::add);

    assertThatThrownBy(
            () ->
                tx.execute(
                    status -> {
                      inbox.handle(record, SagaCommand.class, command -> {});
                      return null;
                    }))
        .isInstanceOf(IllegalStateException.class);
  }

  private void assertInvalid(UUID eventId) {
    assertThat(header(deadLetter(eventId), "x-dlt-exception"))
        .isEqualTo(InvalidMessageException.class.getName());
    assertThat(saga.calls(eventId)).isZero();
  }

  private int stateRows(String tenant, String label) {
    Integer rows =
        TenantContext.callAs(
            tenant,
            () ->
                tx.execute(
                    status ->
                        jdbc.queryForObject(
                            "SELECT count(*) FROM orchestrator.tenant_probe WHERE label = ?",
                            Integer.class,
                            label)));
    return rows;
  }

  private static ProducerRecord<String, String> reserve(String tenant, UUID eventId, String order) {
    return record(tenant, eventId, order, "ReserveInventoryCmd", "{\"orderId\":\"" + order + "\"}");
  }

  private static ProducerRecord<String, String> record(
      String tenant, UUID eventId, String key, String type, String payload) {
    var record = new ProducerRecord<String, String>(TOPIC, key, payload);
    if (tenant != null) {
      record.headers().add("x-tenant-id", tenant.getBytes(UTF_8));
    }
    record.headers().add("x-event-id", eventId.toString().getBytes(UTF_8));
    record.headers().add("x-event-type", type.getBytes(UTF_8));
    record.headers().add("x-schema-version", "1".getBytes(UTF_8));
    return record;
  }

  private static void send(ProducerRecord<String, String> record) {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (var producer = new KafkaProducer<String, String>(props)) {
      producer.send(record).get();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static ConsumerRecord<String, String> deadLetter(UUID eventId) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(DEAD_LETTERS).stream()
              .map(p -> new TopicPartition(DEAD_LETTERS, p.partition()))
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
    return seen.stream()
        .filter(r -> eventId.toString().equals(header(r, "x-event-id")))
        .findFirst()
        .orElseThrow();
  }

  static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), UTF_8);
  }

  /** Counts handler runs per event ID. */
  abstract static class CountingHandler {

    private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

    void count(ConsumerRecord<String, String> record) {
      calls
          .computeIfAbsent(header(record, "x-event-id"), id -> new AtomicInteger())
          .incrementAndGet();
    }

    int calls(UUID eventId) {
      AtomicInteger count = calls.get(eventId.toString());
      return count == null ? 0 : count.get();
    }
  }

  /** A listener as services will write them: the sealed type in, a pattern match out. */
  static class SagaHandler extends CountingHandler {

    private final Map<UUID, String> logContext = new ConcurrentHashMap<>();

    String logContext(UUID eventId) {
      return logContext.get(eventId);
    }

    private final InboxConsumer inbox;
    private final JdbcTemplate jdbc;

    SagaHandler(InboxConsumer inbox, JdbcTemplate jdbc) {
      this.inbox = inbox;
      this.jdbc = jdbc;
    }

    @KafkaListener(topics = TOPIC, groupId = "inbox-it-saga")
    void on(ConsumerRecord<String, String> record) {
      inbox.handle(record, SagaCommand.class, command -> handle(record, command));
    }

    private void handle(ConsumerRecord<String, String> record, SagaCommand command) {
      count(record);
      // What a log line written here would carry.
      logContext.put(
          UUID.fromString(new String(record.headers().lastHeader("x-event-id").value(), UTF_8)),
          MDC.get("traceId") + "|" + MDC.get("tenantId") + "|" + MDC.get("orderId"));
      String label =
          command instanceof ReleaseInventoryCmd
              ? "released " + command.orderId()
              : command.orderId();
      // A state change in the same transaction as the inbox row.
      jdbc.update(
          "INSERT INTO orchestrator.tenant_probe (tenant_id, probe_id, label) VALUES (?, ?, ?)",
          TenantContext.require(),
          UUID.randomUUID(),
          label);
      if (command instanceof ReserveInventoryCmd reserve
          && reserve.orderId().startsWith("poison-")) {
        throw new IllegalStateException("handler failed");
      }
    }
  }

  /** A second consumer group on the same topic, such as a read model. */
  static class ProjectionHandler extends CountingHandler {

    private final InboxConsumer inbox;

    ProjectionHandler(InboxConsumer inbox) {
      this.inbox = inbox;
    }

    @KafkaListener(topics = TOPIC, groupId = "inbox-it-projection")
    void on(ConsumerRecord<String, String> record) {
      inbox.handle(record, SagaCommand.class, command -> count(record));
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @Import(InboxIT.Handlers.class)
  static class App {}

  static class Handlers {
    @Bean
    SagaHandler sagaHandler(InboxConsumer inbox, JdbcTemplate jdbc) {
      return new SagaHandler(inbox, jdbc);
    }

    @Bean
    ProjectionHandler projectionHandler(InboxConsumer inbox) {
      return new ProjectionHandler(inbox);
    }
  }
}
