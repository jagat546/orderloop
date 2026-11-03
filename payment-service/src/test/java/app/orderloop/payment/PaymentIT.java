package app.orderloop.payment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Message;
import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaCommand.ProcessPaymentCmd;
import app.orderloop.dto.SagaCommand.RefundPaymentCmd;
import app.orderloop.dto.Topics;
import app.orderloop.payment.provider.SimulatedPayments;
import app.orderloop.payment.provider.SimulatedPayments.RefundBehaviour;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
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
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Payment verification on real PostgreSQL and Kafka: each {@code ProcessPaymentCmd} gets one reply
 * on {@code orderloop.payment-events}, with the provider's amount exactly. Connects as
 * orderloop_app.
 */
@Testcontainers
@SpringBootTest(
    classes = PaymentServiceApplication.class,
    webEnvironment = WebEnvironment.NONE,
    properties = {
      "orderloop.outbox.poll-interval=100ms",
      "orderloop.inbox.max-retries=1",
      "orderloop.inbox.initial-interval=50ms",
      "orderloop.payment.refund-poll-interval=200ms",
      "orderloop.payment.reconcile-after=1s"
    })
class PaymentIT {

  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES, "payment");
    registry.add("orderloop.outbox.ops-password", () -> TestDatabase.OPS_PASSWORD);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired private JdbcTemplate jdbc;
  @Autowired private SimulatedPayments payments;

  @Test
  void connectsAsTheAppRoleWhichCannotBypassRowLevelSecurity() {
    String role =
        TenantContext.callAs(
            "t_alpha",
            () ->
                jdbc.queryForObject(
                    "SELECT current_user || '|' || rolsuper || '|' || rolbypassrls"
                        + " FROM pg_roles WHERE rolname = current_user",
                    String.class));

    assertThat(role).isEqualTo("orderloop_app|false|false");
  }

  @Test
  void aCapturedPaymentIsReportedWithItsExactAmountInAThreeDecimalCurrency() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new ProcessPaymentCmd(order));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(reply.key()).isEqualTo(order);
    assertThat(header(reply, "x-event-type")).isEqualTo("PaymentProcessedEvt");
    assertThat(header(reply, "x-tenant-id")).isEqualTo("t_alpha");
    assertThat(reply.value())
        .isEqualTo(
            "{\"orderId\":\"%s\",\"method\":\"PREPAID\",\"captured\":true,".formatted(order)
                + "\"amount\":{\"amountMinor\":1234,\"currency\":\"KWD\"}}");
  }

  @Test
  void aCashOnDeliveryPaymentIsReportedNotCapturedInAZeroDecimalCurrency() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.COD, false, new Money(500, "JPY"));
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new ProcessPaymentCmd(order));

    assertThat(awaitReplyTo(command).value())
        .isEqualTo(
            "{\"orderId\":\"%s\",\"method\":\"COD\",\"captured\":false,".formatted(order)
                + "\"amount\":{\"amountMinor\":500,\"currency\":\"JPY\"}}");
  }

  @Test
  void anOrderWithNoPaymentIsReportedFailed() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new ProcessPaymentCmd(order));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(header(reply, "x-event-type")).isEqualTo("PaymentFailedEvt");
    assertThat(reply.value())
        .isEqualTo("{\"orderId\":\"" + order + "\",\"reason\":\"NO_PAYMENT_FOUND\"}");
  }

  @Test
  void anotherTenantsPaymentIsNeverFound() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));
    UUID command = UUID.randomUUID();

    send("t_beta", command, new ProcessPaymentCmd(order));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(header(reply, "x-event-type")).isEqualTo("PaymentFailedEvt");
    assertThat(header(reply, "x-tenant-id")).isEqualTo("t_beta");
  }

  @Test
  void aDuplicateCommandGetsOneReply() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(999, "EUR"));
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new ProcessPaymentCmd(order));
    send("t_alpha", command, new ProcessPaymentCmd(order));
    // A later command on the same order and partition: once it is answered, both copies were seen.
    UUID marker = UUID.randomUUID();
    send("t_alpha", marker, new ProcessPaymentCmd(order));

    List<ConsumerRecord<String, String>> replies =
        recordsUntil(Topics.PAYMENT_EVENTS, causedBy(marker));
    assertThat(replies.stream().filter(causedBy(command))).hasSize(1);
  }

  // --- Refunds (roadmap M1.11b) ---

  @Test
  void aRefundThatTimesOutAfterSucceedingIsRefundedExactlyOnceInTheCapturedCurrency()
      throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));
    payments.onNextRefund("t_alpha", order, RefundBehaviour.TIME_OUT_AFTER_SUCCESS);
    UUID command = UUID.randomUUID();

    send("t_alpha", command, refund(order, new Money(1234, "KWD")));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(header(reply, "x-event-type")).isEqualTo("RefundCompletedEvt");
    assertThat(reply.value())
        .isEqualTo(
            "{\"orderId\":\"%s\",\"refundId\":\"rf-%s\",".formatted(order, order)
                + "\"amount\":{\"amountMinor\":1234,\"currency\":\"KWD\"}}");
    // One refund at the provider, from one call: the reconciler found it rather than sending again.
    assertThat(payments.refunds("t_alpha", order)).containsExactly(new Money(1234, "KWD"));
    assertThat(payments.refundCalls("t_alpha", order)).isEqualTo(1);
    assertThat(intent("t_alpha", order)).isEqualTo("SUCCEEDED|1234|KWD");
    // And one answer, even after more reconciler passes.
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .until(() -> countOn(Topics.PAYMENT_EVENTS, causedBy(command)) == 1);
  }

  @Test
  void aRefundIsCappedAtTheCapturedAmountInAZeroDecimalCurrency() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(500, "JPY"));
    UUID command = UUID.randomUUID();

    send("t_alpha", command, refund(order, new Money(900, "JPY")));

    assertThat(awaitReplyTo(command).value())
        .endsWith("\"amount\":{\"amountMinor\":500,\"currency\":\"JPY\"}}");
    assertThat(payments.refunds("t_alpha", order)).containsExactly(new Money(500, "JPY"));
  }

  @Test
  void aRefundInAnotherCurrencyThanTheCaptureMovesNoMoney() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1000, "EUR"));
    UUID command = UUID.randomUUID();

    send("t_alpha", command, refund(order, new Money(1000, "USD")));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(header(reply, "x-event-type")).isEqualTo("RefundFailedEvt");
    assertThat(reply.value())
        .isEqualTo(
            "{\"orderId\":\"%s\",\"refundId\":\"rf-%s\",\"reason\":\"CURRENCY_MISMATCH\"}"
                .formatted(order, order));
    assertThat(payments.refunds("t_alpha", order)).isEmpty();
    assertThat(payments.refundCalls("t_alpha", order)).isZero();
  }

  @Test
  void aRejectedRefundIsReportedAndAnOperatorRetryRefundsOnce() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1999, "USD"));
    payments.onNextRefund("t_alpha", order, RefundBehaviour.REJECT);
    UUID first = UUID.randomUUID();

    send("t_alpha", first, refund(order, new Money(1999, "USD")));
    assertThat(header(awaitReplyTo(first), "x-event-type")).isEqualTo("RefundFailedEvt");

    // RetryStep resends the command under a new event ID; it is a fresh attempt with a fresh key.
    UUID retry = UUID.randomUUID();
    send("t_alpha", retry, refund(order, new Money(1999, "USD")));

    assertThat(header(awaitReplyTo(retry), "x-event-type")).isEqualTo("RefundCompletedEvt");
    assertThat(payments.refunds("t_alpha", order)).containsExactly(new Money(1999, "USD"));
    assertThat(payments.refundCalls("t_alpha", order)).isEqualTo(2);
  }

  @Test
  void aRefundLeftSentByACrashBeforeTheCallIsRefundedOnce() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(750, "EUR"));
    UUID cause = UUID.randomUUID();
    // As a crash leaves it: marked SENT long ago, and the call never reached the provider.
    TenantContext.callAs(
        "t_alpha",
        () ->
            jdbc.update(
                "INSERT INTO payment.refund_intent (tenant_id, refund_id, order_id, attempt,"
                    + " amount_minor, currency, idempotency_key, status, cause_event_id,"
                    + " updated_at) VALUES ('t_alpha', ?, ?, 1, 750, 'EUR', ?, 'SENT', ?,"
                    + " now() - interval '1 hour')",
                "rf-" + order,
                order,
                "rf-" + order + ":1",
                cause));

    assertThat(header(awaitReplyTo(cause), "x-event-type")).isEqualTo("RefundCompletedEvt");
    assertThat(payments.refunds("t_alpha", order)).containsExactly(new Money(750, "EUR"));
    assertThat(payments.refundCalls("t_alpha", order)).isEqualTo(1);
  }

  @Test
  void aRepeatedRefundCommandAfterSuccessAnswersAgainWithoutRefundingAgain() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(300, "GBP"));
    UUID first = UUID.randomUUID();
    send("t_alpha", first, refund(order, new Money(300, "GBP")));
    awaitReplyTo(first);

    UUID again = UUID.randomUUID();
    send("t_alpha", again, refund(order, new Money(300, "GBP")));

    assertThat(header(awaitReplyTo(again), "x-event-type")).isEqualTo("RefundCompletedEvt");
    assertThat(payments.refunds("t_alpha", order)).containsExactly(new Money(300, "GBP"));
    assertThat(payments.refundCalls("t_alpha", order)).isEqualTo(1);
  }

  @Test
  void anotherTenantCannotReadOrChangeARefund() throws Exception {
    String order = newOrderId();
    payments.record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(100, "EUR"));
    UUID command = UUID.randomUUID();
    send("t_alpha", command, refund(order, new Money(100, "EUR")));
    awaitReplyTo(command);

    int seen =
        TenantContext.callAs(
            "t_beta",
            () ->
                jdbc.queryForObject(
                    "SELECT count(*) FROM payment.refund_intent WHERE order_id = ?",
                    Integer.class,
                    order));
    int changed =
        TenantContext.callAs(
            "t_beta",
            () ->
                jdbc.update(
                    "UPDATE payment.refund_intent SET status = 'INTENT' WHERE order_id = ?",
                    order));

    assertThat(seen).isZero();
    assertThat(changed).isZero();
    assertThat(intent("t_alpha", order)).isEqualTo("SUCCEEDED|100|EUR");
  }

  @Test
  void theReconcilerRoleCanReadRefundsButNeverChangeThem() {
    var ops = new PGSimpleDataSource();
    ops.setUrl(POSTGRES.getJdbcUrl());
    ops.setUser("orderloop_ops");
    ops.setPassword(TestDatabase.OPS_PASSWORD);
    var opsJdbc = new JdbcTemplate(ops);

    opsJdbc.queryForObject("SELECT count(*) FROM payment.refund_intent", Integer.class);
    assertThatThrownBy(() -> opsJdbc.update("UPDATE payment.refund_intent SET status = 'INTENT'"))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("permission denied");
  }

  @Test
  void anotherTenantCannotSeeWhichCommandsWereHandled() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();
    send("t_alpha", command, new ProcessPaymentCmd(order));
    awaitReplyTo(command);

    assertThat(handled("t_alpha", command)).isEqualTo(1);
    assertThat(handled("t_beta", command)).isZero();
  }

  private int handled(String tenant, UUID eventId) {
    return TenantContext.callAs(
        tenant,
        () ->
            jdbc.queryForObject(
                "SELECT count(*) FROM payment.processed_event WHERE event_id = ?",
                Integer.class,
                eventId));
  }

  private static RefundPaymentCmd refund(String order, Money amount) {
    return new RefundPaymentCmd(order, "rf-" + order, amount);
  }

  /** The refund as status|amountMinor|currency, read as the tenant. */
  private String intent(String tenant, String order) {
    return TenantContext.callAs(
        tenant,
        () ->
            jdbc.queryForObject(
                "SELECT status || '|' || amount_minor || '|' || currency"
                    + " FROM payment.refund_intent WHERE order_id = ?",
                String.class,
                order));
  }

  /** How many records on the topic match, reading it from the start to its current end. */
  private static long countOn(String topic, Predicate<ConsumerRecord<String, String>> match) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(p -> new TopicPartition(topic, p.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      long count = 0;
      while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
        for (var record : consumer.poll(Duration.ofMillis(200))) {
          count += match.test(record) ? 1 : 0;
        }
      }
      return count;
    }
  }

  private static String newOrderId() {
    return "o-" + UUID.randomUUID();
  }

  private static ConsumerRecord<String, String> awaitReplyTo(UUID command) {
    return recordsUntil(Topics.PAYMENT_EVENTS, causedBy(command)).stream()
        .filter(causedBy(command))
        .findFirst()
        .orElseThrow();
  }

  private static Predicate<ConsumerRecord<String, String>> causedBy(UUID eventId) {
    return r -> eventId.toString().equals(header(r, "x-causation-id"));
  }

  private static void send(String tenant, UUID eventId, Message message) throws Exception {
    var record =
        new ProducerRecord<String, String>(
            Topics.PAYMENT_COMMANDS, message.orderId(), JSON.writeValueAsString(message));
    record.headers().add("x-tenant-id", tenant.getBytes(UTF_8));
    record.headers().add("x-event-id", eventId.toString().getBytes(UTF_8));
    record.headers().add("x-event-type", message.getClass().getSimpleName().getBytes(UTF_8));
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
      producer.send(record).get();
    }
  }

  /** Every record on the topic, from the start, polled until one matches. */
  private static List<ConsumerRecord<String, String>> recordsUntil(
      String topic, Predicate<ConsumerRecord<String, String>> done) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      List<TopicPartition> partitions =
          consumer.partitionsFor(topic).stream()
              .map(p -> new TopicPartition(topic, p.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      List<ConsumerRecord<String, String>> seen = new ArrayList<>();
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> {
                consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
                return seen.stream().anyMatch(done);
              });
      return seen;
    }
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), UTF_8);
  }
}
