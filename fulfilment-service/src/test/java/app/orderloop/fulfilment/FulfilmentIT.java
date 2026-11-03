package app.orderloop.fulfilment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Message;
import app.orderloop.dto.SagaCommand.BookShipmentCmd;
import app.orderloop.dto.SagaCommand.ReattemptDeliveryCmd;
import app.orderloop.dto.Topics;
import app.orderloop.fulfilment.provider.SimulatedCarrier;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * The fulfilment service on real PostgreSQL and Kafka: shipment commands get one reply each, and
 * the simulated carrier's events reach {@code orderloop.fulfilment-events} through the service's
 * outbox. Connects as orderloop_app.
 */
@Testcontainers
@SpringBootTest(
    classes = FulfilmentServiceApplication.class,
    webEnvironment = WebEnvironment.NONE,
    properties = {
      "orderloop.outbox.poll-interval=100ms",
      "orderloop.inbox.max-retries=1",
      "orderloop.inbox.initial-interval=50ms"
    })
class FulfilmentIT {

  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES, "fulfilment");
    registry.add("orderloop.outbox.ops-password", () -> TestDatabase.OPS_PASSWORD);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  @Autowired private JdbcTemplate jdbc;
  @Autowired private SimulatedCarrier carrier;

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
  void aBookingRepliesShipmentBooked() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new BookShipmentCmd(order));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(reply.key()).isEqualTo(order);
    assertThat(header(reply, "x-event-type")).isEqualTo("ShipmentBookedEvt");
    assertThat(header(reply, "x-tenant-id")).isEqualTo("t_alpha");
    assertThat(reply.value()).isEqualTo("{\"orderId\":\"" + order + "\"}");
  }

  @Test
  void anOrderNoCourierTakesRepliesShipmentFailed() throws Exception {
    String order = newOrderId() + "-no-courier";
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new BookShipmentCmd(order));

    ConsumerRecord<String, String> reply = awaitReplyTo(command);
    assertThat(header(reply, "x-event-type")).isEqualTo("ShipmentFailedEvt");
    assertThat(reply.value())
        .isEqualTo("{\"orderId\":\"" + order + "\",\"reason\":\"NO_COURIER\"}");
  }

  @Test
  void aDuplicateCommandGetsOneReply() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();

    send("t_alpha", command, new BookShipmentCmd(order));
    send("t_alpha", command, new BookShipmentCmd(order));
    // A later command on the same order and partition: once it is answered, both copies were seen.
    UUID marker = UUID.randomUUID();
    send("t_alpha", marker, new BookShipmentCmd(order));

    List<ConsumerRecord<String, String>> replies =
        recordsUntil(Topics.FULFILMENT_EVENTS, causedBy(marker));
    assertThat(replies.stream().filter(causedBy(command))).hasSize(1);
  }

  @Test
  void theCarrierReportsEachStepOfABookedShipmentInOrder() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();
    send("t_alpha", command, new BookShipmentCmd(order));
    awaitReplyTo(command);

    carrier.failDelivery("t_alpha", order, "CUSTOMER_UNAVAILABLE");
    carrier.startRto("t_alpha", order);
    carrier.receiveRto("t_alpha", order);
    carrier.deliver("t_alpha", order); // Out of order on purpose: the carrier reports what it saw.

    List<ConsumerRecord<String, String>> events =
        recordsUntil(
                Topics.FULFILMENT_EVENTS,
                r -> order.equals(r.key()) && "DeliveredEvt".equals(header(r, "x-event-type")))
            .stream()
            .filter(r -> order.equals(r.key()))
            .toList();
    assertThat(events.stream().map(r -> header(r, "x-event-type")))
        .containsExactly(
            "ShipmentBookedEvt",
            "DeliveryFailedEvt",
            "RtoStartedEvt",
            "RtoReceivedEvt",
            "DeliveredEvt");
    assertThat(events).allSatisfy(r -> assertThat(header(r, "x-tenant-id")).isEqualTo("t_alpha"));
    assertThat(events.get(1).value())
        .isEqualTo("{\"orderId\":\"" + order + "\",\"reason\":\"CUSTOMER_UNAVAILABLE\"}");
  }

  @Test
  void theCarrierReportsNothingForAShipmentThisTenantNeverBooked() throws Exception {
    String order = newOrderId();
    UUID command = UUID.randomUUID();
    send("t_alpha", command, new BookShipmentCmd(order));
    awaitReplyTo(command);

    assertThatThrownBy(() -> carrier.deliver("t_alpha", newOrderId()))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> carrier.deliver("t_beta", order))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void aReattemptRequestIsHandledOnceAndAnsweredByTheCarrierLater() throws Exception {
    String order = newOrderId();
    UUID booking = UUID.randomUUID();
    send("t_alpha", booking, new BookShipmentCmd(order));
    awaitReplyTo(booking);
    UUID reattempt = UUID.randomUUID();

    send("t_alpha", reattempt, new ReattemptDeliveryCmd(order));

    await().atMost(Duration.ofSeconds(30)).until(() -> handled("t_alpha", reattempt) == 1);
  }

  @Test
  void anotherTenantCannotSeeWhichCommandsWereHandled() throws Exception {
    UUID command = UUID.randomUUID();
    send("t_alpha", command, new BookShipmentCmd(newOrderId()));
    awaitReplyTo(command);

    assertThat(handled("t_alpha", command)).isEqualTo(1);
    assertThat(handled("t_beta", command)).isZero();
  }

  private int handled(String tenant, UUID eventId) {
    return TenantContext.callAs(
        tenant,
        () ->
            jdbc.queryForObject(
                "SELECT count(*) FROM fulfilment.processed_event WHERE event_id = ?",
                Integer.class,
                eventId));
  }

  private static String newOrderId() {
    return "o-" + UUID.randomUUID();
  }

  private static ConsumerRecord<String, String> awaitReplyTo(UUID command) {
    return recordsUntil(Topics.FULFILMENT_EVENTS, causedBy(command)).stream()
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
            Topics.FULFILMENT_COMMANDS, message.orderId(), JSON.writeValueAsString(message));
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
