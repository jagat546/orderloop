package app.orderloop.orchestrator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.Topics;
import app.orderloop.fulfilment.FulfilmentServiceApplication;
import app.orderloop.fulfilment.provider.SimulatedCarrier;
import app.orderloop.inventory.InventoryServiceApplication;
import app.orderloop.inventory.provider.SimulatedInventory;
import app.orderloop.payment.PaymentServiceApplication;
import app.orderloop.payment.provider.SimulatedPayments;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Roadmaps M1.10 to M1.12, end to end: the real orchestrator, inventory, payment and fulfilment
 * services, each in its own Spring context and schema, talking only through PostgreSQL outboxes and
 * Kafka. All connect as orderloop_app.
 */
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class SagaEndToEndIT {

  private static final String KEY = "alpha-dev-key-0123456789";
  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  private static ConfigurableApplicationContext orchestrator;
  private static ConfigurableApplicationContext inventory;
  private static ConfigurableApplicationContext payment;
  private static ConfigurableApplicationContext fulfilment;

  private final HttpClient http = HttpClient.newHttpClient();

  @BeforeAll
  static void start() {
    orchestrator =
        new SpringApplicationBuilder(OrderOrchestratorApplication.class)
            .run(
                args(
                    "orchestrator",
                    "--server.port=0",
                    "--orderloop.security.dev-api-keys-enabled=true",
                    "--orderloop.security.dev-api-keys[0].tenant=t_alpha",
                    "--orderloop.security.dev-api-keys[0].key=" + KEY));
    // Every service's application.yml shares a classpath here, so every setting that differs is an
    // argument, which outranks any file.
    inventory =
        new SpringApplicationBuilder(InventoryServiceApplication.class)
            .web(WebApplicationType.NONE)
            .run(args("inventory", "--spring.application.name=inventory-service"));
    payment =
        new SpringApplicationBuilder(PaymentServiceApplication.class)
            .web(WebApplicationType.NONE)
            .run(args("payment", "--spring.application.name=payment-service"));
    fulfilment =
        new SpringApplicationBuilder(FulfilmentServiceApplication.class)
            .web(WebApplicationType.NONE)
            .run(args("fulfilment", "--spring.application.name=fulfilment-service"));
  }

  @AfterAll
  static void stop() {
    for (var context :
        new ConfigurableApplicationContext[] {fulfilment, payment, inventory, orchestrator}) {
      if (context != null) {
        context.close();
      }
    }
  }

  @Test
  void aPaidOrderPassesVerifyingPaymentAndBookingAndIsInTransit() {
    String order = "o-" + UUID.randomUUID();
    payments().record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));

    assertThat(create(order)).isEqualTo(201);

    awaitState(order, "InTransit");
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit");
    assertThat(exceptions(order)).isEmpty();
  }

  @Test
  void aDeliveredOrderClosesOnceItsMoneySettles() {
    String order = "o-" + UUID.randomUUID();
    payments().record("t_alpha", order, PaymentMethod.COD, false, new Money(1234, "KWD"));
    assertThat(create(order)).isEqualTo(201);
    awaitState(order, "InTransit");

    carrier().deliver("t_alpha", order);
    awaitState(order, "AwaitingSettlement");
    payments().settle("t_alpha", order);

    awaitState(order, "Closed");
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit",
            "InTransit->AwaitingSettlement",
            "AwaitingSettlement->Closed");
    assertThat(exceptions(order)).isEmpty();
  }

  @Test
  void aPrepaidOrderWhoseMoneySettlesBeforeDeliveryClosesOnDelivery() {
    String order = "o-" + UUID.randomUUID();
    payments().record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));
    assertThat(create(order)).isEqualTo(201);
    awaitState(order, "InTransit");

    payments().settle("t_alpha", order);
    // The payout is recorded on the transit step before the carrier reports anything.
    await()
        .atMost(Duration.ofSeconds(60))
        .until(() -> steps(order).contains("InTransit->InTransit"));
    assertThat(state(order)).isEqualTo("InTransit");
    carrier().deliver("t_alpha", order);

    awaitState(order, "Closed");
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit",
            "InTransit->InTransit",
            "InTransit->Closed");
    assertThat(exceptions(order)).isEmpty();
  }

  @Test
  void aReturnedOrderClosesAsRtoOnceItsStockIsBack() {
    String order = "o-" + UUID.randomUUID();
    payments().record("t_alpha", order, PaymentMethod.COD, false, new Money(1234, "KWD"));
    assertThat(create(order)).isEqualTo(201);
    awaitState(order, "InTransit");

    carrier().startRto("t_alpha", order);
    awaitState(order, "Returning");
    carrier().receiveRto("t_alpha", order);
    awaitState(order, "Restocking");
    inventory.getBean(SimulatedInventory.class).restockedByHand("t_alpha", order);

    awaitState(order, "ClosedRto");
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit",
            "InTransit->Returning",
            "Returning->Restocking",
            "Restocking->ClosedRto");
    assertThat(exceptions(order)).isEmpty();
  }

  @Test
  void demoOrdersCloseOrReturnWithNothingDrivingTheSimulators() {
    String delivered = "demo-" + UUID.randomUUID() + "-delivered-INR-149900";
    String returned = "demo-" + UUID.randomUUID() + "-rto-JPY-4500";

    assertThat(create(delivered, new Money(149900, "INR"))).isEqualTo(201);
    assertThat(create(returned, new Money(4500, "JPY"))).isEqualTo(201);

    awaitState(delivered, "Closed");
    awaitState(returned, "ClosedRto");
    // Settlement may land before delivery (early) or after it; either way the order closes.
    List<String> deliveredSteps = steps(delivered);
    assertThat(deliveredSteps)
        .startsWith(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit");
    assertThat(deliveredSteps.getLast()).isIn("AwaitingSettlement->Closed", "InTransit->Closed");
    assertThat(steps(returned))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Booking",
            "Booking->InTransit",
            "InTransit->Returning",
            "Returning->Restocking",
            "Restocking->ClosedRto");
    // Cash on delivery was never captured, so the return needs no refund (invariant 8).
    assertThat(payments().refundCalls("t_alpha", returned)).isZero();
    assertThat(exceptions(delivered)).isEmpty();
    assertThat(exceptions(returned)).isEmpty();
  }

  @Test
  void aFailedPaymentReleasesStockAndClosesTheOrderAsCancelled() {
    String order = "o-" + UUID.randomUUID(); // No payment recorded, so verification fails.

    assertThat(create(order)).isEqualTo(201);

    await().atMost(Duration.ofSeconds(60)).until(() -> "ClosedCancelled".equals(state(order)));
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating",
            "Allocating->VerifyingPayment",
            "VerifyingPayment->Releasing",
            "Releasing->ClosedCancelled");
    assertThat(exceptions(order)).isEmpty();
  }

  @Test
  void anOutOfStockOrderNeedsAttentionWithOneOpenException() {
    String order = "o-" + UUID.randomUUID() + "-out-of-stock";

    assertThat(create(order)).isEqualTo(201);

    await().atMost(Duration.ofSeconds(60)).until(() -> "NeedsAttention".equals(state(order)));
    assertThat(exceptions(order)).containsExactly("OPEN|INVENTORY_FAILED|1234|KWD");
  }

  @Test
  void oneTraceFollowsAnOrderAcrossEveryService(CapturedOutput output) {
    String order = "o-" + UUID.randomUUID();
    String traceId = UUID.randomUUID().toString().replace("-", "");
    payments().record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));

    int status =
        send(HttpRequest.newBuilder(uri("/api/v1/orders"))
                .header("Content-Type", "application/json")
                .header("traceparent", "00-" + traceId + "-00f067aa0ba902b7-01")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        "{\"orderId\":\"%s\",\"total\":{\"amountMinor\":1234,\"currency\":\"KWD\"}}"
                            .formatted(order))))
            .statusCode();
    assertThat(status).isEqualTo(201);
    awaitState(order, "InTransit");

    // Every message about the order, from the orchestrator and from each service's reply.
    Map<String, List<String>> traceparents = traceparentsByTopic(order, Topics.ALL.size());
    assertThat(traceparents).containsOnlyKeys(Topics.ALL);
    List<String> all = traceparents.values().stream().flatMap(List::stream).toList();
    assertThat(all)
        .allSatisfy(
            header -> assertThat(header).matches("00-" + traceId + "-[0-9a-f]{16}-[0-9a-f]{2}"));
    // Each hop continued the trace in a span of its own, so the parent span IDs differ.
    assertThat(all.stream().map(header -> header.substring(36, 52)).distinct().count())
        .isGreaterThan(1);
    // JSON logs carry the trace, the tenant and the order for a reply the orchestrator handled.
    assertThat(output.getOut().lines())
        .anySatisfy(
            line ->
                assertThat(line)
                    .startsWith("{")
                    .contains("\"traceId\":\"" + traceId + "\"")
                    .contains("\"tenantId\":\"t_alpha\"")
                    .contains("\"orderId\":\"" + order + "\""));
  }

  /** Reads each topic from the start and collects the traceparent of every record for the order. */
  private static Map<String, List<String>> traceparentsByTopic(String order, int topics) {
    Map<String, Object> config =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "trace-test-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest");
    Map<String, List<String>> found = new HashMap<>();
    try (var consumer =
        new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(Topics.ALL);
      await()
          .atMost(Duration.ofSeconds(60))
          .until(
              () -> {
                for (ConsumerRecord<String, String> record :
                    consumer.poll(Duration.ofMillis(500))) {
                  if (order.equals(record.key())) {
                    Header header = record.headers().lastHeader("traceparent");
                    found
                        .computeIfAbsent(record.topic(), t -> new ArrayList<>())
                        .add(header == null ? "none" : new String(header.value(), UTF_8));
                  }
                }
                return found.size() == topics;
              });
    }
    return found;
  }

  private static String[] args(String schema, String... more) {
    return Stream.concat(
            Stream.of(
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=orderloop_app",
                "--spring.datasource.password=" + TestDatabase.APP_PASSWORD,
                "--spring.flyway.user=orderloop_owner",
                "--spring.flyway.password=" + TestDatabase.OWNER_PASSWORD,
                "--spring.flyway.schemas=" + schema,
                "--spring.flyway.locations=classpath:db/migration/" + schema,
                "--orderloop.outbox.schema=" + schema,
                "--orderloop.outbox.ops-password=" + TestDatabase.OPS_PASSWORD,
                "--orderloop.outbox.poll-interval=100ms",
                "--orderloop.simulator.demo-step=1s",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers()),
            Stream.of(more))
        .toArray(String[]::new);
  }

  private int create(String order) {
    return create(order, new Money(1234, "KWD"));
  }

  private int create(String order, Money total) {
    String body =
        "{\"orderId\":\"%s\",\"total\":{\"amountMinor\":%d,\"currency\":\"%s\"}}"
            .formatted(order, total.amountMinor(), total.currency());
    return send(HttpRequest.newBuilder(uri("/api/v1/orders"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)))
        .statusCode();
  }

  private String state(String order) {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/api/v1/orders/" + order)));
    return JSON.readTree(response.body()).get("state").asString();
  }

  private static SimulatedPayments payments() {
    return payment.getBean(SimulatedPayments.class);
  }

  private static SimulatedCarrier carrier() {
    return fulfilment.getBean(SimulatedCarrier.class);
  }

  private void awaitState(String order, String expected) {
    await().atMost(Duration.ofSeconds(60)).until(() -> expected.equals(state(order)));
  }

  private static List<String> steps(String order) {
    JdbcTemplate jdbc = orchestrator.getBean(JdbcTemplate.class);
    return TenantContext.callAs(
        "t_alpha",
        () ->
            jdbc.queryForList(
                "SELECT from_state || '->' || to_state FROM orchestrator.saga_step_log"
                    + " WHERE order_id = ? ORDER BY seq",
                String.class,
                order));
  }

  private static List<String> exceptions(String order) {
    JdbcTemplate jdbc = orchestrator.getBean(JdbcTemplate.class);
    return TenantContext.callAs(
        "t_alpha",
        () ->
            jdbc.queryForList(
                "SELECT status || '|' || reason || '|' || amount_at_risk_minor || '|' || currency"
                    + " FROM orchestrator.exception_case WHERE order_id = ?",
                String.class,
                order));
  }

  private HttpResponse<String> send(HttpRequest.Builder request) {
    try {
      return http.send(
          request.header("X-Api-Key", KEY).build(), HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static URI uri(String path) {
    return URI.create(
        "http://localhost:"
            + orchestrator.getEnvironment().getProperty("local.server.port")
            + path);
  }
}
