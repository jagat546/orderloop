package app.orderloop.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.inventory.InventoryServiceApplication;
import app.orderloop.payment.PaymentServiceApplication;
import app.orderloop.payment.provider.SimulatedPayments;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Roadmaps M1.10 and M1.11a, end to end: the real orchestrator, inventory and payment services,
 * each in its own Spring context and schema, talking only through PostgreSQL outboxes and Kafka.
 * All connect as orderloop_app.
 */
@Testcontainers
class SagaEndToEndIT {

  private static final String KEY = "alpha-dev-key-0123456789";
  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  private static ConfigurableApplicationContext orchestrator;
  private static ConfigurableApplicationContext inventory;
  private static ConfigurableApplicationContext payment;

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
  }

  @AfterAll
  static void stop() {
    for (var context : new ConfigurableApplicationContext[] {payment, inventory, orchestrator}) {
      if (context != null) {
        context.close();
      }
    }
  }

  @Test
  void aPaidOrderPassesVerifyingPaymentAndReachesBooking() {
    String order = "o-" + UUID.randomUUID();
    payment
        .getBean(SimulatedPayments.class)
        .record("t_alpha", order, PaymentMethod.PREPAID, true, new Money(1234, "KWD"));

    assertThat(create(order)).isEqualTo(201);

    await().atMost(Duration.ofSeconds(60)).until(() -> "Booking".equals(state(order)));
    assertThat(steps(order))
        .containsExactly(
            "Received->Allocating", "Allocating->VerifyingPayment", "VerifyingPayment->Booking");
    assertThat(exceptions(order)).isEmpty();
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
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers()),
            Stream.of(more))
        .toArray(String[]::new);
  }

  private int create(String order) {
    String body =
        "{\"orderId\":\"%s\",\"total\":{\"amountMinor\":1234,\"currency\":\"KWD\"}}"
            .formatted(order);
    return send(HttpRequest.newBuilder(uri("/api/v1/orders"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)))
        .statusCode();
  }

  private String state(String order) {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/api/v1/orders/" + order)));
    return JSON.readTree(response.body()).get("state").asString();
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
