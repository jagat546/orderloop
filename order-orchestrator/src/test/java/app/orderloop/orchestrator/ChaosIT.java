package app.orderloop.orchestrator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Headers;
import app.orderloop.dto.MessageTypes;
import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.DeliveredEvt;
import app.orderloop.dto.SagaEvent.PaymentFailedEvt;
import app.orderloop.dto.SagaEvent.PaymentProcessedEvt;
import app.orderloop.dto.SagaEvent.SettlementObservedEvt;
import app.orderloop.dto.SagaEvent.ShipmentBookedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.fulfilment.FulfilmentServiceApplication;
import app.orderloop.fulfilment.provider.SimulatedCarrier;
import app.orderloop.inventory.InventoryServiceApplication;
import app.orderloop.orchestrator.order.SagaService;
import app.orderloop.orchestrator.saga.SagaInput.RefundApproved;
import app.orderloop.payment.PaymentServiceApplication;
import app.orderloop.payment.provider.SimulatedPayments;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Roadmap M1.15, the chaos suite: the real orchestrator, inventory, payment and fulfilment
 * services, each in its own Spring context, as in {@link SagaEndToEndIT}. Every crash point is
 * reproduced by leaving a service exactly as a crash there would leave it, then starting a fresh
 * one:
 *
 * <ul>
 *   <li>after a handler commits but before the relay publishes: an instance with its relay off;
 *   <li>after a handler commits but before the consumer commits its offset: the group rewound;
 *   <li>after Kafka acknowledged a batch but before the relay marked it: every row unpublished.
 * </ul>
 *
 * Replies are also delivered out of order, and Kafka itself is stopped and started. The last test
 * checks every order placed here: it ended in a named state with the right money, and no tenant can
 * see another's rows. Containers are capped in memory, so the suite fits a small machine.
 *
 * <p>The simulators stand in for providers outside the service, so a restarted service gets back
 * what the provider would still hold: what checkout charged, and which shipments the carrier
 * booked.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChaosIT {

  private static final String ALPHA = "t_alpha";
  private static final String BETA = "t_beta";
  private static final Map<String, String> KEYS =
      Map.of(ALPHA, "alpha-dev-key-0123456789", BETA, "beta-dev-key-01234567890");
  // Different currencies per tenant, one with three decimals and one with none (invariant 7).
  private static final Map<String, Money> TOTALS =
      Map.of(ALPHA, new Money(12_345, "KWD"), BETA, new Money(5_000, "JPY"));
  private static final List<String> HAPPY_PATH =
      List.of(
          "Received->Allocating",
          "Allocating->VerifyingPayment",
          "VerifyingPayment->Booking",
          "Booking->InTransit",
          "InTransit->AwaitingSettlement",
          "AwaitingSettlement->Closed");
  private static final Duration WAIT = Duration.ofSeconds(90);
  private static final long MB = 1024L * 1024;
  private static final Duration STARTUP = Duration.ofMinutes(3);
  private static final JsonMapper JSON = JsonMapper.builder().build();

  /** A fixed host port, so Kafka keeps its advertised address across a stop and start. */
  private static final int KAFKA_PORT = freePort();

  private static final PostgreSQLContainer POSTGRES =
      TestDatabase.newContainer()
          // A capped container on a busy small machine can take longer than the default minute.
          .withStartupTimeout(STARTUP)
          .withCreateContainerCmdModifier(
              cmd -> cmd.getHostConfig().withMemory(512 * MB).withMemorySwap(512 * MB));

  private static final KafkaContainer KAFKA =
      new KafkaContainer("apache/kafka:4.4.0")
          .withStartupTimeout(STARTUP)
          .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx384m")
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.getHostConfig()
                      .withMemory(768 * MB)
                      .withMemorySwap(768 * MB)
                      .withPortBindings(
                          new PortBinding(
                              Ports.Binding.bindPort(KAFKA_PORT), new ExposedPort(9092))));

  /** The services, their schemas, consumer groups and the topics they read. */
  enum Service {
    ORCHESTRATOR(
        "orchestrator",
        "order-orchestrator",
        List.of(Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS, Topics.FULFILMENT_EVENTS)),
    INVENTORY("inventory", "inventory-service", List.of(Topics.INVENTORY_COMMANDS)),
    PAYMENT("payment", "payment-service", List.of(Topics.PAYMENT_COMMANDS)),
    FULFILMENT("fulfilment", "fulfilment-service", List.of(Topics.FULFILMENT_COMMANDS));

    final String schema;
    final String group;
    final List<String> topics;

    Service(String schema, String group, List<String> topics) {
      this.schema = schema;
      this.group = group;
      this.topics = topics;
    }
  }

  /** An order placed by this suite, and the state it must end in. */
  private record Placed(
      String tenant, String orderId, Money total, PaymentMethod method, String endState) {}

  private static final Map<Service, ConfigurableApplicationContext> RUNNING =
      new EnumMap<>(Service.class);
  private static final List<Placed> PLACED = new CopyOnWriteArrayList<>();

  /** What checkout charged, as the payment provider would still hold it after a restart. */
  private static final Map<Placed, Boolean> CHECKOUT = new ConcurrentHashMap<>();

  /** The order refunded after a late capture, checked again after every duplicate. */
  private static Placed refunded;

  private static Admin admin;
  private static KafkaProducer<String, String> producer;
  private final HttpClient http = HttpClient.newHttpClient();

  @BeforeAll
  static void start() {
    Startables.deepStart(POSTGRES, KAFKA).join();
    admin =
        Admin.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                60_000));
    producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG,
                "all"),
            new StringSerializer(),
            new StringSerializer());
    for (Service service : Service.values()) {
      startService(service);
    }
  }

  @AfterAll
  static void stop() {
    for (Service service : List.of(Service.values()).reversed()) {
      kill(service);
    }
    if (producer != null) {
      producer.close();
    }
    if (admin != null) {
      admin.close();
    }
    KAFKA.stop();
    POSTGRES.stop();
  }

  @Order(1)
  @ParameterizedTest
  @EnumSource(Service.class)
  void aServiceKilledAfterItCommitsButBeforeItPublishesLosesNothing(Service service) {
    kill(service);
    startService(service, "--orderloop.outbox.relay-enabled=false");

    Placed prepaid = place(ALPHA, PaymentMethod.PREPAID);
    Placed cod = place(BETA, PaymentMethod.COD);
    // The doomed instance handled its step and committed it, but nothing left its outbox.
    for (Placed order : List.of(prepaid, cod)) {
      await().atMost(WAIT).until(() -> unpublished(service.schema, order.orderId()) > 0);
    }
    kill(service);
    startService(service);

    for (Placed order : List.of(prepaid, cod)) {
      closeNormally(order);
      assertThat(steps(order)).containsExactlyElementsOf(HAPPY_PATH);
    }
  }

  @Order(2)
  @Test
  void carrierAndSettlementEventsInAnyOrderCloseTheOrder() {
    // With fulfilment down, each order waits in Booking and only the injected events reach it.
    kill(Service.FULFILMENT);
    List<List<String>> orders =
        List.of(
            List.of("booked", "delivered", "settled"),
            List.of("booked", "settled", "delivered"),
            List.of("delivered", "booked", "settled"),
            List.of("delivered", "settled", "booked"),
            List.of("settled", "booked", "delivered"),
            List.of("settled", "delivered", "booked"));
    List<Placed> placed = new ArrayList<>();
    for (int i = 0; i < orders.size(); i++) {
      Placed order = place(i % 2 == 0 ? ALPHA : BETA, PaymentMethod.PREPAID);
      awaitState(order, "Booking");
      placed.add(order);
    }

    for (int i = 0; i < orders.size(); i++) {
      Placed order = placed.get(i);
      for (String event : orders.get(i)) {
        switch (event) {
          case "booked" ->
              reply(order, Topics.FULFILMENT_EVENTS, new ShipmentBookedEvt(order.orderId()));
          case "delivered" ->
              reply(order, Topics.FULFILMENT_EVENTS, new DeliveredEvt(order.orderId()));
          default ->
              reply(
                  order,
                  Topics.PAYMENT_EVENTS,
                  new SettlementObservedEvt(order.orderId(), order.total()));
        }
      }
      awaitState(order, "Closed");
    }

    // The carrier now books every shipment and reports it, long after the orders closed.
    startService(Service.FULFILMENT);
    awaitQuiet();
    for (Placed order : placed) {
      assertThat(state(order)).isEqualTo("Closed");
      assertThat(steps(order)).noneMatch(step -> step.contains("NeedsAttention"));
      assertThat(steps(order).getLast()).endsWith("->Closed");
      assertThat(openExceptions(order)).isEmpty();
    }
  }

  @Order(3)
  @Test
  void aFailureThenALateCaptureIsRefundedOnceAfterApproval() {
    kill(Service.PAYMENT);
    Placed order = place(ALPHA, PaymentMethod.PREPAID, "ClosedCancelled");
    awaitState(order, "VerifyingPayment");

    // The failure overtakes the capture.
    reply(order, Topics.PAYMENT_EVENTS, new PaymentFailedEvt(order.orderId(), "NO_PAYMENT_FOUND"));
    awaitState(order, "ClosedCancelled");
    reply(
        order,
        Topics.PAYMENT_EVENTS,
        new PaymentProcessedEvt(order.orderId(), PaymentMethod.PREPAID, true, order.total()));
    awaitState(order, "RefundProposed");
    assertThat(findings(order)).containsExactly("PAID_NOT_FULFILLED|" + money(order.total()));

    // The payment service comes back and answers the original check too; it changes nothing.
    startService(Service.PAYMENT);
    awaitQuiet();
    assertThat(state(order)).isEqualTo("RefundProposed");
    assertThat(refundIntents(order)).isEmpty(); // Nothing moves without a human (invariant 8).

    TenantContext.callAs(
        order.tenant(), () -> sagas().apply(order.orderId(), new RefundApproved(), null));
    awaitState(order, "ClosedCancelled");
    awaitQuiet();

    assertThat(refundIntents(order)).containsExactly("SUCCEEDED|" + money(order.total()));
    assertThat(payments().refunds(order.tenant(), order.orderId())).containsExactly(order.total());
    assertThat(openExceptions(order)).isEmpty();
    refunded = order;
  }

  @Order(4)
  @ParameterizedTest
  @EnumSource(Service.class)
  void aServiceKilledBeforeItCommitsItsOffsetsHandlesEverythingAgainWithoutEffect(Service service) {
    awaitQuiet();
    List<String> before = snapshot();

    kill(service);
    rewindToStart(service);
    startService(service);
    awaitQuiet();

    assertThat(snapshot()).containsExactlyElementsOf(before);
  }

  @Order(5)
  @Test
  void aRelayKilledAfterKafkaAcknowledgedButBeforeItMarkedTheRowsRepublishesWithoutEffect() {
    awaitQuiet();
    List<String> before = snapshot();
    int refundCalls = payments().refundCalls(refunded.tenant(), refunded.orderId());

    // Every message ever sent goes out again, the approved refund command included.
    for (Service service : Service.values()) {
      superuserUpdate("UPDATE " + service.schema + ".outbox SET published_at = NULL");
    }
    awaitQuiet();

    assertThat(snapshot()).containsExactlyElementsOf(before);
    assertThat(payments().refundCalls(refunded.tenant(), refunded.orderId()))
        .isEqualTo(refundCalls);
    assertThat(refundIntents(refunded)).containsExactly("SUCCEEDED|" + money(refunded.total()));
  }

  @Order(6)
  @Test
  void stoppingKafkaDelaysOrdersButLosesNone() {
    List<Placed> orders = new ArrayList<>();
    orders.add(place(ALPHA, PaymentMethod.PREPAID));
    orders.add(place(BETA, PaymentMethod.COD));

    var docker = KAFKA.getDockerClient();
    docker.stopContainerCmd(KAFKA.getContainerId()).exec();
    // Orders still arrive; their commands wait in the outbox.
    orders.add(place(ALPHA, PaymentMethod.COD));
    orders.add(place(BETA, PaymentMethod.PREPAID));
    await().atMost(WAIT).until(() -> unpublished("orchestrator", orders.get(3).orderId()) > 0);

    docker.startContainerCmd(KAFKA.getContainerId()).exec();
    await()
        .atMost(WAIT)
        .ignoreExceptions()
        .until(() -> !admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).isEmpty());

    for (Placed order : orders) {
      closeNormally(order);
      assertThat(steps(order)).containsExactlyElementsOf(HAPPY_PATH);
    }
  }

  @Order(7)
  @Test
  void everyOrderEndsInANamedStateWithTheRightMoneyAndNoTenantSeesAnother() throws Exception {
    awaitQuiet();
    // Two per service killed before publishing, six reordered, one refunded, four through Kafka.
    assertThat(PLACED).hasSize(19);

    for (Placed order : PLACED) {
      // Named, and the one this order had to reach.
      assertThat(state(order)).as(order.orderId()).isEqualTo(order.endState());
      assertThat(
              superuser(
                  "SELECT tenant_id || '|' || order_total_minor || '|' || currency"
                      + " FROM orchestrator.order_saga WHERE order_id = ?",
                  order.orderId()))
          .as("one saga, in its own tenant, for its own amount")
          .containsExactly(order.tenant() + "|" + money(order.total()));
      assertThat(openExceptions(order)).as(order.orderId()).isEmpty();
      // A refund only where one was approved, and then exactly the captured amount.
      assertThat(refundIntents(order))
          .as(order.orderId())
          .isEqualTo(
              order.equals(refunded) ? List.of("SUCCEEDED|" + money(order.total())) : List.of());
      // Every message about the order, in every service, stayed in the order's tenant.
      for (Service service : Service.values()) {
        assertThat(
                superuser(
                    "SELECT DISTINCT tenant_id FROM "
                        + service.schema
                        + ".outbox WHERE aggregate_id = ?",
                    order.orderId()))
            .as(service.schema + " outbox for " + order.orderId())
            .isSubsetOf(order.tenant()); // Empty where the order never reached the service.
      }
      // The other tenant's key finds nothing, as if the order did not exist.
      String other = order.tenant().equals(ALPHA) ? BETA : ALPHA;
      assertThat(get(other, order.orderId()).statusCode()).isEqualTo(404);
    }

    List<String> tables =
        superuser(
            "SELECT table_schema || '.' || table_name FROM information_schema.columns"
                + " WHERE column_name = 'tenant_id'"
                + " AND table_schema IN ('orchestrator', 'inventory', 'payment', 'fulfilment')"
                + " AND has_table_privilege('orderloop_app',"
                + " quote_ident(table_schema) || '.' || quote_ident(table_name), 'SELECT')"
                + " ORDER BY 1");
    assertThat(tables)
        .contains(
            "orchestrator.order_saga", "orchestrator.exception_case", "payment.refund_intent");
    try (Connection app =
        DriverManager.getConnection(
            POSTGRES.getJdbcUrl(), "orderloop_app", TestDatabase.APP_PASSWORD)) {
      for (String table : tables) {
        assertThat(count(app, table, null)).as(table + " with no tenant bound").isZero();
        for (String tenant : List.of(ALPHA, BETA)) {
          long all =
              Long.parseLong(
                  superuser("SELECT count(*) FROM " + table + " WHERE tenant_id = ?", tenant)
                      .getFirst());
          assertThat(count(app, table, tenant)).as(table + " as " + tenant).isEqualTo(all);
          try (PreparedStatement others =
              app.prepareStatement("SELECT count(*) FROM " + table + " WHERE tenant_id <> ?")) {
            others.setString(1, tenant);
            assertThat(single(others)).as(table + " seen by " + tenant).isZero();
          }
        }
      }
    }
  }

  // --- Services -------------------------------------------------------------------------------

  private static void startService(Service service, String... extra) {
    String[] args = args(service, extra);
    ConfigurableApplicationContext context =
        switch (service) {
          case ORCHESTRATOR -> {
            List<String> all = new ArrayList<>(List.of(args));
            all.add("--server.port=0");
            all.add("--orderloop.security.dev-api-keys-enabled=true");
            int i = 0;
            for (var key : KEYS.entrySet()) {
              all.add("--orderloop.security.dev-api-keys[" + i + "].tenant=" + key.getKey());
              all.add("--orderloop.security.dev-api-keys[" + i + "].key=" + key.getValue());
              i++;
            }
            yield new SpringApplicationBuilder(OrderOrchestratorApplication.class)
                .run(all.toArray(String[]::new));
          }
          case INVENTORY ->
              new SpringApplicationBuilder(InventoryServiceApplication.class)
                  .web(WebApplicationType.NONE)
                  .run(args);
          case PAYMENT ->
              new SpringApplicationBuilder(PaymentServiceApplication.class)
                  .web(WebApplicationType.NONE)
                  .run(args);
          case FULFILMENT ->
              new SpringApplicationBuilder(FulfilmentServiceApplication.class)
                  .web(WebApplicationType.NONE)
                  .run(args);
        };
    RUNNING.put(service, context);
    // What the providers still hold, as they live outside the service.
    if (service == Service.PAYMENT) {
      CHECKOUT.forEach(
          (order, captured) ->
              payments()
                  .record(
                      order.tenant(), order.orderId(), order.method(), captured, order.total()));
    }
    if (service == Service.FULFILMENT) {
      SimulatedCarrier carrier = context.getBean(SimulatedCarrier.class);
      for (String booked :
          superuser(
              "SELECT DISTINCT tenant_id || '|' || aggregate_id FROM fulfilment.outbox"
                  + " WHERE event_type = 'ShipmentBookedEvt'")) {
        String[] parts = booked.split("\\|", 2);
        carrier.book(parts[0], parts[1]);
      }
    }
  }

  private static void kill(Service service) {
    ConfigurableApplicationContext context = RUNNING.remove(service);
    if (context != null) {
      context.close();
    }
  }

  private static String[] args(Service service, String... more) {
    return Stream.concat(
            Stream.of(
                "--spring.application.name=" + service.group,
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=orderloop_app",
                "--spring.datasource.password=" + TestDatabase.APP_PASSWORD,
                "--spring.datasource.hikari.maximum-pool-size=4",
                "--spring.flyway.user=orderloop_owner",
                "--spring.flyway.password=" + TestDatabase.OWNER_PASSWORD,
                "--spring.flyway.schemas=" + service.schema,
                "--spring.flyway.locations=classpath:db/migration/" + service.schema,
                "--orderloop.outbox.schema=" + service.schema,
                "--orderloop.outbox.ops-password=" + TestDatabase.OPS_PASSWORD,
                "--orderloop.outbox.poll-interval=100ms",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers()),
            Stream.of(more))
        .toArray(String[]::new);
  }

  private static SagaService sagas() {
    return RUNNING.get(Service.ORCHESTRATOR).getBean(SagaService.class);
  }

  private static SimulatedPayments payments() {
    return RUNNING.get(Service.PAYMENT).getBean(SimulatedPayments.class);
  }

  private static SimulatedCarrier carrier() {
    return RUNNING.get(Service.FULFILMENT).getBean(SimulatedCarrier.class);
  }

  // --- Orders ---------------------------------------------------------------------------------

  private Placed place(String tenant, PaymentMethod method) {
    return place(tenant, method, "Closed");
  }

  /** Charges the order at checkout (prepaid is captured, COD is not), then creates it. */
  private Placed place(String tenant, PaymentMethod method, String endState) {
    String id = "o-" + UUID.randomUUID().toString().substring(0, 18);
    Placed order = new Placed(tenant, id, TOTALS.get(tenant), method, endState);
    boolean captured = method == PaymentMethod.PREPAID;
    CHECKOUT.put(order, captured);
    if (RUNNING.containsKey(Service.PAYMENT)) {
      payments().record(tenant, id, method, captured, order.total());
    }
    String body =
        "{\"orderId\":\"%s\",\"total\":{\"amountMinor\":%d,\"currency\":\"%s\"}}"
            .formatted(id, order.total().amountMinor(), order.total().currency());
    HttpResponse<String> response =
        send(
            tenant,
            HttpRequest.newBuilder(uri("/api/v1/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)));
    assertThat(response.statusCode()).isEqualTo(201);
    PLACED.add(order);
    return order;
  }

  /** The carrier delivers and the money settles, through the simulators, as in production. */
  private void closeNormally(Placed order) {
    awaitState(order, "InTransit");
    carrier().deliver(order.tenant(), order.orderId());
    awaitState(order, "AwaitingSettlement");
    payments().settle(order.tenant(), order.orderId());
    awaitState(order, "Closed");
  }

  /**
   * Delivers a reply as a provider or service would, and waits until the orchestrator has handled
   * it, so the next one cannot overtake it.
   */
  private static void reply(Placed order, String topic, SagaEvent event) {
    UUID eventId = UUID.randomUUID();
    var record =
        new ProducerRecord<String, String>(topic, order.orderId(), JSON.writeValueAsString(event));
    record.headers().add(Headers.TENANT_ID, order.tenant().getBytes(UTF_8));
    record.headers().add(Headers.EVENT_ID, eventId.toString().getBytes(UTF_8));
    record.headers().add(Headers.EVENT_TYPE, MessageTypes.nameOf(event.getClass()).getBytes(UTF_8));
    record
        .headers()
        .add(Headers.SCHEMA_VERSION, Integer.toString(MessageTypes.SCHEMA_VERSION).getBytes(UTF_8));
    try {
      producer.send(record).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    await()
        .atMost(WAIT)
        .until(
            () ->
                !superuser(
                        "SELECT 1 FROM orchestrator.processed_event WHERE event_id = ?::uuid",
                        eventId.toString())
                    .isEmpty());
  }

  private String state(Placed order) {
    HttpResponse<String> response = get(order.tenant(), order.orderId());
    assertThat(response.statusCode()).as(order.orderId()).isEqualTo(200);
    return JSON.readTree(response.body()).get("state").asString();
  }

  private void awaitState(Placed order, String expected) {
    await().atMost(WAIT).until(() -> expected.equals(state(order)));
  }

  private static List<String> steps(Placed order) {
    return superuser(
        "SELECT from_state || '->' || to_state FROM orchestrator.saga_step_log"
            + " WHERE tenant_id = ? AND order_id = ? ORDER BY seq",
        order.tenant(),
        order.orderId());
  }

  private static List<String> openExceptions(Placed order) {
    return superuser(
        "SELECT reason FROM orchestrator.exception_case"
            + " WHERE tenant_id = ? AND order_id = ? AND status = 'OPEN'",
        order.tenant(),
        order.orderId());
  }

  private static List<String> findings(Placed order) {
    return superuser(
        "SELECT detector || '|' || amount_at_risk_minor || '|' || currency"
            + " FROM orchestrator.leak_finding WHERE tenant_id = ? AND order_id = ?",
        order.tenant(),
        order.orderId());
  }

  private static List<String> refundIntents(Placed order) {
    return superuser(
        "SELECT status || '|' || amount_minor || '|' || currency FROM payment.refund_intent"
            + " WHERE tenant_id = ? AND order_id = ? ORDER BY attempt",
        order.tenant(),
        order.orderId());
  }

  private static String money(Money money) {
    return money.amountMinor() + "|" + money.currency();
  }

  // --- Quiet, snapshots and offsets -----------------------------------------------------------

  /** Waits until every outbox is empty and every consumer group has read to the end. */
  private static void awaitQuiet() {
    await()
        .atMost(WAIT)
        .pollInterval(Duration.ofMillis(500))
        .ignoreExceptions()
        .until(
            () -> {
              for (Service service : Service.values()) {
                if (unpublished(service.schema, null) > 0 || lag(service) > 0) {
                  return false;
                }
              }
              return true;
            });
  }

  private static long lag(Service service) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> committed =
        admin
            .listConsumerGroupOffsets(service.group)
            .partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS);
    Map<TopicPartition, OffsetSpec> latest =
        partitions(service).stream()
            .collect(Collectors.toMap(partition -> partition, partition -> OffsetSpec.latest()));
    var ends = admin.listOffsets(latest).all().get(10, TimeUnit.SECONDS);
    long lag = 0;
    for (TopicPartition partition : latest.keySet()) {
      OffsetAndMetadata at = committed.get(partition);
      lag += ends.get(partition).offset() - (at == null ? 0 : at.offset());
    }
    return lag;
  }

  private static Set<TopicPartition> partitions(Service service) throws Exception {
    return admin
        .describeTopics(service.topics)
        .allTopicNames()
        .get(10, TimeUnit.SECONDS)
        .values()
        .stream()
        .flatMap(
            topic ->
                topic.partitions().stream()
                    .map(p -> new TopicPartition(topic.name(), p.partition())))
        .collect(Collectors.toSet());
  }

  /** As if the service crashed after every commit but before any offset commit. */
  private static void rewindToStart(Service service) {
    await()
        .atMost(WAIT)
        .ignoreExceptions()
        .until(
            () -> {
              Map<TopicPartition, OffsetAndMetadata> start =
                  partitions(service).stream()
                      .collect(Collectors.toMap(p -> p, p -> new OffsetAndMetadata(0)));
              // Fails while the group still has members; the stopped service leaves it shortly.
              admin.alterConsumerGroupOffsets(service.group, start).all().get(10, TimeUnit.SECONDS);
              return true;
            });
  }

  /**
   * Everything the services decided: every domain row in full, and how many messages each service
   * sent and handled. A redelivery that changed anything shows up here.
   */
  private static List<String> snapshot() {
    List<String> rows = new ArrayList<>();
    for (String table :
        List.of(
            "orchestrator.order_saga",
            "orchestrator.saga_step_log",
            "orchestrator.exception_case",
            "orchestrator.leak_finding",
            "orchestrator.saga_deadline",
            "payment.refund_intent")) {
      superuser("SELECT '" + table + " ' || t::text FROM " + table + " t ORDER BY 1")
          .forEach(rows::add);
    }
    for (Service service : Service.values()) {
      for (String table : List.of("outbox", "processed_event")) {
        String name = service.schema + "." + table;
        rows.add(name + " " + superuser("SELECT count(*) FROM " + name).getFirst());
      }
    }
    return rows;
  }

  private static long unpublished(String schema, String orderId) throws Exception {
    String sql = "SELECT count(*) FROM " + schema + ".outbox WHERE published_at IS NULL";
    List<String> result =
        orderId == null ? superuser(sql) : superuser(sql + " AND aggregate_id = ?", orderId);
    return Long.parseLong(result.getFirst());
  }

  // --- Database -------------------------------------------------------------------------------

  /**
   * Reads as the container's superuser, which RLS does not bind, to see every tenant's rows. The
   * services never connect like this; the last test checks what orderloop_app can see.
   */
  private static List<String> superuser(String sql, Object... args) {
    try (Connection connection = superuserConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
      List<String> rows = new ArrayList<>();
      try (ResultSet result = statement.executeQuery()) {
        while (result.next()) {
          rows.add(result.getString(1));
        }
      }
      return rows;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void superuserUpdate(String sql) {
    try (Connection connection = superuserConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Connection superuserConnection() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  /** Counts a table's rows as orderloop_app with the tenant bound as the services bind it. */
  private static long count(Connection app, String table, String tenant) throws SQLException {
    try (PreparedStatement bind =
        app.prepareStatement("SELECT set_config('app.tenant_id', ?, false)")) {
      bind.setString(1, tenant == null ? "" : tenant);
      bind.execute();
    }
    try (PreparedStatement count = app.prepareStatement("SELECT count(*) FROM " + table)) {
      return single(count);
    }
  }

  private static long single(PreparedStatement statement) throws SQLException {
    try (ResultSet result = statement.executeQuery()) {
      result.next();
      return result.getLong(1);
    }
  }

  // --- HTTP -----------------------------------------------------------------------------------

  private HttpResponse<String> get(String tenant, String orderId) {
    return send(tenant, HttpRequest.newBuilder(uri("/api/v1/orders/" + orderId)));
  }

  private HttpResponse<String> send(String tenant, HttpRequest.Builder request) {
    try {
      return http.send(
          request.header("X-Api-Key", KEYS.get(tenant)).build(),
          HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static URI uri(String path) {
    return URI.create(
        "http://localhost:"
            + RUNNING.get(Service.ORCHESTRATOR).getEnvironment().getProperty("local.server.port")
            + path);
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
