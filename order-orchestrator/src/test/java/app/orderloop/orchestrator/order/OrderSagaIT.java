package app.orderloop.orchestrator.order;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Money;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.InventoryFailedEvt;
import app.orderloop.dto.SagaEvent.InventoryReservedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.orchestrator.OrderOrchestratorApplication;
import app.orderloop.orchestrator.saga.SagaInput;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.orchestrator.saga.SagaInput.RetryStep;
import app.orderloop.orchestrator.saga.SagaState.Allocating;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Roadmap M1.9: creating an order emits {@code ReserveInventoryCmd} through the outbox, and
 * concurrent replies are safe. Connects as orderloop_app, never a superuser, so row-level security
 * is in force for every query the service and these tests make.
 */
@Testcontainers
@SpringBootTest(
    classes = OrderOrchestratorApplication.class,
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "orderloop.security.dev-api-keys-enabled=true",
      "orderloop.security.dev-api-keys[0].tenant=t_alpha",
      "orderloop.security.dev-api-keys[0].key=" + OrderSagaIT.ALPHA_KEY,
      "orderloop.security.dev-api-keys[1].tenant=t_beta",
      "orderloop.security.dev-api-keys[1].key=" + OrderSagaIT.BETA_KEY,
      "orderloop.outbox.poll-interval=100ms",
      "orderloop.inbox.initial-interval=50ms"
    })
class OrderSagaIT {

  static final String ALPHA_KEY = "alpha-dev-key-0123456789";
  static final String BETA_KEY = "beta-dev-key-0123456789";
  private static final JsonMapper JSON = JsonMapper.builder().build();

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  @DynamicPropertySource
  static void infrastructure(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES);
    registry.add("orderloop.outbox.ops-password", () -> TestDatabase.OPS_PASSWORD);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  private final HttpClient http = HttpClient.newHttpClient();

  @Value("${local.server.port}")
  private int port;

  @Autowired private SagaService sagas;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private PlatformTransactionManager transactions;
  @Autowired private EntityManagerFactory emf;
  @PersistenceContext private EntityManager em;

  // --- The role every query runs as ---

  @Test
  void connectsAsTheAppRoleWhichCannotBypassRowLevelSecurity() {
    String role =
        as(
            "t_alpha",
            () ->
                jdbc.queryForObject(
                    "SELECT current_user || '|' || rolsuper || '|' || rolbypassrls"
                        + " FROM pg_roles WHERE rolname = current_user",
                    String.class));

    assertThat(role).isEqualTo("orderloop_app|false|false");
  }

  // --- Creating an order ---

  @Test
  void creatingAnOrderEmitsReserveInventoryCmdThroughTheOutbox() throws Exception {
    String order = newOrderId();

    HttpResponse<String> response = post(ALPHA_KEY, order, 1234, "KWD");

    assertThat(response.statusCode()).isEqualTo(201);
    JsonNode body = JSON.readTree(response.body());
    assertThat(body.get("orderId").asString()).isEqualTo(order);
    assertThat(body.get("state").asString()).isEqualTo("Allocating");
    assertThat(body.get("total").get("amountMinor").asLong()).isEqualTo(1234);
    assertThat(body.get("total").get("currency").asString()).isEqualTo("KWD");

    ConsumerRecord<String, String> command = awaitRecord(Topics.INVENTORY_COMMANDS, order);
    assertThat(header(command, "x-event-type")).isEqualTo("ReserveInventoryCmd");
    assertThat(header(command, "x-tenant-id")).isEqualTo("t_alpha");
    assertThat(command.value()).isEqualTo("{\"orderId\":\"" + order + "\"}");
    assertThat(stepLog("t_alpha", order)).containsExactly("Received->Allocating");
  }

  @Test
  void creatingTheSameOrderTwiceEmitsTheCommandOnce() throws Exception {
    String order = newOrderId();

    int first = post(ALPHA_KEY, order, 500, "JPY").statusCode();
    int second = post(ALPHA_KEY, order, 500, "JPY").statusCode();

    assertThat(first).isEqualTo(201);
    assertThat(second).isEqualTo(200);
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd");
  }

  @Test
  void aTotalInAZeroDecimalCurrencyIsStoredExactly() throws Exception {
    String order = newOrderId();
    post(ALPHA_KEY, order, 500, "JPY");

    JsonNode total = JSON.readTree(get(ALPHA_KEY, order).body()).get("total");

    assertThat(total.get("amountMinor").asLong()).isEqualTo(500);
    assertThat(total.get("currency").asString()).isEqualTo("JPY");
  }

  @Test
  void aNegativeTotalIsRejectedAsAProblem() throws Exception {
    HttpResponse<String> response = post(ALPHA_KEY, newOrderId(), -1, "EUR");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.headers().firstValue("Content-Type")).hasValue("application/problem+json");
    assertThat(JSON.readTree(response.body()).get("type").asString())
        .isEqualTo("urn:orderloop:problem:bad-request");
  }

  @Test
  void anUnknownCurrencyIsRejectedAsAProblem() throws Exception {
    HttpResponse<String> response = post(ALPHA_KEY, newOrderId(), 100, "XYZ");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.headers().firstValue("Content-Type")).hasValue("application/problem+json");
  }

  @Test
  void anOrderNeedsACredential() throws Exception {
    assertThat(post(null, newOrderId(), 100, "EUR").statusCode()).isEqualTo(401);
  }

  // --- Replies ---

  @Test
  void aReplyMovesTheSagaOnAndADuplicateChangesItOnce() throws Exception {
    String order = created("t_alpha");
    UUID reply = UUID.randomUUID();

    sendReply("t_alpha", reply, new InventoryReservedEvt(order));
    sendReply("t_alpha", reply, new InventoryReservedEvt(order));
    // A later event on the same order and partition: once it is handled, both copies were seen.
    UUID marker = UUID.randomUUID();
    sendReply("t_alpha", marker, new InventoryReservedEvt(order));

    await().atMost(Duration.ofSeconds(30)).until(() -> processed("t_alpha", marker));
    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
    assertThat(stepLog("t_alpha", order))
        .containsExactly("Received->Allocating", "Allocating->VerifyingPayment");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ProcessPaymentCmd");
  }

  @Test
  void aStaleWriterFailsAndCommitsNothing() {
    String order = created("t_alpha");
    var outer = new TransactionTemplate(transactions);
    var inner = new TransactionTemplate(transactions);
    inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

    assertThatThrownBy(
            () ->
                as(
                    "t_alpha",
                    () ->
                        outer.execute(
                            status -> {
                              // This writer reads the saga in Allocating...
                              em.find(OrderSaga.class, order);
                              // ...another commits a reply first...
                              inner.executeWithoutResult(
                                  s -> sagas.apply(order, reserved(order), UUID.randomUUID()));
                              // ...so this one decides on a state that no longer exists.
                              sagas.apply(order, failed(order), UUID.randomUUID());
                              return null;
                            })))
        .isInstanceOf(OptimisticLockingFailureException.class);

    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
    assertThat(stepLog("t_alpha", order))
        .containsExactly("Received->Allocating", "Allocating->VerifyingPayment");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ProcessPaymentCmd");

    // The inbox retries the loser against the state that won, where the late failure is ignored.
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));
    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
  }

  @Test
  void racingRepliesTakeOneTransitionAndEmitOneCommand() throws Exception {
    String order = created("t_alpha");
    Runnable reply = () -> applyRetryingLikeTheInbox(order, reserved(order));

    race(reply, reply, reply, reply, reply, reply, reply, reply);

    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
    assertThat(stepLog("t_alpha", order))
        .containsExactly("Received->Allocating", "Allocating->VerifyingPayment");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ProcessPaymentCmd");
  }

  // --- Exceptions ---

  @Test
  void anInventoryFailureParksTheSagaAndOpensAnExceptionForTheOrderValue() throws Exception {
    String order = created("t_alpha");

    sendReply("t_alpha", UUID.randomUUID(), new InventoryFailedEvt(order, "OUT_OF_STOCK"));

    await().atMost(Duration.ofSeconds(30)).until(() -> !exceptions("t_alpha", order).isEmpty());
    assertThat(state("t_alpha", order)).isEqualTo("NeedsAttention");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  @Test
  void aRepeatedFailureOpensOneException() {
    String order = created("t_alpha");

    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));

    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  @Test
  void aLateReservationResumesTheSagaAndResolvesTheException() {
    String order = created("t_alpha");
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));

    runAs(
        "t_alpha",
        () -> inTransaction(() -> sagas.apply(order, reserved(order), UUID.randomUUID())));

    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
    assertThat(exceptions("t_alpha", order))
        .containsExactly("RESOLVED|INVENTORY_FAILED|1000|EUR|InventoryReservedEvt");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ProcessPaymentCmd");
  }

  @Test
  void racingFailedAndReservedRepliesLeaveTheExceptionInStepWithTheSaga() throws Exception {
    // Either reply may commit first; the loser is retried against the state that won. Several
    // rounds, so both orders are likely to occur.
    for (int round = 0; round < 6; round++) {
      String order = created("t_alpha");

      race(
          () -> applyRetryingLikeTheInbox(order, failed(order)),
          () -> applyRetryingLikeTheInbox(order, reserved(order)));

      // Failed first parks the saga and the reservation resumes it, resolving the exception.
      // Reserved first makes the failure late, so it is ignored and opens nothing.
      assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
      assertThat(exceptions("t_alpha", order))
          .isSubsetOf("RESOLVED|INVENTORY_FAILED|1000|EUR|InventoryReservedEvt")
          .doesNotHaveDuplicates();
      assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ProcessPaymentCmd");
    }
  }

  @Test
  void anOperatorRetryResolvesTheExceptionAndASecondFailureOpensANewOne() {
    String order = created("t_alpha");
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));

    runAs("t_alpha", () -> inTransaction(() -> sagas.apply(order, new RetryStep(), null)));

    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
    assertThat(exceptions("t_alpha", order))
        .containsExactly("RESOLVED|INVENTORY_FAILED|1000|EUR|RetryStep");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd", "ReserveInventoryCmd");

    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));

    assertThat(state("t_alpha", order)).isEqualTo("NeedsAttention");
    assertThat(exceptions("t_alpha", order))
        .containsExactly(
            "RESOLVED|INVENTORY_FAILED|1000|EUR|RetryStep", "OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  @Test
  void aFailureDeliveredTwiceOpensOneException() throws Exception {
    String order = created("t_alpha");
    UUID failure = UUID.randomUUID();

    sendReply("t_alpha", failure, new InventoryFailedEvt(order, "OUT_OF_STOCK"));
    sendReply("t_alpha", failure, new InventoryFailedEvt(order, "OUT_OF_STOCK"));
    // A later event on the same order and partition: once it is handled, both copies were seen.
    UUID marker = UUID.randomUUID();
    sendReply("t_alpha", marker, new InventoryFailedEvt(order, "OUT_OF_STOCK"));

    await().atMost(Duration.ofSeconds(30)).until(() -> processed("t_alpha", marker));
    assertThat(stepLog("t_alpha", order))
        .containsExactly("Received->Allocating", "Allocating->NeedsAttention");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  @Test
  void leavingNeedsAttentionWithNoOpenExceptionFailsLoudly() {
    String order = created("t_alpha");
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));
    // Drift: the open exception is gone, as for a saga parked before exceptions were stored.
    runAs(
        "t_alpha",
        () ->
            jdbc.update(
                "UPDATE orchestrator.exception_case SET status = 'RESOLVED' WHERE order_id = ?",
                order));

    assertThatThrownBy(
            () ->
                runAs(
                    "t_alpha",
                    () ->
                        inTransaction(
                            () -> sagas.apply(order, reserved(order), UUID.randomUUID()))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("open exception");

    assertThat(state("t_alpha", order)).isEqualTo("NeedsAttention");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd");
  }

  @Test
  void parkingASagaThatAlreadyHasAnOpenExceptionFailsLoudly() {
    String order = created("t_alpha");
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));
    // Drift: the saga row is back in Allocating while its exception is still open.
    runAs(
        "t_alpha",
        () ->
            jdbc.update(
                "UPDATE orchestrator.order_saga SET state = 'Allocating', state_data = ?"
                    + " WHERE order_id = ?",
                SagaStateJson.write(new Allocating(order)),
                order));

    assertThatThrownBy(
            () ->
                runAs(
                    "t_alpha",
                    () ->
                        inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID()))))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  @Test
  void anotherTenantCannotReadOrResolveTheException() {
    String order = created("t_alpha");
    runAs(
        "t_alpha", () -> inTransaction(() -> sagas.apply(order, failed(order), UUID.randomUUID())));

    int seen =
        as(
            "t_beta",
            () ->
                jdbc.queryForObject(
                    "SELECT count(*) FROM orchestrator.exception_case WHERE order_id = ?",
                    Integer.class,
                    order));
    int resolved =
        as(
            "t_beta",
            () ->
                jdbc.update(
                    "UPDATE orchestrator.exception_case SET status = 'RESOLVED' WHERE order_id = ?",
                    order));

    assertThat(seen).isZero();
    assertThat(resolved).isZero();
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|INVENTORY_FAILED|1000|EUR|null");
  }

  // --- Tenant isolation ---

  @Test
  void anotherTenantCannotSeeTheOrderOverTheApi() throws Exception {
    String order = newOrderId();
    post(ALPHA_KEY, order, 100, "EUR");

    assertThat(get(BETA_KEY, order).statusCode()).isEqualTo(404);
    assertThat(get(ALPHA_KEY, order).statusCode()).isEqualTo(200);
  }

  @Test
  void anotherTenantMayUseTheSameOrderIdWithoutTouchingTheFirst() throws Exception {
    String order = newOrderId();
    post(ALPHA_KEY, order, 100, "EUR");

    assertThat(post(BETA_KEY, order, 900, "USD").statusCode()).isEqualTo(201);

    JsonNode alpha = JSON.readTree(get(ALPHA_KEY, order).body()).get("total");
    assertThat(alpha.get("amountMinor").asLong()).isEqualTo(100);
    assertThat(alpha.get("currency").asString()).isEqualTo("EUR");
  }

  @Test
  void anotherTenantCannotReadUpdateOrDeleteTheSagaWithNativeSql() {
    String order = created("t_alpha");

    int sagasSeen =
        as(
            "t_beta",
            () ->
                jdbc.queryForObject(
                    "SELECT count(*) FROM orchestrator.order_saga WHERE order_id = ?",
                    Integer.class,
                    order));
    int stepsSeen =
        as(
            "t_beta",
            () ->
                jdbc.queryForObject(
                    "SELECT count(*) FROM orchestrator.saga_step_log WHERE order_id = ?",
                    Integer.class,
                    order));
    int updated =
        as(
            "t_beta",
            () ->
                jdbc.update(
                    "UPDATE orchestrator.order_saga SET state = 'Closed' WHERE order_id = ?",
                    order));

    assertThat(sagasSeen).isZero();
    assertThat(stepsSeen).isZero();
    assertThat(updated).isZero();
    assertThatThrownBy(
            () ->
                as(
                    "t_beta",
                    () ->
                        jdbc.update(
                            "DELETE FROM orchestrator.order_saga WHERE order_id = ?", order)))
        .isInstanceOf(DataAccessException.class);
    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
  }

  @Test
  void anotherTenantCannotPlantASagaForTheFirstTenant() {
    assertThatThrownBy(
            () ->
                as(
                    "t_beta",
                    () ->
                        jdbc.update(
                            "INSERT INTO orchestrator.order_saga (tenant_id, order_id, state,"
                                + " state_data, order_total_minor, currency)"
                                + " VALUES ('t_alpha', ?, 'Received', '{}', 0, 'EUR')",
                            newOrderId())))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("new row violates row-level security policy");
  }

  @Test
  void rowLevelSecurityHidesTheSagaEvenWhenHibernateFiltersForItsOwner() {
    String order = created("t_alpha");
    SessionFactory sessions = emf.unwrap(SessionFactory.class);

    // Give the session the owner's tenant while the connection is bound to another, so only
    // row-level security can hide the row.
    OrderSaga sameTenant = TenantContext.callAs("t_alpha", () -> find(sessions, "t_alpha", order));
    OrderSaga crossed = TenantContext.callAs("t_beta", () -> find(sessions, "t_alpha", order));

    assertThat(sameTenant).isNotNull();
    assertThat(crossed).isNull();
  }

  @Test
  void anotherTenantsReplyCannotMoveTheSaga() {
    String order = created("t_alpha");

    assertThatThrownBy(
            () ->
                runAs(
                    "t_beta",
                    () ->
                        inTransaction(
                            () -> sagas.apply(order, reserved(order), UUID.randomUUID()))))
        .isInstanceOf(UnknownOrderException.class);

    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
    assertThat(outboxTypes(order)).containsExactly("ReserveInventoryCmd");
  }

  // --- Helpers ---

  /** Starts every racer at once as tenant t_alpha, and rethrows any racer's failure. */
  private void race(Runnable... racers) throws Exception {
    var start = new CyclicBarrier(racers.length);
    List<Future<?>> racing = new ArrayList<>();
    try (var threads = Executors.newFixedThreadPool(racers.length)) {
      for (Runnable racer : racers) {
        racing.add(
            threads.submit(
                () -> {
                  start.await();
                  // A scoped value does not reach a pool thread, so each one binds its tenant.
                  runAs("t_alpha", racer);
                  return null;
                }));
      }
      for (Future<?> racer : racing) {
        racer.get();
      }
    }
  }

  /** Retries an optimistic lock conflict the way the inbox's error handler does. */
  private void applyRetryingLikeTheInbox(String order, SagaInput input) {
    for (int attempt = 1; ; attempt++) {
      try {
        inTransaction(() -> sagas.apply(order, input, UUID.randomUUID()));
        return;
      } catch (OptimisticLockingFailureException e) {
        if (attempt == 20) {
          throw e;
        }
      }
    }
  }

  private String created(String tenant) {
    String order = newOrderId();
    runAs(tenant, () -> sagas.create(order, new Money(1000, "EUR")));
    return order;
  }

  private static Reply reserved(String order) {
    return new Reply(new InventoryReservedEvt(order));
  }

  private static Reply failed(String order) {
    return new Reply(new InventoryFailedEvt(order, "out of stock"));
  }

  private void inTransaction(Runnable work) {
    new TransactionTemplate(transactions).executeWithoutResult(status -> work.run());
  }

  private <T> T as(String tenant, ScopedValue.CallableOp<T, RuntimeException> work) {
    return TenantContext.callAs(tenant, work);
  }

  private void runAs(String tenant, Runnable work) {
    TenantContext.callAs(
        tenant,
        () -> {
          work.run();
          return null;
        });
  }

  private String state(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc.queryForObject(
                "SELECT state FROM orchestrator.order_saga WHERE order_id = ?",
                String.class,
                order));
  }

  private List<String> stepLog(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc.queryForList(
                "SELECT from_state || '->' || to_state FROM orchestrator.saga_step_log"
                    + " WHERE order_id = ? ORDER BY seq",
                String.class,
                order));
  }

  /** Each exception as status|reason|amountMinor|currency|resolvedBy, oldest first. */
  private List<String> exceptions(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc.queryForList(
                "SELECT status || '|' || reason || '|' || amount_at_risk_minor || '|' || currency"
                    + " || '|' || coalesce(resolved_by, 'null')"
                    + " FROM orchestrator.exception_case WHERE order_id = ? ORDER BY opened_at",
                String.class,
                order));
  }

  private boolean processed(String tenant, UUID eventId) {
    return as(
        tenant,
        () ->
            jdbc.queryForObject(
                    "SELECT count(*) FROM orchestrator.processed_event WHERE event_id = ?",
                    Integer.class,
                    eventId)
                > 0);
  }

  /** Every command written for the order, read as the relay's role, which sees all tenants. */
  private static List<String> outboxTypes(String order) {
    var ops = new PGSimpleDataSource();
    ops.setUrl(POSTGRES.getJdbcUrl());
    ops.setUser("orderloop_ops");
    ops.setPassword(TestDatabase.OPS_PASSWORD);
    return new JdbcTemplate(ops)
        .queryForList(
            "SELECT event_type FROM orchestrator.outbox WHERE aggregate_id = ? ORDER BY id",
            String.class,
            order);
  }

  private static OrderSaga find(SessionFactory sessions, String sessionTenant, String order) {
    try (Session session =
        sessions.withOptions().tenantIdentifier((Object) sessionTenant).openSession()) {
      return session.find(OrderSaga.class, order);
    }
  }

  private static String newOrderId() {
    return "o-" + UUID.randomUUID();
  }

  private HttpResponse<String> post(String key, String order, long amountMinor, String currency)
      throws Exception {
    String body =
        "{\"orderId\":\"%s\",\"total\":{\"amountMinor\":%d,\"currency\":\"%s\"}}"
            .formatted(order, amountMinor, currency);
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/orders"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));
    if (key != null) {
      request.header("X-Api-Key", key);
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(String key, String order) throws Exception {
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/orders/" + order))
            .header("X-Api-Key", key)
            .GET()
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static void sendReply(String tenant, UUID eventId, SagaEvent event) throws Exception {
    var record =
        new ProducerRecord<String, String>(
            Topics.INVENTORY_EVENTS, event.orderId(), JSON.writeValueAsString(event));
    record.headers().add("x-tenant-id", tenant.getBytes(UTF_8));
    record.headers().add("x-event-id", eventId.toString().getBytes(UTF_8));
    record.headers().add("x-event-type", event.getClass().getSimpleName().getBytes(UTF_8));
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
      producer.send(record).get();
    }
  }

  private static ConsumerRecord<String, String> awaitRecord(String topic, String key) {
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
      List<ConsumerRecord<String, String>> found = new ArrayList<>();
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> {
                consumer.poll(Duration.ofMillis(200)).forEach(found::add);
                return found.stream().anyMatch(r -> key.equals(r.key()));
              });
      return found.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow();
    }
  }

  private static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), UTF_8);
  }
}
