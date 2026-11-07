package app.orderloop.orchestrator.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.InventoryReservedEvt;
import app.orderloop.dto.SagaEvent.PaymentProcessedEvt;
import app.orderloop.dto.SagaEvent.RtoReceivedEvt;
import app.orderloop.dto.SagaEvent.RtoStartedEvt;
import app.orderloop.dto.SagaEvent.SettlementObservedEvt;
import app.orderloop.dto.SagaEvent.ShipmentBookedEvt;
import app.orderloop.orchestrator.OrderOrchestratorApplication;
import app.orderloop.orchestrator.saga.DeadlineKind;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.platform.TestDatabase;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Roadmap M1.13: deadlines are stored with the saga, a missed one parks the saga with an exception
 * and a finding, and a deadline stored before a restart still fires after it. Every query runs as
 * orderloop_app; the scanner finds due deadlines as orderloop_ops.
 */
@Testcontainers
class DeadlineIT {

  private static final Money TOTAL = new Money(1234, "KWD");

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.4.0");

  private static ConfigurableApplicationContext orchestrator;

  @BeforeAll
  static void start() {
    orchestrator = startOrchestrator();
  }

  @AfterAll
  static void stop() {
    if (orchestrator != null) {
      orchestrator.close();
    }
  }

  // --- Storing the deadline ---

  @Test
  void eachTransitionStoresTheDeadlineOfTheStateItEnters() {
    Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    String order = created("t_alpha");

    Deadline allocating = deadline("t_alpha", order);
    assertThat(allocating.kind()).isEqualTo("ALLOCATING");
    assertThat(allocating.dueAt())
        .isBetween(before.plus(Duration.ofMinutes(5)), Instant.now().plus(Duration.ofMinutes(5)));

    apply("t_alpha", order, new InventoryReservedEvt(order));

    assertThat(deadline("t_alpha", order).kind()).isEqualTo("VERIFYING_PAYMENT");
  }

  @Test
  void aTransitionThatKeepsTheDeadlineLeavesItAsItWas() {
    String order = inTransit("t_alpha", PaymentMethod.PREPAID, true, TOTAL);
    Deadline inTransit = deadline("t_alpha", order);

    apply("t_alpha", order, new SettlementObservedEvt(order, TOTAL));
    apply("t_alpha", order, new ShipmentBookedEvt(order)); // Ignored: a duplicate.

    assertThat(stepLog("t_alpha", order)).endsWith("Booking->InTransit", "InTransit->InTransit");
    assertThat(deadline("t_alpha", order)).isEqualTo(inTransit);
  }

  // --- A missed deadline ---

  @Test
  void aMissedDeadlineParksTheSagaWithAnExceptionAndAFinding() {
    String order = created("t_alpha");

    expire("t_alpha", order);

    awaitState("t_alpha", order, "NeedsAttention");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|DEADLINE_MISSED|1234|KWD");
    assertThat(findings("t_alpha", order)).containsExactly("STUCK_ORDER|1234|KWD|false|OPEN");
    assertThat(deadlines("t_alpha", order)).isEmpty();
    assertThat(stepLog("t_alpha", order)).endsWith("Allocating->NeedsAttention");
  }

  @Test
  void aMissedDeadlineAndTheTransitionsItCausesAreCountedOnceCommitted() {
    double missed = count("orderloop.saga.deadlines.missed", "kind", "ALLOCATING");
    double parked = count("orderloop.saga.transitions", "to", "NeedsAttention");
    String order = created("t_alpha");
    Deadline due = deadline("t_alpha", order);

    // A stale firing commits nothing, so it counts nothing.
    TenantContext.callAs(
        "t_alpha",
        () ->
            sagas()
                .fireDeadline(
                    order, DeadlineKind.ALLOCATING, due.dueAt().minus(Duration.ofSeconds(1))));
    assertThat(count("orderloop.saga.deadlines.missed", "kind", "ALLOCATING")).isEqualTo(missed);

    expire("t_alpha", order);

    awaitState("t_alpha", order, "NeedsAttention");
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> count("orderloop.saga.deadlines.missed", "kind", "ALLOCATING") >= missed + 1);
    assertThat(count("orderloop.saga.transitions", "to", "NeedsAttention"))
        .isGreaterThanOrEqualTo(parked + 1);
  }

  private static double count(String name, String tag, String value) {
    var counter = orchestrator.getBean(MeterRegistry.class).find(name).tag(tag, value).counter();
    return counter == null ? 0 : counter.count();
  }

  @Test
  void aPaidOrderNotShippedInTimeIsAtRiskForWhatWasCaptured() {
    String order = created("t_alpha");
    apply("t_alpha", order, new InventoryReservedEvt(order));
    apply(
        "t_alpha",
        order,
        new PaymentProcessedEvt(order, PaymentMethod.PREPAID, true, new Money(1000, "KWD")));

    expire("t_alpha", order);

    awaitState("t_alpha", order, "NeedsAttention");
    assertThat(findings("t_alpha", order))
        .containsExactly("PAID_NOT_FULFILLED|1000|KWD|false|OPEN");
  }

  @Test
  void aReturnNotRestockedInTimeIsAtRiskForTheOrderValueAsAnEstimate() {
    String order = inTransit("t_alpha", PaymentMethod.COD, false, TOTAL);
    apply("t_alpha", order, new RtoStartedEvt(order));
    apply("t_alpha", order, new RtoReceivedEvt(order));

    expire("t_alpha", order);

    awaitState("t_alpha", order, "NeedsAttention");
    assertThat(findings("t_alpha", order)).containsExactly("RTO_NOT_RESTOCKED|1234|KWD|true|OPEN");
  }

  @Test
  void aDeadlineFromAnEarlierStepIsNotFired() {
    String order = created("t_alpha");
    Deadline allocating = deadline("t_alpha", order);
    apply("t_alpha", order, new InventoryReservedEvt(order));

    boolean fired =
        TenantContext.callAs(
            "t_alpha",
            () -> sagas().fireDeadline(order, DeadlineKind.ALLOCATING, allocating.dueAt()));

    assertThat(fired).isFalse();
    assertThat(state("t_alpha", order)).isEqualTo("VerifyingPayment");
    assertThat(exceptions("t_alpha", order)).isEmpty();
  }

  @Test
  void twoScannersFiringTheSameDeadlineParkTheSagaOnce() throws Exception {
    String order = created("t_alpha");
    Deadline due = deadline("t_alpha", order);
    var barrier = new CyclicBarrier(2);

    Supplier<Boolean> scanner =
        () -> {
          awaitBarrier(barrier);
          return TenantContext.callAs(
              "t_alpha", () -> sagas().fireDeadline(order, DeadlineKind.ALLOCATING, due.dueAt()));
        };
    List<Boolean> fired;
    try (var pool = Executors.newFixedThreadPool(2)) {
      CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(scanner, pool);
      CompletableFuture<Boolean> second = CompletableFuture.supplyAsync(scanner, pool);
      fired = List.of(first.join(), second.join());
    }

    assertThat(fired).containsExactlyInAnyOrder(true, false);
    assertThat(stepLog("t_alpha", order)).containsOnlyOnce("Allocating->NeedsAttention");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|DEADLINE_MISSED|1234|KWD");
  }

  @Test
  void aFirstMissedPaymentCheckAsksAgainWithAFreshDeadline() {
    String order = created("t_alpha");
    apply("t_alpha", order, new InventoryReservedEvt(order));
    Deadline first = deadline("t_alpha", order);

    expire("t_alpha", order);

    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> stepLog("t_alpha", order).contains("VerifyingPayment->VerifyingPayment"));
    Deadline second = deadline("t_alpha", order);
    assertThat(second.kind()).isEqualTo("VERIFYING_PAYMENT");
    assertThat(second.dueAt()).isAfter(Instant.now());
    assertThat(second.dueAt()).isNotEqualTo(first.dueAt());
    assertThat(exceptions("t_alpha", order)).isEmpty();
  }

  @Test
  void aStoredDeadlineThatDisagreesWithTheStateIsDroppedNotFiredForever() {
    String order = created("t_alpha");

    as(
        "t_alpha",
        () ->
            jdbc()
                .update(
                    "UPDATE orchestrator.saga_deadline SET kind = 'BOOKING',"
                        + " due_at = now() - interval '1 second' WHERE order_id = ?",
                    order));

    await().atMost(Duration.ofSeconds(30)).until(() -> deadlines("t_alpha", order).isEmpty());
    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
    assertThat(exceptions("t_alpha", order)).isEmpty();
  }

  @Test
  void theScannerCarriesOnPastADeadlineItCannotRead() {
    String unreadable = created("t_alpha");
    String due = created("t_alpha");
    as(
        "t_alpha",
        () ->
            jdbc()
                .update(
                    "UPDATE orchestrator.saga_deadline SET kind = 'NO_SUCH_KIND',"
                        + " due_at = now() - interval '1 minute' WHERE order_id = ?",
                    unreadable));

    expire("t_alpha", due);

    awaitState("t_alpha", due, "NeedsAttention");
    assertThat(state("t_alpha", unreadable)).isEqualTo("Allocating");
  }

  // --- A restart ---

  @Test
  void aDeadlineStoredBeforeARestartFiresAfterIt() throws Exception {
    String order = created("t_alpha");
    orchestrator.close();
    try {

      // Nothing of the old process is left; only the database knows about the deadline.
      try (var connection =
          DriverManager.getConnection(
              POSTGRES.getJdbcUrl(), "orderloop_app", TestDatabase.APP_PASSWORD)) {
        connection
            .createStatement()
            .execute("SELECT set_config('app.tenant_id', 't_alpha', false)");
        var expire =
            connection.prepareStatement(
                "UPDATE orchestrator.saga_deadline SET due_at = now() - interval '1 second'"
                    + " WHERE order_id = ?");
        expire.setString(1, order);
        assertThat(expire.executeUpdate()).isEqualTo(1);
      }
    } finally {
      orchestrator = startOrchestrator();
    }

    awaitState("t_alpha", order, "NeedsAttention");
    assertThat(exceptions("t_alpha", order)).containsExactly("OPEN|DEADLINE_MISSED|1234|KWD");
    assertThat(findings("t_alpha", order)).containsExactly("STUCK_ORDER|1234|KWD|false|OPEN");
  }

  // --- Tenants ---

  @Test
  void eachTenantsMissedDeadlineIsFiredInItsOwnScope() {
    String alpha = created("t_alpha");
    String beta = created("t_beta");

    expire("t_alpha", alpha);
    expire("t_beta", beta);

    awaitState("t_alpha", alpha, "NeedsAttention");
    awaitState("t_beta", beta, "NeedsAttention");
    assertThat(exceptions("t_alpha", alpha)).hasSize(1);
    assertThat(exceptions("t_beta", beta)).hasSize(1);
    assertThat(exceptions("t_beta", alpha)).isEmpty();
    assertThat(findings("t_beta", alpha)).isEmpty();
    assertThat(findings("t_alpha", beta)).isEmpty();
  }

  @Test
  void anotherTenantCannotReadMoveOrDeleteTheDeadline() {
    String order = created("t_alpha");
    Deadline before = deadline("t_alpha", order);

    assertThat(deadlines("t_beta", order)).isEmpty();
    int moved =
        as(
            "t_beta",
            () ->
                jdbc()
                    .update(
                        "UPDATE orchestrator.saga_deadline SET due_at = now() WHERE order_id = ?",
                        order));
    int deleted =
        as(
            "t_beta",
            () ->
                jdbc().update("DELETE FROM orchestrator.saga_deadline WHERE order_id = ?", order));

    assertThat(moved).isZero();
    assertThat(deleted).isZero();
    assertThat(deadline("t_alpha", order)).isEqualTo(before);
    assertThat(state("t_alpha", order)).isEqualTo("Allocating");
  }

  @Test
  void anotherTenantCannotPlantADeadlineForTheFirst() {
    String order = created("t_alpha");
    as(
        "t_alpha",
        () -> jdbc().update("DELETE FROM orchestrator.saga_deadline WHERE order_id = ?", order));

    assertThatThrownBy(
            () ->
                as(
                    "t_beta",
                    () ->
                        jdbc()
                            .update(
                                "INSERT INTO orchestrator.saga_deadline"
                                    + " (tenant_id, order_id, kind, due_at)"
                                    + " VALUES ('t_alpha', ?, 'ALLOCATING', now())",
                                order)))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("row-level security");
    assertThat(deadlines("t_alpha", order)).isEmpty();
  }

  @Test
  void anotherTenantCannotResolveOrDeleteTheFinding() {
    String order = created("t_alpha");
    expire("t_alpha", order);
    awaitState("t_alpha", order, "NeedsAttention");

    int resolved =
        as(
            "t_beta",
            () ->
                jdbc()
                    .update(
                        "UPDATE orchestrator.leak_finding SET status = 'RESOLVED'"
                            + " WHERE order_id = ?",
                        order));
    assertThatThrownBy(
            () ->
                as(
                    "t_beta",
                    () ->
                        jdbc()
                            .update(
                                "DELETE FROM orchestrator.leak_finding WHERE order_id = ?", order)))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("permission denied");

    assertThat(resolved).isZero();
    assertThat(findings("t_alpha", order)).containsExactly("STUCK_ORDER|1234|KWD|false|OPEN");
  }

  @Test
  void theOpsRoleMayOnlyReadDeadlines() throws Exception {
    try (var ops =
        DriverManager.getConnection(
            POSTGRES.getJdbcUrl(), "orderloop_ops", TestDatabase.OPS_PASSWORD)) {
      ops.createStatement().executeQuery("SELECT count(*) FROM orchestrator.saga_deadline");
      for (String write :
          List.of(
              "UPDATE orchestrator.saga_deadline SET due_at = now()",
              "DELETE FROM orchestrator.saga_deadline",
              "SELECT count(*) FROM orchestrator.leak_finding")) {
        assertThatThrownBy(() -> ops.createStatement().execute(write))
            .as(write)
            .isInstanceOf(SQLException.class)
            .hasMessageContaining("permission denied");
      }
    }
  }

  // --- helpers ---

  record Deadline(String kind, Instant dueAt) {}

  private static ConfigurableApplicationContext startOrchestrator() {
    return new SpringApplicationBuilder(OrderOrchestratorApplication.class)
        .run(
            "--server.port=0",
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=orderloop_app",
            "--spring.datasource.password=" + TestDatabase.APP_PASSWORD,
            "--spring.flyway.user=orderloop_owner",
            "--spring.flyway.password=" + TestDatabase.OWNER_PASSWORD,
            "--orderloop.outbox.ops-password=" + TestDatabase.OPS_PASSWORD,
            "--orderloop.outbox.poll-interval=100ms",
            "--orderloop.orchestrator.deadline-poll-interval=100ms",
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers());
  }

  private static SagaService sagas() {
    return orchestrator.getBean(SagaService.class);
  }

  private static JdbcTemplate jdbc() {
    return orchestrator.getBean(JdbcTemplate.class);
  }

  private static String created(String tenant) {
    String order = "o-" + UUID.randomUUID();
    as(tenant, () -> sagas().create(order, TOTAL));
    return order;
  }

  /** Drives a new order to InTransit with the given payment. */
  private static String inTransit(
      String tenant, PaymentMethod method, boolean captured, Money paid) {
    String order = created(tenant);
    apply(tenant, order, new InventoryReservedEvt(order));
    apply(tenant, order, new PaymentProcessedEvt(order, method, captured, paid));
    apply(tenant, order, new ShipmentBookedEvt(order));
    return order;
  }

  private static void apply(String tenant, String order, SagaEvent event) {
    as(
        tenant,
        () -> {
          sagas().apply(order, new Reply(event), UUID.randomUUID());
          return null;
        });
  }

  /** Moves the stored deadline into the past, as time passing would. */
  private static void expire(String tenant, String order) {
    int moved =
        as(
            tenant,
            () ->
                jdbc()
                    .update(
                        "UPDATE orchestrator.saga_deadline SET due_at = now() - interval '1 second'"
                            + " WHERE order_id = ?",
                        order));
    assertThat(moved).isEqualTo(1);
  }

  private static Deadline deadline(String tenant, String order) {
    List<Deadline> found = deadlines(tenant, order);
    assertThat(found).hasSize(1);
    return found.getFirst();
  }

  private static List<Deadline> deadlines(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc()
                .query(
                    "SELECT kind, due_at FROM orchestrator.saga_deadline WHERE order_id = ?",
                    (row, n) ->
                        new Deadline(row.getString("kind"), row.getTimestamp("due_at").toInstant()),
                    order));
  }

  private static String state(String tenant, String order) {
    return as(tenant, () -> sagas().find(order).orElseThrow().state());
  }

  private static void awaitState(String tenant, String order, String expected) {
    await().atMost(Duration.ofSeconds(30)).until(() -> expected.equals(state(tenant, order)));
  }

  private static List<String> stepLog(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc()
                .queryForList(
                    "SELECT from_state || '->' || to_state FROM orchestrator.saga_step_log"
                        + " WHERE order_id = ? ORDER BY seq",
                    String.class,
                    order));
  }

  private static List<String> exceptions(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc()
                .queryForList(
                    "SELECT status || '|' || reason || '|' || amount_at_risk_minor || '|' || currency"
                        + " FROM orchestrator.exception_case WHERE order_id = ?",
                    String.class,
                    order));
  }

  private static List<String> findings(String tenant, String order) {
    return as(
        tenant,
        () ->
            jdbc()
                .queryForList(
                    "SELECT detector || '|' || amount_at_risk_minor || '|' || currency || '|'"
                        + " || is_estimate || '|' || status"
                        + " FROM orchestrator.leak_finding WHERE order_id = ?",
                    String.class,
                    order));
  }

  private static void awaitBarrier(CyclicBarrier barrier) {
    try {
      barrier.await();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static <T> T as(String tenant, ScopedValue.CallableOp<T, RuntimeException> work) {
    return TenantContext.callAs(tenant, work);
  }
}
