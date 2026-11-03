package app.orderloop.orchestrator.order;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Money;
import app.orderloop.dto.SagaCommand;
import app.orderloop.dto.Topics;
import app.orderloop.orchestrator.saga.DeadlineKind;
import app.orderloop.orchestrator.saga.Finding;
import app.orderloop.orchestrator.saga.SagaEngine;
import app.orderloop.orchestrator.saga.SagaInput;
import app.orderloop.orchestrator.saga.SagaInput.DeadlinePassed;
import app.orderloop.orchestrator.saga.SagaInput.OrderAccepted;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.orchestrator.saga.SagaState;
import app.orderloop.orchestrator.saga.SagaState.Allocating;
import app.orderloop.orchestrator.saga.SagaState.AwaitingSettlement;
import app.orderloop.orchestrator.saga.SagaState.Booking;
import app.orderloop.orchestrator.saga.SagaState.Closed;
import app.orderloop.orchestrator.saga.SagaState.ClosedCancelled;
import app.orderloop.orchestrator.saga.SagaState.ClosedRto;
import app.orderloop.orchestrator.saga.SagaState.InTransit;
import app.orderloop.orchestrator.saga.SagaState.NdrPending;
import app.orderloop.orchestrator.saga.SagaState.NeedsAttention;
import app.orderloop.orchestrator.saga.SagaState.Payment;
import app.orderloop.orchestrator.saga.SagaState.Received;
import app.orderloop.orchestrator.saga.SagaState.RefundProposed;
import app.orderloop.orchestrator.saga.SagaState.Refunding;
import app.orderloop.orchestrator.saga.SagaState.Releasing;
import app.orderloop.orchestrator.saga.SagaState.Restocking;
import app.orderloop.orchestrator.saga.SagaState.Returning;
import app.orderloop.orchestrator.saga.SagaState.VerifyingPayment;
import app.orderloop.orchestrator.saga.Transition;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies {@link SagaEngine} decisions. Each transition commits in one transaction: the saga row
 * (only if no one moved it since it was read), its step log row, and its commands in the outbox.
 * Callers bind the tenant before the transaction starts.
 */
@Service
public class SagaService {

  private static final Logger log = LoggerFactory.getLogger(SagaService.class);

  private static final String INSERT =
      "INSERT INTO orchestrator.order_saga"
          + " (tenant_id, order_id, state, state_data, order_total_minor, currency)"
          + " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";
  private static final String MOVE =
      "UPDATE orchestrator.order_saga SET state = ?, state_data = ?, version = version + 1,"
          + " updated_at = ? WHERE order_id = ? AND version = ?";
  private static final String LOG_STEP =
      "INSERT INTO orchestrator.saga_step_log"
          + " (tenant_id, order_id, seq, from_state, to_state, cause_event_id, early, at)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  // The partial unique index allows one open exception per order. A second one is drift, so it
  // fails and rolls the transition back rather than vanishing.
  private static final String OPEN_EXCEPTION =
      "INSERT INTO orchestrator.exception_case (tenant_id, case_id, order_id, reason,"
          + " amount_at_risk_minor, currency, status, opened_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?)";
  private static final String RESOLVE_EXCEPTION =
      "UPDATE orchestrator.exception_case SET status = 'RESOLVED', resolved_at = ?,"
          + " resolved_by = ? WHERE order_id = ? AND status = 'OPEN'";
  private static final String SET_DEADLINE =
      "INSERT INTO orchestrator.saga_deadline (tenant_id, order_id, kind, due_at)"
          + " VALUES (?, ?, ?, ?) ON CONFLICT (tenant_id, order_id)"
          + " DO UPDATE SET kind = EXCLUDED.kind, due_at = EXCLUDED.due_at";
  private static final String CLEAR_DEADLINE =
      "DELETE FROM orchestrator.saga_deadline WHERE order_id = ?";
  // Locks the saga row first, the order a reply takes its locks in (saga, then deadline), so a
  // reply
  // and a firing at the same moment queue up rather than deadlock.
  private static final String LOCK_SAGA =
      "SELECT 1 FROM orchestrator.order_saga WHERE order_id = ? FOR UPDATE";
  // Locks the deadline, so of two scanners firing it, the second waits and then finds it gone.
  private static final String LOCK_DUE_DEADLINE =
      "SELECT 1 FROM orchestrator.saga_deadline WHERE order_id = ? AND kind = ? AND due_at = ?"
          + " FOR UPDATE";
  // The same leak found again, after an operator retried the step, is the same finding reopened.
  private static final String OPEN_FINDING =
      "INSERT INTO orchestrator.leak_finding (tenant_id, detector, order_id,"
          + " amount_at_risk_minor, currency, is_estimate, status, detected_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?) ON CONFLICT (tenant_id, detector, order_id)"
          + " DO UPDATE SET amount_at_risk_minor = EXCLUDED.amount_at_risk_minor,"
          + " currency = EXCLUDED.currency, is_estimate = EXCLUDED.is_estimate,"
          + " recovered_minor = 0, status = 'OPEN', detected_at = EXCLUDED.detected_at";

  private final OrderSagaRepository sagas;
  private final JdbcTemplate jdbc;
  private final OutboxWriter outbox;
  private final Clock clock;
  private final MeterRegistry meters;

  @PersistenceContext private EntityManager em;

  SagaService(
      OrderSagaRepository sagas,
      JdbcTemplate jdbc,
      OutboxWriter outbox,
      Clock clock,
      MeterRegistry meters) {
    this.sagas = sagas;
    this.jdbc = jdbc;
    this.outbox = outbox;
    this.clock = clock;
    this.meters = meters;
  }

  public record Created(SagaView saga, boolean isNew) {}

  /**
   * Starts the saga of an order Shopify accepted. A repeat, even a concurrent one, finds the saga
   * already there and returns it without emitting anything again.
   */
  @Transactional
  public Created create(String orderId, Money total) {
    String tenant = TenantContext.require();
    SagaState received = new Received(orderId);
    boolean isNew =
        jdbc.update(
                INSERT,
                tenant,
                orderId,
                SagaStateJson.name(received),
                SagaStateJson.write(received),
                total.amountMinor(),
                total.currency())
            == 1;
    if (isNew) {
      apply(orderId, new OrderAccepted(), null);
    }
    return new Created(find(orderId).orElseThrow(), isNew);
  }

  /**
   * Decides an input against the saga's current state and applies the result.
   *
   * @param causeEventId the event that caused this, or null for an action with no event
   * @return false if the input was ignored as late, duplicate or stale, so nothing changed
   * @throws OptimisticLockingFailureException if another transaction moved the saga after it was
   *     read; nothing is written, and a retry decides against the state that won
   * @throws UnknownOrderException if the bound tenant has no saga for the order
   */
  @Transactional
  public boolean apply(String orderId, SagaInput input, UUID causeEventId) {
    String tenant = TenantContext.require();
    OrderSaga saga = sagas.findById(orderId).orElseThrow(() -> new UnknownOrderException(orderId));
    Instant now = clock.instant();
    SagaState current = SagaStateJson.read(saga.stateData());
    Transition transition = SagaEngine.decide(current, input, now);

    if (transition.ignored()) {
      meters
          .counter("orderloop.saga.ignored", "state", saga.state(), "input", nameOf(input))
          .increment();
      log.info("Ignored {} for order {} in {}", nameOf(input), orderId, saga.state());
      return false;
    }
    SagaState next = transition.next();
    String to = SagaStateJson.name(next);
    if (jdbc.update(
            MOVE, to, SagaStateJson.write(next), Timestamp.from(now), orderId, saga.version())
        == 0) {
      throw new OptimisticLockingFailureException(
          "Order " + orderId + " moved on since version " + saga.version());
    }
    em.detach(saga); // A later read in this transaction sees the row as written.
    jdbc.update(
        LOG_STEP,
        tenant,
        orderId,
        saga.version() + 1,
        saga.state(),
        to,
        causeEventId,
        transition.early(),
        Timestamp.from(now));
    for (SagaCommand command : transition.commands()) {
      outbox.append(
          Topics.of(command), new Envelope(UUID.randomUUID(), tenant, causeEventId, command));
    }
    // An exception is open exactly while the saga is parked.
    if (next instanceof NeedsAttention parked && !(current instanceof NeedsAttention)) {
      Money atRisk = saga.total(); // The order value, as for a stuck order (docs/PRODUCT.md).
      jdbc.update(
          OPEN_EXCEPTION,
          tenant,
          UUID.randomUUID(),
          orderId,
          parked.reason().name(),
          atRisk.amountMinor(),
          atRisk.currency(),
          Timestamp.from(now));
      log.warn("Order {} needs attention: {}", orderId, parked.reason());
    } else if (current instanceof NeedsAttention && !(next instanceof NeedsAttention)) {
      int resolved = jdbc.update(RESOLVE_EXCEPTION, Timestamp.from(now), nameOf(input), orderId);
      if (resolved != 1) {
        // Rolls the transition back: the saga stays parked, and the failure is retried, then
        // dead-lettered, where someone sees it.
        throw new IllegalStateException(
            "Order " + orderId + " left NeedsAttention with " + resolved + " open exception(s)");
      }
    }
    // A step that only recorded a fact, such as an early settlement, keeps the running deadline.
    if (!transition.keepsDeadline()) {
      transition
          .deadline()
          .ifPresentOrElse(
              due ->
                  jdbc.update(
                      SET_DEADLINE,
                      tenant,
                      orderId,
                      due.kind().name(),
                      Timestamp.from(due.dueAt())),
              () -> jdbc.update(CLEAR_DEADLINE, orderId));
    }
    transition
        .finding()
        .ifPresent(
            finding -> {
              Money atRisk = atRisk(finding, next, saga.total());
              jdbc.update(
                  OPEN_FINDING,
                  tenant,
                  finding.name(),
                  orderId,
                  atRisk.amountMinor(),
                  atRisk.currency(),
                  finding == Finding.RTO_NOT_RESTOCKED, // Order value, as no stock cost is known.
                  Timestamp.from(now));
              log.warn("Order {} has money at risk: {}", orderId, finding);
            });
    log.info("Order {} moved from {} to {}", orderId, saga.state(), to);
    return true;
  }

  /**
   * Fires a deadline the scanner found due, if it is still the saga's deadline. A deadline replaced
   * or fired since the scan, by another scanner or a transition, is stale and does nothing.
   *
   * @return whether the deadline was fired
   */
  @Transactional
  public boolean fireDeadline(String orderId, DeadlineKind kind, Instant dueAt) {
    if (jdbc.queryForList(LOCK_SAGA, orderId).isEmpty()
        || jdbc.queryForList(LOCK_DUE_DEADLINE, orderId, kind.name(), Timestamp.from(dueAt))
            .isEmpty()) {
      log.debug("Deadline {} for order {} is no longer due", kind, orderId);
      return false;
    }
    if (!apply(orderId, new DeadlinePassed(kind), null)) {
      // The stored deadline disagrees with the saga's state. Drop it, or it would fire every poll.
      jdbc.update(CLEAR_DEADLINE, orderId);
      log.warn("Deadline {} for order {} did not match its state and was dropped", kind, orderId);
      return false;
    }
    return true;
  }

  /**
   * The amount a finding puts at risk (docs/PRODUCT.md): what was paid for a paid-not-fulfilled or
   * COD remittance finding, otherwise the order value.
   */
  private static Money atRisk(Finding finding, SagaState next, Money orderValue) {
    return switch (finding) {
      case PAID_NOT_FULFILLED, COD_REMITTANCE_GAP ->
          payment(next).map(Payment::amount).orElse(orderValue);
      case NDR_PAST_SLA, RTO_NOT_RESTOCKED, STUCK_ORDER -> orderValue;
    };
  }

  private static Optional<Payment> payment(SagaState state) {
    return switch (state) {
      case NeedsAttention parked -> payment(parked.prior());
      case Booking s -> Optional.of(s.payment());
      case InTransit s -> Optional.of(s.payment());
      case NdrPending s -> Optional.of(s.payment());
      case Returning s -> Optional.of(s.payment());
      case Restocking s -> Optional.of(s.payment());
      case RefundProposed s -> Optional.of(s.payment());
      case Refunding s -> Optional.of(s.payment());
      case AwaitingSettlement s -> Optional.of(s.payment());
      case Releasing s -> Optional.ofNullable(s.lateCapture());
      case Received s -> Optional.empty();
      case Allocating s -> Optional.empty();
      case VerifyingPayment s -> Optional.empty();
      case Closed s -> Optional.empty();
      case ClosedCancelled s -> Optional.empty();
      case ClosedRto s -> Optional.empty();
    };
  }

  @Transactional(readOnly = true)
  public Optional<SagaView> find(String orderId) {
    return sagas
        .findById(orderId)
        .map(saga -> new SagaView(saga.orderId(), saga.state(), saga.total()));
  }

  private static String nameOf(SagaInput input) {
    return input instanceof Reply(var event)
        ? event.getClass().getSimpleName()
        : input.getClass().getSimpleName();
  }
}
