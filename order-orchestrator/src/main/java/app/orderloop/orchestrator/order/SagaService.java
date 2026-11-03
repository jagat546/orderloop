package app.orderloop.orchestrator.order;

import app.orderloop.dto.Envelope;
import app.orderloop.dto.Money;
import app.orderloop.dto.SagaCommand;
import app.orderloop.dto.Topics;
import app.orderloop.orchestrator.saga.SagaEngine;
import app.orderloop.orchestrator.saga.SagaInput;
import app.orderloop.orchestrator.saga.SagaInput.OrderAccepted;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.orchestrator.saga.SagaState;
import app.orderloop.orchestrator.saga.SagaState.Received;
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
   * @throws OptimisticLockingFailureException if another transaction moved the saga after it was
   *     read; nothing is written, and a retry decides against the state that won
   * @throws UnknownOrderException if the bound tenant has no saga for the order
   */
  @Transactional
  public void apply(String orderId, SagaInput input, UUID causeEventId) {
    String tenant = TenantContext.require();
    OrderSaga saga = sagas.findById(orderId).orElseThrow(() -> new UnknownOrderException(orderId));
    Instant now = clock.instant();
    Transition transition = SagaEngine.decide(SagaStateJson.read(saga.stateData()), input, now);

    if (transition.ignored()) {
      meters
          .counter("orderloop.saga.ignored", "state", saga.state(), "input", nameOf(input))
          .increment();
      log.info("Ignored {} for order {} in {}", nameOf(input), orderId, saga.state());
      return;
    }
    if (transition.finding().isPresent()) {
      // shortcut: only a missed deadline opens a finding, and deadlines are not stored or fired
      // until M1.13, which adds leak_finding. Refuse rather than lose a finding.
      throw new IllegalStateException("Findings are not stored yet (M1.13)");
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
    // shortcut: transition.deadline() is not stored until M1.13 adds saga_deadline. Exceptions for
    // NeedsAttention are opened from M1.10. The saga row still shows the state.
    log.info("Order {} moved from {} to {}", orderId, saga.state(), to);
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
