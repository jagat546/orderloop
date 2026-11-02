package app.orderloop.orchestrator.saga;

import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.DEADLINE;
import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.EARLY;
import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.FAILURE;
import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.LATE;
import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.RETRY;
import static app.orderloop.orchestrator.saga.SagaEngineTest.Kind.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaCommand;
import app.orderloop.dto.SagaCommand.BookShipmentCmd;
import app.orderloop.dto.SagaCommand.ProcessPaymentCmd;
import app.orderloop.dto.SagaCommand.ReattemptDeliveryCmd;
import app.orderloop.dto.SagaCommand.RefundPaymentCmd;
import app.orderloop.dto.SagaCommand.ReleaseInventoryCmd;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaCommand.RestockInventoryCmd;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.DeliveredEvt;
import app.orderloop.dto.SagaEvent.DeliveryFailedEvt;
import app.orderloop.dto.SagaEvent.InventoryFailedEvt;
import app.orderloop.dto.SagaEvent.InventoryReleasedEvt;
import app.orderloop.dto.SagaEvent.InventoryReservedEvt;
import app.orderloop.dto.SagaEvent.InventoryRestockedEvt;
import app.orderloop.dto.SagaEvent.PaymentFailedEvt;
import app.orderloop.dto.SagaEvent.PaymentProcessedEvt;
import app.orderloop.dto.SagaEvent.RefundCompletedEvt;
import app.orderloop.dto.SagaEvent.RefundFailedEvt;
import app.orderloop.dto.SagaEvent.RtoReceivedEvt;
import app.orderloop.dto.SagaEvent.RtoStartedEvt;
import app.orderloop.dto.SagaEvent.SettlementObservedEvt;
import app.orderloop.dto.SagaEvent.ShipmentBookedEvt;
import app.orderloop.dto.SagaEvent.ShipmentFailedEvt;
import app.orderloop.orchestrator.saga.SagaInput.DeadlinePassed;
import app.orderloop.orchestrator.saga.SagaInput.OrderAccepted;
import app.orderloop.orchestrator.saga.SagaInput.ReattemptApproved;
import app.orderloop.orchestrator.saga.SagaInput.RefundApproved;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.orchestrator.saga.SagaInput.RestockApproved;
import app.orderloop.orchestrator.saga.SagaInput.RetryStep;
import app.orderloop.orchestrator.saga.SagaInput.ReturnDecided;
import app.orderloop.orchestrator.saga.SagaState.AfterRefund;
import app.orderloop.orchestrator.saga.SagaState.Allocating;
import app.orderloop.orchestrator.saga.SagaState.AttentionReason;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Table-driven tests over {@link SagaEngine#decide}, one block per row of docs/SAGA.md. No Spring,
 * database, broker or clock: the engine is a pure function.
 */
class SagaEngineTest {

  enum Kind {
    SUCCESS,
    FAILURE,
    LATE,
    DEADLINE,
    EARLY,
    RETRY
  }

  private static final Instant NOW = Instant.parse("2026-11-02T10:00:00Z");
  private static final String O = "o-1";
  private static final String REFUND_ID = "rf-" + O;
  private static final Money TOTAL = new Money(149900, "INR");
  private static final Payment PAID = new Payment(PaymentMethod.PREPAID, true, TOTAL);
  private static final Payment COD = new Payment(PaymentMethod.COD, false, TOTAL);
  private static final Payment UNCAPTURED = new Payment(PaymentMethod.PREPAID, false, TOTAL);
  private static final Payment PAID_KWD =
      new Payment(PaymentMethod.PREPAID, true, new Money(1234, "KWD"));

  // Releasing after a failed payment may still learn the payment was captured; other paths may not.
  private static final Releasing RELEASING_AFTER_FAILED_PAYMENT = new Releasing(O, true, null);
  private static final Releasing RELEASING = new Releasing(O, false, null);

  static Stream<Row> rows() {
    return Stream.of(
        // Received: OrderAccepted starts the saga.
        row(SUCCESS, new Received(O), new OrderAccepted(), new Allocating(O))
            .emits(new ReserveInventoryCmd(O)),
        ignored(LATE, new Received(O), reply(new InventoryReservedEvt(O))),

        // Allocating
        row(
                SUCCESS,
                new Allocating(O),
                reply(new InventoryReservedEvt(O)),
                new VerifyingPayment(O, 1))
            .emits(new ProcessPaymentCmd(O)),
        row(
            FAILURE,
            new Allocating(O),
            reply(new InventoryFailedEvt(O, "OUT_OF_STOCK")),
            attention(new Allocating(O), AttentionReason.INVENTORY_FAILED)),
        ignored(LATE, new Allocating(O), reply(processed(PAID))),
        missed(new Allocating(O), DeadlineKind.ALLOCATING, Finding.STUCK_ORDER),
        ignored(LATE, new Allocating(O), new DeadlinePassed(DeadlineKind.BOOKING)),
        ignored(LATE, new Allocating(O), new RetryStep()),

        // VerifyingPayment
        row(SUCCESS, new VerifyingPayment(O, 1), reply(processed(PAID)), new Booking(O, PAID))
            .emits(new BookShipmentCmd(O)),
        row(SUCCESS, new VerifyingPayment(O, 1), reply(processed(COD)), new Booking(O, COD))
            .emits(new BookShipmentCmd(O)),
        row(
            FAILURE,
            new VerifyingPayment(O, 1),
            reply(processed(UNCAPTURED)),
            attention(new VerifyingPayment(O, 1), AttentionReason.PAYMENT_NOT_CAPTURED)),
        row(
                FAILURE,
                new VerifyingPayment(O, 1),
                reply(new PaymentFailedEvt(O, "NOT_CAPTURED")),
                RELEASING_AFTER_FAILED_PAYMENT)
            .emits(new ReleaseInventoryCmd(O)),
        ignored(LATE, new VerifyingPayment(O, 1), reply(new InventoryReservedEvt(O))),
        row(
                DEADLINE,
                new VerifyingPayment(O, 1),
                new DeadlinePassed(DeadlineKind.VERIFYING_PAYMENT),
                new VerifyingPayment(O, 2))
            .emits(new ProcessPaymentCmd(O)),
        missed(new VerifyingPayment(O, 2), DeadlineKind.VERIFYING_PAYMENT, Finding.STUCK_ORDER),

        // Booking
        row(SUCCESS, new Booking(O, PAID), reply(new ShipmentBookedEvt(O)), new InTransit(O, PAID)),
        row(
            FAILURE,
            new Booking(O, PAID),
            reply(new ShipmentFailedEvt(O, "CARRIER_REJECTED")),
            new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK)),
        row(
                FAILURE,
                new Booking(O, COD),
                reply(new ShipmentFailedEvt(O, "CARRIER_REJECTED")),
                RELEASING)
            .emits(new ReleaseInventoryCmd(O)),
        ignored(LATE, new Booking(O, PAID), reply(processed(PAID))),
        // No automatic refund: the money at risk is flagged for a human.
        missed(new Booking(O, PAID), DeadlineKind.BOOKING, Finding.PAID_NOT_FULFILLED),
        missed(new Booking(O, COD), DeadlineKind.BOOKING, Finding.STUCK_ORDER),
        row(
                EARLY,
                new Booking(O, PAID),
                reply(new DeliveredEvt(O)),
                new AwaitingSettlement(O, PAID))
            .movedEarly(),
        row(
                EARLY,
                new Booking(O, PAID),
                reply(new DeliveryFailedEvt(O, "CUSTOMER_UNAVAILABLE")),
                new NdrPending(O, PAID))
            .movedEarly(),
        row(EARLY, new Booking(O, PAID), reply(new RtoStartedEvt(O)), new Returning(O, PAID))
            .movedEarly(),
        row(
                EARLY,
                new Booking(O, PAID),
                reply(new RtoReceivedEvt(O)),
                new Restocking(O, PAID, false))
            .movedEarly(),

        // Releasing: a compensation
        row(SUCCESS, RELEASING, reply(new InventoryReleasedEvt(O)), new ClosedCancelled(O, false)),
        row(
            SUCCESS,
            RELEASING_AFTER_FAILED_PAYMENT,
            reply(new InventoryReleasedEvt(O)),
            new ClosedCancelled(O, true)),
        row(
            FAILURE,
            RELEASING,
            reply(new InventoryFailedEvt(O, "LOCATION_GONE")),
            attention(RELEASING, AttentionReason.RELEASE_FAILED)),
        ignored(LATE, RELEASING, reply(new PaymentFailedEvt(O, "NOT_CAPTURED"))),
        missed(RELEASING, DeadlineKind.RELEASING, Finding.STUCK_ORDER),
        // The payment was captured after all: never cancel silently with the customer's money.
        row(
            LATE,
            RELEASING_AFTER_FAILED_PAYMENT,
            reply(processed(PAID)),
            new Releasing(O, true, PAID)),
        row(
            SUCCESS,
            new Releasing(O, true, PAID),
            reply(new InventoryReleasedEvt(O)),
            new RefundProposed(O, PAID, AfterRefund.CLOSE_CANCELLED)),
        ignored(LATE, new Releasing(O, true, PAID), reply(processed(PAID))),
        ignored(LATE, RELEASING_AFTER_FAILED_PAYMENT, reply(processed(COD))),
        // A duplicate of a payment that was already refunded or was COD.
        ignored(LATE, RELEASING, reply(processed(PAID))),

        // InTransit
        row(
            SUCCESS,
            new InTransit(O, PAID),
            reply(new DeliveredEvt(O)),
            new AwaitingSettlement(O, PAID)),
        row(
            FAILURE,
            new InTransit(O, PAID),
            reply(new DeliveryFailedEvt(O, "CUSTOMER_UNAVAILABLE")),
            new NdrPending(O, PAID)),
        row(FAILURE, new InTransit(O, PAID), reply(new RtoStartedEvt(O)), new Returning(O, PAID)),
        row(
                EARLY,
                new InTransit(O, PAID),
                reply(new RtoReceivedEvt(O)),
                new Restocking(O, PAID, false))
            .movedEarly(),
        ignored(LATE, new InTransit(O, PAID), reply(new ShipmentBookedEvt(O))),
        missed(new InTransit(O, PAID), DeadlineKind.IN_TRANSIT, Finding.STUCK_ORDER),

        // NdrPending
        row(SUCCESS, new NdrPending(O, PAID), new ReattemptApproved(), new InTransit(O, PAID))
            .emits(new ReattemptDeliveryCmd(O)),
        row(
            SUCCESS,
            new NdrPending(O, PAID),
            reply(new DeliveredEvt(O)),
            new AwaitingSettlement(O, PAID)),
        row(FAILURE, new NdrPending(O, PAID), new ReturnDecided(), new Returning(O, PAID)),
        row(FAILURE, new NdrPending(O, PAID), reply(new RtoStartedEvt(O)), new Returning(O, PAID)),
        row(
                EARLY,
                new NdrPending(O, PAID),
                reply(new RtoReceivedEvt(O)),
                new Restocking(O, PAID, false))
            .movedEarly(),
        ignored(LATE, new NdrPending(O, PAID), reply(new DeliveryFailedEvt(O, "AGAIN"))),
        missed(new NdrPending(O, PAID), DeadlineKind.NDR_PENDING, Finding.NDR_PAST_SLA),

        // Returning: no failure reply; a missed deadline is how it fails.
        row(
            SUCCESS,
            new Returning(O, PAID),
            reply(new RtoReceivedEvt(O)),
            new Restocking(O, PAID, false)),
        ignored(LATE, new Returning(O, PAID), reply(new RtoStartedEvt(O))),
        missed(new Returning(O, PAID), DeadlineKind.RETURNING, Finding.STUCK_ORDER),

        // Restocking
        row(
                SUCCESS,
                new Restocking(O, PAID, false),
                new RestockApproved(),
                new Restocking(O, PAID, true))
            .emits(new RestockInventoryCmd(O)),
        row(
            SUCCESS,
            new Restocking(O, PAID, true),
            reply(new InventoryRestockedEvt(O)),
            new RefundProposed(O, PAID, AfterRefund.CLOSE_RTO)),
        // The merchant restocked without OrderLoop: observed, and the refund still follows.
        row(
            SUCCESS,
            new Restocking(O, PAID, false),
            reply(new InventoryRestockedEvt(O)),
            new RefundProposed(O, PAID, AfterRefund.CLOSE_RTO)),
        row(
            SUCCESS,
            new Restocking(O, COD, true),
            reply(new InventoryRestockedEvt(O)),
            new ClosedRto(O)),
        row(
            FAILURE,
            new Restocking(O, PAID, true),
            reply(new InventoryFailedEvt(O, "NO_LOCATION")),
            attention(new Restocking(O, PAID, true), AttentionReason.RESTOCK_FAILED)),
        ignored(LATE, new Restocking(O, PAID, true), new RestockApproved()),
        ignored(LATE, new Restocking(O, PAID, false), reply(new RtoReceivedEvt(O))),
        missed(new Restocking(O, PAID, false), DeadlineKind.RESTOCKING, Finding.RTO_NOT_RESTOCKED),

        // RefundProposed: waits for a human; a missed deadline is how it fails.
        row(
                SUCCESS,
                new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK),
                new RefundApproved(),
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK))
            .emits(new RefundPaymentCmd(O, REFUND_ID, TOTAL)),
        row(
                SUCCESS,
                new RefundProposed(O, PAID_KWD, AfterRefund.CLOSE_RTO),
                new RefundApproved(),
                new Refunding(O, PAID_KWD, AfterRefund.CLOSE_RTO))
            .emits(new RefundPaymentCmd(O, REFUND_ID, new Money(1234, "KWD"))),
        ignored(
            LATE,
            new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK),
            reply(new ShipmentFailedEvt(O, "CARRIER_REJECTED"))),
        missed(
            new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK),
            DeadlineKind.REFUND_PROPOSED,
            Finding.PAID_NOT_FULFILLED),

        // Refunding
        row(
                SUCCESS,
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK),
                reply(new RefundCompletedEvt(O, REFUND_ID, TOTAL)),
                RELEASING)
            .emits(new ReleaseInventoryCmd(O)),
        row(
            SUCCESS,
            new Refunding(O, PAID, AfterRefund.CLOSE_RTO),
            reply(new RefundCompletedEvt(O, REFUND_ID, TOTAL)),
            new ClosedRto(O)),
        row(
            SUCCESS,
            new Refunding(O, PAID, AfterRefund.CLOSE_CANCELLED),
            reply(new RefundCompletedEvt(O, REFUND_ID, TOTAL)),
            new ClosedCancelled(O, false)),
        row(
            FAILURE,
            new Refunding(O, PAID, AfterRefund.RELEASE_STOCK),
            reply(new RefundFailedEvt(O, REFUND_ID, "PROVIDER_DECLINED")),
            attention(
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK), AttentionReason.REFUND_FAILED)),
        ignored(LATE, new Refunding(O, PAID, AfterRefund.RELEASE_STOCK), new RefundApproved()),
        missed(
            new Refunding(O, PAID, AfterRefund.RELEASE_STOCK),
            DeadlineKind.REFUNDING,
            Finding.STUCK_ORDER),

        // AwaitingSettlement: a missed deadline is how it fails.
        row(
            SUCCESS,
            new AwaitingSettlement(O, COD),
            reply(new SettlementObservedEvt(O, TOTAL)),
            new Closed(O)),
        ignored(LATE, new AwaitingSettlement(O, COD), reply(new DeliveredEvt(O))),
        missed(
            new AwaitingSettlement(O, COD),
            DeadlineKind.AWAITING_SETTLEMENT,
            Finding.COD_REMITTANCE_GAP),
        missed(
            new AwaitingSettlement(O, PAID), DeadlineKind.AWAITING_SETTLEMENT, Finding.STUCK_ORDER),

        // NeedsAttention: anything but a deadline is decided against the prior state.
        row(
            SUCCESS,
            attention(new Booking(O, PAID), AttentionReason.DEADLINE_MISSED),
            reply(new ShipmentBookedEvt(O)),
            new InTransit(O, PAID)),
        row(
                SUCCESS,
                attention(
                    new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK),
                    AttentionReason.DEADLINE_MISSED),
                new RefundApproved(),
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK))
            .emits(new RefundPaymentCmd(O, REFUND_ID, TOTAL)),
        row(
                SUCCESS,
                attention(new VerifyingPayment(O, 1), AttentionReason.PAYMENT_NOT_CAPTURED),
                reply(processed(PAID)),
                new Booking(O, PAID))
            .emits(new BookShipmentCmd(O)),
        ignored(
            LATE,
            attention(new Booking(O, PAID), AttentionReason.DEADLINE_MISSED),
            reply(processed(PAID))),
        ignored(
            LATE,
            attention(new Booking(O, PAID), AttentionReason.DEADLINE_MISSED),
            new DeadlinePassed(DeadlineKind.BOOKING)),
        // A duplicate failure while parked must not open a second exception.
        ignored(
            LATE,
            attention(RELEASING, AttentionReason.RELEASE_FAILED),
            reply(new InventoryFailedEvt(O, "LOCATION_GONE"))),
        ignored(
            LATE,
            attention(
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK), AttentionReason.REFUND_FAILED),
            reply(new RefundFailedEvt(O, REFUND_ID, "PROVIDER_DECLINED"))),

        // RetryStep: an operator re-runs the parked step.
        row(
                RETRY,
                attention(new Allocating(O), AttentionReason.INVENTORY_FAILED),
                new RetryStep(),
                new Allocating(O))
            .emits(new ReserveInventoryCmd(O)),
        row(
                RETRY,
                attention(new VerifyingPayment(O, 2), AttentionReason.DEADLINE_MISSED),
                new RetryStep(),
                new VerifyingPayment(O, 1))
            .emits(new ProcessPaymentCmd(O)),
        row(
                RETRY,
                attention(new Booking(O, PAID), AttentionReason.DEADLINE_MISSED),
                new RetryStep(),
                new Booking(O, PAID))
            .emits(new BookShipmentCmd(O)),
        row(
                RETRY,
                attention(RELEASING_AFTER_FAILED_PAYMENT, AttentionReason.RELEASE_FAILED),
                new RetryStep(),
                RELEASING_AFTER_FAILED_PAYMENT)
            .emits(new ReleaseInventoryCmd(O)),
        row(
                RETRY,
                attention(new Restocking(O, PAID, true), AttentionReason.RESTOCK_FAILED),
                new RetryStep(),
                new Restocking(O, PAID, true))
            .emits(new RestockInventoryCmd(O)),
        row(
                RETRY,
                attention(
                    new Refunding(O, PAID, AfterRefund.RELEASE_STOCK),
                    AttentionReason.REFUND_FAILED),
                new RetryStep(),
                new Refunding(O, PAID, AfterRefund.RELEASE_STOCK))
            .emits(new RefundPaymentCmd(O, REFUND_ID, TOTAL)),
        // A proposal still needs its approval; retrying only restarts the wait.
        row(
            RETRY,
            attention(
                new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK),
                AttentionReason.DEADLINE_MISSED),
            new RetryStep(),
            new RefundProposed(O, PAID, AfterRefund.RELEASE_STOCK)),
        row(
            RETRY,
            attention(new InTransit(O, PAID), AttentionReason.DEADLINE_MISSED),
            new RetryStep(),
            new InTransit(O, PAID)),
        ignored(LATE, new Booking(O, PAID), new RetryStep()),

        // Terminal states ignore everything, except a payment captured after a cancel.
        ignored(LATE, new Closed(O), reply(new SettlementObservedEvt(O, TOTAL))),
        ignored(LATE, new ClosedCancelled(O, false), reply(new InventoryReleasedEvt(O))),
        ignored(LATE, new ClosedCancelled(O, false), reply(processed(PAID))),
        ignored(LATE, new ClosedCancelled(O, true), reply(processed(COD))),
        row(
                LATE,
                new ClosedCancelled(O, true),
                reply(processed(PAID)),
                new RefundProposed(O, PAID, AfterRefund.CLOSE_CANCELLED))
            .finding(Finding.PAID_NOT_FULFILLED),
        ignored(LATE, new ClosedRto(O), new RefundApproved()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("rows")
  void decides(Row row) {
    Transition transition = SagaEngine.decide(row.given(), row.input(), NOW);

    assertThat(transition.ignored()).as("ignored").isEqualTo(row.ignored());
    assertThat(transition.next()).as("next state").isEqualTo(row.next());
    assertThat(transition.commands()).as("commands").containsExactlyElementsOf(row.commands());
    assertThat(transition.finding()).as("finding").isEqualTo(Optional.ofNullable(row.finding()));
    assertThat(transition.early()).as("early").isEqualTo(row.early());
    if (!row.ignored()) {
      assertThat(transition.deadline()).as("deadline").isEqualTo(expectedDeadline(row.next()));
    }
  }

  /** The deadlines in docs/SAGA.md, kind and duration both written out, not taken from the code. */
  private static final Map<Class<? extends SagaState>, Deadline> SAGA_MD_DEADLINES =
      new LinkedHashMap<>();

  static {
    expect(Allocating.class, DeadlineKind.ALLOCATING, Duration.ofMinutes(5));
    expect(VerifyingPayment.class, DeadlineKind.VERIFYING_PAYMENT, Duration.ofMinutes(15));
    expect(Booking.class, DeadlineKind.BOOKING, Duration.ofHours(2));
    expect(Releasing.class, DeadlineKind.RELEASING, Duration.ofMinutes(15));
    expect(InTransit.class, DeadlineKind.IN_TRANSIT, Duration.ofDays(10));
    expect(NdrPending.class, DeadlineKind.NDR_PENDING, Duration.ofHours(24));
    expect(Returning.class, DeadlineKind.RETURNING, Duration.ofDays(15));
    expect(Restocking.class, DeadlineKind.RESTOCKING, Duration.ofHours(48));
    expect(RefundProposed.class, DeadlineKind.REFUND_PROPOSED, Duration.ofHours(24));
    expect(Refunding.class, DeadlineKind.REFUNDING, Duration.ofMinutes(30));
    expect(AwaitingSettlement.class, DeadlineKind.AWAITING_SETTLEMENT, Duration.ofDays(10));
  }

  private static void expect(Class<? extends SagaState> state, DeadlineKind kind, Duration after) {
    SAGA_MD_DEADLINES.put(state, new Deadline(kind, NOW.plus(after)));
  }

  /** The states whose row in docs/SAGA.md names a failure reply or action of its own. */
  private static final Set<Class<? extends SagaState>> STATES_WITH_A_FAILURE_REPLY =
      Set.of(
          Allocating.class,
          VerifyingPayment.class,
          Booking.class,
          Releasing.class,
          InTransit.class,
          NdrPending.class,
          Restocking.class,
          Refunding.class);

  private static Optional<Deadline> expectedDeadline(SagaState state) {
    return Optional.ofNullable(SAGA_MD_DEADLINES.get(state.getClass()));
  }

  @Test
  void onlyReceivedNeedsAttentionAndTerminalStatesHaveNoDeadline() {
    Set<Class<?>> withoutDeadline =
        Set.of(
            Received.class,
            NeedsAttention.class,
            Closed.class,
            ClosedCancelled.class,
            ClosedRto.class);
    for (Class<?> state : SagaState.class.getPermittedSubclasses()) {
      assertThat(SAGA_MD_DEADLINES.containsKey(state) || withoutDeadline.contains(state))
          .as(state.getSimpleName() + " is listed")
          .isTrue();
    }
  }

  @Test
  void eachStateActsOnItsOwnDeadline() {
    // Guards against two kinds swapped in the code: the state would never time out.
    rows()
        .map(Row::given)
        .filter(state -> SAGA_MD_DEADLINES.containsKey(state.getClass()))
        .forEach(
            state -> {
              DeadlineKind own = SAGA_MD_DEADLINES.get(state.getClass()).kind();
              assertThat(SagaEngine.decide(state, new DeadlinePassed(own), NOW).ignored())
                  .as(state + " on " + own)
                  .isFalse();
            });
  }

  @Test
  void anIgnoredTransitionHasNoDeadlineToApply() {
    Transition ignored = SagaEngine.decide(new Closed(O), new RefundApproved(), NOW);

    assertThat(ignored.ignored()).isTrue();
    assertThatThrownBy(ignored::deadline).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void everyStateAppearsInTheTable() {
    Set<Class<?>> covered = rows().map(row -> row.given().getClass()).collect(Collectors.toSet());

    assertThat(covered).containsExactlyInAnyOrder(SagaState.class.getPermittedSubclasses());
  }

  @Test
  void everySagaMdRowHasSuccessLateAndDeadlineCasesAndFailureWhereItHasOne() {
    for (Class<?> state : SAGA_MD_DEADLINES.keySet()) {
      Set<Kind> kinds =
          rows()
              .filter(row -> row.given().getClass() == state)
              .map(Row::kind)
              .collect(Collectors.toCollection(() -> EnumSet.noneOf(Kind.class)));
      assertThat(kinds).as(state.getSimpleName()).contains(SUCCESS, LATE, DEADLINE);
      if (STATES_WITH_A_FAILURE_REPLY.contains(state)) {
        assertThat(kinds).as(state.getSimpleName()).contains(FAILURE);
      }
    }
  }

  @Test
  void everyInputKindIsExercised() {
    Set<Class<?>> inputs = rows().map(row -> row.input().getClass()).collect(Collectors.toSet());
    Set<Class<?>> replies =
        rows()
            .map(Row::input)
            .filter(Reply.class::isInstance)
            .map(input -> ((Reply) input).event().getClass())
            .collect(Collectors.toSet());

    assertThat(inputs).containsAll(Arrays.asList(SagaInput.class.getPermittedSubclasses()));
    assertThat(replies).containsAll(Arrays.asList(SagaEvent.class.getPermittedSubclasses()));
  }

  // --- table helpers ---

  record Row(
      Kind kind,
      SagaState given,
      SagaInput input,
      SagaState next,
      List<SagaCommand> commands,
      Finding finding,
      boolean early,
      boolean ignored) {

    Row emits(SagaCommand... more) {
      return new Row(kind, given, input, next, List.of(more), finding, early, ignored);
    }

    Row finding(Finding opened) {
      return new Row(kind, given, input, next, commands, opened, early, ignored);
    }

    Row movedEarly() {
      return new Row(kind, given, input, next, commands, finding, true, ignored);
    }

    @Override
    public String toString() {
      return kind + ": " + name(given) + " + " + name(input) + " -> " + name(next);
    }

    private static String name(Object value) {
      return switch (value) {
        case Reply(SagaEvent event) -> event.getClass().getSimpleName();
        case NeedsAttention(String id, SagaState prior, AttentionReason reason) ->
            "NeedsAttention(" + prior.getClass().getSimpleName() + ", " + reason + ")";
        default -> value.getClass().getSimpleName();
      };
    }
  }

  private static Row row(Kind kind, SagaState given, SagaInput input, SagaState next) {
    return new Row(kind, given, input, next, List.of(), null, false, false);
  }

  private static Row ignored(Kind kind, SagaState given, SagaInput input) {
    return new Row(kind, given, input, given, List.of(), null, false, true);
  }

  /** A state's own deadline passed: park it with an exception, and open the named finding. */
  private static Row missed(SagaState state, DeadlineKind kind, Finding finding) {
    return row(
            DEADLINE,
            state,
            new DeadlinePassed(kind),
            attention(state, AttentionReason.DEADLINE_MISSED))
        .finding(finding);
  }

  private static Reply reply(SagaEvent event) {
    return new Reply(event);
  }

  private static PaymentProcessedEvt processed(Payment payment) {
    return new PaymentProcessedEvt(O, payment.method(), payment.captured(), payment.amount());
  }

  private static NeedsAttention attention(SagaState prior, AttentionReason reason) {
    return new NeedsAttention(O, prior, reason);
  }
}
