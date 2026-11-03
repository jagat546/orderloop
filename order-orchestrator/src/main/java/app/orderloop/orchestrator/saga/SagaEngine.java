package app.orderloop.orchestrator.saga;

import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.DEADLINE_MISSED;
import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.INVENTORY_FAILED;
import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.PAYMENT_NOT_CAPTURED;
import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.REFUND_FAILED;
import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.RELEASE_FAILED;
import static app.orderloop.orchestrator.saga.SagaState.AttentionReason.RESTOCK_FAILED;
import static app.orderloop.orchestrator.saga.Transition.enter;

import app.orderloop.dto.PaymentMethod;
import app.orderloop.dto.SagaCommand.BookShipmentCmd;
import app.orderloop.dto.SagaCommand.ProcessPaymentCmd;
import app.orderloop.dto.SagaCommand.ReattemptDeliveryCmd;
import app.orderloop.dto.SagaCommand.RefundPaymentCmd;
import app.orderloop.dto.SagaCommand.ReleaseInventoryCmd;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaCommand.RestockInventoryCmd;
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
import java.time.Instant;

/**
 * The order saga as a pure function: no I/O, no clock, no Spring. Every state lists what moves it
 * and ends with a catch-all that ignores anything else as late, duplicate or stale. There is no
 * default branch, so a new state or input fails to compile until it is handled here.
 */
public final class SagaEngine {

  private SagaEngine() {}

  private record Step(SagaState state, SagaInput input) {}

  public static Transition decide(SagaState state, SagaInput input, Instant now) {
    return switch (new Step(state, input)) {
      case Step(Received s, OrderAccepted a) ->
          enter(new Allocating(s.orderId()), now).emit(new ReserveInventoryCmd(s.orderId()));
      case Step(Received s, SagaInput other) -> Transition.ignore(s);

      case Step(Allocating s, Reply(InventoryReservedEvt e)) ->
          enter(new VerifyingPayment(s.orderId(), 1), now).emit(new ProcessPaymentCmd(s.orderId()));
      case Step(Allocating s, Reply(InventoryFailedEvt e)) -> attention(s, INVENTORY_FAILED);
      case Step(Allocating s, DeadlinePassed d) when d.kind() == DeadlineKind.ALLOCATING ->
          missed(s, Finding.STUCK_ORDER);
      case Step(Allocating s, SagaInput other) -> Transition.ignore(s);

      // A prepaid payment must be captured before anything ships; COD is settled on delivery.
      case Step(VerifyingPayment s, Reply(PaymentProcessedEvt e))
          when !e.captured() && e.method() != PaymentMethod.COD ->
          attention(s, PAYMENT_NOT_CAPTURED);
      case Step(VerifyingPayment s, Reply(PaymentProcessedEvt e)) ->
          enter(new Booking(s.orderId(), payment(e)), now).emit(new BookShipmentCmd(s.orderId()));
      case Step(VerifyingPayment s, Reply(PaymentFailedEvt e)) -> release(s.orderId(), true, now);
      case Step(VerifyingPayment s, DeadlinePassed d)
          when d.kind() == DeadlineKind.VERIFYING_PAYMENT && s.attempt() == 1 ->
          // ProcessPaymentCmd only verifies, so asking again is safe.
          enter(new VerifyingPayment(s.orderId(), 2), now).emit(new ProcessPaymentCmd(s.orderId()));
      case Step(VerifyingPayment s, DeadlinePassed d)
          when d.kind() == DeadlineKind.VERIFYING_PAYMENT ->
          missed(s, Finding.STUCK_ORDER);
      case Step(VerifyingPayment s, SagaInput other) -> Transition.ignore(s);

      case Step(Booking s, Reply(ShipmentBookedEvt e)) ->
          enter(new InTransit(s.orderId(), s.payment()), now);
      case Step(Booking s, Reply(ShipmentFailedEvt e)) ->
          s.payment().captured()
              ? enter(new RefundProposed(s.orderId(), s.payment(), AfterRefund.RELEASE_STOCK), now)
              : release(s.orderId(), false, now);
      case Step(Booking s, Reply(DeliveredEvt e)) ->
          delivered(s.orderId(), s.payment(), now).markEarly();
      case Step(Booking s, Reply(SettlementObservedEvt e)) when !s.payment().settled() ->
          Transition.stay(new Booking(s.orderId(), s.payment().asSettled()));
      case Step(Booking s, Reply(DeliveryFailedEvt e)) ->
          enter(new NdrPending(s.orderId(), s.payment()), now).markEarly();
      case Step(Booking s, Reply(RtoStartedEvt e)) ->
          enter(new Returning(s.orderId(), s.payment()), now).markEarly();
      case Step(Booking s, Reply(RtoReceivedEvt e)) ->
          enter(new Restocking(s.orderId(), s.payment(), false), now).markEarly();
      // No automatic refund: the money at risk is flagged and a human decides.
      case Step(Booking s, DeadlinePassed d) when d.kind() == DeadlineKind.BOOKING ->
          missed(s, s.payment().captured() ? Finding.PAID_NOT_FULFILLED : Finding.STUCK_ORDER);
      case Step(Booking s, SagaInput other) -> Transition.ignore(s);

      // The payment failed, then was captured after all: refund it once the stock is back.
      case Step(Releasing s, Reply(PaymentProcessedEvt e))
          when s.paymentFailed() && s.lateCapture() == null && e.captured() ->
          enter(new Releasing(s.orderId(), true, payment(e)), now);
      case Step(Releasing s, Reply(InventoryReleasedEvt e)) ->
          s.lateCapture() != null
              ? enter(
                  new RefundProposed(s.orderId(), s.lateCapture(), AfterRefund.CLOSE_CANCELLED),
                  now)
              : enter(new ClosedCancelled(s.orderId(), s.paymentFailed()), now);
      case Step(Releasing s, Reply(InventoryFailedEvt e)) -> attention(s, RELEASE_FAILED);
      case Step(Releasing s, DeadlinePassed d) when d.kind() == DeadlineKind.RELEASING ->
          missed(s, Finding.STUCK_ORDER);
      case Step(Releasing s, SagaInput other) -> Transition.ignore(s);

      case Step(InTransit s, Reply(DeliveredEvt e)) -> delivered(s.orderId(), s.payment(), now);
      // A prepaid payout often lands before delivery: remember it, keep the transit deadline.
      case Step(InTransit s, Reply(SettlementObservedEvt e)) when !s.payment().settled() ->
          Transition.stay(new InTransit(s.orderId(), s.payment().asSettled()));
      case Step(InTransit s, Reply(DeliveryFailedEvt e)) ->
          enter(new NdrPending(s.orderId(), s.payment()), now);
      case Step(InTransit s, Reply(RtoStartedEvt e)) ->
          enter(new Returning(s.orderId(), s.payment()), now);
      case Step(InTransit s, Reply(RtoReceivedEvt e)) ->
          enter(new Restocking(s.orderId(), s.payment(), false), now).markEarly();
      case Step(InTransit s, DeadlinePassed d) when d.kind() == DeadlineKind.IN_TRANSIT ->
          missed(s, Finding.STUCK_ORDER);
      case Step(InTransit s, SagaInput other) -> Transition.ignore(s);

      case Step(NdrPending s, ReattemptApproved a) ->
          enter(new InTransit(s.orderId(), s.payment()), now)
              .emit(new ReattemptDeliveryCmd(s.orderId()));
      case Step(NdrPending s, Reply(DeliveredEvt e)) -> delivered(s.orderId(), s.payment(), now);
      case Step(NdrPending s, Reply(SettlementObservedEvt e)) when !s.payment().settled() ->
          Transition.stay(new NdrPending(s.orderId(), s.payment().asSettled()));
      case Step(NdrPending s, ReturnDecided r) ->
          enter(new Returning(s.orderId(), s.payment()), now);
      case Step(NdrPending s, Reply(RtoStartedEvt e)) ->
          enter(new Returning(s.orderId(), s.payment()), now);
      case Step(NdrPending s, Reply(RtoReceivedEvt e)) ->
          enter(new Restocking(s.orderId(), s.payment(), false), now).markEarly();
      case Step(NdrPending s, DeadlinePassed d) when d.kind() == DeadlineKind.NDR_PENDING ->
          missed(s, Finding.NDR_PAST_SLA);
      case Step(NdrPending s, SagaInput other) -> Transition.ignore(s);

      case Step(Returning s, Reply(RtoReceivedEvt e)) ->
          enter(new Restocking(s.orderId(), s.payment(), false), now);
      case Step(Returning s, DeadlinePassed d) when d.kind() == DeadlineKind.RETURNING ->
          missed(s, Finding.STUCK_ORDER);
      case Step(Returning s, SagaInput other) -> Transition.ignore(s);

      case Step(Restocking s, RestockApproved a) when !s.requested() -> restock(s, now);
      // Accepted with or without our command: the merchant may restock by hand.
      case Step(Restocking s, Reply(InventoryRestockedEvt e)) ->
          s.payment().captured()
              ? enter(new RefundProposed(s.orderId(), s.payment(), AfterRefund.CLOSE_RTO), now)
              : enter(new ClosedRto(s.orderId()), now);
      case Step(Restocking s, Reply(InventoryFailedEvt e)) -> attention(s, RESTOCK_FAILED);
      case Step(Restocking s, DeadlinePassed d) when d.kind() == DeadlineKind.RESTOCKING ->
          missed(s, Finding.RTO_NOT_RESTOCKED);
      case Step(Restocking s, SagaInput other) -> Transition.ignore(s);

      // The only way to a refund: a human approved this proposal.
      case Step(RefundProposed s, RefundApproved a) ->
          refund(s.orderId(), s.payment(), s.then(), now);
      case Step(RefundProposed s, DeadlinePassed d) when d.kind() == DeadlineKind.REFUND_PROPOSED ->
          missed(s, Finding.PAID_NOT_FULFILLED);
      case Step(RefundProposed s, SagaInput other) -> Transition.ignore(s);

      case Step(Refunding s, Reply(RefundCompletedEvt e)) ->
          switch (s.then()) {
            case RELEASE_STOCK -> release(s.orderId(), false, now);
            case CLOSE_RTO -> enter(new ClosedRto(s.orderId()), now);
            case CLOSE_CANCELLED -> enter(new ClosedCancelled(s.orderId(), false), now);
          };
      case Step(Refunding s, Reply(RefundFailedEvt e)) -> attention(s, REFUND_FAILED);
      // The payment service's reconciler settles an unknown refund; the saga only flags it.
      case Step(Refunding s, DeadlinePassed d) when d.kind() == DeadlineKind.REFUNDING ->
          missed(s, Finding.STUCK_ORDER);
      case Step(Refunding s, SagaInput other) -> Transition.ignore(s);

      case Step(AwaitingSettlement s, Reply(SettlementObservedEvt e)) ->
          enter(new Closed(s.orderId()), now);
      case Step(AwaitingSettlement s, DeadlinePassed d)
          when d.kind() == DeadlineKind.AWAITING_SETTLEMENT ->
          missed(
              s,
              s.payment().method() == PaymentMethod.COD
                  ? Finding.COD_REMITTANCE_GAP
                  : Finding.STUCK_ORDER);
      case Step(AwaitingSettlement s, SagaInput other) -> Transition.ignore(s);

      // An exception is open and there is no deadline here.
      case Step(NeedsAttention s, DeadlinePassed d) -> Transition.ignore(s);
      case Step(NeedsAttention s, RetryStep r) -> retry(s, now);
      case Step(NeedsAttention s, SagaInput other) -> resume(s, other, now);

      // A capture reported after a cancel for a failed payment: the customer paid for nothing.
      case Step(ClosedCancelled s, Reply(PaymentProcessedEvt e))
          when s.paymentFailed() && e.captured() ->
          enter(new RefundProposed(s.orderId(), payment(e), AfterRefund.CLOSE_CANCELLED), now)
              .open(Finding.PAID_NOT_FULFILLED);
      case Step(Closed s, SagaInput other) -> Transition.ignore(s);
      case Step(ClosedCancelled s, SagaInput other) -> Transition.ignore(s);
      case Step(ClosedRto s, SagaInput other) -> Transition.ignore(s);
    };
  }

  /**
   * Decides anything but a deadline or a retry against the prior state, so a late provider event or
   * a human can move the saga on. A reply that would only park it again is a duplicate.
   */
  private static Transition resume(NeedsAttention parked, SagaInput input, Instant now) {
    Transition fromPrior = decide(parked.prior(), input, now);
    if (fromPrior.ignored() || fromPrior.next() instanceof NeedsAttention) {
      return Transition.ignore(parked);
    }
    // A fact recorded on the parked step, such as an early settlement: still stuck, stay parked.
    return fromPrior.keepsDeadline()
        ? Transition.stay(new NeedsAttention(parked.orderId(), fromPrior.next(), parked.reason()))
        : fromPrior;
  }

  /** Delivered: close at once if the money already settled, otherwise wait for it. */
  private static Transition delivered(String orderId, Payment payment, Instant now) {
    return payment.settled()
        ? enter(new Closed(orderId), now).markEarly()
        : enter(new AwaitingSettlement(orderId, payment), now);
  }

  /** Re-runs the step the saga stopped at: its command again, with a fresh deadline. */
  private static Transition retry(NeedsAttention parked, Instant now) {
    String id = parked.orderId();
    return switch (parked.prior()) {
      case Allocating s -> enter(s, now).emit(new ReserveInventoryCmd(id));
      case VerifyingPayment s ->
          enter(new VerifyingPayment(id, 1), now).emit(new ProcessPaymentCmd(id));
      case Booking s -> enter(s, now).emit(new BookShipmentCmd(id));
      case Releasing s -> enter(s, now).emit(new ReleaseInventoryCmd(id));
      case Restocking s -> restock(s, now);
      case Refunding s ->
          enter(s, now).emit(new RefundPaymentCmd(id, refundId(id), s.payment().amount()));
      // Steps that wait for the world, or for an approval: wait again.
      case InTransit s -> enter(s, now);
      case NdrPending s -> enter(s, now);
      case Returning s -> enter(s, now);
      case RefundProposed s -> enter(s, now);
      case AwaitingSettlement s -> enter(s, now);
      // Never parked: nothing to retry.
      case Received s -> Transition.ignore(parked);
      case NeedsAttention s -> Transition.ignore(parked);
      case Closed s -> Transition.ignore(parked);
      case ClosedCancelled s -> Transition.ignore(parked);
      case ClosedRto s -> Transition.ignore(parked);
    };
  }

  /** A compensation: give back reserved stock. */
  private static Transition release(String orderId, boolean paymentFailed, Instant now) {
    return enter(new Releasing(orderId, paymentFailed, null), now)
        .emit(new ReleaseInventoryCmd(orderId));
  }

  private static Transition restock(Restocking s, Instant now) {
    return enter(new Restocking(s.orderId(), s.payment(), true), now)
        .emit(new RestockInventoryCmd(s.orderId()));
  }

  /** The captured amount in its currency, under a fixed ID so a repeat is the same refund. */
  private static Transition refund(String orderId, Payment payment, AfterRefund then, Instant now) {
    return enter(new Refunding(orderId, payment, then), now)
        .emit(new RefundPaymentCmd(orderId, refundId(orderId), payment.amount()));
  }

  private static String refundId(String orderId) {
    return "rf-" + orderId;
  }

  private static Transition attention(SagaState prior, AttentionReason reason) {
    // NeedsAttention has no deadline, so no clock is needed to enter it.
    return enter(new NeedsAttention(prior.orderId(), prior, reason), Instant.EPOCH);
  }

  /** A state's deadline passed: park it with an exception and open the money-at-risk finding. */
  private static Transition missed(SagaState prior, Finding finding) {
    return attention(prior, DEADLINE_MISSED).open(finding);
  }

  private static Payment payment(PaymentProcessedEvt e) {
    return new Payment(e.method(), e.captured(), e.amount());
  }
}
