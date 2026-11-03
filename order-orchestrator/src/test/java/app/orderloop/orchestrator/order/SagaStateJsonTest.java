package app.orderloop.orchestrator.order;

import static org.assertj.core.api.Assertions.assertThat;

import app.orderloop.dto.Money;
import app.orderloop.dto.PaymentMethod;
import app.orderloop.orchestrator.saga.SagaState;
import app.orderloop.orchestrator.saga.SagaState.AfterRefund;
import app.orderloop.orchestrator.saga.SagaState.AttentionReason;
import app.orderloop.orchestrator.saga.SagaState.Payment;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SagaStateJsonTest {

  // A three-decimal and a zero-decimal currency, so no amount passes through a two-decimal guess.
  private static final Payment KWD =
      new Payment(PaymentMethod.PREPAID, true, new Money(1234, "KWD"));
  private static final Payment JPY = new Payment(PaymentMethod.COD, false, new Money(500, "JPY"));

  private static final List<SagaState> EVERY_STATE =
      List.of(
          new SagaState.Received("o-1"),
          new SagaState.Allocating("o-1"),
          new SagaState.VerifyingPayment("o-1", 2),
          new SagaState.Booking("o-1", KWD),
          new SagaState.InTransit("o-1", JPY),
          new SagaState.NdrPending("o-1", KWD),
          new SagaState.Returning("o-1", JPY),
          new SagaState.Restocking("o-1", KWD, true),
          new SagaState.RefundProposed("o-1", KWD, AfterRefund.CLOSE_RTO),
          new SagaState.Refunding("o-1", KWD, AfterRefund.RELEASE_STOCK),
          new SagaState.Releasing("o-1", true, null),
          new SagaState.Releasing("o-1", true, KWD),
          new SagaState.AwaitingSettlement("o-1", JPY),
          new SagaState.NeedsAttention(
              "o-1", new SagaState.Booking("o-1", KWD), AttentionReason.DEADLINE_MISSED),
          new SagaState.Closed("o-1"),
          new SagaState.ClosedCancelled("o-1", true),
          new SagaState.ClosedRto("o-1"));

  @Test
  void everyStateSurvivesTheRoundTripExactly() {
    for (SagaState state : EVERY_STATE) {
      assertThat(SagaStateJson.read(SagaStateJson.write(state)))
          .as(state.toString())
          .isEqualTo(state);
    }
  }

  @Test
  void theListCoversEveryKindOfState() {
    assertThat(EVERY_STATE.stream().map(Object::getClass).distinct())
        .containsExactlyInAnyOrderElementsOf(
            Arrays.asList(SagaState.class.getPermittedSubclasses()));
  }

  @Test
  void theStoredNameIsTheStateName() {
    assertThat(SagaStateJson.name(new SagaState.VerifyingPayment("o-1", 1)))
        .isEqualTo("VerifyingPayment");
  }
}
