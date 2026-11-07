package app.orderloop.dto;

import static org.assertj.core.api.Assertions.assertThat;

import app.orderloop.dto.DemoOrder.Outcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DemoOrderTest {

  @Test
  void aDeliveredOrderCarriesItsExactAmountInAZeroDecimalCurrency() {
    assertThat(DemoOrder.of("demo-1794050899-7-delivered-JPY-4500"))
        .contains(new DemoOrder(Outcome.DELIVERED, new Money(4500, "JPY")));
  }

  @Test
  void aReturnedOrderCarriesItsExactAmountInAThreeDecimalCurrency() {
    assertThat(DemoOrder.of("demo-1-rto-KWD-12345"))
        .contains(new DemoOrder(Outcome.RTO, new Money(12345, "KWD")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "o-6b1c1a52-delivered", // no currency or amount: an ordinary order
        "demo-1-out-of-stock",
        "demo-1-delivered-INR-", // no amount
        "demo-1-delivered-inr-100", // not an ISO 4217 code
        "demo-1-delivered-XXX-100", // a code with no minor unit
        "demo-1-shipped-INR-100", // an outcome the simulators do not know
        "delivered-INR-100", // nothing before the outcome
        "demo-1-delivered-INR-1234567890123456" // more digits than any order total
      })
  void anyOtherOrderIdIsNotADemoOrder(String orderId) {
    assertThat(DemoOrder.of(orderId)).isEmpty();
  }
}
