package app.orderloop.dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

class MoneyTest {

  private final JsonMapper json = JsonMapper.builder().build();

  @Test
  void addingTwoCurrenciesThrows() {
    assertThatThrownBy(() -> new Money(100, "USD").plus(new Money(100, "EUR")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("USD")
        .hasMessageContaining("EUR");
  }

  @Test
  void subtractingTwoCurrenciesThrows() {
    assertThatThrownBy(() -> new Money(100, "KWD").minus(new Money(100, "JPY")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void addsAndSubtractsSameCurrencyExactly() {
    assertThat(new Money(1999, "USD").plus(new Money(1, "USD"))).isEqualTo(new Money(2000, "USD"));
    assertThat(new Money(1999, "USD").minus(new Money(2000, "USD")))
        .isEqualTo(new Money(-1, "USD"));
  }

  @Test
  void overflowThrowsInsteadOfWrapping() {
    assertThatThrownBy(() -> new Money(Long.MAX_VALUE, "USD").plus(new Money(1, "USD")))
        .isInstanceOf(ArithmeticException.class);
  }

  @ParameterizedTest
  @CsvSource({"JPY, 0", "USD, 2", "KWD, 3"})
  void usesTheIso4217Exponent(String currency, int exponent) {
    assertThat(new Money(0, currency).exponent()).isEqualTo(exponent);
  }

  @ParameterizedTest
  @CsvSource({
    "JPY, 1500, 1500",
    "JPY, -7, -7",
    "USD, 12.34, 1234",
    "USD, 0.05, 5",
    "USD, -0.05, -5",
    "KWD, 1.234, 1234",
    "KWD, 0.001, 1",
    "USD, 92233720368547758.07, 9223372036854775807",
    "USD, -92233720368547758.08, -9223372036854775808"
  })
  void majorAmountsRoundTripExactly(String currency, String major, long minor) throws Exception {
    Money money = Money.parse(major, currency);

    assertThat(money).isEqualTo(new Money(minor, currency));
    assertThat(money.toMajorString()).isEqualTo(major);
    assertThat(json.readValue(json.writeValueAsString(money), Money.class)).isEqualTo(money);
  }

  @Test
  void serialisesAsMinorUnitsAndCurrency() {
    assertThat(json.writeValueAsString(new Money(1234, "KWD")))
        .isEqualTo("{\"amountMinor\":1234,\"currency\":\"KWD\"}");
  }

  @Test
  void padsMissingMinorDigits() {
    assertThat(Money.parse("12.3", "USD")).isEqualTo(new Money(1230, "USD"));
    assertThat(Money.parse("12", "KWD")).isEqualTo(new Money(12000, "KWD"));
  }

  @ParameterizedTest
  @CsvSource({"12.345, USD", "1500.5, JPY", "1.2345, KWD"})
  void rejectsMorePrecisionThanTheCurrencyHas(String major, String currency) {
    assertThatThrownBy(() -> Money.parse(major, currency))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "12,34", "1.2.3", ".5", "5.", "+5", "1e3", " 5"})
  void rejectsMalformedAmounts(String major) {
    assertThatThrownBy(() -> Money.parse(major, "USD"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAmountsTooLargeForLong() {
    assertThatThrownBy(() -> Money.parse("92233720368547758.08", "USD"))
        .isInstanceOf(ArithmeticException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"usd", "US", "ABC", "XXX"})
  void rejectsCodesThatAreNotIso4217CurrenciesWithMinorUnits(String currency) {
    assertThatThrownBy(() -> new Money(1, currency)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsMissingCurrency() {
    assertThatThrownBy(() -> new Money(1, null)).isInstanceOf(NullPointerException.class);
  }
}
