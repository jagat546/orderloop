package app.orderloop.dto;

import java.util.Currency;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * An amount in integer minor units of an ISO 4217 currency (ADR 0007). JSON form: {@code
 * {"amountMinor":1234,"currency":"KWD"}}. Amounts in different currencies never mix; there is no
 * conversion.
 */
public record Money(long amountMinor, String currency) {

  private static final Pattern CODE = Pattern.compile("[A-Z]{3}");
  private static final Pattern MAJOR = Pattern.compile("-?(0|[1-9][0-9]*)(\\.([0-9]+))?");

  public Money {
    Objects.requireNonNull(currency, "currency");
    if (!CODE.matcher(currency).matches()) {
      throw new IllegalArgumentException("Currency must be an ISO 4217 code: " + currency);
    }
    if (exponentOf(currency) < 0) {
      throw new IllegalArgumentException("Currency has no minor unit: " + currency);
    }
  }

  /**
   * Parses a major-unit amount such as {@code "12.34"} exactly. Rejects more decimals than the
   * currency's ISO 4217 exponent allows, rather than rounding.
   */
  public static Money parse(String major, String currency) {
    Objects.requireNonNull(major, "major");
    int exponent = new Money(0, currency).exponent();
    var match = MAJOR.matcher(major);
    if (!match.matches()) {
      throw new IllegalArgumentException("Not a plain decimal amount: '" + major + "'");
    }
    String fraction = match.group(3) == null ? "" : match.group(3);
    if (fraction.length() > exponent) {
      throw new IllegalArgumentException(
          currency + " allows " + exponent + " decimals, got '" + major + "'");
    }
    String digits =
        (major.startsWith("-") ? "-" : "")
            + match.group(1)
            + fraction
            + "0".repeat(exponent - fraction.length());
    try {
      return new Money(Long.parseLong(digits), currency);
    } catch (NumberFormatException e) {
      // The pattern already matched, so the only cause left is overflow.
      throw new ArithmeticException("Amount does not fit in a long: '" + major + "'");
    }
  }

  /** Number of minor-unit digits: JPY 0, USD 2, KWD 3. */
  public int exponent() {
    return exponentOf(currency);
  }

  public Money plus(Money other) {
    return new Money(Math.addExact(amountMinor, sameCurrency(other).amountMinor), currency);
  }

  public Money minus(Money other) {
    return new Money(Math.subtractExact(amountMinor, sameCurrency(other).amountMinor), currency);
  }

  /** Formats in major units with exactly the currency's exponent, such as {@code "1.234"}. */
  public String toMajorString() {
    int exponent = exponent();
    String text = Long.toString(amountMinor);
    String sign = text.startsWith("-") ? "-" : "";
    String digits = text.substring(sign.length());
    digits = "0".repeat(Math.max(0, exponent + 1 - digits.length())) + digits;
    int point = digits.length() - exponent;
    return exponent == 0
        ? sign + digits
        : sign + digits.substring(0, point) + "." + digits.substring(point);
  }

  private Money sameCurrency(Money other) {
    Objects.requireNonNull(other, "other");
    if (!currency.equals(other.currency)) {
      throw new IllegalArgumentException(
          "Cannot combine " + currency + " with " + other.currency + "; currencies never mix");
    }
    return other;
  }

  private static int exponentOf(String currency) {
    try {
      return Currency.getInstance(currency).getDefaultFractionDigits();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown ISO 4217 currency: " + currency, e);
    }
  }
}
