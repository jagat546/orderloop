package app.orderloop.dto;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * An order the demo generator made. Its ID tells the simulators what happens to it, the way a test
 * card number does: {@code <prefix>-delivered-<currency>-<amountMinor>} is prepaid, delivered and
 * settled; {@code <prefix>-rto-<currency>-<amountMinor>} is cash on delivery, returned and
 * restocked by hand. Real providers ignore the form, and any other ID is not a demo order.
 */
public record DemoOrder(Outcome outcome, Money total) {

  public enum Outcome {
    DELIVERED,
    RTO
  }

  // 15 digits fit in a long and exceed any order total.
  private static final Pattern ID = Pattern.compile(".+-(delivered|rto)-([A-Z]{3})-([0-9]{1,15})");

  public static Optional<DemoOrder> of(String orderId) {
    var match = ID.matcher(orderId);
    if (!match.matches()) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          new DemoOrder(
              Outcome.valueOf(match.group(1).toUpperCase(Locale.ROOT)),
              new Money(Long.parseLong(match.group(3)), match.group(2))));
    } catch (IllegalArgumentException e) {
      return Optional.empty(); // Not an ISO 4217 currency with a minor unit.
    }
  }
}
