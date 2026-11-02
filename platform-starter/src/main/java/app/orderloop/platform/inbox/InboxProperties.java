package app.orderloop.platform.inbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Retries before a failing record is dead-lettered: exponential, doubling up to the maximum. */
@ConfigurationProperties("orderloop.inbox")
public record InboxProperties(
    @DefaultValue("4") int maxRetries,
    @DefaultValue("200ms") Duration initialInterval,
    @DefaultValue("10s") Duration maxInterval) {

  public InboxProperties {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("orderloop.inbox.max-retries must not be negative");
    }
  }
}
