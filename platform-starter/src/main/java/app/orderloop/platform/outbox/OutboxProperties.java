package app.orderloop.platform.outbox;

import java.time.Duration;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The outbox of one service. Setting {@code orderloop.outbox.schema} switches the outbox on.
 *
 * @param schema the service's schema, which holds its {@code outbox} table
 * @param opsUsername the relay's role, which may read and mark every tenant's rows
 * @param relayEnabled whether this instance polls; tests switch it off to drive batches
 * @param deliveryTimeout how long the producer may try to deliver one record before it fails
 */
@ConfigurationProperties("orderloop.outbox")
public record OutboxProperties(
    String schema,
    @DefaultValue("orderloop_ops") String opsUsername,
    String opsPassword,
    @DefaultValue("true") boolean relayEnabled,
    @DefaultValue("100") int batchSize,
    @DefaultValue("500ms") Duration pollInterval,
    @DefaultValue("15s") Duration deliveryTimeout) {

  // The schema is placed into SQL text, so allow only a plain identifier.
  private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]*");

  public OutboxProperties {
    if (schema == null || !IDENTIFIER.matcher(schema).matches()) {
      throw new IllegalArgumentException("orderloop.outbox.schema must be a plain schema name");
    }
    if (opsPassword == null || opsPassword.isBlank()) {
      throw new IllegalArgumentException("orderloop.outbox.ops-password is required");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("orderloop.outbox.batch-size must be positive");
    }
    if (deliveryTimeout.compareTo(Duration.ofSeconds(3)) < 0) {
      throw new IllegalArgumentException("orderloop.outbox.delivery-timeout must be at least 3s");
    }
  }

  /** One attempt's limit: a third of the delivery timeout, so the producer can retry. */
  Duration requestTimeout() {
    return deliveryTimeout.dividedBy(3);
  }
}
