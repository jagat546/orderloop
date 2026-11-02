package app.orderloop.platform.outbox;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class OutboxPropertiesTest {

  @Test
  void aMissingOpsPasswordStopsStartup() {
    assertThatThrownBy(() -> properties("orchestrator", null, Duration.ofSeconds(15)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ops-password");
  }

  @Test
  void theSchemaMustBeAPlainIdentifier() {
    assertThatThrownBy(() -> properties("x; DROP TABLE y", "secret", Duration.ofSeconds(15)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theDeliveryTimeoutLeavesRoomForARequestTimeout() {
    assertThatThrownBy(() -> properties("orchestrator", "secret", Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static OutboxProperties properties(String schema, String password, Duration delivery) {
    return new OutboxProperties(
        schema, "orderloop_ops", password, true, 100, Duration.ofMillis(500), delivery);
  }
}
