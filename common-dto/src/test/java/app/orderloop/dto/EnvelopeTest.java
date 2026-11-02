package app.orderloop.dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class EnvelopeTest {

  private static final UUID EVENT = UUID.fromString("00000000-0000-0000-0000-000000000001");

  @Test
  void isKeyedByOrderAndNamedByRegisteredType() {
    Envelope envelope = new Envelope(EVENT, "t_1", null, new ReserveInventoryCmd("o-1"));

    assertThat(envelope.key()).isEqualTo("o-1");
    assertThat(envelope.type()).isEqualTo("ReserveInventoryCmd");
    assertThat(envelope.schemaVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = " ")
  void rejectsAMissingTenant(String tenantId) {
    assertThatThrownBy(() -> new Envelope(EVENT, tenantId, null, new ReserveInventoryCmd("o-1")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = " ")
  void rejectsAMissingOrderId(String orderId) {
    assertThatThrownBy(() -> new Envelope(EVENT, "t_1", null, new ReserveInventoryCmd(orderId)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsAMissingEventIdOrPayload() {
    assertThatThrownBy(() -> new Envelope(null, "t_1", null, new ReserveInventoryCmd("o-1")))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new Envelope(EVENT, "t_1", null, null))
        .isInstanceOf(NullPointerException.class);
  }
}
