package app.orderloop.dto;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class MessageJsonTest {

  private static final String ORDER = "o-1";

  /** One instance of every message type. A new type without a sample fails the coverage test. */
  private static final List<Message> SAMPLES =
      List.of(
          new ReserveInventoryCmd(ORDER),
          new ProcessPaymentCmd(ORDER),
          new BookShipmentCmd(ORDER),
          new ReleaseInventoryCmd(ORDER),
          new ReattemptDeliveryCmd(ORDER),
          new RestockInventoryCmd(ORDER),
          new RefundPaymentCmd(ORDER, "r-1", new Money(1999, "USD")),
          new InventoryReservedEvt(ORDER),
          new InventoryFailedEvt(ORDER, "OUT_OF_STOCK"),
          new InventoryReleasedEvt(ORDER),
          new InventoryRestockedEvt(ORDER),
          new PaymentProcessedEvt(ORDER, PaymentMethod.COD, false, new Money(149900, "INR")),
          new PaymentFailedEvt(ORDER, "NOT_CAPTURED"),
          new ShipmentBookedEvt(ORDER),
          new ShipmentFailedEvt(ORDER, "CARRIER_REJECTED"),
          new DeliveredEvt(ORDER),
          new DeliveryFailedEvt(ORDER, "CUSTOMER_UNAVAILABLE"),
          new RtoStartedEvt(ORDER),
          new RtoReceivedEvt(ORDER),
          new RefundCompletedEvt(ORDER, "r-1", new Money(1999, "USD")),
          new RefundFailedEvt(ORDER, "r-1", "PROVIDER_DECLINED"),
          new SettlementObservedEvt(ORDER, new Money(1234, "KWD")));

  private final JsonMapper json = JsonMapper.builder().build();

  @Test
  void everyRegisteredTypeHasASample() {
    assertThat(SAMPLES.stream().map(m -> MessageTypes.nameOf(m.getClass())))
        .containsExactlyInAnyOrderElementsOf(MessageTypes.names());
  }

  @Test
  void everyMessageRoundTripsThroughJsonByItsRegisteredName() {
    for (Message sample : SAMPLES) {
      String name = MessageTypes.nameOf(sample.getClass());
      Message back = json.readValue(json.writeValueAsString(sample), MessageTypes.classOf(name));
      assertThat(back).as(name).isEqualTo(sample);
    }
  }

  /**
   * Fails if the wire format of any message changes. If the change is intended and compatible (an
   * added optional field), update golden/messages.txt in the same commit. Otherwise add a new type,
   * as .claude/skills/add-kafka-contract/SKILL.md describes.
   */
  @Test
  void serialisedFormMatchesTheGoldenFile() throws IOException {
    String actual =
        SAMPLES.stream()
            .map(m -> MessageTypes.nameOf(m.getClass()) + " " + json.writeValueAsString(m))
            .sorted()
            .collect(Collectors.joining("\n"));

    assertThat(actual).isEqualTo(golden("golden/messages.txt"));
  }

  private static String golden(String path) throws IOException {
    try (InputStream in = MessageJsonTest.class.getClassLoader().getResourceAsStream(path)) {
      assertThat(in).as(path).isNotNull();
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n").strip();
    }
  }
}
