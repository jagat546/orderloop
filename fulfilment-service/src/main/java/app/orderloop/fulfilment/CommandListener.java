package app.orderloop.fulfilment;

import static java.nio.charset.StandardCharsets.UTF_8;

import app.orderloop.dto.Headers;
import app.orderloop.dto.SagaCommand;
import app.orderloop.dto.SagaCommand.BookShipmentCmd;
import app.orderloop.dto.SagaCommand.ProcessPaymentCmd;
import app.orderloop.dto.SagaCommand.ReattemptDeliveryCmd;
import app.orderloop.dto.SagaCommand.RefundPaymentCmd;
import app.orderloop.dto.SagaCommand.ReleaseInventoryCmd;
import app.orderloop.dto.SagaCommand.ReserveInventoryCmd;
import app.orderloop.dto.SagaCommand.RestockInventoryCmd;
import app.orderloop.dto.Topics;
import app.orderloop.platform.inbox.InboxConsumer;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Shipment commands, each handled once through the inbox (invariant 5). */
@Component
class CommandListener {

  private final InboxConsumer inbox;
  private final FulfilmentHandler handler;

  CommandListener(InboxConsumer inbox, FulfilmentHandler handler) {
    this.inbox = inbox;
    this.handler = handler;
  }

  @KafkaListener(topics = Topics.FULFILMENT_COMMANDS, groupId = "fulfilment-service")
  void on(ConsumerRecord<String, String> record) {
    inbox.handle(
        record,
        SagaCommand.class,
        command -> {
          switch (command) {
            case BookShipmentCmd c -> handler.book(c, eventId(record));
            case ReattemptDeliveryCmd c -> handler.reattempt(c);
            // Another service's command on this topic is a producer bug.
            case ReserveInventoryCmd c -> throw wrongTopic(c);
            case ReleaseInventoryCmd c -> throw wrongTopic(c);
            case RestockInventoryCmd c -> throw wrongTopic(c);
            case ProcessPaymentCmd c -> throw wrongTopic(c);
            case RefundPaymentCmd c -> throw wrongTopic(c);
          }
        });
  }

  private static IllegalArgumentException wrongTopic(SagaCommand command) {
    return new IllegalArgumentException(
        command.getClass().getSimpleName() + " does not belong on " + Topics.FULFILMENT_COMMANDS);
  }

  // The inbox checked x-event-id before calling the handler.
  private static UUID eventId(ConsumerRecord<String, String> record) {
    return UUID.fromString(
        new String(record.headers().lastHeader(Headers.EVENT_ID).value(), UTF_8));
  }
}
