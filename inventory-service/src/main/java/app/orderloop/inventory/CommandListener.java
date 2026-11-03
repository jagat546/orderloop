package app.orderloop.inventory;

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

/** Inventory commands, each handled once through the inbox (invariant 5). */
@Component
class CommandListener {

  private final InboxConsumer inbox;
  private final InventoryHandler handler;

  CommandListener(InboxConsumer inbox, InventoryHandler handler) {
    this.inbox = inbox;
    this.handler = handler;
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-service")
  void on(ConsumerRecord<String, String> record) {
    inbox.handle(
        record,
        SagaCommand.class,
        command -> {
          switch (command) {
            case ReserveInventoryCmd c -> handler.reserve(c, eventId(record));
            case ReleaseInventoryCmd c -> handler.release(c, eventId(record));
            case RestockInventoryCmd c -> handler.restock(c, eventId(record));
            // Another service's command on this topic is a producer bug.
            case ProcessPaymentCmd c -> throw wrongTopic(c);
            case RefundPaymentCmd c -> throw wrongTopic(c);
            case BookShipmentCmd c -> throw wrongTopic(c);
            case ReattemptDeliveryCmd c -> throw wrongTopic(c);
          }
        });
  }

  private static IllegalArgumentException wrongTopic(SagaCommand command) {
    return new IllegalArgumentException(
        command.getClass().getSimpleName() + " does not belong on " + Topics.INVENTORY_COMMANDS);
  }

  // The inbox checked x-event-id before calling the handler.
  private static UUID eventId(ConsumerRecord<String, String> record) {
    return UUID.fromString(
        new String(record.headers().lastHeader(Headers.EVENT_ID).value(), UTF_8));
  }
}
