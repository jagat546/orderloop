package app.orderloop.payment;

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

/** Payment commands, each handled once through the inbox (invariant 5). */
@Component
class CommandListener {

  private final InboxConsumer inbox;
  private final PaymentHandler handler;
  private final Refunds refunds;

  CommandListener(InboxConsumer inbox, PaymentHandler handler, Refunds refunds) {
    this.inbox = inbox;
    this.handler = handler;
    this.refunds = refunds;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "payment-service")
  void on(ConsumerRecord<String, String> record) {
    inbox.handle(
        record,
        SagaCommand.class,
        command -> {
          switch (command) {
            case ProcessPaymentCmd c -> handler.verify(c, eventId(record));
            // Records the intent only; RefundReconciler moves the money after this commits.
            case RefundPaymentCmd c -> refunds.request(c, eventId(record));
            // Another service's command on this topic is a producer bug.
            case ReserveInventoryCmd c -> throw wrongTopic(c);
            case ReleaseInventoryCmd c -> throw wrongTopic(c);
            case RestockInventoryCmd c -> throw wrongTopic(c);
            case BookShipmentCmd c -> throw wrongTopic(c);
            case ReattemptDeliveryCmd c -> throw wrongTopic(c);
          }
        });
  }

  private static IllegalArgumentException wrongTopic(SagaCommand command) {
    return new IllegalArgumentException(
        command.getClass().getSimpleName() + " does not belong on " + Topics.PAYMENT_COMMANDS);
  }

  // The inbox checked x-event-id before calling the handler.
  private static UUID eventId(ConsumerRecord<String, String> record) {
    return UUID.fromString(
        new String(record.headers().lastHeader(Headers.EVENT_ID).value(), UTF_8));
  }
}
