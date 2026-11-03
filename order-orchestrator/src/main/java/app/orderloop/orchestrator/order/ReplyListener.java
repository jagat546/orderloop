package app.orderloop.orchestrator.order;

import static java.nio.charset.StandardCharsets.UTF_8;

import app.orderloop.dto.Headers;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.Topics;
import app.orderloop.orchestrator.saga.SagaInput.Reply;
import app.orderloop.platform.inbox.InboxConsumer;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Every downstream reply moves its order's saga. The inbox makes a redelivery a no-op. If another
 * reply for the same order commits first, {@link SagaService#apply} fails with an optimistic lock
 * error, the inbox row rolls back with it, and the error handler retries against the new state.
 */
@Component
class ReplyListener {

  private final InboxConsumer inbox;
  private final SagaService sagas;

  ReplyListener(InboxConsumer inbox, SagaService sagas) {
    this.inbox = inbox;
    this.sagas = sagas;
  }

  @KafkaListener(
      topics = {Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS, Topics.FULFILMENT_EVENTS},
      groupId = "order-orchestrator")
  void on(ConsumerRecord<String, String> record) {
    inbox.handle(
        record,
        SagaEvent.class,
        // The inbox checked x-event-id before calling this.
        event -> sagas.apply(event.orderId(), new Reply(event), eventId(record)));
  }

  private static UUID eventId(ConsumerRecord<String, String> record) {
    return UUID.fromString(
        new String(record.headers().lastHeader(Headers.EVENT_ID).value(), UTF_8));
  }
}
