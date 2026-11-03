---
paths:
  - "common-dto/**"
  - "**/messaging/**"
  - "**/outbox/**"
  - "**/inbox/**"
  - "**/*Listener.java"
  - "**/*Consumer.java"
---

# Messaging rules

**Why:** Kafka delivers at least once and a process can die between any two lines. Duplicates and crashes are normal, not edge cases.

## Publishing

- Save the domain change and an `outbox` row in one local transaction. Code other than the relay never touches `KafkaTemplate`.
- One relay per service, guarded by `pg_try_advisory_xact_lock`. Each batch runs in one transaction: lock, read unpublished rows in ID order, send and wait for acks, mark published.
- A row Kafka rejects stays unpublished, and the same order's later rows wait behind it; other orders still publish. A row whose transaction commits late goes out in a later batch, so the guarantee is per-order sequence, not global ID order.
- Write with `OutboxWriter.append(topic, envelope)` inside the domain transaction. It serialises with a fixed mapper, so the wire form matches `common-dto`'s golden file.
- A crash after send and before commit republishes the batch. The inbox absorbs it.
- Producer settings: `acks=all`, `enable.idempotence=true`.
- Keep published rows for 7 days so events can be replayed after a Kafka loss.

## Consuming

Every `@KafkaListener` delegates to `InboxConsumer`:

```java
@KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-service")
void on(ConsumerRecord<String, String> record) {
  inboxConsumer.handle(record, SagaCommand.class, command -> switch (command) {
    case ReserveInventoryCmd c -> handler.reserve(c);
    case ReleaseInventoryCmd c -> handler.release(c);
    // ... one case per command this topic carries; no default branch
  });
}
```

Pass the sealed parent when a topic carries several types. The concrete type comes from `x-event-type`. Call `handle` from the listener thread, outside any transaction.

`InboxConsumer` does this, in order:

1. Reads `x-tenant-id`, `x-event-id` and `x-event-type`, and throws `InvalidMessageException` (dead-lettered at once) if one is missing or malformed, the type is not a subtype of the one given, or the payload is unreadable or has no `orderId`. Integers must be JSON integers: `12.9`, `"500"` and `1e3` are refused, never truncated. Unknown fields are ignored. Today only the tenant ID's format is checked; rejecting a well-formed but unknown tenant needs the tenant table (M2.2).
2. Continues the record's `traceparent` in a consumer span, copies the order ID to MDC, and binds `TenantContext`, which copies the tenant to MDC. Outbox rows the handler appends carry the same trace.
3. Opens a transaction and inserts into `processed_event`. On conflict it commits and returns.
4. Runs the handler in that same transaction, and commits.

## Contracts

- Commands and events are records in `common-dto`, implementing sealed interfaces.
- Topics are keyed by `orderId`, so one order's messages stay in order.
- Headers: `x-tenant-id`, `x-event-id`, `x-event-type`, `x-schema-version`, `x-causation-id`, `traceparent`.
- Payloads carry IDs and `Money` amounts (minor units and currency). They never carry names, phones, emails or addresses.
- Add fields compatibly. A breaking change needs a new type and a new version.
- Use exponential backoff with a bounded number of retries, then a dead-letter topic. Keep the tenant header on dead letters.
- `InboxAutoConfiguration` sets this up for every listener: `orderloop.inbox.max-retries` (default 4), doubling from `initial-interval`. A record that can never be handled (no tenant or event ID, wrong type, unreadable) throws `InvalidMessageException` and is dead-lettered without retries.
- Dead letters go to `<topic>.dlt` through `OutboxRelay.deadLetter`, so the relay stays the only class that sends to Kafka. They carry every original header plus `x-dlt-original-topic`, `-partition`, `-offset` and `x-dlt-exception` (a class name only, since messages can quote payloads). Each `.dlt` topic must exist, because auto-create is off.
- A dead-lettered record's offset is committed only when a later record on its partition succeeds. After a restart on a quiet partition the record is handled again, so it can be dead-lettered twice, or dead-lettered and later handled. The inbox keeps state correct. Anything that counts or replays dead letters must deduplicate by `x-dlt-original-topic` plus `x-event-id`, and check `processed_event` before replaying.
- `processed_event.consumer` is the listener's consumer group, so two groups on one topic each handle every event once.

## Provider calls

Never make a provider call inside a transaction. Follow `docs/ARCHITECTURE.md`, section "Provider side effects".
