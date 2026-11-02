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
- A crash after send and before commit republishes the batch. The inbox absorbs it.
- Producer settings: `acks=all`, `enable.idempotence=true`.
- Keep published rows for 7 days so events can be replayed after a Kafka loss.

## Consuming

Every `@KafkaListener` delegates to `InboxConsumer`:

```java
@KafkaListener(topics = Topics.INVENTORY_COMMANDS)
void on(ConsumerRecord<String, byte[]> record) {
  inboxConsumer.handle(record, ReserveInventoryCmd.class, handler::reserve);
}
```

`InboxConsumer` does this, in order:

1. Reads `x-tenant-id` and `x-event-id` from headers and sends the record to the dead-letter topic if either is missing or the tenant is unknown.
2. Binds `TenantContext` and copies the tenant to MDC.
3. Opens a transaction and inserts into `processed_event`. On conflict it commits and returns.
4. Runs the handler in that same transaction, and commits.

## Contracts

- Commands and events are records in `common-dto`, implementing sealed interfaces.
- Topics are keyed by `orderId`, so one order's messages stay in order.
- Headers: `x-tenant-id`, `x-event-id`, `x-event-type`, `x-schema-version`, `x-causation-id`, `traceparent`.
- Payloads carry IDs and paise amounts. They never carry names, phones, emails or addresses.
- Add fields compatibly. A breaking change needs a new type and a new version.
- Use exponential backoff with a bounded number of retries, then a dead-letter topic. Keep the tenant header on dead letters.

## Provider calls

Never make a provider call inside a transaction. Follow `docs/ARCHITECTURE.md`, section "Provider side effects".
