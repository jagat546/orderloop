---
name: add-kafka-contract
description: Use when adding or changing a Kafka command or event, its topic, or its schema in OrderLoop.
argument-hint: "[MessageName]"
---

# Add or change a Kafka contract

Message: `$ARGUMENTS`

1. Add a record in `common-dto` implementing the right sealed interface. Keep it free of Spring, and of PII.
2. Register its type name and version in the message registry.
3. Add a JSON round-trip test, plus a golden-file test that fails if the serialized form changes by accident.
4. For a change to an existing message:
   - Adding an optional field is compatible, and consumers must ignore unknown fields.
   - Removing or renaming a field, or changing its meaning, needs a new type or version. Keep the old consumer until no old messages remain.
5. Add any new topic to `Topics` and the topic configuration, keyed by `orderId`, with its dead-letter topic.
6. Publish only through the outbox, and consume only through `InboxConsumer`.
7. Run `/verify`.
