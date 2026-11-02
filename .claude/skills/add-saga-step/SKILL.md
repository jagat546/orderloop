---
name: add-saga-step
description: Use when adding or changing a state, command, reply or compensation in the OrderLoop order saga.
argument-hint: "[step name]"
---

# Add or change a saga step

Step: `$ARGUMENTS`

1. Update `docs/SAGA.md` first: state, command, success and failure replies, deadline, deadline action, compensation, and observe-mode behaviour.
2. Add rows to the table-driven transition test for success, failure, late reply and missed deadline. Watch them fail.
3. Add new records to the sealed types in `common-dto`. Follow `/add-kafka-contract` for each new message.
4. Add cases to `decide`. Let the compiler's exhaustiveness check show every place that needs handling.
5. Add the downstream handler through `InboxConsumer`. If it calls a provider, follow the intent, key, result and reconcile pattern in `docs/ARCHITECTURE.md`.
6. Add an integration test that runs the step end to end, including a duplicate command and a crash before the reply.
7. Run `/verify`, and ask the `saga-reviewer` agent to review the diff.
