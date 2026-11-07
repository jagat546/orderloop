# Contributing to OrderLoop

Thank you for helping. OrderLoop handles merchants' money, so it is built in small, verified slices, and a few rules hold for every change. Read `README.md` first to run the demo and see how the system fits together.

## Prerequisites

| Tool | Version | Used for |
|---|---|---|
| Java | 25 (LTS) | Building and running the services; `ScopedValue` is final only from 25 (ADR 0001) |
| Maven | 3.9 or newer | The multi-module build |
| Docker | Compose v2.24.4 or newer | Integration tests (Testcontainers), the development infrastructure and the demo |
| Git | Any current version | On Windows, run the scripts from Git Bash or WSL |
| Node.js | Current LTS | Only for `web-dashboard` |

Integration tests start PostgreSQL and Kafka in containers, so Docker must be running for `mvn verify`.

## Workflow

Work comes from `docs/ROADMAP.md`, one slice at a time. Each item has a "Done when", and an item is ticked only when every condition in it has passed.

1. **Pick the slice.** The first unchecked item in the lowest unfinished milestone. With Claude Code, `/next-slice` does this and reads only the docs that slice needs. Items marked `[human]` need a person, not code.
2. **State the plan** in a few lines: what you will build and how you will prove it. If it conflicts with a document or an invariant below, stop and discuss it in an issue first.
3. **Write the failing test first** for anything touching the saga, tenancy, messaging or money, and watch it fail for the expected reason.
4. **Build the smallest thing** that meets the "Done when". Do not start the next slice in the same change.
5. **Verify** (below), then tick the item and update the docs that describe what you changed (`docs/SAGA.md` for saga logic, `docs/INTEGRATIONS.md` for providers and simulators, and so on).
6. **Record decisions.** A changed design decision or invariant gets an ADR in `docs/adr/` (with Claude Code, `/record-decision`).

Commit messages start with the slice: `M1.16b: demo order generator and ...`.

## Before every commit

Run these from the repository root, in order, and fix the first failure before moving on. With Claude Code, `/verify` runs the same steps.

```bash
bash scripts/check-invariants.sh   # repository invariants that can be checked mechanically
bash scripts/test-hooks.sh         # if anything under .claude/hooks/ or scripts/ changed
mvn -q spotless:apply              # formatting (google-java-format)
mvn clean verify                   # every module: unit, integration and architecture tests
```

The reactor build must be green across all modules. CI runs the invariant check, the hook tests and `mvn verify` on every push and pull request. If `web-dashboard` changed, also run `npm --prefix web-dashboard run lint` and `npm --prefix web-dashboard test`. If you changed the demo (`compose.yaml`, `Dockerfile`, simulators), run `bash scripts/demo-smoke.sh` as well.

Never weaken a test, a hook or an invariant check to get a build through. If one is wrong, fix it in its own change and say why.

## Invariants

`CLAUDE.md` holds the full list. Breaking one is a defect even if the tests pass. The ones every change meets:

- **The saga is a pure function.** `SagaEngine.decide(state, input, now)` in `order-orchestrator` does no I/O, reads no clock and uses no Spring. It returns a `Transition`, and `SagaService` applies it. New states and steps use pattern matching over sealed types, and only the orchestrator decides what happens next; downstream services never call each other.
- **Outbox only.** A domain change and its outbox row commit in one transaction, written with `OutboxWriter.append`. Only the relay touches `KafkaTemplate`.
- **Every listener goes through `InboxConsumer`.** Its `processed_event` row commits with the handler's changes, so a redelivery does nothing. Redis locks never provide correctness.
- **The database enforces tenant isolation.** The tenant comes from a verified credential, never from a header alone. It lives in `ScopedValue` (no `ThreadLocal`, no `@Async`). Every tenant table has `tenant_id` and forced row-level security, and services connect as `orderloop_app`, which cannot bypass it.
- **Money is `Money`:** a `long` in minor units plus an ISO 4217 code. Never `double`, `float` or `BigDecimal` in the domain, never an assumed two decimals, and never arithmetic across currencies.
- **A human authorises money movement.** Refunds need a human approval in version 1, and observe mode is the default for every tenant. Provider side effects use intent, idempotency key, result and reconciler, with no HTTP inside a database transaction.
- **No PII in logs or Kafka.** Reference orders by ID.
- **Applied migrations are immutable.** Add a new one.

## Testing standards

| If you change | Prove it with |
|---|---|
| Saga logic | Table-driven unit tests over `decide`, with no Spring: success, failure, late or duplicate reply, and missed deadline |
| A tenant table or repository | An integration test, connected as `orderloop_app`, showing tenant B cannot read, update or delete tenant A's rows, through JPA and native SQL |
| A listener | An integration test delivering the same event twice and seeing one change |
| A publisher | A failure after commit still publishes, and a rollback publishes nothing |
| A provider call | A crash between call and result does not repeat the side effect |
| Money | Exact minor units and currency with no rounding, including a zero-decimal (JPY) and a three-decimal (KWD) currency; mixing currencies fails |

Integration tests use Testcontainers with real PostgreSQL and Kafka, never H2 or embedded Kafka. They connect as `orderloop_app`, because superusers bypass row-level security and an isolation test run as one proves nothing. Poll with Awaitility instead of sleeping, inject a `Clock` for time, and name tests after behaviour, such as `refundIsNotSentTwiceWhenProviderTimesOut`. ArchUnit rules in `architecture-tests` enforce module boundaries.

## Code style

- Records for DTOs, events, requests and responses. JPA entities are plain classes with no public setters. No Lombok.
- API errors go through `@RestControllerAdvice` with `ProblemDetail`.
- Structured JSON logs through SLF4J with parameters, carrying `traceId`, `spanId` and `tenantId` from MDC.
- Before coding against a provider API, check its current documentation; do not trust remembered endpoints or field names.
- Contribute only code you have the right to contribute. Never include code, data or documents from an employer or client.

## Licence

OrderLoop is licensed under the Apache License 2.0 (`LICENSE`). By submitting a contribution, you agree that it is licensed under the same terms, as section 5 of the licence provides. There is no separate contributor licence agreement.

## Security issues

Do not report vulnerabilities in public issues. See `SECURITY.md`.
