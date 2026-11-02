# OrderLoop

Open-source Shopify app that finds and closes money leaking from stuck orders, in any currency.
It runs on simulators with no accounts or keys; real providers plug in behind interfaces (ADR 0007).
Merchants trust it with money. Saga, outbox and multi-tenancy are how we earn that trust, not the product.

Status and next work: `docs/ROADMAP.md`. Run `/next-slice` to continue.

## Stack

- Java 25 LTS, Spring Boot 4.1.x (Web, Data JPA, Kafka, Validation, Actuator, Security), Maven multi-module
- PostgreSQL with row-level security, Flyway, Apache Kafka in KRaft mode, Redis
- React, TypeScript and Vite for the embedded Shopify dashboard
- JUnit 5, AssertJ, Testcontainers, ArchUnit, Playwright

Java 21 is not acceptable here: `ScopedValue` is only final from Java 25. See `docs/adr/0001-java-25-spring-boot-4.md`.

## Commands

| Task | Command |
|---|---|
| Build and test everything | `mvn clean install` |
| Unit tests only | `mvn test` |
| Unit and integration tests | `mvn verify` |
| One module | `mvn -pl order-orchestrator -am verify` |
| Format | `mvn spotless:apply` |
| Check repo invariants | `bash scripts/check-invariants.sh` |
| Test the Claude Code hooks | `bash scripts/test-hooks.sh` |
| Start infrastructure | `docker compose up -d` |
| Stop, keeping data | `docker compose down` |
| Stop and wipe data (asks first) | `docker compose down -v` |
| Dashboard | `npm --prefix web-dashboard run dev` |

## Modules

| Module | Owns |
|---|---|
| `common-dto` | Commands, events and envelope as records. Pure Java, no Spring |
| `platform-starter` | Tenant context, RLS DataSource, outbox relay, inbox template, logging |
| `integration-gateway` | Shopify OAuth, webhook verification, raw event storage |
| `order-orchestrator` | Saga state machine, deadlines, exceptions, leak findings, dashboard API |
| `inventory-service` | Reacts to `ReserveInventoryCmd`, replies `InventoryReservedEvt` or `InventoryFailedEvt` |
| `payment-service` | Reacts to `ProcessPaymentCmd`, replies `PaymentProcessedEvt` or `PaymentFailedEvt`; runs refunds. Talks to `PaymentProvider`: simulator by default, Stripe first, Razorpay optional |
| `fulfilment-service` | Reacts to `BookShipmentCmd`, replies `ShipmentBookedEvt` or `ShipmentFailedEvt`; tracks NDR and RTO. Talks to `FulfilmentProvider`: simulator by default, Shopify first, Shiprocket optional |
| `web-dashboard` | Embedded app: money at risk, exceptions, approvals |
| `deploy` | Production Compose, Caddy, backups |

Downstream services never call each other and never decide what happens next. Only the orchestrator does.

## Invariants

Breaking one of these is a defect even if tests pass. Hooks and `scripts/check-invariants.sh` enforce what can be checked mechanically.

1. **Tenant comes from a verified credential.** Shopify session token, API key or webhook signature. `X-Tenant-ID` is only ever compared against it; a mismatch returns 403.
2. **The database enforces isolation.** Every tenant table has `tenant_id` and forced RLS. The app connects as a non-owner role without `BYPASSRLS`. Hibernate `@TenantId` is a convenience, not the boundary.
3. **Tenant context lives in `ScopedValue`.** No `ThreadLocal` for tenant identity, and no `@Async`. MDC holds a copy for logs only.
4. **Outbox only.** Domain change and outbox row commit in one transaction. Only the relay touches `KafkaTemplate`.
5. **Every listener goes through `InboxConsumer`.** The inbox row commits with the handler's changes. Redis locks never provide correctness.
6. **Provider side effects use intent, key, result, reconcile.** No HTTP inside a database transaction.
7. **Money is `long` minor units plus an ISO 4217 currency code**, as `Money` in `common-dto`. Never `double`, `float` or `BigDecimal` in the domain, never an assumed two decimals, and never arithmetic or totals across currencies.
8. **Refunds need human approval in version 1.** Observe mode is the default for every tenant.
9. **No PII in logs or Kafka.** Reference orders by ID.
10. **Applied migrations are immutable.** Add a new one.

## How to work

- Work one roadmap slice at a time. Finish it, verify it, then stop and report.
- Write the failing test first for anything touching saga, tenancy, messaging or money.
- Use records for DTOs, events, requests and responses. JPA entities are plain classes with no public setters. No Lombok.
- Use pattern matching over sealed types for saga evaluation.
- Use `@RestControllerAdvice` with `ProblemDetail` for all API errors.
- Log structured JSON through SLF4J with `traceId`, `spanId` and `tenantId` in MDC.
- Run `/verify` before saying work is done, and report what it actually printed.
- Before coding against a provider API, check its current docs with the `provider-docs-researcher` agent. Do not trust remembered endpoints or field names.
- If you need to break an invariant or change a design decision, stop and say why. Record agreed changes with `/record-decision`.
- Never use code, data or documents from any employer or client in this repo.
- Commit or push only when asked.

## Skills and plugins

The design in `docs/` is approved, so do not run brainstorming to restart it. Project skills, rules and this file win where they overlap with a plugin skill.

| Work | Use |
|---|---|
| Picking the next piece of work | `/next-slice` |
| Planning a slice with several steps | `superpowers:writing-plans` |
| Test-first development | `superpowers:test-driven-development`; `test-engineer` agent for failure paths |
| Saga, tenant, contract, provider or detector changes | `/add-saga-step`, `/add-tenant-entity`, `/add-kafka-contract`, `/add-integration`, `/add-leak-detector` |
| Debugging | `superpowers:systematic-debugging` |
| Code review | `/code-review`, `superpowers:requesting-code-review`; `saga-reviewer` and `tenant-auditor` agents |
| Verification | `/verify`. `superpowers:verification-before-completion` adds only: no claim without fresh output |
| CI | `cicd-automation:github-actions-templates` |
| Frontend | `frontend-design:frontend-design`, `playwright-cli` for end-to-end tests |
| Design decisions | `/record-decision` |

## Where to look

| Question | File |
|---|---|
| What it does and for whom | `docs/PRODUCT.md` |
| How services and tenancy fit together | `docs/ARCHITECTURE.md` |
| States, commands, compensations, deadlines | `docs/SAGA.md` |
| Tables, roles, RLS policies | `docs/DATA_MODEL.md` |
| Shopify, Stripe, simulators and optional adapters | `docs/INTEGRATIONS.md` |
| Secrets, PII, compliance | `docs/SECURITY.md` |
| Running in production | `docs/DEPLOYMENT.md` |
| Licence options (not yet chosen) | `docs/LICENSING.md` |
| Pricing and sales (optional) | `docs/business/` |
| Why a decision was made | `docs/adr/` |

Path-scoped rules in `.claude/rules/` load automatically when you touch matching files.
