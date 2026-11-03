# Roadmap

Work top to bottom, one slice at a time. Items marked `[human]` need you, not Claude Code.

OrderLoop ships first as an open-source project (ADR 0007). M1 builds the reliable core and ends in a one-command Docker demo on simulators, with no accounts or API keys. Pricing and sales are optional and live in `docs/business/`, outside this build path.

## M1: reliable core and simulator demo

- [ ] M1.1 Create the Maven multi-module skeleton on Java 25 and the newest supported Spring Boot 4.x, with Spotless, ArchUnit and CI running `check-invariants.sh` and `mvn verify`. Done when: `mvn verify` passes in CI.
- [x] M1.2 Add `deploy/docker-compose.yml` with PostgreSQL, Kafka in KRaft mode and Redis, plus roles and schemas. Done when: services are healthy from a clean start.
- [x] M1.3 Build `common-dto`: `Money` (minor units and ISO 4217 code), sealed commands and events, envelope, JSON round-trip and golden tests. Done when: it has no Spring dependency, enforced by ArchUnit; adding two currencies throws; and JPY, USD and KWD amounts round-trip exactly.
- [x] M1.4 Build tenant context: `TenantContext`, `TenantBindingFilter`, API-key auth for development. Done when: a missing credential returns 401, a mismatched header returns 403, and scope tests pass on virtual threads.
- [x] M1.5 Build RLS: `TenantAwareDataSource`, Hibernate `@TenantId`, a sample table. Done when: as `orderloop_app`, tenant B cannot read, update or delete tenant A's row through JPA `find` or native SQL, and an unbound scope sees nothing.
- [x] M1.6 Build the outbox and relay. Done when: a rollback publishes nothing, a crash after commit still publishes, and a second relay stays idle while the first holds the lock.
- [x] M1.7 Build `InboxConsumer` and dead-letter handling. Done when: a duplicate changes state once, a poison message reaches the dead-letter topic with its tenant, and a missing tenant header is rejected.
- [x] M1.8 Build the pure saga engine: states, events, `decide`, table-driven tests. Done when: every row in `docs/SAGA.md` has success, failure and late-reply tests, with no Spring.
- [x] M1.9 Build orchestrator persistence and a development order API. Done when: creating an order emits `ReserveInventoryCmd` through the outbox, and concurrent replies are safe.
- [x] M1.10 Build `inventory-service` with a simulator. Done when: an order reaches `VerifyingPayment`, and a failure opens an exception.
- [ ] M1.11 Build `payment-service` behind `PaymentProvider`, with the simulator as default, the intent pattern and a reconciler. Done when: a timeout after success leads to exactly one refund, in the captured currency.
- [ ] M1.12 Build `fulfilment-service` behind `FulfilmentProvider`, with the simulator as default. Done when: orders reach `Closed` and `ClosedRto` through carrier events.
- [ ] M1.13 Build persisted deadlines. Done when: a missed deadline opens an exception, and it survives a restart.
- [ ] M1.14 Add observability: JSON logs, MDC, tracing through Kafka, metrics, a Grafana board. Done when: one trace follows an order across every service.
- [ ] M1.15 Add a chaos suite: kill services at each crash point, duplicate deliveries, reorder replies, stop Kafka. Done when: every order ends in a named state with correct money, and no tenant sees another's data.
- [ ] M1.16 Build the one-command demo: `docker compose up` builds and starts the infrastructure, every service on simulators, and a generator of demo orders in at least two currencies. Done when: on a clean machine with only Docker and Git, that one command shows orders reaching `Closed`, `ClosedRto` and an open exception through the order API and Grafana, with no accounts or keys.
- [ ] M1.17 Write the README: problem, diagram, demo walkthrough, trade-offs. Done when: a stranger runs the demo from the README alone.

## P: before the repository goes public

- [ ] P.1 `[human]` Read your employment contract and get advice on side work and IP. Done when: you know you may publish this code.
- [ ] P.2 `[human]` Choose a licence from `docs/LICENSING.md` and record it with `/record-decision`. Done when: a `LICENSE` file exists.
- [ ] P.3 Prepare publication: secret scan of the whole git history, a check for any employer or client material, `CONTRIBUTING.md` and a security reporting policy. Done when: the scan is clean and both files exist.
- [ ] P.4 `[human]` Make the repository public.

## M2: real adapters

- [ ] M2.1 Spike the Shopify and Stripe docs and test modes. Done when: `docs/INTEGRATIONS.md` has no blocking "to confirm" items for them, or the plan changes.
- [ ] M2.2 Build Shopify OAuth, token encryption and uninstall handling. Done when: install and reinstall work on a development store.
- [ ] M2.3 Build webhook ingestion with signature checks and raw storage. Done when: forged and duplicate webhooks are rejected or absorbed.
- [ ] M2.4 Add mandatory privacy webhooks. Done when: each works and is tested.
- [ ] M2.5 Add observe-mode order ingestion. Done when: a test-store order creates a saga with no provider writes.
- [ ] M2.6 Add the Stripe `PaymentProvider` adapter in test mode. Done when: capture status is verified, a refund works with its idempotency key, and minor-unit mapping is tested for a zero-decimal currency.
- [ ] M2.7 Add a `FulfilmentProvider` adapter that reads Shopify fulfilment and tracking events. Done when: delivery and return events move a saga.
- [ ] M2.8 Run the end-to-end test. Done when: a development-store order closes with real Shopify and Stripe test events.
- [ ] M2.9 Optional: add the Razorpay `PaymentProvider` adapter in test mode. Done when: the M2.6 checks pass against Razorpay.
- [ ] M2.10 Optional: add the Shiprocket `FulfilmentProvider` adapter, with CSV fallback if needed. Done when: delivery, NDR and RTO events move a saga.

## M3: dashboard and detectors

- [ ] M3.1 Build the embedded shell with App Bridge and session tokens. Done when: it loads inside Shopify Admin, scoped to the right tenant.
- [ ] M3.2 Add the money-at-risk API and home screen. Done when: each currency's total matches a fixture to the minor unit, and no total mixes currencies.
- [ ] M3.3 Add the paid-not-fulfilled and stuck-order detectors.
- [ ] M3.4 Add the NDR, RTO-not-restocked and COD-remittance detectors.
- [ ] M3.5 Add the exception queue and order timeline.
- [ ] M3.6 Add approvals, with an audit trail. Done when: a refund needs a click, and a double click sends one refund.
- [ ] M3.7 Add a weekly email digest with no PII.
- [ ] M3.8 `[human]` Run with one real merchant for 2 weeks. Done when: they confirm a finding was real and worth acting on.

## M4: self-hosting in production

- [ ] M4.1 Build production Compose, Caddy and secrets handling. Done when: a clean server comes up from documentation alone.
- [ ] M4.2 Set up backups and do a restore drill. Done when: a restore has been timed and recorded.
- [ ] M4.3 Set up alerts. Done when: each alert has been triggered once on purpose.
- [ ] M4.4 Run a load test at 10 times the expected volume. Done when: limits are known, and memory figures in `docs/DEPLOYMENT.md` are measured.
- [ ] M4.5 Add a release process: versioned images, changelog and upgrade notes. Done when: a self-hoster upgrades from the previous release by following the notes.

## M5: AI layer, optional (after M1)

- [ ] M5.1 Build a read-only MCP server for orders, findings and timelines, scoped per tenant. Done when: a tenant's token cannot reach other tenants.
- [ ] M5.2 Add a triage copilot that explains an exception and drafts the next action. Done when: evals on labelled cases exist, and it can never approve money actions.

## Optional: business track

Not in the build path. Run it only if you decide to sell. See `docs/business/GATE_0.md` and `docs/business/PRICING.md`.

- [ ] B.1 `[human]` Hold 15 interviews using `docs/business/GATE_0.md`. Done when: notes exist for each.
- [ ] B.2 Build a small audit script that joins Shopify, payment-provider and shipping-provider CSV exports and runs the five checks. Done when: it produces the report from sample files, and masks PII.
- [ ] B.3 `[human]` Sell audits. Done when: 3 are paid for, or day 30 passes.
- [ ] B.4 `[human]` Apply the decision rules in `docs/business/GATE_0.md`, and record the result with `/record-decision`.
- [ ] B.5 Add Shopify Billing and plans for a hosted version. Done when: test charges, upgrade and downgrade work.
- [ ] B.6 `[human]` Finish the privacy policy, processor agreement and tax questions, the protected data application, and App Store submission.
