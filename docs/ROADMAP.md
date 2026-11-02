# Roadmap

Work top to bottom, one slice at a time. Items marked `[human]` need you, not Claude Code. Durations are estimates at 12 to 15 hours a week.

Gate 0 and M1 run side by side. M1 has portfolio value whatever Gate 0 finds. Start M2 only after 3 paid audits, or after you decide to build it as a portfolio project anyway.

## Gate 0: prove someone pays (weeks 1 to 4)

- [ ] G0.1 `[human]` Read your employment contract and get advice on side work and IP. Done when: you know whether you can sell this.
- [ ] G0.2 `[human]` Hold 15 interviews using `docs/GATE_0.md`. Done when: notes exist for each.
- [ ] G0.3 Build a small audit script that joins Shopify, Razorpay and Shiprocket CSV exports and runs the five checks. Done when: it produces the report from sample files, and masks PII.
- [ ] G0.4 `[human]` Sell audits. Done when: 3 are paid for, or day 30 passes.
- [ ] G0.5 `[human]` Apply the kill criteria in `docs/GATE_0.md`, and record the result with `/record-decision`.

## M1: reliable core (weeks 3 to 12)

- [ ] M1.1 Create the Maven multi-module skeleton on Java 25 and Spring Boot 4.1.x (check for a newer supported release), with Spotless, ArchUnit and CI running `check-invariants.sh`. Done when: `mvn verify` passes in CI.
- [ ] M1.2 Add `docker-compose.yml` with PostgreSQL, Kafka in KRaft mode and Redis, plus roles and schemas. Done when: services are healthy from a clean start.
- [ ] M1.3 Build `common-dto`: sealed commands and events, envelope, JSON round-trip and golden tests. Done when: it has no Spring dependency, enforced by ArchUnit.
- [ ] M1.4 Build tenant context: `TenantContext`, `TenantBindingFilter`, API-key auth for development. Done when: a missing credential returns 401, a mismatched header returns 403, and scope tests pass on virtual threads.
- [ ] M1.5 Build RLS: `TenantAwareDataSource`, Hibernate `@TenantId`, a sample table. Done when: as `orderloop_app`, tenant B cannot read, update or delete tenant A's row through JPA `find` or native SQL, and an unbound scope sees nothing.
- [ ] M1.6 Build the outbox and relay. Done when: a rollback publishes nothing, a crash after commit still publishes, and a second relay stays idle while the first holds the lock.
- [ ] M1.7 Build `InboxConsumer` and dead-letter handling. Done when: a duplicate changes state once, a poison message reaches the dead-letter topic with its tenant, and a missing tenant header is rejected.
- [ ] M1.8 Build the pure saga engine: states, events, `decide`, table-driven tests. Done when: every row in `docs/SAGA.md` has success, failure and late-reply tests, with no Spring.
- [ ] M1.9 Build orchestrator persistence and a development order API. Done when: creating an order emits `ReserveInventoryCmd` through the outbox, and concurrent replies are safe.
- [ ] M1.10 Build `inventory-service` with a simulator. Done when: an order reaches `VerifyingPayment`, and a failure opens an exception.
- [ ] M1.11 Build `payment-service` with a simulator, the intent pattern and a reconciler. Done when: a timeout after success leads to exactly one refund.
- [ ] M1.12 Build `fulfilment-service` with a simulator. Done when: orders reach `Closed` and `ClosedRto` through carrier events.
- [ ] M1.13 Build persisted deadlines. Done when: a missed deadline opens an exception, and it survives a restart.
- [ ] M1.14 Add observability: JSON logs, MDC, tracing through Kafka, metrics, a Grafana board. Done when: one trace follows an order across every service.
- [ ] M1.15 Add a chaos suite: kill services at each crash point, duplicate deliveries, reorder replies, stop Kafka. Done when: every order ends in a named state with correct money, and no tenant sees another's data.
- [ ] M1.16 Write the portfolio README: problem, diagram, chaos demo recording, trade-offs. Done when: a stranger can run the demo with one command.

## M2: real money rails (weeks 13 to 18)

- [ ] M2.1 Spike Shopify, Razorpay and Shiprocket docs and sandboxes. Done when: `docs/INTEGRATIONS.md` has no blocking "to confirm" items, or the plan changes.
- [ ] M2.2 Build Shopify OAuth, token encryption and uninstall handling. Done when: install and reinstall work on a development store.
- [ ] M2.3 Build webhook ingestion with signature checks and raw storage. Done when: forged and duplicate webhooks are rejected or absorbed.
- [ ] M2.4 Add mandatory privacy webhooks. Done when: each works and is tested.
- [ ] M2.5 Add observe-mode order ingestion. Done when: a test-store order creates a saga with no provider writes.
- [ ] M2.6 Add the Razorpay adapter in test mode. Done when: capture status is verified, and a refund works with its idempotency key.
- [ ] M2.7 Add the Shiprocket adapter, with CSV fallback if needed. Done when: delivery, NDR and RTO events move a saga.
- [ ] M2.8 Run the end-to-end test. Done when: a test-store order closes with real sandbox events.

## M3: dashboard and detectors (weeks 19 to 24)

- [ ] M3.1 Build the embedded shell with App Bridge and session tokens. Done when: it loads inside Shopify Admin, scoped to the right tenant.
- [ ] M3.2 Add the money-at-risk API and home screen. Done when: the total matches a fixture to the paise.
- [ ] M3.3 Add the paid-not-fulfilled and stuck-order detectors.
- [ ] M3.4 Add the NDR, RTO-not-restocked and COD-remittance detectors.
- [ ] M3.5 Add the exception queue and order timeline.
- [ ] M3.6 Add approvals, with an audit trail. Done when: a refund needs a click, and a double click sends one refund.
- [ ] M3.7 Add a weekly email digest with no PII.
- [ ] M3.8 `[human]` Run with one design partner for 2 weeks. Done when: they confirm a finding was real and worth acting on.

## M4: go live (weeks 25 to 30)

- [ ] M4.1 Build production Compose, Caddy and secrets handling. Done when: a clean server comes up from documentation alone.
- [ ] M4.2 Set up backups and do a restore drill. Done when: a restore has been timed and recorded.
- [ ] M4.3 Set up alerts. Done when: each alert has been triggered once on purpose.
- [ ] M4.4 Add Shopify Billing, plans and usage overage. Done when: test charges work, and upgrade and downgrade work.
- [ ] M4.5 `[human]` Finish the privacy policy, processor agreement, tax and GST questions, and protected data application.
- [ ] M4.6 Run a load test at 10 times the expected volume. Done when: limits are known, and memory figures in `docs/DEPLOYMENT.md` are measured.
- [ ] M4.7 `[human]` Submit to the App Store and sign the first paying merchant.

## M5: AI layer, optional (after M1 or first revenue)

- [ ] M5.1 Build a read-only MCP server for orders, findings and timelines, scoped per tenant. Done when: a tenant's token cannot reach other tenants.
- [ ] M5.2 Add a triage copilot that explains an exception and drafts the next action. Done when: evals on labelled cases exist, and it can never approve money actions.
