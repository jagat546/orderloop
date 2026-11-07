# Security policy

OrderLoop handles merchants' order and payment data, so security reports are welcome and taken seriously.

## Reporting a vulnerability

**Do not open a public issue, pull request or discussion for a vulnerability.**

Report it privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Only the maintainers can see the report.

Please include:

- What is affected: the module, endpoint or file, and the commit you tested.
- How to reproduce it, ideally against the local demo (`docker compose up --build --wait`), which needs no accounts or keys.
- What an attacker gains, for example reading another tenant's data or moving money.

Test only against your own local copy. Never test against a merchant's store or a deployment you do not run, and never use real customer data.

### What to expect

This is a volunteer-run open-source project, so these are goals, not contractual promises:

| Step | Goal |
|---|---|
| Acknowledge the report | Within 5 working days |
| First assessment and severity | Within 14 days |
| Fix or mitigation for a confirmed critical or high issue | As fast as possible, normally within 90 days |

We will keep you updated, agree a disclosure date with you, and credit you in the advisory unless you prefer not to be named. Please give us a reasonable chance to fix the issue before disclosing it.

## Supported versions

OrderLoop has no releases yet. Only the latest commit on the default branch receives security fixes. Once versions are released, this table will list which ones are supported.

| Version | Supported |
|---|---|
| Default branch, latest commit | Yes |
| Anything older | No |

The local demo uses public development credentials such as `demo-key-not-a-secret` and `grafana_dev`. They are documented on purpose, and they bind to `localhost` only. Reports that they are "leaked" are not vulnerabilities. Running the demo configuration on a public network is.

## Threat model: tenant isolation

The most important property is that one merchant can never see or change another merchant's data. The design does not rely on every query remembering a filter. The database enforces it, and every layer fails closed.

1. **The tenant comes from a verified credential.** A request is authenticated first: by API key in development, and by Shopify session token or webhook signature later. The authenticated principal names the tenant. An `X-Tenant-ID` header is only ever compared with it, and a mismatch returns 403. A credential with no tenant also returns 403.
2. **Tenant context lives in a `ScopedValue`.** `TenantBindingFilter` and `InboxConsumer` bind the tenant for exactly the scope of one request or one message, and it cannot leak to the next task on a pooled thread, as a `ThreadLocal` could. No `ThreadLocal` and no `@Async` are allowed for tenant identity, and an ArchUnit rule and the invariant checks enforce that. Code that needs a tenant calls `TenantContext.require()`, which throws when none is bound.
3. **Every connection carries the tenant.** `TenantAwareDataSource` sets `app.tenant_id` on each connection checkout, overwriting whatever a pooled connection held before. With no tenant bound it is set to empty.
4. **PostgreSQL row-level security is the boundary.** Every tenant table has a `tenant_id` column and forced row-level security. The policy compares rows with `NULLIF(current_setting('app.tenant_id', true), '')`, so an empty or missing tenant matches no rows: reads return nothing, and writes are refused. This applies to JPA, native SQL and find-by-ID alike. Hibernate's `@TenantId` is a convenience on top, not the boundary.
5. **Services connect as unprivileged roles.** Services run as `orderloop_app`: not the table owner, not a superuser, and without `BYPASSRLS`. Migrations run as `orderloop_owner`. Only `orderloop_ops` may read across tenants, and only the tables its background jobs scan: each service's outbox (the relay), the orchestrator's `saga_deadline` (the deadline scanner) and payment's `refund_intent` (the refund reconciler). Each job then does the tenant's work as `orderloop_app` in that tenant's scope.
6. **Messages keep their tenant.** Every Kafka message carries `x-tenant-id`. A consumer rejects a message with a missing or malformed tenant and sends it to the dead-letter topic, which keeps the tenant header.
7. **Tests prove it.** Isolation tests connect as `orderloop_app`, because a superuser bypasses row-level security and would prove nothing. Tenant B must be unable to read, update or delete tenant A's rows. The chaos suite checks isolation again after crashes and redelivery.

### Other threats and controls

| Threat | Control |
|---|---|
| Money moved twice or without consent | A human approves every refund in version 1; idempotency keys, an intent table and a reconciler; a refund is capped at what was captured minus earlier refunds |
| Forged webhook | Signature checked on the raw bytes in constant time, before parsing (with the Shopify adapter, roadmap M2) |
| Leaked provider token | None are stored yet: the simulators need none. When Shopify OAuth arrives (roadmap M2.2), tokens are encrypted at rest with AES-256-GCM, with the key outside the database, and never logged |
| Personal data in logs or messages | Orders are referenced by ID; logs and Kafka payloads carry no names, phones, emails or addresses |
| Secrets in the repository | Real secrets come only from environment variables (`.env.example` lists them, empty). The full git history is scanned with gitleaks (`.gitleaks.toml`) before publication. Claude Code hooks block reading `.env` files |

`docs/SECURITY.md` has the full engineering checklist, including data protection and payments regulation, and `docs/adr/0002-tenant-identity-and-rls.md` records the tenancy decision.

## Out of scope

- The development credentials above, used as documented on `localhost`.
- Findings that need an already-compromised host, database superuser, or Docker daemon.
- Denial of service against the local demo.
- Real provider adapters that do not exist yet (see `docs/ROADMAP.md`); they are welcome in reports once built.
