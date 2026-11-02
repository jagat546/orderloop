---
paths:
  - "**/src/test/**"
  - "web-dashboard/**/*.test.*"
  - "web-dashboard/e2e/**"
---

# Testing rules

- Use JUnit 5 and AssertJ. Name tests after behaviour: `refundIsNotSentTwiceWhenProviderTimesOut`.
- Saga logic: table-driven unit tests over `decide`, with no Spring.
- Integration tests use Testcontainers with real PostgreSQL, Kafka and Redis. Do not substitute H2 or embedded Kafka.
- **Connect integration tests as `orderloop_app`.** The default Testcontainers user is a superuser, and superusers bypass RLS, so an isolation test run as that user proves nothing.
- Provider tests use WireMock with recorded payloads, including timeouts, 5xx responses and duplicate webhooks.
- ArchUnit enforces module boundaries, and that records are used for DTOs.

## Tests every relevant change needs

| If you touch | Prove |
|---|---|
| A tenant table or repository | Tenant B cannot read, update or delete tenant A's row, through JPA and native SQL |
| A listener | Delivering the same event twice changes state once |
| A publisher | A failure after commit still publishes, and a rollback publishes nothing |
| A provider call | A crash between call and result does not repeat the side effect |
| A saga state | Success, failure, late reply and missed deadline |
| Money | Exact minor units and currency, with no rounding; include a zero-decimal and a three-decimal currency; mixing currencies fails |

Avoid fixed sleeps. Poll with Awaitility, and inject a `Clock` for deadlines.
