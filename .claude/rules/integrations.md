---
paths:
  - "integration-gateway/**"
  - "**/provider/**"
  - "docs/INTEGRATIONS.md"
---

# Integration rules

- Check current provider docs before coding, using the `provider-docs-researcher` agent. Record what you confirmed in `docs/INTEGRATIONS.md`.
- Verify webhook signatures against the raw request bytes, before parsing and in constant time.
- Store the raw event, then answer 2xx quickly. Process it asynchronously through the outbox. Razorpay expects a response within 5 seconds.
- Deduplicate on the provider's event ID, and assume events arrive out of order.
- Each provider sits behind a port interface in its service (`PaymentProvider`, `FulfilmentProvider`). The simulator is the default adapter, so the system runs with no accounts or keys. Real adapters are opt-in by configuration: Shopify and Stripe first, Razorpay and Shiprocket optional.
- Map provider amounts to `Money` explicitly. A provider's minor unit can differ from ISO 4217 for some currencies, so test the mapping.
- Encrypt provider tokens with AES-256-GCM before storing them. Never log them.
- Set a connect and read timeout on every call, and bound concurrency per tenant so one merchant cannot starve others.
- Treat a timeout as "unknown", not "failed". Query the provider before retrying a side effect.
- Default to observe mode: read and verify only. A write to a provider needs both an action mode on the tenant and, for refunds, a human approval.
