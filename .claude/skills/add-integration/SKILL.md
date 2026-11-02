---
name: add-integration
description: Use when adding or changing a call to Shopify, Stripe, Razorpay, Shiprocket or any other external provider API or webhook.
argument-hint: "[provider] [capability]"
---

# Add a provider integration

Target: `$ARGUMENTS`

1. Ask the `provider-docs-researcher` agent to confirm the current endpoint, auth, signature scheme, idempotency support, rate limits and error codes. Record the findings with links in `docs/INTEGRATIONS.md`.
2. If the docs do not support the plan, stop and report it. Do not guess at fields.
3. Define or extend the port interface, using records and `Money`. Map the provider's minor units to ISO 4217 explicitly.
4. Keep the simulator as the default adapter, extend it for the new capability, and add WireMock tests for success, 4xx, 5xx, timeout, duplicate webhook and out-of-order webhook.
5. Implement the adapter: timeouts, a per-tenant concurrency limit, and no PII in logs.
6. For a webhook: verify the signature on raw bytes, store the raw event, answer quickly, deduplicate on the provider event ID.
7. For a side effect: follow the intent, key, result and reconcile pattern in `docs/ARCHITECTURE.md`, and check that observe mode blocks it.
8. Run `/verify`.
