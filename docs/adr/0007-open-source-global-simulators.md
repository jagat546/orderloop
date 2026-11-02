# 0007: Open source first, global money, simulators by default

**Status:** Accepted
**Date:** 2026-11-02

## Context

The plan was a paid Shopify app for Indian D2C brands, with a sales gate (Gate 0) before building, money stored as `long` paise, and Razorpay and Shiprocket as the first providers. The owner has decided to ship OrderLoop first as a global open-source project. A global project cannot assume one currency, and a stranger must be able to run it without creating provider accounts or holding API keys. The architecture and invariants still hold: saga, outbox, inbox, RLS, observe mode and human-approved refunds are what make it trustworthy with money.

## Decision

- **Open source first.** No sales gate before building. Gate 0, pricing and sales material move to `docs/business/` and are optional. The employment-contract and IP check stays as a `[human]` item before the repository goes public. The licence is chosen separately, see `docs/LICENSING.md`.
- **Money is integer minor units plus an ISO 4217 currency code.** In Java this is a `Money(long amountMinor, String currency)` record in `common-dto`. In SQL a money column is `<name>_minor bigint` beside `currency text NOT NULL`. In JSON it is `{"amountMinor": 12345, "currency": "EUR"}`. Never `double`, `float` or `BigDecimal` in the domain. Arithmetic between different currencies throws. There is no exchange-rate conversion: totals are reported per currency. The minor-unit exponent comes from ISO 4217 (JPY 0, USD 2, KWD 3), never an assumed 2.
- **Providers stay behind interfaces, and simulators are the default.** `PaymentProvider` and `FulfilmentProvider` each have a simulator that runs with no account or key. Real adapters are opt-in through configuration.
- **Adapter order:** Shopify and Stripe first. Razorpay and Shiprocket stay optional adapters.
- **M1 ends in a one-command Docker demo** that runs on simulators.

## Consequences

- Anyone can clone, run and evaluate the system without accounts, keys or a sales conversation.
- Multi-currency is right from the first table, which is far cheaper than migrating paise columns later.
- Every money column carries a currency, so schemas and payloads are slightly wider, and the dashboard shows one total per currency instead of a single number.
- Provider minor units can differ from ISO 4217 for some currencies, so each adapter must map them explicitly and test that mapping.
- There is no evidence anyone will pay. The business material is kept, not deleted, so the gate can be run later.
- ADR 0005 sizes a single VPS against revenue. It stays valid for a hosted instance, and self-hosters choose their own size.
- Revisit if a licence choice, an employer IP finding or a hosted commercial offering changes what may be published.
