# Integrations

Facts here were checked on 2026-11-02 unless marked otherwise. Re-check with the `provider-docs-researcher` agent before coding, and add what you confirm.

Simulators are the default for every port, so OrderLoop runs with no provider accounts or keys (ADR 0007). Real adapters are opt-in. Shopify and Stripe come first; Razorpay and Shiprocket are optional. Shopify Billing and the revenue share only matter for a paid hosted version, see `docs/business/PRICING.md`.

## Shopify

| Topic | Status | Note |
|---|---|---|
| Embedded auth | Confirmed | [Session tokens and App Bridge are expected for embedded apps](https://shopify.dev/docs/apps/store/common-rejections) |
| Protected customer data | Confirmed | [Name, address, phone and email are level 2, and need approval](https://Shopify.dev/docs/apps/store/data-protection/protected-customer-data). Level 2 also requires encrypted backups |
| Webhook signature | To confirm | HMAC of the raw body in a header; confirm the current scheme |
| Mandatory privacy webhooks | To confirm | Data request, customer redact and shop redact; confirm current names and timing |
| Inventory and order read scopes | To confirm | Request the minimum |
| Fulfilment and tracking events | To confirm | Whether delivery, failed delivery and return events are exposed, to back the Shopify `FulfilmentProvider` |
| Money fields | To confirm | Which fields give shop and presentment currency, and how amounts are encoded |

## Stripe

Nothing here is confirmed yet. Spike it in M2.1.

| Topic | Status | Note |
|---|---|---|
| Refund idempotency | To confirm | Idempotency key header on refund creation, and how long keys are kept |
| Webhook signature | To confirm | Signature header scheme over the raw body, and timestamp tolerance |
| Webhook delivery | To confirm | Retry window, ordering, response deadline |
| Minor units | To confirm | Zero-decimal currencies and any currency where Stripe's unit differs from ISO 4217 |
| How merchants connect | To confirm | Connect, restricted keys, or merchant API keys |
| Test mode | To confirm | Test keys and test clocks for settlement timing |

## Razorpay (optional)

| Topic | Status | Note |
|---|---|---|
| Refund idempotency | Confirmed | [`X-Refund-Idempotency` header, at least 10 characters](https://razorpay.com/docs/api/refunds/normal-refunds-idempotent) |
| Webhook signature | Confirmed | [HMAC-SHA256 of raw bytes in `X-Razorpay-Signature`](https://razorpay.com/blog/?p=28285) |
| Webhook delivery | Confirmed | [At least once, order not guaranteed, retried for 24 hours, 5-second response window, disabled after persistent failure](https://razorpay.com/blog/?p=28285) |
| Settlement reports | To confirm | Needed to spot gateway settlement gaps |
| How merchants connect | To confirm | OAuth partner flow or merchant API keys |

## Shiprocket (optional)

All of this is unconfirmed, and it carries the most risk. Spike it before M2.10.

- Webhook authentication and which delivery, NDR and RTO statuses are exposed
- Whether NDR re-attempts can be requested through the API
- Whether COD remittance can be read through the API. The fallback is CSV upload
- Rate limits and token expiry

## Port interfaces

```java
public interface PaymentProvider {
  PaymentStatus lookup(String tenantId, String orderId);
  RefundResult refund(String tenantId, RefundRequest request); // Carries the idempotency key and Money
  Optional<RefundResult> findRefund(String tenantId, String idempotencyKey);
}

public interface FulfilmentProvider {
  ShipmentStatus lookup(String tenantId, String orderId);
  BookingResult book(String tenantId, BookingRequest request); // Carries the idempotency key; blocked in observe mode
}

public interface InventoryProvider { // inventory-service, built in M1.10
  Reservation reserve(String tenantId, String orderId); // Reserved, or Failed(reason code)
}
```

The inventory simulator holds stock for every order except one whose ID ends in `-out-of-stock`, which fails with `OUT_OF_STOCK`, the way a test card number chooses a failure. It keeps no state and does no I/O, so it runs inside the inbox transaction; a real adapter must follow the provider side-effect pattern in `docs/ARCHITECTURE.md`.

Each port has a simulator and, where planned, production adapters. The simulator is selected unless configuration names another adapter. It can succeed, fail, time out after succeeding, and send duplicate or out-of-order webhooks, and it supports any ISO 4217 currency.
