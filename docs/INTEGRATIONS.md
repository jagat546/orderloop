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

public interface FulfilmentProvider { // fulfilment-service, built in M1.12
  BookingResult book(String tenantId, String orderId); // Booked, or Failed(reason code); keyed by order
  void requestReattempt(String tenantId, String orderId); // The carrier reports the outcome later
}

public interface InventoryProvider { // inventory-service, built in M1.10 to M1.12
  StockResult reserve(String tenantId, String orderId); // Done, or Failed(reason code)
  StockResult release(String tenantId, String orderId); // A compensation
  StockResult restock(String tenantId, String orderId); // After a human approved it
}
```

`PaymentProvider` is built: `lookup` (M1.11a), and `refund` and `findRefund` (M1.11b). `refund` throws when the outcome is unknown, such as on a timeout. Only providers that honour idempotency keys may implement it, because the reconciler may call again under the same key.

The inventory simulator holds stock for every order except one whose ID ends in `-out-of-stock`, which fails with `OUT_OF_STOCK`, the way a test card number chooses a failure. Release and restock always succeed. It keeps no state. `restockedByHand` reports a restock the merchant made with no command from us, as `InventoryRestockedEvt`.

The carrier simulator books every shipment except one whose order ID ends in `-no-courier`, which fails with `NO_COURIER`. `deliver`, `failDelivery`, `startRto` and `receiveRto` then report what the carrier saw, as its webhooks would, for shipments that tenant booked only. They report in the order called, so tests can send events late or out of order. Bookings are lost on restart.

Simulator reports (carrier events, `settle` and `restockedByHand`) go out through their service's outbox in the tenant's scope, with no causation ID, since no command caused them.

The payment simulator keeps an in-memory ledger per tenant. Tests and the demo record what checkout did for an order (prepaid captured, prepaid authorised only, or COD, with an amount in any ISO 4217 currency), and `lookup` returns exactly that. An order with nothing recorded has no payment, so verification fails with `NO_PAYMENT_FOUND`. Refunds honour idempotency keys and never exceed what is left of the capture, in its currency. `onNextRefund` makes the next refund for an order be rejected, or be made and then time out, so tests can drive every path. `settle` reports that the order's money reached the merchant (the capture, or the COD remittance), as `SettlementObservedEvt`. The ledger is lost on restart.

The simulators do no I/O, so they run inside the inbox transaction. A real adapter must follow the provider side-effect pattern in `docs/ARCHITECTURE.md`, take the tenant's automation mode, and key its writes by order ID, because a retried step resends its command under a new event ID.

Each port has a simulator and, where planned, production adapters. The simulator is selected unless configuration names another adapter. It can succeed, fail, time out after succeeding, and send duplicate or out-of-order webhooks, and it supports any ISO 4217 currency.
