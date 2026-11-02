# Integrations

Facts here were checked on 2026-11-02 unless marked otherwise. Re-check with the `provider-docs-researcher` agent before coding, and add what you confirm.

## Shopify

| Topic | Status | Note |
|---|---|---|
| Billing | Confirmed | [App Store charges must use the Billing API](https://shopify.dev/docs/apps/store/common-rejections). Confirm INR support |
| Revenue share | Confirmed | [0% on the first $1 million of lifetime revenue, 15% after](https://shopify.dev/changelog/posts/update-to-shopifys-app-developer-revenue-share.md) |
| Embedded auth | Confirmed | [Session tokens and App Bridge are expected for embedded apps](https://shopify.dev/docs/apps/store/common-rejections) |
| Protected customer data | Confirmed | [Name, address, phone and email are level 2, and need approval](https://Shopify.dev/docs/apps/store/data-protection/protected-customer-data). Level 2 also requires encrypted backups |
| Webhook signature | To confirm | HMAC of the raw body in a header; confirm the current scheme |
| Mandatory privacy webhooks | To confirm | Data request, customer redact and shop redact; confirm current names and timing |
| Inventory and order read scopes | To confirm | Request the minimum |

## Razorpay

| Topic | Status | Note |
|---|---|---|
| Refund idempotency | Confirmed | [`X-Refund-Idempotency` header, at least 10 characters](https://razorpay.com/docs/api/refunds/normal-refunds-idempotent) |
| Webhook signature | Confirmed | [HMAC-SHA256 of raw bytes in `X-Razorpay-Signature`](https://razorpay.com/blog/?p=28285) |
| Webhook delivery | Confirmed | [At least once, order not guaranteed, retried for 24 hours, 5-second response window, disabled after persistent failure](https://razorpay.com/blog/?p=28285) |
| Settlement reports | To confirm | Needed to spot gateway settlement gaps |
| How merchants connect | To confirm | OAuth partner flow or merchant API keys |

## Shiprocket

All of this is unconfirmed, and it carries the most risk. Spike it first in M2.

- Webhook authentication and which delivery, NDR and RTO statuses are exposed
- Whether NDR re-attempts can be requested through the API
- Whether COD remittance can be read through the API. The fallback is CSV upload
- Rate limits and token expiry

## Port interfaces

```java
public interface PaymentProvider {
  PaymentStatus lookup(String tenantId, String orderId);
  RefundResult refund(String tenantId, RefundRequest request); // Carries the idempotency key
  Optional<RefundResult> findRefund(String tenantId, String idempotencyKey);
}
```

Each port has a production adapter and a simulator. The simulator can succeed, fail, time out after succeeding, and send duplicate or out-of-order webhooks.
