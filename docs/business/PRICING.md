# Pricing and business metrics

**Optional, and not in the build path.** Kept from the original paid-app plan in case a hosted version is offered later. See ADR 0007. Prices are untested and in Indian rupees, because the first market studied was Indian D2C brands.

## Plans (to be tested)

| Plan | Price per month, excl. GST | Orders included | Overage per order |
|---|---|---|---|
| Leak Audit | ₹4,999 once, later free as lead generation | 90 days of history | None |
| Growth | ₹2,999 | 2,000 | ₹1.00 |
| Scale | ₹7,999 | 10,000 | ₹0.75 |

Value check, using assumed inputs: 3,000 orders × 60% COD × 25% RTO = 450 RTO orders. At ₹180 to ₹240 each, that costs ₹81,000 to ₹1,08,000 a month. Preventing 10% saves ₹8,100 to ₹10,800, about 2.7 to 3.6 times the Growth price.

## Selling a hosted version would also need

- Shopify Billing, plans and usage overage. [App Store charges must use the Billing API](https://shopify.dev/docs/apps/store/common-rejections). Check support for each billing currency.
- [Shopify's revenue share: 0% on the first $1 million of lifetime revenue, 15% after](https://shopify.dev/changelog/posts/update-to-shopifys-app-developer-revenue-share.md).
- A privacy policy, a processor agreement, tax registration, and Shopify's protected customer data application.
- App Store submission.

## Business metrics

- Paying merchants, churn, support hours per merchant
- Money recovered per merchant per month, per currency, as the basis for renewal

Validate demand first with `docs/business/GATE_0.md`.
