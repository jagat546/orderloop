# Product

OrderLoop shows a Shopify merchant how much money is stuck in its orders this week, and helps close each case. It is open source, and it runs on simulators out of the box, so anyone can try it without provider accounts. See ADR 0007.

It sits between an order being placed and its money being settled. It does not replace an OMS, a checkout or a shipping aggregator.

## Who it is for

- Shopify merchants whose orders pass through a payment provider and a shipping provider before money settles, and the developers who run OrderLoop for them
- Merchants with cash on delivery, failed deliveries or high return rates leak the most. The first detectors came from studying Indian D2C brands, but nothing in the design is specific to one country or currency
- Teams that run operations from spreadsheets, without a full OMS

Pricing and sales material is optional and lives in `docs/business/`.

## Leak detectors

| Detector | Condition | Amount at risk | Action |
|---|---|---|---|
| Paid, not fulfilled | Payment captured; order cancelled, or no shipment past its deadline | Captured amount | Propose refund, or chase fulfilment |
| NDR past SLA | Delivery failed and no decision made within 24 hours | Order value | Request re-attempt, or return |
| RTO not restocked | Return received; stock still unavailable after 48 hours | Cost of stock if known, otherwise order value, labelled as an estimate | Propose restock |
| COD remittance gap | COD delivered; no remittance by the expected date | COD amount | Add to dispute list |
| Stuck order | Any saga state past its deadline | Order value | Open an exception with its timeline |

Deadlines are defaults and each tenant can configure them. Every amount is in the order's own currency.

## Screens

1. **Money at risk:** this week's total for each currency, split by detector, with the single most valuable next action.
2. **Exceptions:** a queue sorted by amount, with status.
3. **Order timeline:** every saga step and provider event for one order.
4. **Approvals:** refunds and restocks awaiting a human.
5. **Settings:** provider connections, deadlines, mode.
6. **Recovered:** money closed out, per currency.

## Rules of the product

- Observe mode is the default. OrderLoop reads and advises until a merchant opts into an action.
- A human approves every refund in version 1.
- An estimate is labelled as an estimate.
- Amounts in different currencies are never added together or converted. Totals are shown per currency.
- OrderLoop never holds customer or merchant funds. Refunds run through the merchant's own payment provider account.
- Simulators are the default providers. Real adapters are opt-in: Shopify and Stripe first, Razorpay and Shiprocket optional.

## Out of scope for version 1

Warehouse management, catalogue sync, multi-channel marketplaces, checkout tools, automatic refunds, returns portals, currency conversion.

## Metrics

- Activation: connected providers and first finding within 24 hours
- Value: money identified and money recovered per merchant per month, per currency
- Trust: findings dismissed as wrong
