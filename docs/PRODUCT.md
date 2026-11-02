# Product

OrderLoop shows an Indian D2C brand how much money is stuck in its orders this week, and helps close each case.

It sits between an order being placed and its money being settled. It does not replace an OMS, a checkout or a shipping aggregator.

Market assumptions below are unproven until Gate 0 is done. See `docs/GATE_0.md`.

## Ideal customer (assumption)

- Sells on Shopify in India, 1,000 to 10,000 orders a month
- At least 40% cash on delivery
- Uses Razorpay and Shiprocket
- Runs operations from spreadsheets, without a full OMS

## Leak detectors

| Detector | Condition | Amount at risk | Action |
|---|---|---|---|
| Paid, not fulfilled | Payment captured; order cancelled, or no shipment past its deadline | Captured amount | Propose refund, or chase fulfilment |
| NDR past SLA | Delivery failed and no decision made within 24 hours | Order value | Request re-attempt, or return |
| RTO not restocked | Return received; stock still unavailable after 48 hours | Cost of stock if known, otherwise order value, labelled as an estimate | Propose restock |
| COD remittance gap | COD delivered; no remittance by the expected date | COD amount | Add to dispute list |
| Stuck order | Any saga state past its deadline | Order value | Open an exception with its timeline |

Deadlines are defaults and each tenant can configure them.

## Screens

1. **Money at risk:** this week's total in rupees, split by detector, with the single most valuable next action.
2. **Exceptions:** a queue sorted by amount, with status.
3. **Order timeline:** every saga step and provider event for one order.
4. **Approvals:** refunds and restocks awaiting a human.
5. **Settings:** provider connections, deadlines, mode.
6. **Recovered:** money closed out, which is what justifies renewal.

## Rules of the product

- Observe mode is the default. OrderLoop reads and advises until a merchant opts into an action.
- A human approves every refund in version 1.
- An estimate is labelled as an estimate.
- OrderLoop never holds customer or merchant funds. Refunds run through the merchant's own Razorpay account.

## Plans (to be tested)

| Plan | Price per month, excl. GST | Orders included | Overage per order |
|---|---|---|---|
| Leak Audit | ₹4,999 once, later free as lead generation | 90 days of history | None |
| Growth | ₹2,999 | 2,000 | ₹1.00 |
| Scale | ₹7,999 | 10,000 | ₹0.75 |

Value check, using assumed inputs: 3,000 orders × 60% COD × 25% RTO = 450 RTO orders. At ₹180 to ₹240 each, that costs ₹81,000 to ₹1,08,000 a month. Preventing 10% saves ₹8,100 to ₹10,800, about 2.7 to 3.6 times the Growth price.

## Out of scope for version 1

Warehouse management, catalogue sync, multi-channel marketplaces, checkout tools, automatic refunds, returns portals.

## Metrics

- Activation: connected providers and first finding within 24 hours
- Value: rupees identified and rupees recovered per merchant per month
- Trust: findings dismissed as wrong
- Business: paying merchants, churn, support hours per merchant
