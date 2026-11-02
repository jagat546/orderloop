---
name: add-leak-detector
description: Use when adding or changing a detector that finds money at risk in OrderLoop orders.
argument-hint: "[detector name]"
---

# Add a leak detector

Detector: `$ARGUMENTS`

1. Write down, in `docs/PRODUCT.md`, what the leak is, the exact condition, how its rupee amount is calculated, and what the merchant can do about it.
2. Write tests from fixtures: a true positive, a near miss, and an order that later resolves on its own. Include the exact expected paise.
3. Implement the detection as a pure function over order data. Run it through a scheduled scan that handles each tenant inside its own scope.
4. Save a `leak_finding` with a unique `(tenant_id, detector, order_id)`, so repeated scans do not double-count.
5. Close the finding automatically when the condition clears, and record whether money was recovered.
6. Keep `amount_at_risk_paise` honest. If an amount is an estimate, label it as an estimate in the API and the UI.
7. Add it to the dashboard with one clear next action.
8. Run `/verify`.
