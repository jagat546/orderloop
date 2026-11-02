# 0004: Observe mode by default

**Status:** Accepted
**Date:** 2026-11-02

## Context

Target merchants usually already sync Shopify with a shipping aggregator, and Shopify already adjusts stock when an order is placed. If OrderLoop booked shipments or moved stock by default, it could create duplicate shipments and wrong stock counts. Automatic refunds also carry serious trust risk.

## Decision

New tenants start in observe mode: handlers verify state and open findings, and provider writes are blocked at the adapter. Action mode is opt-in per tenant. Refunds always need a human approval in version 1.

## Consequences

- Installing is low risk, and the product needs far less customer data.
- Value depends on findings being accurate, since OrderLoop cannot act by itself at first.
- The saga must accept events that arrive before the matching command's reply.
- Revisit automatic actions once merchants trust the findings and ask for them.
