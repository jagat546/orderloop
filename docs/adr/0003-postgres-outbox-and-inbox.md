# 0003: PostgreSQL outbox and inbox; Redis is never correctness

**Status:** Accepted
**Date:** 2026-11-02

## Context

The original spec allowed Redisson locks or a `processed_events` table for idempotency. A lock can expire while work continues, and a successful handler can crash before recording that it finished. Neither an inbox nor a lock can undo a provider call already sent.

## Decision

- Outbox and inbox rows commit in the same transaction as the business change.
- One relay per service holds a PostgreSQL advisory lock, to keep per-order ordering simple.
- Provider writes use intent, idempotency key, result and reconciler.
- Redis is for caching and rate limits only.

## Consequences

- Delivery is effectively once for database state, without distributed transactions.
- The relay is a throughput limit and adds polling load on PostgreSQL. That suits expected volume.
- Revisit with change data capture, such as Debezium, if outbox lag becomes a real limit.
