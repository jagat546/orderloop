# 0005: One VPS with Docker Compose until revenue justifies more

**Status:** Accepted
**Date:** 2026-11-02

## Context

Kubernetes, managed Kafka and managed PostgreSQL would cost more than expected early revenue, and would add operating work for one person.

## Decision

Run all services on one 16 GB VPS with Docker Compose, a single-broker Kafka and PostgreSQL as the source of truth. Keep published outbox rows for 7 days, so Kafka can be rebuilt. Use encrypted off-site backups and monthly restore drills.

## Consequences

- Low cost and simple operations.
- A server failure means downtime, so version 1 makes no high-availability promise.
- Kafka adds memory cost that a smaller system might avoid. It stays because the product needs ordered, replayable messaging across services, and because it is a stated learning goal.
- Revisit at about 20 paying merchants, or the first SLA contract.
