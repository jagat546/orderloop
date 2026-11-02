# Deployment

The reference production setup is one 16 GB VPS with Docker Compose. All sizing here is an estimate, so measure it during M4. The M1 demo (`docker compose up` with simulators) is for evaluation only, not production.

## Memory budget

| Component | Estimate |
|---|---|
| PostgreSQL | 3 GB |
| Kafka in KRaft mode, one broker | 2 GB |
| Redis | 256 MB |
| 5 Spring Boot services at about 700 MB | 3.5 GB |
| Caddy, serving the dashboard and TLS | 128 MB |
| Grafana, Loki, Tempo and Prometheus | 1.5 GB |
| Spare | About 5.6 GB |

## Choosing a server

Reprice at deploy time. [Hetzner's prices rose sharply in 2026, and its CX and CAX plans showed as unavailable in September and October](https://www.bitdoze.com/es/hetzner-cloud-precios-2026/). Compare regions close to your merchants too, for latency and data location.

## Layout

```text
deploy/
  docker-compose.yml        # Development infrastructure only (M1.2)
  postgres/init/            # Creates roles and schemas on an empty volume
  docker-compose.prod.yml
  Caddyfile
  backup/
  grafana/
  .env              # On the server only
```

Only Caddy exposes ports, and only 80 and 443. PostgreSQL, Kafka and Redis stay on a private Docker network.

## Durability

- PostgreSQL holds the truth. A single Kafka broker with replication factor 1 is acceptable only because published outbox rows are kept for 7 days and can be replayed.
- Archive WAL and take a daily base backup to encrypted object storage in another location.
- Run a restore drill every month, and record its date and duration. An untested backup does not count.

## Release

1. CI builds images, tagged with the git SHA.
2. Run `/deploy-prod <tag>`.
3. Flyway migrates as `orderloop_owner`.
4. Restart services one at a time, waiting for readiness.
5. Check outbox age, consumer lag, error rate and one test-store order.

Webhooks retry, so a short outage delays orders and loses none. Version 1 makes no high-availability promise.

## Rollback

- Redeploy the previous image tag. Migrations must stay compatible with that version.
- Fix a bad migration with a new migration, never by editing it.
- Pause the relay if events are wrong, because it holds work safely in the outbox.

## Replaying after a Kafka loss

1. Restore Kafka and create topics.
2. Set `published_at = NULL` on rows from the affected window.
3. Restart the relays. The inbox absorbs duplicates.

## When to upgrade

Move to a second node and managed PostgreSQL when measured load outgrows one server, or when a merchant needs an SLA.
