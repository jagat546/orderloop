# Deployment

The reference production setup is one 16 GB VPS with Docker Compose. All sizing here is an estimate, so measure it during M4. The M1 demo (`docker compose up` with simulators) is for evaluation only, not production.

## The demo

`docker compose up --build --wait` at the repository root builds the four services from source inside Docker and starts them on simulators, plus `demo-orders`, which posts demo orders through the order API every 30 seconds in INR, JPY and KWD. They reach `Closed`, `ClosedRto`, or an open exception (one in five rounds) on their own; see `docs/INTEGRATIONS.md` for how each ID chooses its outcome. Its log lists each order it created. Everything runs beside PostgreSQL, Kafka, Redis, Prometheus, Tempo and Grafana from `deploy/docker-compose.yml`. It needs only Docker, and fits about 4 GB of Docker memory: each service is capped at 448 MB, PostgreSQL at 512 MB and Kafka at 768 MB. It is a Compose project of its own (`orderloop-demo`), with its own volumes. PostgreSQL, Kafka and Redis publish no host ports in the demo, but Grafana, Prometheus and Tempo use the same host ports as the development telemetry, so stop one before starting the other. `bash scripts/demo-smoke.sh --no-up` checks a running demo: an order created through the order API is handled by the services, the generator's orders reach `Closed`, `ClosedRto` and `NeedsAttention`, and those transitions reach Prometheus.

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
compose.yaml                # The demo: infrastructure, telemetry and every service (M1.16a)
Dockerfile                  # Service images built from source, for the demo
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
