---
paths:
  - "**/db/migration/**"
---

# Migration rules

- Each service owns its schema and migrations under `src/main/resources/db/migration`.
- Name files `V<number>__<what_changed>.sql`. Committed migrations are immutable; add a new one.
- Flyway runs as `orderloop_owner`. The app never owns tables, because owners can bypass RLS unless it is forced.
- Copy the table template from `docs/DATA_MODEL.md`. Put `tenant_id text NOT NULL` on its own line so checks can see it.
- Grant only what the role needs, in the same migration that creates the table.
- Money columns are `bigint` with a `_minor` suffix, beside `currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$')`. Timestamps are `timestamptz`.
- RLS is forced, so the owner sees no rows. A data backfill needs the temporary `owner_backfill` policy in `docs/DATA_MODEL.md`.
- Stay backward compatible with the running version: add nullable first, backfill, then constrain in a later migration.
- Avoid long locks on tables that hold data. Use `CREATE INDEX CONCURRENTLY` in its own non-transactional migration.
