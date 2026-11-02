---
name: add-tenant-entity
description: Use when adding a new table or JPA entity that holds merchant data to an OrderLoop service.
argument-hint: "[service] [EntityName]"
---

# Add a tenant entity

Target: `$ARGUMENTS`

1. Write a failing integration test first, connected as `orderloop_app`. Tenant B must not read, update or delete tenant A's row, through JPA `find` or native SQL.
2. Add a new migration using the table template in `docs/DATA_MODEL.md`:
   - `tenant_id text NOT NULL` on its own line
   - primary, unique and foreign keys that include `tenant_id`
   - `ENABLE` and `FORCE ROW LEVEL SECURITY`
   - the `tenant_isolation` policy
   - grants to `orderloop_app` only
3. Add the entity as a plain class:
   - `@TenantId` on `tenantId`
   - protected no-argument constructor
   - domain methods, no public setters
   - money as `long` with a `Paise` suffix
4. Add the repository, and expose records from the service layer rather than the entity.
5. Update `docs/DATA_MODEL.md`.
6. Run `/verify`, and ask the `tenant-auditor` agent to review the diff.
