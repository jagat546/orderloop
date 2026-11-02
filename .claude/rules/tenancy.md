---
paths:
  - "**/tenant/**"
  - "**/domain/**"
  - "**/*Entity.java"
  - "**/*Repository.java"
  - "**/db/migration/**"
---

# Tenancy rules

**Why:** one leaked order between merchants ends this business. Isolation must hold when application code is wrong.

## Identity

- Tenant comes from a verified credential: Shopify session token, API key or webhook signature.
- If `X-Tenant-ID` is present and differs from the verified tenant, return 403. Never use it as the source.
- Access tenant identity only through `TenantContext.require()`. It throws when unbound.

## Scope

```java
TenantContext.callAs(tenantId, () -> tx.execute(status -> handler.handle(event)));
```

- Bind the scope before any connection is acquired, so the RLS DataSource sets the right tenant.
- Never start a transaction outside a scope and rebind inside it.
- Scoped values reach child threads only through `StructuredTaskScope`, which is still preview in Java 25. Pass the tenant explicitly to any other thread and rebind it there.

## Database

- Each tenant table has `tenant_id text NOT NULL`, forced RLS and the policy in `docs/DATA_MODEL.md`.
- Primary keys, unique keys and foreign keys include `tenant_id`, so a row cannot reference another tenant's row.
- Indexes start with `tenant_id`.
- The app role is `orderloop_app`: not an owner, not a superuser, no `BYPASSRLS`.
- Only the relay and schedulers use `orderloop_ops`, and only on their own tables. They read due rows, then handle each one inside that row's tenant scope.

## JPA

- Entities use Hibernate `@TenantId`. `@Filter` and `@FilterDef` are banned.
- `find` by ID is not filtered by Hibernate. RLS is what protects it, so a cross-tenant lookup must return empty in tests.
- `spring.jpa.open-in-view=false` everywhere.

## Exemptions

A table or entity with no tenant data needs a `tenant-exempt: <reason>` comment. `scripts/check-invariants.sh` lists every exemption for review.
