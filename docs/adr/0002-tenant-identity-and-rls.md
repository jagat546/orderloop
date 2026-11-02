# 0002: Verified tenant identity, enforced by PostgreSQL RLS

**Status:** Accepted
**Date:** 2026-11-02

## Context

The original spec took the tenant from a client-supplied `X-Tenant-ID` header and enforced isolation with Hibernate `@Filter`. Any client could claim any tenant. [Hibernate filters do not apply to find-by-ID lookups](https://discourse.hibernate.org/t/find-by-id-and-filters/7632) or native SQL, and they must be enabled on each session.

## Decision

- The tenant comes from a verified credential. A mismatched header returns 403.
- PostgreSQL forced RLS is the isolation boundary, using a non-owner role without `BYPASSRLS`.
- Hibernate `@TenantId` populates and filters tenant IDs as a convenience.
- Tenant context lives in `ScopedValue`.

## Consequences

- A forgotten filter or a raw query cannot cross tenants.
- Each connection checkout runs an extra statement, and we must use separate roles for app, ops and migrations.
- Integration tests must connect as `orderloop_app`, because superusers bypass RLS.
- Revisit if per-checkout cost becomes measurable at scale.
