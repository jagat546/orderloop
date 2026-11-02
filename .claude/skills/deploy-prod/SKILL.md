---
name: deploy-prod
description: Use when the user explicitly asks to deploy OrderLoop to production.
disable-model-invocation: true
argument-hint: "[git tag]"
---

# Deploy to production

Release: `$ARGUMENTS`

Follow `docs/DEPLOYMENT.md`. Every command that reaches the server needs the user's approval, so show each one and its purpose before running it.

Before deploying, confirm all of these and report the evidence:

1. `/verify` passes on the exact tag.
2. Migrations are backward compatible with the running version.
3. The latest backup is recent and the last restore drill passed.
4. No open incident exists, and it is a time when someone can watch the release.

Then:

1. Pull the images for the tag.
2. Let Flyway migrate as `orderloop_owner`.
3. Restart services one at a time and wait for each readiness check.
4. Check the outbox backlog, consumer lag, error rate and one test-store order.
5. Report the result.

If a check fails, stop and offer the rollback in `docs/DEPLOYMENT.md`. Never roll back a migration by editing it.
