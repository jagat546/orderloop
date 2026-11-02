---
name: verify
description: Use when about to commit or about to say work in OrderLoop is complete, fixed or passing.
---

# Verify

Run these in order, and stop at the first failure to fix it. Skip a step only if the thing it checks does not exist yet, and say that you skipped it.

1. `bash scripts/check-invariants.sh`
2. `bash scripts/test-hooks.sh`, if anything under `.claude/hooks/` or `scripts/` changed
3. `mvn -q spotless:apply`, if a `pom.xml` exists
4. `mvn -q verify`, limited with `-pl <module> -am` when only one module changed
5. `npm --prefix web-dashboard run lint` and `npm --prefix web-dashboard test`, if `web-dashboard` changed

Then check the change itself:

- Tenant data: is there a cross-tenant test running as `orderloop_app`?
- Listener: is there a duplicate-delivery test?
- Saga or money: is there a failure-path test, and did `docs/SAGA.md` change with the logic?
- API or event contract: did the OpenAPI document or `common-dto` change compatibly?

Report the commands and their real results, including any test count. If something could not run, such as Docker being unavailable, say so plainly and do not describe the work as verified.
