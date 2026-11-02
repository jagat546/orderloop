# OrderLoop

An open-source Shopify app that finds and closes money leaking from stuck orders, in any currency. Built as a multi-tenant, saga-orchestrated Spring Boot system. Payment and fulfilment providers sit behind interfaces, and simulators are the default, so it runs with no accounts or API keys.

This repository holds the design and the Claude Code setup. No application code exists yet. The one-command Docker demo arrives at the end of M1 in `docs/ROADMAP.md`. No licence has been chosen yet; see `docs/LICENSING.md`.

## Start here

1. Install Claude Code, Java 25, Maven, Docker, `jq` and Git. On Windows, run from WSL or Git Bash so the hooks work.
2. Put these files in a new Git repository and commit them.
3. Run `bash scripts/test-hooks.sh`. It should report no failures.
4. Run `claude` from the repository root and accept the workspace trust prompt, so project hooks and settings load.
5. Run `/hooks` and `/memory` to confirm they loaded.
6. Run `/next-slice`.

Pricing and sales material is optional and lives in `docs/business/`, outside the build path.

## What is here

| Path | Purpose |
|---|---|
| `CLAUDE.md` | Always-loaded project index and invariants |
| `.claude/rules/` | Rules that load when matching files are touched |
| `.claude/skills/` | `/next-slice`, `/verify`, `/add-tenant-entity`, `/add-saga-step`, `/add-kafka-contract`, `/add-integration`, `/add-leak-detector`, `/deploy-prod`, `/record-decision` |
| `.claude/agents/` | `saga-reviewer`, `tenant-auditor`, `test-engineer`, `provider-docs-researcher` |
| `.claude/hooks/` | Blocks banned code, edits to committed migrations, secrets access and destructive commands |
| `.claude/settings.json` | Permissions and hook registration |
| `scripts/` | Invariant checks and hook tests |
| `docs/` | Product, architecture, saga, data model, integrations, security, deployment, roadmap, licence options, ADRs |
| `docs/business/` | Optional: Gate 0 and pricing |

Put personal preferences in `CLAUDE.local.md` or `.claude/settings.local.json`. Both are ignored by Git.

## Limits of the guardrails

Hooks and permission rules prevent common mistakes, and they match text. They are not a security boundary, so review diffs and keep CI as the final check.
