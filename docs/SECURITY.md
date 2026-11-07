# Security and data protection

This is an engineering checklist, not legal advice. Have a qualified person confirm the legal items before charging customers.

## Threats that matter most

1. One merchant reads another's data.
2. A forged webhook changes order state.
3. A bug or replay refunds money twice.
4. A leaked provider token.
5. Customer PII leaks through logs or backups.

## Controls

| Threat | Control |
|---|---|
| Cross-tenant access | Verified tenant identity, forced RLS, non-owner role, cross-tenant tests |
| Forged webhook | Signature check on raw bytes in constant time, before parsing |
| Double refund | Human approval, idempotency key, intent table, reconciler, refund limited to captured minus refunded |
| Token theft | AES-256-GCM encryption at rest, key held outside the database, no tokens in logs, least-privilege scopes |
| PII leak | Observe mode stores none, IDs in logs and Kafka, raw events purged after 14 days, encrypted backups |
| Dashboard attacks | Session tokens, strict content security policy, generated API types, no tokens in browser storage |
| Secret exposure | `.env` outside git, secret scanning of the whole git history in CI (gitleaks, `.gitleaks.toml`), deny rules and hooks for Claude Code |

## Shopify

- Apply for only the protected customer data levels the feature needs. See `docs/INTEGRATIONS.md`.
- Implement the mandatory privacy webhooks before submission.
- On uninstall: revoke tokens, stop processing and schedule deletion.

## Data protection law

Which law applies depends on where the operator and the merchants' customers are, for example GDPR in the EU and UK, or India's DPDP Act, whose [data fiduciary duties apply from May 2027](https://bhattandjoshiassociates.com/?p=33581) under the published timeline. Whoever runs OrderLoop for merchants generally acts as their processor, which a lawyer should confirm. Plan for:

- A processor agreement with each merchant
- Purpose limits and deletion on request or at the end of the contract
- A breach response plan and incident log
- A record of where data is stored

## Payments regulation

OrderLoop must never hold or route customer funds. Refunds run through the merchant's own payment provider account. Get advice before changing this, because holding funds may bring payment-aggregator regulation.

## Open source

- Before the repository goes public: scan the whole git history for secrets, and confirm it holds no employer or client material. See roadmap section P.
- Simulators need no secrets, so the demo and CI run without any.
- Publish a security reporting policy, so vulnerabilities are not reported in public issues.

## Audit trail

Save who approved each refund or restock, when, and the exact amount shown. Never update or delete those rows.
