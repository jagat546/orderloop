# 0008: Licence the code under Apache-2.0

**Status:** Accepted

**Date:** 2026-11-07

## Context

ADR 0007 made OrderLoop open source first and left the licence open. `docs/LICENSING.md` compared the two candidates, Apache-2.0 and AGPL-3.0, and named the questions that decide between them: whether a hosted version will be sold, whether company contributors are wanted, and whether an employment contract affects who owns the code. Until a `LICENSE` file exists, all rights are reserved, and the repository cannot usefully go public (roadmap P.2, P.4).

The project's value now is as a reference system that people can clone, run, read and reuse: a multi-tenant, saga-orchestrated core built on simulators. No hosted commercial offering is planned (ADR 0007 keeps the business material optional), and the dependencies it builds on (Spring, Kafka clients) are themselves Apache-2.0.

## Decision

License all code and documentation in this repository under the Apache License, Version 2.0. The full text is in `LICENSE` at the root, unchanged from https://www.apache.org/licenses/LICENSE-2.0.txt.

Contributions are accepted under the same licence, as its section 5 provides, with no separate contributor licence agreement.

## Consequences

- Anyone may use, modify, redistribute and sell OrderLoop, including inside closed-source products, if they keep the licence and notices and mark the files they change. Most companies allow Apache-2.0 without legal review, which helps adoption and outside contributions.
- Contributors grant an explicit patent licence, which ends for anyone who sues over patents in the work.
- A hosted, closed fork may compete with any future hosted offering and give nothing back. Dual licensing is not available as leverage, and a later move to a stricter licence would cover only new versions; everything released stays Apache-2.0.
- Each dependency's licence must still be confirmed compatible before publication, as `docs/LICENSING.md` says. The known ones are Apache-2.0 or BSD.
- The employment-contract and IP check (roadmap P.1) is still open. This decision chooses the licence the owner will grant; it cannot grant rights the owner does not hold, so P.1 must pass before the repository goes public (P.4).
- Revisit if a hosted commercial offering becomes the plan before the first public release, or if P.1 changes who owns the code. After a public release, only a relicence of new versions remains possible.
