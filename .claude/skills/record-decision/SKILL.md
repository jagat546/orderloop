---
name: record-decision
description: Use when a design decision is made, reversed or changes an invariant in OrderLoop.
argument-hint: "[decision title]"
---

# Record a decision

Decision: `$ARGUMENTS`

Create `docs/adr/NNNN-short-title.md` using the next free number, with these parts in order:

- **Status:** Proposed, Accepted, or Superseded by NNNN
- **Date**
- **Context:** the forces at play, in a few sentences
- **Decision:** what we will do
- **Consequences:** what gets better, what gets worse, and what would make us revisit it

If it replaces an earlier ADR, mark that one superseded instead of editing its reasoning. Update `CLAUDE.md` or the relevant rule if behaviour changes. Do not record a decision the user has not agreed to as Accepted.
