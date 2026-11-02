---
name: next-slice
description: Use when the user wants to continue building OrderLoop or asks what to work on next.
disable-model-invocation: true
---

# Next slice

1. Read `docs/ROADMAP.md` and pick the first unchecked item in the lowest unfinished milestone. If `$ARGUMENTS` names a slice, use that instead.
2. If the item is marked `[human]`, tell the user what it needs and stop.
3. Read only the docs and existing code that slice needs.
4. State in three lines or fewer what you will build and how you will prove it. If the slice conflicts with a doc or an invariant, stop and ask.
5. For saga, tenancy, messaging or money: write the failing test first and watch it fail for the expected reason.
6. Build the smallest thing that satisfies the slice's "Done when". Do not start the next slice.
7. Run `/verify`.
8. Tick the item in `docs/ROADMAP.md` only if every "Done when" condition has passed.
9. Report what changed, what you ran, the actual results, and anything you found that the roadmap missed.

If a slice is too large for one session, split it in the roadmap first and do only the first part.
