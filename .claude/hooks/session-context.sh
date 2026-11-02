#!/usr/bin/env bash
# SessionStart. Stdout is added to Claude's context, so keep it to what changes between sessions.
set -uo pipefail

root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

if git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Branch: $(git -C "$root" branch --show-current 2>/dev/null)"
  changed="$(git -C "$root" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
  echo "Uncommitted files: $changed"
fi

if [ -f "$root/docs/ROADMAP.md" ]; then
  next="$(grep -m1 -E '^- \[ \]' "$root/docs/ROADMAP.md" || true)"
  [ -n "$next" ] && echo "Next roadmap item: ${next#- \[ \] }"
fi
exit 0
