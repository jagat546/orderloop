#!/usr/bin/env bash
# Stop. If Java or SQL changed, make Claude fix invariant violations before finishing.
# Exit 2 keeps Claude working. stop_hook_active stops an endless loop on a second attempt.
set -uo pipefail

command -v jq >/dev/null 2>&1 || exit 0
input="$(cat)"
[ "$(jq -r '.stop_hook_active // false' <<<"$input")" = "true" ] && exit 0

root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1 || exit 0
git -C "$root" status --porcelain 2>/dev/null | grep -qE '\.(java|sql)"?$' || exit 0

if ! output="$(cd "$root" && bash scripts/check-invariants.sh 2>&1)"; then
  {
    echo "scripts/check-invariants.sh failed. Fix these before finishing:"
    echo "$output"
  } >&2
  exit 2
fi
exit 0
