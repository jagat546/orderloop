#!/usr/bin/env bash
# PreToolUse for Edit, Write and MultiEdit. Exit 2 blocks the edit and shows stderr to Claude.
set -uo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "guard-edits: jq is required so project rules can be enforced. Install jq." >&2
  exit 2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../scripts/lib-invariants.sh
source "$here/../../scripts/lib-invariants.sh"

input="$(cat)"
file="$(jq -r '.tool_input.file_path // empty' <<<"$input")"
[ -z "$file" ] && exit 0
# Claude Code on Windows sends backslash paths; every pattern below expects forward slashes.
file="${file//\\//}"

new_text="$(jq -r '[.tool_input.content, .tool_input.new_string, (.tool_input.edits // [] | .[].new_string)]
  | map(select(. != null)) | join("\n")' <<<"$input")"

fail() {
  echo "Blocked edit to $file: $1" >&2
  exit 2
}

name="$(basename "$file")"
case "$name" in
  .env.example) ;;
  .env|.env.*|*.pem|*.key) fail "secrets stay out of Claude's reach. Ask the human to change this file." ;;
esac
case "$file" in
  secrets/*|*/secrets/*) fail "secrets stay out of Claude's reach. Ask the human to change this file." ;;
esac

# A migration that git already tracks may have been applied somewhere, so it is immutable.
case "$file" in
  */db/migration/*.sql|db/migration/*.sql)
    dir="$(dirname "$file")"
    if [ -f "$file" ] && git -C "$dir" ls-files --error-unmatch "$name" >/dev/null 2>&1; then
      fail "committed migrations are immutable. Add a new V<n>__ migration instead."
    fi
    ;;
esac

violations="$(
  java_violations "$file" <<<"$new_text"
  migration_violations "$file" <<<"$new_text"
)"
if [ -n "$violations" ]; then
  echo "Blocked edit to $file:" >&2
  while IFS= read -r line; do echo "- $line" >&2; done <<<"$violations"
  exit 2
fi
exit 0
