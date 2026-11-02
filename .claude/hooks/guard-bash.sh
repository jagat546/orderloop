#!/usr/bin/env bash
# PreToolUse for Bash. Exit 2 blocks the command and shows stderr to Claude.
# This stops accidents. It matches command text, so it is not a security boundary.
set -uo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "guard-bash: jq is required so project rules can be enforced. Install jq." >&2
  exit 2
fi

cmd="$(jq -r '.tool_input.command // empty')"
[ -z "$cmd" ] && exit 0

block() {
  echo "Blocked command: $1" >&2
  exit 2
}

grep -qiE 'flyway[: ]+clean' <<<"$cmd" &&
  block "flyway clean wipes the schema. Ask the human to do this."
grep -qiE 'drop[[:space:]]+(database|schema)' <<<"$cmd" &&
  block "dropping a database or schema is the human's call."
grep -qE 'git[[:space:]]+push\b.*(--force|[[:space:]]-f\b)' <<<"$cmd" &&
  block "force push rewrites shared history. Ask the human."
grep -qE 'git[[:space:]]+reset\b.*--hard' <<<"$cmd" &&
  block "git reset --hard discards work. Ask the human."
grep -qE 'git[[:space:]]+clean\b.*-[a-zA-Z]*f' <<<"$cmd" &&
  block "git clean -f deletes untracked files. Ask the human."
grep -qE '(^|[[:space:]])--no-verify\b' <<<"$cmd" &&
  block "do not skip checks. Fix what they report."
grep -qE '(curl|wget)\b.*\|[[:space:]]*(sudo[[:space:]]+)?(ba|z)?sh\b' <<<"$cmd" &&
  block "do not pipe downloads into a shell."
grep -qE 'rm[[:space:]]+-[a-zA-Z]*r[a-zA-Z]*f?[a-zA-Z]*[[:space:]]+([^|;&]*[[:space:]])?(/|~|\$HOME|\.\.|\.git|\.claude|docs)(/|[[:space:]]|$)' <<<"$cmd" &&
  block "that recursive delete targets something this project needs."

# Secrets: remove the safe example first, then look for real env files.
scrubbed="${cmd//.env.example/}"
grep -qE "(^|[[:space:]/'\"=])\.env(\.[A-Za-z0-9_-]+)?([[:space:]'\"]|$)" <<<"$scrubbed" &&
  block "commands may not touch .env files. Use .env.example, or ask the human."

exit 0
