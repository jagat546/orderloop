#!/usr/bin/env bash
# Whole-repo invariant check. Run locally, from the Stop hook and in CI. Exit 1 on any violation.
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 1
# shellcheck source=lib-invariants.sh
source scripts/lib-invariants.sh

failures=0
report() {
  echo "$1: $2"
  failures=$((failures + 1))
}

while IFS= read -r -d '' file; do
  while IFS= read -r line; do
    [ -n "$line" ] && report "$file" "$line"
  done < <(java_violations "$file" <"$file")

  # Every JPA entity is tenant-scoped unless it states why not.
  if grep -qE '^[[:space:]]*@Entity\b' "$file"; then
    grep -qE '@TenantId\b|tenant-exempt:' "$file" ||
      report "$file" "@Entity without @TenantId (or a // tenant-exempt: reason comment)"
  fi
done < <(find . -type f -name '*.java' -path '*/src/main/java/*' -not -path './.git/*' -print0)

while IFS= read -r -d '' file; do
  while IFS= read -r line; do
    [ -n "$line" ] && report "$file" "$line"
  done < <(migration_violations "$file" <"$file")
done < <(find . -type f -name '*.sql' -path '*/db/migration/*' -not -path './.git/*' -not -path '*/target/*' -print0)

# Exemptions are allowed, but they stay visible.
exemptions="$(grep -rlE 'tenant-exempt:' --include='*.java' --include='*.sql' . 2>/dev/null | grep -v '/target/' || true)"
if [ -n "$exemptions" ]; then
  echo "Tenant exemptions to review:"
  while IFS= read -r line; do echo "  $line"; done <<<"$exemptions"
fi

if [ "$failures" -gt 0 ]; then
  echo "Invariant check failed with $failures violation(s)."
  exit 1
fi
echo "Invariant check passed."
