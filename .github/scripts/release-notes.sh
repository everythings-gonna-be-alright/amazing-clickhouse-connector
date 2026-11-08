#!/usr/bin/env bash
set -euo pipefail

version="${1:?usage: release-notes.sh VERSION}"
changelog="${2:-CHANGELOG.md}"

notes="$(awk -v header="## [${version}]" '
  index($0, header) == 1 { found = 1; next }
  found && /^## \[/ { exit }
  found && /^\[[^]]+\]: / { exit }
  found { print }
' "$changelog")"

if [ -z "$(printf '%s' "$notes" | tr -d '[:space:]')" ]; then
  echo "no changelog entry for version ${version} in ${changelog}" >&2
  exit 1
fi

printf '%s\n' "$notes"
