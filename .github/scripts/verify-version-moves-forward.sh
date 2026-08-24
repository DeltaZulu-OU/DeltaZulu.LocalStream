#!/usr/bin/env bash
#
# Verify a package version moves forward from what is already published.
#
# Why this exists
# ---------------
# The estate's publish workflows checked only that the release tag was free. That
# is what let DeltaZulu.LocalStream's declared version move from an implicit 1.0.0
# down to an explicit 0.1.0 AFTER 1.0.0 had been published: the tag v0.1.0 was
# genuinely unused, so the guard passed while its intent inverted. Consumers pinned
# at 1.0.0 would never have resolved the result.
#
# Two sources of truth, in order
# ------------------------------
# The package feed is authoritative: it records what was actually published. Tags
# are secondary, because they record only what was published BY A WORKFLOW THAT
# TAGS. DeltaZulu.DurableBuffer publishes without tagging at all, so its released
# 1.0.0 is invisible to any tag-based check — which is precisely the hole this
# ordering closes.
#
# If the feed cannot be queried the check falls back to tags and says so, rather
# than failing the publish on an infrastructure problem.
#
# Usage: verify-version-moves-forward.sh <package_id> <package_version>
set -euo pipefail

PACKAGE_ID="${1:?usage: $0 <package_id> <package_version>}"
PACKAGE_VERSION="${2:?usage: $0 <package_id> <package_version>}"
FEED="${GITHUB_NUGET_FEED:-https://nuget.pkg.github.com/DeltaZulu-OU}"

lower_id="$(printf '%s' "$PACKAGE_ID" | tr '[:upper:]' '[:lower:]')"
published=""
source="feed"

if [ -n "${GITHUB_TOKEN:-}" ]; then
  published="$(curl -sSf -u "x-access-token:${GITHUB_TOKEN}" \
      "${FEED}/download/${lower_id}/index.json" 2>/dev/null \
    | python3 -c 'import json,sys
try:
    print("\n".join(json.load(sys.stdin).get("versions", [])))
except Exception:
    pass' || true)"
fi

if [ -z "$published" ]; then
  source="tags (feed unavailable)"
  published="$(git ls-remote --tags --refs origin 'refs/tags/v*' 2>/dev/null \
    | sed 's#.*refs/tags/v##' || true)"
fi

if [ -z "$published" ]; then
  echo "No published versions found via ${source}; '${PACKAGE_VERSION}' is the first."
  exit 0
fi

echo "Comparing against versions known from ${source}."

# The version list is passed by FILE, not by pipe: `python3 - <<'PY'` already uses
# stdin for the script itself, so a piped list never reaches sys.stdin and every
# comparison silently passes. That defect was caught by testing the fallback path.
versions_file="$(mktemp)"
trap 'rm -f "$versions_file"' EXIT
printf '%s\n' "$published" > "$versions_file"

python3 - "$PACKAGE_VERSION" "$versions_file" <<'PY'
import sys

def key(v):
    base, _, pre = v.partition("-")
    try:
        nums = tuple(int(p) for p in base.split("."))
    except ValueError:
        return None
    # A release outranks any prerelease sharing its base version.
    if not pre:
        return (nums, 1, ())
    return (nums, 0, tuple(
        (0, int(p)) if p.isdigit() else (1, p)
        for p in pre.replace("-", ".").split(".")
    ))

new, versions_path = sys.argv[1], sys.argv[2]
new_key = key(new)
if new_key is None:
    raise SystemExit(f"'{new}' is not a comparable version.")

best, best_key = None, None
for line in open(versions_path, encoding="utf-8"):
    v = line.strip()
    if not v:
        continue
    k = key(v)
    if k is None:
        continue
    if best_key is None or k > best_key:
        best, best_key = v, k

if best is None:
    print(f"No comparable published version; '{new}' accepted.")
    raise SystemExit(0)

if new_key == best_key:
    raise SystemExit(
        f"Version '{new}' is already published. Update the declared version."
    )
if new_key < best_key:
    raise SystemExit(
        f"Version '{new}' does not move forward from the highest published version "
        f"'{best}'. Publishing it would leave consumers pinned at '{best}' unable to "
        f"resolve it."
    )
print(f"'{new}' moves forward from '{best}'.")
PY
