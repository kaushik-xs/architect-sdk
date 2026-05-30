#!/usr/bin/env bash
set -euo pipefail

BUMP="${1:-}"
if [[ "$BUMP" != "major" && "$BUMP" != "minor" && "$BUMP" != "patch" ]]; then
  echo "Usage: $0 <major|minor|patch>"
  exit 1
fi

CURRENT=$(grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "$BUMP" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEW_VERSION}"

echo "Bumping ${CURRENT} → ${NEW_VERSION} (${BUMP})"

# Run coverage and update README before touching anything else.
# Fail the release if tests don't pass.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo ""
echo "── Running coverage ─────────────────────────────────────────────────────"
bash "${SCRIPT_DIR}/update_coverage.sh"
echo "── Coverage done ────────────────────────────────────────────────────────"
echo ""

# Update version in all Cargo.toml files in the workspace
sed -i '' "s/^version = \"${CURRENT}\"/version = \"${NEW_VERSION}\"/" Cargo.toml
if [[ -f example_consumer/Cargo.toml ]]; then
  sed -i '' "s/^version = \"${CURRENT}\"/version = \"${NEW_VERSION}\"/" example_consumer/Cargo.toml
fi

cargo build --quiet 2>&1 | tail -5

git add Cargo.toml README.md
[[ -f example_consumer/Cargo.toml ]] && git add example_consumer/Cargo.toml
git commit -m "chore: bump version to ${NEW_VERSION}"

git tag -a "${TAG}" -m "Release ${TAG}"

BRANCH=$(git rev-parse --abbrev-ref HEAD)
git push origin "${BRANCH}"
git push origin "${TAG}"

echo "Released ${TAG} on ${BRANCH}"
