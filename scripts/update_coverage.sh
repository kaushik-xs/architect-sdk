#!/usr/bin/env bash
# update_coverage.sh — run cargo-llvm-cov, parse results, and patch README.md.
#
# Usage:
#   ./scripts/update_coverage.sh           # auto-detect LLVM tools
#   ./scripts/update_coverage.sh --dry-run # print what would change, don't write
#
# Requirements:
#   cargo-llvm-cov  (cargo install cargo-llvm-cov)
#   llvm-cov + llvm-profdata  (Homebrew: brew install llvm)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
README="$REPO_ROOT/README.md"
DRY_RUN=0

for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=1
done

# ── locate LLVM tools ─────────────────────────────────────────────────────────

find_llvm_tool() {
  local name="$1"
  # 1. Already on PATH (rustup toolchain or custom install)
  if command -v "$name" &>/dev/null; then printf '%s' "$(command -v "$name")"; return; fi
  # 2. Homebrew Cellar (versioned)
  local found
  found=$(find /opt/homebrew/Cellar/llvm -name "$name" -type f 2>/dev/null | sort -V | tail -1)
  if [[ -n "$found" ]]; then printf '%s' "$found"; return; fi
  # 3. Homebrew opt symlink
  if [[ -f "/opt/homebrew/opt/llvm/bin/$name" ]]; then
    printf '%s' "/opt/homebrew/opt/llvm/bin/$name"; return
  fi
  echo "ERROR: $name not found. Install with: brew install llvm" >&2
  exit 1
}

export LLVM_COV="$(find_llvm_tool llvm-cov)"
export LLVM_PROFDATA="$(find_llvm_tool llvm-profdata)"
echo "llvm-cov:      $LLVM_COV"
echo "llvm-profdata: $LLVM_PROFDATA"

# ── run tests + collect coverage ─────────────────────────────────────────────

echo ""
echo "Running cargo llvm-cov ..."
cd "$REPO_ROOT"

COV_OUTPUT=$(cargo llvm-cov --summary-only 2>&1)

# Print the full run so the caller can see test results
echo "$COV_OUTPUT"

# ── parse TOTAL line ──────────────────────────────────────────────────────────

TOTAL_LINE=$(echo "$COV_OUTPUT" | grep '^TOTAL')

# Columns (1-based after splitting on whitespace):
#   TOTAL  regions  missed_regions  regions_pct  functions  missed_functions  functions_pct  lines  missed_lines  lines_pct  ...
extract_pct() {
  echo "$TOTAL_LINE" | awk "{print \$$1}"
}

REGIONS_PCT=$(extract_pct 4)
FUNCTIONS_PCT=$(extract_pct 7)
LINES_PCT=$(extract_pct 10)

echo ""
echo "Coverage totals:"
echo "  Lines:     $LINES_PCT"
echo "  Functions: $FUNCTIONS_PCT"
echo "  Regions:   $REGIONS_PCT"

# ── count tests ──────────────────────────────────────────────────────────────

count_tests() {
  echo "$COV_OUTPUT" | grep -E '^test result: ok\.' | awk '{sum += $4} END {print sum+0}'
}

count_suite() {
  local label="$1"   # "lib" or integration test name pattern
  echo "$COV_OUTPUT" | grep -E "^test result: ok\." | awk "NR==$2 {print \$4+0}"
}

TOTAL_TESTS=$(count_tests)
UNIT_TESTS=$(echo "$COV_OUTPUT" | grep -E "^test result: ok\." | awk 'NR==1 {print $4+0}')
INTEG_TESTS=$(echo "$COV_OUTPUT" | grep -E "^test result: ok\." | awk 'NR==2 {print $4+0}')

echo "  Total tests: $TOTAL_TESTS ($UNIT_TESTS unit + $INTEG_TESTS integration)"

# ── parse per-file coverage ───────────────────────────────────────────────────

# Extract the three coverage percentages for a given filename fragment.
# $1 = filename fragment to match (e.g. "case.rs")
file_pcts() {
  local file="$1"
  echo "$COV_OUTPUT" | grep "$file" | grep -v "^-" | awk '{print $4, $7, $10}'
}

read -r CASE_REGIONS CASE_FUNCS CASE_LINES <<< "$(file_pcts 'case.rs' | awk '{print $1, $2, $3}')"
read -r VALID_REGIONS VALID_FUNCS VALID_LINES <<< "$(file_pcts 'service/validation.rs' | awk '{print $1, $2, $3}')"
read -r CFGVAL_REGIONS CFGVAL_FUNCS CFGVAL_LINES <<< "$(file_pcts 'config/validator.rs' | awk '{print $1, $2, $3}')"
read -r RSQL_REGIONS RSQL_FUNCS RSQL_LINES <<< "$(file_pcts 'sql/rsql.rs' | awk '{print $1, $2, $3}')"
read -r CRUD_REGIONS CRUD_FUNCS CRUD_LINES <<< "$(file_pcts 'service/crud.rs' | awk '{print $1, $2, $3}')"
read -r BUILDER_REGIONS BUILDER_FUNCS BUILDER_LINES <<< "$(file_pcts 'sql/builder.rs' | awk '{print $1, $2, $3}')"
read -r STORE_REGIONS STORE_FUNCS STORE_LINES <<< "$(file_pcts 'store.rs' | awk '{print $1, $2, $3}')"
read -r MIGRATION_REGIONS MIGRATION_FUNCS MIGRATION_LINES <<< "$(file_pcts 'migration.rs' | awk '{print $1, $2, $3}')"
read -r LOADER_REGIONS LOADER_FUNCS LOADER_LINES <<< "$(file_pcts 'config/loader.rs' | awk '{print $1, $2, $3}')"
read -r SQLITE_REGIONS SQLITE_FUNCS SQLITE_LINES <<< "$(file_pcts 'db/sqlite.rs' | awk '{print $1, $2, $3}')"

# ── build replacement blocks ──────────────────────────────────────────────────

NEW_SUMMARY="Measured with [\`cargo-llvm-cov\`](https://github.com/taiki-e/cargo-llvm-cov) (LLVM instrumentation), across **${TOTAL_TESTS} tests** (${UNIT_TESTS} unit + ${INTEG_TESTS} SQLite integration):"

NEW_CODE_BLOCK="\`\`\`
TOTAL   lines: ${LINES_PCT}   functions: ${FUNCTIONS_PCT}   regions: ${REGIONS_PCT}
\`\`\`"

NEW_UNIT_TABLE="| File | Lines | Functions | Regions | Tests |
|---|---|---|---|---|
| \`src/case.rs\` | **${CASE_LINES}** | **${CASE_FUNCS}** | **${CASE_REGIONS}** | 18 |
| \`src/service/validation.rs\` | **${VALID_LINES}** | **${VALID_FUNCS}** | **${VALID_REGIONS}** | 24 |
| \`src/config/validator.rs\` | **${CFGVAL_LINES}** | **${CFGVAL_FUNCS}** | **${CFGVAL_REGIONS}** | 9 |
| \`src/sql/rsql.rs\` | **${RSQL_LINES}** | **${RSQL_FUNCS}** | **${RSQL_REGIONS}** | 14 |"

NEW_INTEG_TABLE="| File | Lines | Functions | Regions |
|---|---|---|---|
| \`src/service/crud.rs\` | **${CRUD_LINES}** | **${CRUD_FUNCS}** | **${CRUD_REGIONS}** |
| \`src/sql/builder.rs\` | **${BUILDER_LINES}** | **${BUILDER_FUNCS}** | **${BUILDER_REGIONS}** |
| \`src/store.rs\` | **${STORE_LINES}** | **${STORE_FUNCS}** | **${STORE_REGIONS}** |
| \`src/migration.rs\` | **${MIGRATION_LINES}** | **${MIGRATION_FUNCS}** | **${MIGRATION_REGIONS}** |
| \`src/config/loader.rs\` | **${LOADER_LINES}** | **${LOADER_FUNCS}** | **${LOADER_REGIONS}** |
| \`src/db/sqlite.rs\` | **${SQLITE_LINES}** | **${SQLITE_FUNCS}** | **${SQLITE_REGIONS}** |"

# ── patch README with Python (portable sed-alternative for multi-line blocks) ─

PATCH_SCRIPT=$(cat <<'PYEOF'
import re, sys

readme   = sys.argv[1]
summary  = sys.argv[2]
code     = sys.argv[3]
unit_tbl = sys.argv[4]
int_tbl  = sys.argv[5]

with open(readme) as f:
    text = f.read()

# 1. Summary line (the "Measured with..." sentence)
text = re.sub(
    r'Measured with \[`cargo-llvm-cov`\].*?integration\):',
    summary,
    text,
    flags=re.DOTALL
)

# 2. Code block with TOTAL numbers
text = re.sub(
    r'```\nTOTAL\s+lines:.*?```',
    code,
    text,
    flags=re.DOTALL
)

# 3. Unit test table (between "### Unit tests" header and the next "###" or blank line+###)
text = re.sub(
    r'(### Unit tests.*?\n\n)\| File \| Lines.*?(?=\n\n)',
    lambda m: m.group(1) + unit_tbl,
    text,
    flags=re.DOTALL
)

# 4. Integration test table (between "### SQLite integration" header and the blank line before "To regenerate")
text = re.sub(
    r'(### SQLite integration tests.*?\n\n)\| File \| Lines.*?(?=\n\nTo regenerate)',
    lambda m: m.group(1) + int_tbl,
    text,
    flags=re.DOTALL
)

with open(readme, 'w') as f:
    f.write(text)

print("README.md patched successfully.")
PYEOF
)

if [[ $DRY_RUN -eq 1 ]]; then
  echo ""
  echo "--- DRY RUN: would write the following to README.md ---"
  echo ""
  echo "Summary line:    $NEW_SUMMARY"
  echo ""
  echo "Code block:"
  echo "$NEW_CODE_BLOCK"
  echo ""
  echo "Unit table:"
  echo "$NEW_UNIT_TABLE"
  echo ""
  echo "Integration table:"
  echo "$NEW_INTEG_TABLE"
else
  python3 - \
    "$README" \
    "$NEW_SUMMARY" \
    "$NEW_CODE_BLOCK" \
    "$NEW_UNIT_TABLE" \
    "$NEW_INTEG_TABLE" \
    <<< "$PATCH_SCRIPT"
  echo ""
  echo "README.md updated."
fi
