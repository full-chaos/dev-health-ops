#!/usr/bin/env bash
# Entry point for the dedicated shell test suites (CHAOS-11422). The list is ci/suites.tsv (kind=shell).
#   bash ci/run_shell_tests.sh --list            every shell suite with its status
#   bash ci/run_shell_tests.sh <suite>           run one suite
#   bash ci/run_shell_tests.sh                   run every suite that needs no argument and no network
# A mode=manual suite is listed with its reason and is never run by the no-argument form.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CATALOG="${ROOT}/ci/suites.tsv"

rows() {
  # columns out: suite, script (first input), trigger, mode, why_manual
  awk -F'\t' '/^#/ || $1 == "suite" || NF == 0 { next } $11 == "shell" { split($5, in_, ";"); print $1 "\t" in_[1] "\t" $6 "\t" $12 "\t" $13 }' "${CATALOG}"
}

case "${1:-}" in
  --list)
    n=0
    while IFS=$'\t' read -r suite command trigger mode why; do
      n=$((n + 1))
      if [ "${mode}" = ci ]; then
        printf '%s\tautomated\t%s\t%s\n' "${suite}" "${command}" "${trigger}"
      else
        printf '%s\tmanual\t%s\t%s\n' "${suite}" "${command}" "${why}"
      fi
    done < <(rows)
    printf 'shell suites: %d\n' "${n}"
    ;;
  "")
    failed=0
    while IFS=$'\t' read -r suite command _ mode _; do
      [ "${mode}" = ci ] || continue
      printf '=== %s\n' "${suite}"
      bash "${ROOT}/${command}" || { printf 'FAIL: %s\n' "${suite}" >&2; failed=$((failed + 1)); }
    done < <(rows)
    [ "${failed}" -eq 0 ] || exit 1
    ;;
  *)
    while IFS=$'\t' read -r suite command _ _ _; do
      if [ "${suite}" = "$1" ]; then
                exec bash "${ROOT}/${command}"
      fi
    done < <(rows)
    printf 'unknown shell suite: %s (see --list)\n' "$1" >&2
    exit 2
    ;;
esac
