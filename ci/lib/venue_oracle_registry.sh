#!/usr/bin/env bash
# Sourced library: the venue-oracles registry check (CHAOS-6584, Trap #392).
# Expects ROOT (repo root). Sourced by ci/check_go.sh (the venue-oracles verb)
# and run alone by ci/check_venue_oracle_registry.sh, which needs no Go.

VENUE_ORACLE_LOCAL_ONLY_MARKER='//venueoracle:local-only'

# venue_oracle_package_tests reads one package's *_test.go files (its
# arguments) and prints "RUN <Test>" for each discovered test and "LOCAL
# <Test> <file>" for one carrying the local-only marker (see
# "Discovery" in ci/check_go.sh).
venue_oracle_package_tests() {
  awk -v marker="${VENUE_ORACLE_LOCAL_ONLY_MARKER}" -f "${ROOT}/ci/venue_oracle_discovery.awk" "$@"
}


# ---------------------------------------------------------------------------
# Registry (CHAOS-6584, Trap #392). ci/venue_oracle_registry.d/ names EVERY
# venue test, one row per test: "<package dir>\t<test>\t<run|local>" (package
# dir relative to the repo root; `local` = carries the local-only marker).
# The venue-oracles verb RUNS the registry, not a discovery result, so a test
# the discovery cannot reach is never silently dropped: it is either in the
# registry (and run, proof required) or the verb fails BEFORE running
# anything. check_venue_oracle_registry (also the public verb
# `venue-oracle-registry`, run by tests/tooling/test_venue_oracle_registry.py
# on every PR, no Go toolchain needed) fails when
#   - the registry is empty or malformed, a file's rows are unsorted or
#     duplicated, a row sits in the wrong package's file, or the retired
#     single file ci/venue_oracle_registry.tsv exists;
#   - a registry row names a package dir or a top-level Test function that
#     does not exist, or one declared more than once in the package (an
#     internal and an external test package can both declare a name);
#   - a test found by STRUCTURE (ci/venue_oracle_discovery.awk, in every
#     package importing the harness) or by NAME (ci/venue_oracle_names.awk:
#     any Test*VenueOracle*, in any package) is not in the registry, or the
#     registry's run/local mode contradicts the local-only marker.
# The registry may name MORE than discovery finds (an explicit entry is how a
# test that reaches the harness in a way discovery cannot follow gets run);
# it can never name fewer.
VENUE_ORACLE_REGISTRY_DIR="${ROOT}/ci/venue_oracle_registry.d"
# The retired single-file registry (CHAOS-6724): it must not exist. A row left in
# it (or a PR rebased across the migration that still edits it) fails loudly
# instead of being silently ignored.
VENUE_ORACLE_LEGACY_REGISTRY="${ROOT}/ci/venue_oracle_registry.tsv"

# venue_oracle_registry_rows prints every registry row ("pkg test mode", space
# separated) from ci/venue_oracle_registry.d/<package with / as __>.tsv, one file
# per package (CHAOS-6724: one shared file was touched by 30 of the last 120
# merged PRs, each touch a merge conflict for the next). It refuses: a missing
# directory, the retired single file, a stray entry that is not <package>.tsv (or
# README.md), an empty file, a malformed row, a row whose package does not match
# its file name, and rows out of order within a file.
venue_oracle_registry_rows() {
  local f base bad=0
  if [ -e "${VENUE_ORACLE_LEGACY_REGISTRY}" ]; then
    printf 'venue-oracles registry: %s must not exist any more (CHAOS-6724): put the row in %s/<package with / as __>.tsv\n' "${VENUE_ORACLE_LEGACY_REGISTRY}" "${VENUE_ORACLE_REGISTRY_DIR}" >&2
    return 1
  fi
  [ -d "${VENUE_ORACLE_REGISTRY_DIR}" ] || { printf 'venue-oracles registry: %s does not exist\n' "${VENUE_ORACLE_REGISTRY_DIR}" >&2; return 1; }
  for f in "${VENUE_ORACLE_REGISTRY_DIR}"/* "${VENUE_ORACLE_REGISTRY_DIR}"/.[!.]*; do
    [ -e "${f}" ] || continue
    base="$(basename "${f}")"
    case "${base}" in
      README.md) continue ;;
      *.tsv) ;;
      *) printf 'venue-oracles registry: stray entry %s in %s (only <package with / as __>.tsv and README.md are read)\n' "${base}" "${VENUE_ORACLE_REGISTRY_DIR}" >&2; bad=1; continue ;;
    esac
    LC_ALL=C awk -F'\t' -v file="${base}" '
      function pkgfile(p) { gsub("/", "__", p); return p ".tsv" }
      /^[[:space:]]*(#|$)/ { next }
      NF != 3 || $1 == "" || $2 == "" || ($3 != "run" && $3 != "local") {
        printf "venue-oracles registry: %s line %d is not <package>\\t<test>\\t<run|local>: %s\n", file, NR, $0 > "/dev/stderr"; bad = 1; next
      }
      pkgfile($1) != file {
        printf "venue-oracles registry: %s line %d: package %s belongs in %s, not this file\n", file, NR, $1, pkgfile($1) > "/dev/stderr"; bad = 1; next
      }
      n > 0 && $2 <= last {
        printf "venue-oracles registry: %s line %d: %s is not after %s (rows must be sorted and unique within a file)\n", file, NR, $2, last > "/dev/stderr"; bad = 1
      }
      { print $1, $2, $3; last = $2; n++ }
      END { if (n == 0) { printf "venue-oracles registry: %s holds no rows\n", file > "/dev/stderr"; bad = 1 } exit bad }
    ' "${f}" || bad=1
  done
  return "${bad}"
}

# venue_oracle_expected_rows prints "pkg test mode" for every venue test
# discovery finds by structure or by name, sorted and de-duplicated.
venue_oracle_expected_rows() {
  local dir rel kind name file mode
  {
    while IFS= read -r dir; do
      rel="${dir#"${ROOT}"/}"
      while read -r kind name file; do
        case "${kind}" in
          RUN) printf '%s %s run\n' "${rel}" "${name}" ;;
          LOCAL) printf '%s %s local\n' "${rel}" "${name}" ;;
        esac
      done < <(venue_oracle_package_tests "${dir}"/*_test.go)
    done < <(grep -rlF --include='*_test.go' '"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"' "${ROOT}" | xargs -n1 dirname | LC_ALL=C sort -u)
    while read -r name file mode; do
      rel="$(dirname "${file}")"
      printf '%s %s %s\n' "${rel#"${ROOT}"/}" "${name}" "${mode}"
    done < <(grep -rlE --include='*_test.go' '^func Test[A-Za-z0-9_]*VenueOracle' "${ROOT}" | LC_ALL=C sort | xargs -r awk -v marker="${VENUE_ORACLE_LOCAL_ONLY_MARKER}" -f "${ROOT}/ci/venue_oracle_names.awk")
  } | LC_ALL=C sort -u
}

# venue_oracle_proof_tests prints "<package dir> <Test>" for every Test
# function in the registry's `run` packages (and the harness importers) that can
# reach a proof writer (CHAOS-6806; see ci/venue_oracle_proof.awk).
venue_oracle_proof_tests() {
  local rows="$1" files
  files="$( {
      printf '%s\n' "${rows}" | awk '$3 == "run" { print $1 }' | LC_ALL=C sort -u | while IFS= read -r d; do
        [ -d "${ROOT}/${d}" ] && find "${ROOT}/${d}" -maxdepth 1 -name '*.go'
      done
      grep -rlF --include='*.go' '"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"' "${ROOT}" || true
    } | LC_ALL=C sort -u)"
  [ -n "${files}" ] || return 0
  printf 'FILES %s\n' "$(printf '%s\n' "${files}" | wc -l)"
  # shellcheck disable=SC2086
  printf '%s\n' "${files}" | xargs awk -f "${ROOT}/ci/venue_oracle_proof.awk" | while read -r _ dir name; do
    printf '%s %s\n' "${dir#"${ROOT}"/}" "${name}"
  done | LC_ALL=C sort -u
}

check_venue_oracle_registry() {
  local rows expected pkg test mode failed=0 declared sorted_keys key
  rows="$(venue_oracle_registry_rows)" || return 1
  [ -n "${rows}" ] || { printf 'venue-oracles registry: %s holds no rows -- an empty registry must not read as covered\n' "${VENUE_ORACLE_REGISTRY_DIR}" >&2; return 1; }

  sorted_keys="$(printf '%s\n' "${rows}" | awk '{print $1 "\t" $2}')"
  key="$(printf '%s\n' "${sorted_keys}" | LC_ALL=C sort | uniq -d)"
  if [ -n "${key}" ]; then
    printf 'venue-oracles registry: duplicate (package, test) row(s):\n%s\n' "${key}" >&2
    failed=1
  fi

  while read -r pkg test mode; do
    if [ ! -d "${ROOT}/${pkg}" ]; then
      printf 'venue-oracles registry: %s %s -- package dir %s does not exist\n' "${pkg}" "${test}" "${pkg}" >&2
      failed=1
      continue
    fi
    declared="$(cat "${ROOT}/${pkg}"/*_test.go 2>/dev/null | grep -cE "^func ${test}[(\[]" || true)"
    if [ "${declared}" -ne 1 ]; then
      printf 'venue-oracles registry: %s %s -- expected exactly one top-level declaration in the package, found %s\n' "${pkg}" "${test}" "${declared}" >&2
      failed=1
    fi
  done < <(printf '%s\n' "${rows}")

  expected="$(venue_oracle_expected_rows)"
  while read -r pkg test mode; do
    [ -n "${pkg}" ] || continue
    local reg_mode
    reg_mode="$(printf '%s\n' "${rows}" | awk -v p="${pkg}" -v t="${test}" '$1 == p && $2 == t { print $3 }')"
    if [ -z "${reg_mode}" ]; then
      printf 'venue-oracles registry: %s %s (%s) exists but is NOT in ci/venue_oracle_registry.d/ -- add the row to ci/venue_oracle_registry.d/%s.tsv, or it is never run\n' "${pkg}" "${test}" "${mode}" "${pkg//\//__}" >&2
      failed=1
    elif [ "${reg_mode}" != "${mode}" ]; then
      printf 'venue-oracles registry: %s %s is registered as %s but the source says %s (local-only marker)\n' "${pkg}" "${test}" "${reg_mode}" "${mode}" >&2
      failed=1
    fi
  done < <(printf '%s\n' "${expected}")

  # A `run` row leaves a proof file in the venue or the hosted verb fails on
  # main (CHAOS-6806): the test, or a helper it calls, must reach
  # venueoracle.Diff / WriteProof / WriteGoOnlyProof.
  local proofs nl=$'\n'
  proofs="$(venue_oracle_proof_tests "${rows}")"
  local scanned proven
  scanned="$(printf '%s\n' "${proofs}" | sed -n 's/^FILES //p')"
  proven="$(printf '%s\n' "${proofs}" | grep -vc '^FILES ' || true)"
  while read -r pkg test mode; do
    [ "${mode}" = "run" ] || continue
    [ -d "${ROOT}/${pkg}" ] || continue
    # No `printf | grep -q`: grep exits at the first match and, under pipefail, the
    # writer's SIGPIPE (rc 141) fails the pipeline -- a flaky false "no proof".
    if [[ "${nl}${proofs}${nl}" != *"${nl}${pkg} ${test}${nl}"* ]]; then
      printf 'venue-oracles registry: %s %s is a run row but neither it nor a helper it calls reaches venueoracle.Diff, WriteProof or WriteGoOnlyProof, so it leaves no proof file and the hosted venue-oracles verb fails on main -- call venueoracle.WriteProof(t) after a both-planes comparison, or venueoracle.WriteGoOnlyProof(t, "<what it measures>") for a Go-only check (proof scan read %s file(s) and found %s reaching test(s); a count far below the registry means the scan, not the test, is wrong)\n' "${pkg}" "${test}" "${scanned:-0}" "${proven}" >&2
      failed=1
    fi
  done < <(printf '%s\n' "${rows}")

  [ "${failed}" -eq 0 ] || return 1
  printf 'venue-oracle-registry: %d registered test(s), all found by discovery and by name\n' "$(printf '%s\n' "${rows}" | wc -l)"
}

