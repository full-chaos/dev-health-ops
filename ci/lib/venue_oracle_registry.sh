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
# Registry (CHAOS-6584, Trap #392). ci/venue_oracle_registry.tsv names EVERY
# venue test, one row per test: "<package dir>\t<test>\t<run|local>" (package
# dir relative to the repo root; `local` = carries the local-only marker).
# The venue-oracles verb RUNS the registry, not a discovery result, so a test
# the discovery cannot reach is never silently dropped: it is either in the
# registry (and run, proof required) or the verb fails BEFORE running
# anything. check_venue_oracle_registry (also the public verb
# `venue-oracle-registry`, run by tests/tooling/test_venue_oracle_registry.py
# on every PR, no Go toolchain needed) fails when
#   - the registry is empty, malformed, unsorted (LC_ALL=C), or has a
#     duplicate (package, test) row;
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
VENUE_ORACLE_REGISTRY="${ROOT}/ci/venue_oracle_registry.tsv"

# venue_oracle_registry_rows prints the registry's data rows ("pkg test mode",
# space separated) in file order, refusing a malformed line.
venue_oracle_registry_rows() {
  [ -f "${VENUE_ORACLE_REGISTRY}" ] || { printf 'venue-oracles registry: %s does not exist\n' "${VENUE_ORACLE_REGISTRY}" >&2; return 1; }
  awk -F'\t' '
    /^[[:space:]]*(#|$)/ { next }
    NF != 3 || $1 == "" || $2 == "" || ($3 != "run" && $3 != "local") {
      printf "venue-oracles registry: line %d is not <package>\\t<test>\\t<run|local>: %s\n", NR, $0 > "/dev/stderr"; bad = 1; next
    }
    { print $1, $2, $3 }
    END { exit bad }
  ' "${VENUE_ORACLE_REGISTRY}"
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

check_venue_oracle_registry() {
  local rows expected pkg test mode failed=0 declared sorted_keys key
  rows="$(venue_oracle_registry_rows)" || return 1
  [ -n "${rows}" ] || { printf 'venue-oracles registry: %s holds no rows -- an empty registry must not read as covered\n' "${VENUE_ORACLE_REGISTRY}" >&2; return 1; }

  sorted_keys="$(printf '%s\n' "${rows}" | awk '{print $1 "\t" $2}')"
  if [ "${sorted_keys}" != "$(printf '%s\n' "${sorted_keys}" | LC_ALL=C sort)" ]; then
    printf 'venue-oracles registry: rows are not sorted by (package, test) under LC_ALL=C -- keep the file sorted so a diff shows exactly one added or removed row\n' >&2
    failed=1
  fi
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
      printf 'venue-oracles registry: %s %s (%s) exists but is NOT in ci/venue_oracle_registry.tsv -- add it, or it is never run\n' "${pkg}" "${test}" "${mode}" >&2
      failed=1
    elif [ "${reg_mode}" != "${mode}" ]; then
      printf 'venue-oracles registry: %s %s is registered as %s but the source says %s (local-only marker)\n' "${pkg}" "${test}" "${reg_mode}" "${mode}" >&2
      failed=1
    fi
  done < <(printf '%s\n' "${expected}")

  [ "${failed}" -eq 0 ] || return 1
  printf 'venue-oracle-registry: %d registered test(s), all found by discovery and by name\n' "$(printf '%s\n' "${rows}" | wc -l)"
}

