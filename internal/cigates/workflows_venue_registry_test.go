package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10177 (B2): Go port of test_the_verb_source_consumes_the_registry_not_discovery from
// tests/tooling/test_venue_oracle_registry.py: the body of check_venue_oracles in ci/check_go.sh runs the registry;
// discovery belongs to the cross-check in check_venue_oracle_registry. The ~45 tests that EXECUTE the registry check
// script and the verb stay in Python until the venue-script port (CHAOS-10178).

const checkGoFile = "ci/check_go.sh"

func venueVerbSourceViolations(t *testing.T, root string) []string {
	t.Helper()
	script := readFile(t, filepath.Join(root, checkGoFile))
	start := strings.Index(script, "check_venue_oracles() {")
	if start < 0 {
		return []string{"check_venue_oracles() { not found in ci/check_go.sh: renamed or removed; a guard that finds nothing must fail"}
	}
	end := strings.Index(script[start:], "\n}\n")
	if end < 0 {
		return []string{"the body of check_venue_oracles in ci/check_go.sh has no closing brace line"}
	}
	code := shellCode(script[start : start+end])
	var out []string
	call, loop := -1, -1
	for i, l := range code {
		switch {
		case call < 0 && l.indent == 2 && registryCall.MatchString(l.text):
			call = i
		case loop < 0 && l.indent == 2 && registryLoop.MatchString(l.text):
			loop = i
		}
		if strings.Contains(l.text, "venue_oracle_package_tests") {
			out = append(out, "check_venue_oracles reads venue_oracle_package_tests: it must run the registry, discovery belongs to the cross-check in check_venue_oracle_registry")
		}
	}
	if call < 0 {
		out = append(out, "check_venue_oracles never calls check_venue_oracle_registry as a top-level `check_venue_oracle_registry >&2 || die` statement: the registry is not validated against the tree before it runs (a comment, an echo or a call nested in a branch is not that call)")
	}
	if loop < 0 {
		out = append(out, "check_venue_oracles has no top-level `done < <(venue_oracle_registry_rows ...)` loop: it must run the registry rows")
	}
	for i := 0; call >= 0 && i < call; i++ {
		if code[i].indent == 2 && earlyExit.MatchString(code[i].text) {
			out = append(out, fmt.Sprintf("check_venue_oracles leaves unconditionally before the registry cross-check: %q", code[i].text))
		}
	}
	return out
}

var (
	registryCall = regexp.MustCompile(`^check_venue_oracle_registry >&2 \|\| die\b`)
	registryLoop = regexp.MustCompile(`^done < <\(venue_oracle_registry_rows\b`)
	earlyExit    = regexp.MustCompile(`^(return|exit|false|:)(\s|$)`)
)

func TestVenueVerbConsumesTheRegistryNotDiscovery(t *testing.T) {
	expectNoViolations(t, venueVerbSourceViolations(t, repoRoot(t)))
}

func TestVenueVerbSourcePlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const f = checkGoFile
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"the verb reads discovery beside the registry", []mutation{{f, `list_rows="$(venue_oracle_registry_rows`, `list_rows="$(venue_oracle_package_tests; venue_oracle_registry_rows`}}, []string{"venue_oracle_package_tests"}},
		{"the verb stops calling the cross-check", []mutation{{f, `check_venue_oracle_registry >&2 || die "venue-oracles:`, `true >&2 || die "venue-oracles:`}}, []string{"never calls check_venue_oracle_registry"}},
		{"the verb stops reading registry rows", []mutation{{f, "venue_oracle_registry_rows", allMarker + "venue_oracle_registry_rowz"}}, []string{"no top-level `done < <(venue_oracle_registry_rows"}},
		{"the cross-check is echoed, not called", []mutation{{f, `check_venue_oracle_registry >&2 || die "venue-oracles:`, `echo check_venue_oracle_registry >&2 || die "venue-oracles:`}}, []string{"never calls check_venue_oracle_registry"}},
		{"the cross-check sits in a dead branch", []mutation{{f, `check_venue_oracle_registry >&2 || die "venue-oracles: ci/venue_oracle_registry.d/ disagrees with the tree (see above)"`, "if false; then\n    check_venue_oracle_registry >&2 || die \"venue-oracles: ci/venue_oracle_registry.d/ disagrees with the tree (see above)\"\n  fi"}}, []string{"never calls check_venue_oracle_registry"}},
		{"the verb returns before the cross-check", []mutation{{f, "  check_venue_oracle_registry >&2 || die", "  return 0\n  check_venue_oracle_registry >&2 || die"}}, []string{"leaves unconditionally before the registry cross-check"}},
		{"the registry loop is fed something else", []mutation{{f, "  done < <(venue_oracle_registry_rows", "  done < <(true; venue_oracle_registry_rows"}}, []string{"no top-level `done < <(venue_oracle_registry_rows"}},
		{"the verb is renamed", []mutation{{f, "check_venue_oracles() {", "check_venue_oraclez() {"}}, []string{"not found"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{f}, c.muts...)
			expectViolation(t, venueVerbSourceViolations(t, tree), c.want...)
		})
	}

	t.Run("a function that only comments the names does not pass", func(t *testing.T) {
		tree := t.TempDir()
		script := "check_venue_oracles() {\n  # check_venue_oracle_registry\n  # venue_oracle_registry_rows\n  return 0\n}\n"
		if err := os.MkdirAll(filepath.Join(tree, "ci"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tree, f), []byte(script), 0o644); err != nil {
			t.Fatal(err)
		}
		expectViolation(t, venueVerbSourceViolations(t, tree), "never calls check_venue_oracle_registry", "no top-level `done < <(venue_oracle_registry_rows")
	})
	t.Run("a trailing comment does not count as the call", func(t *testing.T) {
		tree := plantTree(t, root, []string{f}, mutation{f, `check_venue_oracle_registry >&2 || die "venue-oracles:`, `true # check_venue_oracle_registry >&2 || die "venue-oracles:`})
		expectViolation(t, venueVerbSourceViolations(t, tree), "never calls check_venue_oracle_registry")
	})
	t.Run("a comment that names discovery is not a read of it", func(t *testing.T) {
		tree := plantTree(t, root, []string{f}, mutation{f, "check_venue_oracles() {\n", "check_venue_oracles() {\n  # discovery (venue_oracle_package_tests) belongs to the cross-check\n"})
		expectNoViolations(t, venueVerbSourceViolations(t, tree))
	})
}
