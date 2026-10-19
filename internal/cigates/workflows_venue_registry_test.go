package cigates_test

import (
	"fmt"
	"path/filepath"
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
	body := script[start : start+end]
	var out []string
	if !strings.Contains(body, "check_venue_oracle_registry") {
		out = append(out, "check_venue_oracles no longer calls check_venue_oracle_registry: the registry is not validated against the tree before it runs")
	}
	if !strings.Contains(body, "venue_oracle_registry_rows") {
		out = append(out, "check_venue_oracles no longer reads venue_oracle_registry_rows: it must run the registry")
	}
	if strings.Contains(body, "venue_oracle_package_tests") {
		out = append(out, fmt.Sprintf("check_venue_oracles reads venue_oracle_package_tests: it must run the registry, discovery belongs to the cross-check in check_venue_oracle_registry"))
	}
	return out
}

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
		{"the verb stops calling the cross-check", []mutation{{f, `check_venue_oracle_registry >&2 || die "venue-oracles:`, `true >&2 || die "venue-oracles:`}}, []string{"no longer calls check_venue_oracle_registry"}},
		{"the verb stops reading registry rows", []mutation{{f, "venue_oracle_registry_rows", allMarker + "venue_oracle_registry_rowz"}}, []string{"no longer reads venue_oracle_registry_rows"}},
		{"the verb is renamed", []mutation{{f, "check_venue_oracles() {", "check_venue_oraclez() {"}}, []string{"not found"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{f}, c.muts...)
			expectViolation(t, venueVerbSourceViolations(t, tree), c.want...)
		})
	}
}
