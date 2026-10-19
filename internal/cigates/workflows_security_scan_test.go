package cigates_test

import (
	"fmt"
	"reflect"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_security_scan_fires_on_every_base.py (CHAOS-6973). A pull_request
// trigger filtered by base branch never fires on a stacked PR (base != main), so Gitleaks/pip-audit/Semgrep were
// silently skipped. The push trigger stays scoped to main so a branch push does not double-run the workflow.

const securityScanFile = ".github/workflows/security-scan.yml"

func securityScanViolations(t *testing.T, root string) []string {
	t.Helper()
	doc := loadWf(t, root, "security-scan.yml")
	triggers := asMap(doc["on"])
	if triggers == nil {
		return []string{"security-scan.yml declares no top-level trigger"}
	}
	var out []string
	pr, ok := triggers["pull_request"]
	if !ok {
		out = append(out, "security-scan.yml no longer runs on pull_request")
	} else if got := asList(asMap(pr)["branches"]); !reflect.DeepEqual(got, []any{"**"}) {
		out = append(out, fmt.Sprintf("security-scan.yml's pull_request.branches is %v, want [**]: a base-branch filter means a stacked PR (base != main) never triggers the scans (CHAOS-6973)", got))
	}
	if got := asList(asMap(triggers["push"])["branches"]); !reflect.DeepEqual(got, []any{"main"}) {
		out = append(out, fmt.Sprintf("security-scan.yml push.branches is %v, want [main]: widen pull_request only, never push", got))
	}
	return out
}

func TestSecurityScanFiresOnEveryBase(t *testing.T) {
	expectNoViolations(t, securityScanViolations(t, repoRoot(t)))
}

func TestSecurityScanPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"pull_request filtered to main", []mutation{{securityScanFile, "branches: [\"**\"]", "branches: [main]"}}, []string{"pull_request.branches", "stacked PR"}},
		{"push widened to every branch", []mutation{{securityScanFile, "push:\n    branches: [main]", "push:\n    branches: [\"**\"]"}}, []string{"push.branches"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, []string{securityScanFile}, c.muts...)
			expectViolation(t, securityScanViolations(t, tree), c.want...)
		})
	}
}
