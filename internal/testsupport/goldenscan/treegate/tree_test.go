// Package treegate holds the check that reads every golden of the repository for a credential (CHAOS-7890). It scans millions of
// leaves, so it lives in its own package: the race shard that runs it is weighed on its own (ci/go_race_weights.tsv), as the
// token-shape walk of goldenhygiene is.
package treegate

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/goldenscan"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedfiles"
)

// Every golden of the repository passes the unpacked secret scan through an exact allowlist row (runs on every PR).
func TestTheTreeHoldsNoCredentialShapedValueWithoutAnAllowlistRow(t *testing.T) {
	repo, err := recordedfiles.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := goldenscan.Allowlist()
	if err != nil {
		t.Fatal(err)
	}
	problems, err := goldenscan.TreeProblems(repo, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) > 0 {
		t.Fatalf("the unpacked secret scan refuses %d thing(s):\n%s", len(problems), strings.Join(problems, "\n"))
	}
}
