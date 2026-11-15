package analytics

import (
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// The pooled pipeline source leaves out the runs the daily writer leaves out: its pinned predicate is the writer's rule
// (testops.StatusDeleted through ExcludedFromPipelineCounts, the writer's SQL in testops_native_clickhouse.go). A change
// on either side fails here.
func TestPooledRunPredicateIsTheWritersDeletedRule(t *testing.T) {
	want := "lowerUTF8(trimBoth(ifNull(status, ''))) != '" + testops.StatusDeleted + "'"
	if pooledCountedRunPredicate != want {
		t.Fatalf("pooled predicate %q, want the writer's %q", pooledCountedRunPredicate, want)
	}
	if !strings.Contains(pooledPipelineRunsSource, "AND "+want) {
		t.Fatalf("the pooled pipeline source does not apply %q:\n%s", want, pooledPipelineRunsSource)
	}
	writer, err := os.ReadFile("../../jobs/metrics/daily/testops_native_clickhouse.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(writer), "AND lowerUTF8(trimBoth(ifNull(status, ''))) != '` + testops.StatusDeleted + `'") {
		t.Fatal("the daily writer's pipeline source no longer builds the predicate the pooled source pins")
	}
	for status, excluded := range map[string]bool{"deleted": true, " Deleted ": true, "DELETED": true, "success": false, "": false, "cancelled": false} {
		if testops.ExcludedFromPipelineCounts(status) != excluded {
			t.Fatalf("ExcludedFromPipelineCounts(%q) = %v: the writer's rule moved, move the pinned predicate with it", status, !excluded)
		}
	}
}
