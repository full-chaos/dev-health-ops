package liverow

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// CHAOS-14616 changes the pooled live gate only. The shared rule for every other reader and for the writers, the argMax pick
// of the newest row (Table.LiveHaving, NewestPredicate) and the one-row test (Table.LiveRow, Predicate), keeps its exact text
// on the tables the pooled sources read.
func TestTheSharedRuleKeepsItsTextOnThePooledTables(t *testing.T) {
	t.Parallel()
	registry := []string{"repo_metrics_daily", "deploy_metrics_daily", "incident_metrics_daily", "testops_pipeline_metrics_daily", "testops_test_metrics_daily"}
	for _, table := range append(append([]string(nil), registry...), "cicd_metrics_daily") {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			for qualifier, prefix := range map[string]string{"": table + ".", "q": "q."} {
				want := "(argMax(" + prefix + "retracted, " + prefix + "computed_at) = 0)"
				if got := NewestPredicate(table, qualifier); got != want {
					t.Errorf("NewestPredicate(%q, %q) = %q, want %q", table, qualifier, got, want)
				}
				if got, want := Predicate(table, qualifier), "("+prefix+"retracted = 0)"; got != want {
					t.Errorf("Predicate(%q, %q) = %q, want %q", table, qualifier, got, want)
				}
			}
		})
	}
	for _, table := range registry {
		declared, ok := teamkeytables.ByTable(table)
		if !ok {
			t.Fatalf("%s is not in the registry", table)
		}
		if got, want := declared.LiveHaving(""), "argMax(retracted, computed_at) = 0"; got != want {
			t.Errorf("%s LiveHaving = %q, want %q", table, got, want)
		}
		if got, want := declared.LiveRow("x."), "(x.retracted = 0)"; got != want {
			t.Errorf("%s LiveRow = %q, want %q", table, got, want)
		}
	}
}
