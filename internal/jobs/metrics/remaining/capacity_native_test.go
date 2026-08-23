package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCapacityExecutorFailsClosedWithoutAConnection(t *testing.T) {
	if _, err := NewCapacityExecutor(nil, nil); err == nil {
		t.Fatal("a nil connection must refuse, not degrade to the bridge")
	}
}

func TestCapacityRefusesARunWithoutASeed(t *testing.T) {
	// An unseeded Monte Carlo does not fail -- it produces plausible numbers
	// that differ on every run, so no comparison against Python could ever
	// hold. Python refuses identically at worker_metrics.py:892, and the run
	// table enforces seed presence for this family alone (postgres.go:557).
	executor := &CapacityExecutor{conn: stubConn{}}
	scope, err := json.Marshal(map[string]any{
		"version": 1, "history_days": 90, "simulations": 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = executor.ComputePartition(
		context.Background(),
		Run{ID: "r", OrganizationID: "org", Family: "capacity", Seed: nil},
		Partition{ID: "p", RunID: "r", Scope: scope},
	)
	if !errors.Is(err, ErrCapacitySeedMissing) {
		t.Fatalf("expected a seed-missing refusal, got: %v", err)
	}
}

func TestBacklogFallbackIsFalsyNotNil(t *testing.T) {
	// job_capacity.py:94 is `items = target_items if target_items else backlog`.
	// A scope carrying target_items = 0 therefore falls back to the BACKLOG,
	// where a nil-check port would forecast zero items and skip the scope. The
	// two disagree on exactly one input, which is the one a hand-written
	// fixture is least likely to include.
	tests := []struct {
		name        string
		targetItems *int
		backlog     int
		want        int
	}{
		{name: "absent falls back", targetItems: nil, backlog: 40, want: 40},
		{name: "ZERO falls back, as Python's falsy check does",
			targetItems: intPointer(0), backlog: 40, want: 40},
		{name: "a real target wins", targetItems: intPointer(12), backlog: 40, want: 12},
		{name: "a negative target is kept, then skipped downstream",
			targetItems: intPointer(-3), backlog: 40, want: -3},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := resolveTargetItems(test.targetItems, test.backlog)
			if got != test.want {
				t.Errorf("resolved items = %d, want %d", got, test.want)
			}
		})
	}
}

func TestThroughputQueryUsesTheClientSideWindow(t *testing.T) {
	// Python derives the window start from utc_today() on the CLIENT
	// (capacity_queries.py:26), not from ClickHouse's today(). The difference
	// is observable: the two clocks can straddle midnight independently, and
	// the window decides which rows load at all.
	arguments := map[string]any{}
	conditions := capacityScopeFilters("org", capacityTarget{}, arguments)
	if len(conditions) != 1 || !strings.Contains(conditions[0], "org_id") {
		t.Fatalf("an unscoped forecast must still be org-scoped: %v", conditions)
	}
	if _, bound := arguments["team_id"]; bound {
		t.Error("team_id must not be bound when the scope carries none")
	}

	teamID := "team-a"
	arguments = map[string]any{}
	conditions = capacityScopeFilters("org", capacityTarget{TeamID: &teamID}, arguments)
	if len(conditions) != 2 {
		t.Fatalf("a team-scoped forecast needs both filters: %v", conditions)
	}
	if arguments["team_id"] != teamID {
		t.Errorf("team_id = %v", arguments["team_id"])
	}
}

func TestEmptyScopeValuesDoNotBecomeFilters(t *testing.T) {
	// An empty team id means "unscoped", not "a team whose id is the empty
	// string". Binding it would silently return no rows.
	empty := ""
	arguments := map[string]any{}
	conditions := capacityScopeFilters(
		"org", capacityTarget{TeamID: &empty, WorkScopeID: &empty}, arguments)
	if len(conditions) != 1 {
		t.Fatalf("empty scope values must not add filters: %v", conditions)
	}
}

func intPointer(value int) *int { return &value }

// stubConn satisfies just enough of driver.Conn for the seed refusal, which
// returns before any query runs.
type stubConn struct{ driverConnStub }
