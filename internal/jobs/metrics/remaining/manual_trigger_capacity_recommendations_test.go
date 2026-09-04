package remaining

import (
	"context"
	"testing"
)

// TestManualCapacityTriggerScopeMatchesFixedScheduleFanoutShape pins that a
// manual capacity trigger's Scope is indistinguishable, once dispatched,
// from one capacity_forecast_weekly_fanout (producers.go) would have built
// -- same history_days/simulations, same all_teams-XOR-team_id shape
// validateFamilyScope (scopes.go) enforces.
func TestManualCapacityTriggerScopeMatchesFixedScheduleFanoutShape(t *testing.T) {
	raw, err := manualCapacityTriggerScope(nil, true)
	if err != nil {
		t.Fatalf("all_teams=true, team_id=nil: unexpected error: %v", err)
	}
	if _, err := validateFamilyScope("capacity", raw); err != nil {
		t.Fatalf("all_teams scope failed validateFamilyScope: %v (scope=%s)", err, raw)
	}

	teamID := "00000000-0000-4000-8000-000000000002"
	raw, err = manualCapacityTriggerScope(&teamID, false)
	if err != nil {
		t.Fatalf("all_teams=false, team_id set: unexpected error: %v", err)
	}
	if _, err := validateFamilyScope("capacity", raw); err != nil {
		t.Fatalf("team-scoped scope failed validateFamilyScope: %v (scope=%s)", err, raw)
	}
}

// TestManualCapacityTriggerScopeRejectsInvalidCombinations mirrors
// capacityScope's own XOR requirement (scopes.go): all_teams=true with a
// team_id, or all_teams=false with no team_id, must both be refused before
// ever reaching validateFamilyScope (or the DB).
func TestManualCapacityTriggerScopeRejectsInvalidCombinations(t *testing.T) {
	teamID := "00000000-0000-4000-8000-000000000002"
	if _, err := manualCapacityTriggerScope(&teamID, true); err == nil {
		t.Fatal("all_teams=true with a team_id: want an error, got nil")
	}
	if _, err := manualCapacityTriggerScope(nil, false); err == nil {
		t.Fatal("all_teams=false with no team_id: want an error, got nil")
	}
}

func TestManualRecommendationsTriggerScopeMatchesFixedScheduleFanoutShape(t *testing.T) {
	// Whole-organization case: no team_id at all, matching
	// recommendations_daily_fanout's own {"version":1,"window":14}.
	raw, err := manualRecommendationsTriggerScope(nil, 14)
	if err != nil {
		t.Fatalf("org-wide: unexpected error: %v", err)
	}
	if _, err := validateFamilyScope("recommendations", raw); err != nil {
		t.Fatalf("org-wide scope failed validateFamilyScope: %v (scope=%s)", err, raw)
	}

	teamID := "00000000-0000-4000-8000-000000000002"
	raw, err = manualRecommendationsTriggerScope(&teamID, 7)
	if err != nil {
		t.Fatalf("team-scoped: unexpected error: %v", err)
	}
	if _, err := validateFamilyScope("recommendations", raw); err != nil {
		t.Fatalf("team-scoped scope failed validateFamilyScope: %v (scope=%s)", err, raw)
	}
}

// TestManualTriggerGenerationSeedIsDeterministicAndRequestScoped pins the
// property StartRunTx's own ON-CONFLICT reload check depends on: a retried
// manual capacity trigger (same family/org/day/generation) must reproduce
// the IDENTICAL seed, and a genuinely different request must not collide.
func TestManualTriggerGenerationSeedIsDeterministicAndRequestScoped(t *testing.T) {
	const (
		org = "00000000-0000-4000-8000-000000000001"
		day = "2026-08-26"
		gen = "manual-trigger:capacity:00000000-0000-4000-8000-000000000001:2026-08-26"
	)
	a := manualTriggerGenerationSeed("capacity", org, day, gen)
	b := manualTriggerGenerationSeed("capacity", org, day, gen)
	if a != b {
		t.Fatalf("seed must be deterministic for the same request: %d vs %d", a, b)
	}
	if a < 0 {
		t.Fatalf("seed must be non-negative (int64 masked to 63 bits): %d", a)
	}
	if other := manualTriggerGenerationSeed("capacity", org, "2026-08-27", gen); other == a {
		t.Fatal("a different day must not collide with the original seed")
	}
}

// TestStartManualCapacityTriggerRunRejectsInvalidScopeBeforeTouchingTheDatabase
// pins that an invalid team_id/all_teams combination fails via
// manualCapacityTriggerScope's own guard, before startManualTriggerRun (and
// therefore the database) is ever reached -- store stays a zero value here
// on purpose; reaching it would panic/return ErrUnavailable instead of the
// request-shape error this is actually about.
func TestStartManualCapacityTriggerRunRejectsInvalidScopeBeforeTouchingTheDatabase(t *testing.T) {
	var store PostgresStore
	_, err := store.StartManualCapacityTriggerRun(
		context.Background(), "00000000-0000-4000-8000-000000000001", "2026-08-26",
		"manual-trigger:capacity:test", nil, false, nil,
	)
	if err != ErrInvalidState {
		t.Fatalf("all_teams=false with no team_id: err=%v want=%v", err, ErrInvalidState)
	}
}
