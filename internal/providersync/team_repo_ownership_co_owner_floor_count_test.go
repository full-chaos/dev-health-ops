package providersync

import "testing"

// TestTheShareFloorCountsTheCoOwnersItDrops: the dropped co-owners are
// counted for the run's log line and stats, main owners never.
func TestTheShareFloorCountsTheCoOwnersItDrops(t *testing.T) {
	got := deriveRankedOwner(map[rankedOwnerTeam]map[string]int{
		{"linear", "owner"}:   {"native": 1},
		{"jira", "mobile"}:    {"explicit_text": 1},
		{"jira", "payments"}:  {"explicit_text": 1},
		{"github", "pandas"}:  {"heuristic": 1},
		{"gitlab", "biggest"}: {"explicit_text": 296},
	})
	if got.CoOwnersBelowShareFloor != 3 {
		t.Fatalf("CoOwnersBelowShareFloor = %d, want 3 (the main owner at 1/300 is not counted)", got.CoOwnersBelowShareFloor)
	}
}
