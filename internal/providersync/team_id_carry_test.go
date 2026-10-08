package providersync

import (
	"errors"
	"testing"
	"time"
)

// The guard refuses a planned row that would write an id with no provider
// key, before any write.
func TestTeamIDCarryGuardRefusesAnUnkeyedID(t *testing.T) {
	index := map[string]int{"id": 0, "is_active": 1, "team_id": 2, "valid_to": 3, "entity_id": 4, "status": 5}
	row := func(id string, active uint8, validTo *time.Time, status string) chRow {
		return chRow{index: index, vals: []any{id, active, id, validTo, id, status}}
	}
	closed := time.Now()
	for _, tc := range []struct {
		table string
		row   chRow
		want  bool
	}{
		{"teams", row("ENG", 1, nil, ""), true},
		{"teams", row("ENG", 0, nil, ""), false},
		{"teams", row("linear:ENG", 1, nil, ""), false},
		{"team_memberships", row("ENG", 0, nil, ""), true},
		{"team_memberships", row("ENG", 0, &closed, ""), false},
		{"team_repo_ownership", row("ENG", 0, nil, ""), true},
		{"team_sync_policies", row("ENG", 0, nil, ""), true},
		{"team_provider_observations", row("ENG", 0, nil, ""), true},
		{"team_drift_changes", row("ENG", 0, nil, "pending"), true},
		{"team_drift_changes", row("ENG", 0, nil, "superseded"), false},
	} {
		err := teamIDCarryGuard(teamIDCarryWrite{table: tc.table, rows: []chRow{tc.row}})
		if got := errors.Is(err, errTeamIDCarryUnkeyed); got != tc.want {
			t.Errorf("%s %v: refused = %v, want %v", tc.table, tc.row.vals, got, tc.want)
		}
	}
}
