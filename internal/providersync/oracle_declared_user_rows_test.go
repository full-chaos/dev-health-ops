package providersync

import (
	"strconv"
	"strings"
	"testing"
)

// The frozen Python output of work_item_user_metrics_daily holds a person that is
// in two teams on one scope and day as two rows, because Python grouped by team.
// The table stores one row for each (scope, person, day) -- its key has no team --
// so the Go compute makes ONE row for the person (CHAOS-8859, chris: "It's a user
// metric, why would it key on team?"): the items summed over the teams, the cycle
// percentiles over all the person's items, the team of the row the one with the
// most items that day, the lowest team id on a tie. declaredMergedUserRows below
// is that row for each frozen case that holds such a person; a case that holds
// two rows for a person and has no entry here fails the comparison.
type declaredMergedUserRow struct {
	// sourceRows is the number of frozen Python rows of the person the declaration merges.
	sourceRows       int
	teamID, teamName string
	p50, p90         string
	reason           string
}

var declaredMergedUserRows = map[string]declaredMergedUserRow{
	"linked_donor_splits_the_group_by_team": {
		2, "team-linear", "Linear Team", "2.5", "2.9000000000000004",
		"one item each for team-linear and unassigned: a tie, so the lowest team id labels the row; cycle hours [2, 3] give p50 2.5 and p90 2.9 (2.9000000000000004 in float64, the interpolation the compute uses for every percentile)",
	},
	"one_user_two_teams_stays_two_compute_rows": {
		2, "team-a", "Team A", "3.0", "3.8",
		"one item each for team-a and team-b: a tie, so the lowest team id labels the row; cycle hours [2, 4] give p50 3.0 and p90 3.8",
	},
}

const userMetricsSnapshotSuffix = "_work-items_user-metrics-daily"

// withDeclaredMergedUserRows replaces, in the frozen rows of a user-metrics
// snapshot, the rows of a person that Python left in several teams by the
// declared merged row. Other snapshots and other cases are returned as they are.
func withDeclaredMergedUserRows(
	t *testing.T, snapshotName string, rows map[string]map[string]any,
) map[string]map[string]any {
	t.Helper()
	if !strings.HasSuffix(snapshotName, userMetricsSnapshotSuffix) {
		return rows
	}
	provider := strings.TrimSuffix(snapshotName, userMetricsSnapshotSuffix)
	for caseID, row := range rows {
		days := columnValues(t, caseID, row, "day")
		providers := columnValues(t, caseID, row, "provider")
		scopes := columnValues(t, caseID, row, "work_scope_id")
		users := columnValues(t, caseID, row, "user_identity")
		seen := map[[4]string]bool{}
		duplicated := false
		for index := range users {
			key := [4]string{days[index], providers[index], scopes[index], users[index]}
			if seen[key] {
				duplicated = true
			}
			seen[key] = true
		}
		if !duplicated {
			continue
		}
		distinct := seen
		declared, ok := declaredMergedUserRows[strings.TrimPrefix(caseID, provider+"_")]
		if !ok {
			t.Fatalf("frozen case %q of %s holds several rows for one person and declaredMergedUserRows has no entry for it", caseID, snapshotName)
		}
		teams := columnValues(t, caseID, row, "team_id")
		if len(users) != declared.sourceRows {
			t.Fatalf("frozen case %q of %s holds %d rows, the declaration merges %d", caseID, snapshotName, len(users), declared.sourceRows)
		}
		if !containsString(teams, declared.teamID) {
			t.Fatalf("frozen case %q of %s: the declared label %q is not one of the frozen teams %v", caseID, snapshotName, declared.teamID, teams)
		}
		if declared.reason == "" {
			t.Fatalf("declaredMergedUserRows[%q] needs a written reason", caseID)
		}
		if len(distinct) != 1 {
			t.Fatalf("frozen case %q of %s: the declared merge is for one person, the case has %d", caseID, snapshotName, len(distinct))
		}
		rows[caseID] = mergeFrozenUserRow(t, caseID, row, declared)
	}
	return rows
}

func columnValues(t *testing.T, caseID string, row map[string]any, column string) []string {
	t.Helper()
	list, ok := row[column].([]any)
	if !ok {
		t.Fatalf("frozen case %q: column %q is not a list", caseID, column)
	}
	values := make([]string, 0, len(list))
	for _, cell := range list {
		tagged, ok := cell.(map[string]any)
		if !ok {
			t.Fatalf("frozen case %q: column %q holds a cell that is not type-tagged: %v", caseID, column, cell)
		}
		value, _ := tagged["v"].(string)
		values = append(values, value)
	}
	return values
}

func mergeFrozenUserRow(t *testing.T, caseID string, row map[string]any, declared declaredMergedUserRow) map[string]any {
	t.Helper()
	merged := make(map[string]any, len(row))
	for column, raw := range row {
		list := raw.([]any)
		first := list[0].(map[string]any)
		cell := map[string]any{"t": first["t"], "v": first["v"]}
		switch column {
		case "items_started", "items_completed", "wip_count_end_of_day":
			sum := 0
			for _, value := range columnValues(t, caseID, row, column) {
				number, err := strconv.Atoi(value)
				if err != nil {
					t.Fatalf("frozen case %q: %s holds %q", caseID, column, value)
				}
				sum += number
			}
			cell["v"] = strconv.Itoa(sum)
		case "team_id":
			cell["v"] = declared.teamID
		case "team_name":
			cell["v"] = declared.teamName
		case "cycle_time_p50_hours":
			cell["v"] = declared.p50
		case "cycle_time_p90_hours":
			cell["v"] = declared.p90
		}
		merged[column] = []any{cell}
	}
	return merged
}
