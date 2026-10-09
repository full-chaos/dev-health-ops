//go:build integration

package operatingreview

import (
	"testing"
	"time"
)

// The operating-review read that takes an average over rows, against a day
// that was computed again after a team id changed. The rows are on the schema
// of the migration chain.
//
// A key whose newest row is a row of zeros holds no measure: the two averages
// of the state durations must not take it as a sample of 0. A real row with
// an average WIP of 0 stays a sample.
//
// The rows are stored by plain INSERT statements, so the same file runs on a
// tree without the writer rule and without the reader clause.
func TestOperatingReviewReadsLeaveOutAKeyThatARecomputeSuperseded(t *testing.T) {
	ctx, conn, client := startOperatingReviewSchema(t)
	const org = "operating-review-superseded-keys-it"
	for _, statement := range []string{
		// The first compute, under the old team id.
		`INSERT INTO work_item_state_durations_daily
    (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', 'Platform', 'in_progress', 10, 2, 2, '2026-01-03 06:00:00', '` + org + `')`,
		// The recompute: the same work under the new id, a row of zeros over
		// the old key, and another team with a real average WIP of 0.
		`INSERT INTO work_item_state_durations_daily
    (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, avg_wip, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'linear:platform', 'Platform', 'in_progress', 10, 2, 2, '2026-01-09 06:00:00', '` + org + `'),
       ('2026-01-02', 'linear', 'proj', 'linear:apps', 'Apps', 'in_progress', 4, 1, 0, '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_state_durations_daily (day, provider, work_scope_id, team_id, status, computed_at, org_id)
VALUES ('2026-01-02', 'linear', 'proj', 'platform', 'in_progress', '2026-01-09 06:00:00', '` + org + `')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	start, end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	for name, teams := range map[string]teamSelection{
		"all teams":     nil,
		"several teams": {"platform", "linear:platform", "linear:apps"},
	} {
		durations, err := fetchStateDurations(ctx, client, org, teams, start, end)
		if err != nil {
			t.Fatalf("%s: state durations: %v", name, err)
		}
		if len(durations) != 1 || durations[0].itemsTouched != 3 || durations[0].durationHours != 7 || durations[0].avgWip != 1 {
			t.Errorf("%s: state durations = %+v, want one status with 3 items, an average of 7 hours and an average WIP of 1 "+
				"over the two keys that hold a measure", name, durations)
		}
	}
}
