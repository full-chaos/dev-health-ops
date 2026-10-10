//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/icfinalize"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// TestTheLandscapeFinalizeGivesNoPersonAnInactiveTeam runs the ic_finalize
// family, as the production wiring builds it, over the stored rows a team-id
// carry leaves behind. The team ENG was replaced by jira:ENG: ENG is inactive.
// Stored rows still hold "ENG":
//
//   - alice: her one row of the day is of a repository this run did not
//     compute; an earlier finalize stored it under ENG.
//   - bob: work items and no commit. An earlier finalize stored his row of the
//     day under a made-up repository id, with ENG.
//   - dave: no row on the day; one row ten days before it, under ENG.
//   - carol: a row ten days before under ENG, and a row of the day that the
//     repository/user family wrote as unassigned.
//   - erin: an older row under one active team, the newest row under another.
//   - frank: a row of the day under ENG; he has a membership in an active team.
//   - henry: no row on the day; the row of the older day was computed last.
//   - ivy: two rows of one day and one compute time, of two repositories and
//     two active teams.
//
// The family reads every newest row of the day back and writes it again, and
// it gives each person of the trailing 30 days a point in the landscape of the
// day. After the run no row of the day and no live point may sit under ENG:
// such a row is written again under the mapped team or as unassigned. A row of
// a day the run does not compute keeps its stored id, and it gives no point
// that id.
func TestTheLandscapeFinalizeGivesNoPersonAnInactiveTeam(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-00000009083a"
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	tenDaysBefore, fiveDaysBefore := day.AddDate(0, 0, -10), day.AddDate(0, 0, -5)
	earlier := day.Add(30 * time.Hour)
	later := earlier.Add(time.Hour)
	repoOld := uuid.MustParse("00000000-0000-4000-8000-0000000908a1")
	repoLive := uuid.MustParse("00000000-0000-4000-8000-0000000908a2")
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	t0 := day.Add(-72 * time.Hour)
	for _, team := range []struct {
		id, provider string
		active       uint8
		members      []string
	}{
		{"ENG", "jira", 0, []string{}},
		{"jira:ENG", "jira", 1, []string{}},
		{"github:platform", "github", 1, []string{}},
	} {
		exec("insert team "+team.id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			team.id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.id)), "Team "+team.id, team.members,
			[]string{}, t0, t0, org, team.provider, team.active)
	}

	exec("insert frank's membership", `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, ?, ?, ?, ?, 'native', ?, ?, ?, ?, ?, ?, ?)`,
		org, "github", "github:platform", "frank", "frank@example.com", uint8(1), uint16(100), int32(10), t0, (*time.Time)(nil), t0,
		[]string{"frank@example.com"})

	userRow := func(repo uuid.UUID, rowDay time.Time, person, team string, computedAt time.Time) {
		t.Helper()
		exec("insert user metrics of "+person, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, ?, ?, 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
			repo, rowDay, person, person, team, "Team "+team, computedAt, org)
	}
	userRow(repoOld, day, "alice@example.com", "ENG", earlier)
	userRow(icfinalize.SynthesizedRepoID(org, "bob@example.com"), day, "bob@example.com", "ENG", earlier)
	userRow(repoLive, tenDaysBefore, "dave@example.com", "ENG", earlier)
	userRow(repoLive, tenDaysBefore, "carol@example.com", "ENG", earlier)
	userRow(repoLive, day, "carol@example.com", "unassigned", later)
	userRow(repoLive, fiveDaysBefore, "erin@example.com", "github:platform", earlier)
	userRow(repoLive, day, "erin@example.com", "jira:ENG", later)
	userRow(repoLive, day, "frank@example.com", "ENG", earlier)
	// The team of a person is the team of the NEWEST row by compute time, not
	// of the newest day: henry's older day was computed last.
	userRow(repoLive, tenDaysBefore, "henry@example.com", "jira:ENG", later)
	userRow(repoLive, fiveDaysBefore, "henry@example.com", "github:platform", earlier)
	// One day, one compute time: the repository id decides, so two runs agree.
	userRow(repoOld, fiveDaysBefore, "ivy@example.com", "jira:ENG", earlier)
	userRow(repoLive, fiveDaysBefore, "ivy@example.com", "github:platform", earlier)
	exec("insert bob's work items", `INSERT INTO work_item_user_metrics_daily
    (day, provider, work_scope_id, user_identity, team_id, team_name, items_started, items_completed, wip_count_end_of_day, computed_at, org_id)
    VALUES (?, 'jira', 'ENGPROJ', 'bob@example.com', 'jira:ENG', 'Team jira:ENG', 1, 2, 1, ?, ?)`, day, later, org)
	// The points an earlier finalize left under ENG for the day.
	for _, person := range []string{"alice@example.com", "dave@example.com"} {
		for _, mapName := range []string{"churn_throughput", "cycle_throughput", "wip_throughput"} {
			exec("insert the earlier point of "+person, `INSERT INTO ic_landscape_rolling_30d
    (repo_id, as_of_day, identity_id, team_id, map_name, x_raw, y_raw, x_norm, y_norm, churn_loc_30d, delivery_units_30d, computed_at, org_id)
    VALUES (?, ?, ?, 'ENG', ?, 3, 4, 0.5, 0.5, 30, 4, ?, ?)`, uuid.Nil, day, person, mapName, earlier, org)
		}
	}

	type answers struct {
		// The team of each person's live points of the day.
		Points map[string][]string
		// The team id and the team name of each person's newest row of the day.
		Rows map[string]string
		// The team of the newest row of the day before the 30 days' rows.
		DaveTenDaysBefore string
	}
	read := func() answers {
		t.Helper()
		out := answers{Points: map[string][]string{}, Rows: map[string]string{}}
		points, err := conn.Query(ctx, `SELECT identity_id, groupUniqArray(toString(team_id)) FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND `+teamkeytables.ICLandscapeRolling30d.LiveRow("")+` GROUP BY identity_id`, org, day)
		if err != nil {
			t.Fatalf("read the points: %v", err)
		}
		defer points.Close()
		for points.Next() {
			var person string
			var teams []string
			if err := points.Scan(&person, &teams); err != nil {
				t.Fatal(err)
			}
			out.Points[person] = teams
		}
		if err := points.Err(); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, `SELECT author_email, concat(toString(team_id), ' / ', toString(team_name)) FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day = ?`, org, day)
		if err != nil {
			t.Fatalf("read the rows: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var person, team string
			if err := rows.Scan(&person, &team); err != nil {
				t.Fatal(err)
			}
			if previous, twice := out.Rows[person]; twice {
				t.Fatalf("%s has two rows of the day (teams %q and %q)", person, previous, team)
			}
			out.Rows[person] = team
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT toString(argMax(team_id, computed_at)) FROM user_metrics_daily
WHERE org_id = ? AND day = ? AND author_email = 'dave@example.com'`, org, tenDaysBefore).Scan(&out.DaveTenDaysBefore); err != nil {
			t.Fatalf("read dave's old row: %v", err)
		}
		return out
	}

	want := answers{
		Points: map[string][]string{
			"alice@example.com": {"unassigned"}, "bob@example.com": {"unassigned"}, "carol@example.com": {"unassigned"},
			"dave@example.com": {"unassigned"}, "erin@example.com": {"jira:ENG"}, "frank@example.com": {"github:platform"},
			"henry@example.com": {"jira:ENG"}, "ivy@example.com": {"github:platform"},
		},
		// A row read with an inactive team gets the fallback id AND its name.
		// A mapped row keeps the name it was read with (frank), as every row
		// the family maps does.
		Rows: map[string]string{
			"alice@example.com": "unassigned / Unassigned", "bob@example.com": "unassigned / Unassigned",
			"carol@example.com": "unassigned / Team unassigned",
			"erin@example.com":  "jira:ENG / Team jira:ENG", "frank@example.com": "github:platform / Unassigned",
		},
		// Not a row of the day: the run does not write it again.
		DaveTenDaysBefore: "ENG",
	}
	executor := NewICFinalizeExecutor(conn)
	for _, pass := range []string{"the run", "the same run again"} {
		if _, err := executor.ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
			t.Fatalf("%s: %v", pass, err)
		}
		got := read()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("after %s:\n got  %+v\n want %+v", pass, got, want)
		}
		var underInactive uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND team_id = 'ENG' AND `+teamkeytables.ICLandscapeRolling30d.LiveRow(""), org, day).Scan(&underInactive); err != nil {
			t.Fatal(err)
		}
		if underInactive != 0 {
			t.Errorf("after %s: %d live points of the day sit under the inactive team", pass, underInactive)
		}
	}
}
