//go:build integration

package daily

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
)

// TestThePersonsTeamsOfTheLandscapeAreTheMembershipsValidAtTheDay runs the
// ic_finalize family, as the production wiring builds it, over membership
// rows in team_memberships. The team of a person on the day the family
// computes is every ACTIVE team with a membership that is valid AT that day:
//
//   - two: a member of two active teams. One point in EACH team; the person's
//     one row of the day holds the primary one.
//   - valid: one membership that started before the day and is open.
//   - closed: a membership that ended the day before the day.
//   - future: a membership that starts the day after the day.
//   - reopened: the newest version of the membership row closed it before the
//     day; an older version of the same row was open.
//   - inactive: a member of an inactive team only.
//   - mixed: a member of an inactive team (primary) and of an active team.
//   - roster: named in the roster column teams.members of an active team, with
//     no membership row. The column is not what says who is a member.
//   - none: no membership.
//
// A person with no team on the day is unassigned. The same people are read
// again for a day two days later, when the future membership is valid: the
// time of the read is the day computed, not the time of the run.
func TestThePersonsTeamsOfTheLandscapeAreTheMembershipsValidAtTheDay(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-00000009084a"
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	twoDaysLater := day.AddDate(0, 0, 2)
	longBefore := day.AddDate(0, 0, -30)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000908c1")
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	for _, team := range []struct {
		id, provider string
		active       uint8
		roster       []string
	}{
		{"jira:ENG", "jira", 1, []string{"roster@example.com"}},
		{"github:platform", "github", 1, []string{}},
		{"gitlab:ops", "gitlab", 1, []string{}},
		{"ENG", "jira", 0, []string{}},
	} {
		exec("insert team "+team.id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			team.id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+team.id)), "Team "+team.id, team.roster,
			[]string{}, longBefore, longBefore, org, team.provider, team.active)
	}
	membership := func(person, provider, teamID string, primary uint8, validFrom time.Time, validTo *time.Time, updatedAt time.Time) {
		t.Helper()
		exec("insert membership of "+person+" in "+teamID, `INSERT INTO team_memberships
    (org_id, provider, team_id, member_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, identity_facets)
    VALUES (?, ?, ?, ?, ?, 'native', ?, 100, 10, ?, ?, ?, ?)`,
			org, provider, teamID, person, person+"@example.com", primary, validFrom, validTo, updatedAt,
			[]string{person + "@example.com"})
	}
	open := (*time.Time)(nil)
	dayBefore, dayAfter := day.AddDate(0, 0, -1), day.AddDate(0, 0, 1)
	membership("two", "github", "github:platform", 0, longBefore, open, longBefore)
	membership("two", "jira", "jira:ENG", 1, longBefore, open, longBefore)
	membership("valid", "github", "github:platform", 1, longBefore, open, longBefore)
	membership("closed", "jira", "jira:ENG", 1, longBefore, &dayBefore, longBefore)
	membership("future", "jira", "jira:ENG", 1, dayAfter, open, longBefore)
	// Two versions of one row (same key, same valid_from): the newer one
	// closes the membership before the day.
	membership("reopened", "jira", "jira:ENG", 1, longBefore, open, longBefore)
	membership("reopened", "jira", "jira:ENG", 1, longBefore, &dayBefore, longBefore.Add(time.Hour))
	membership("inactive", "jira", "ENG", 1, longBefore, open, longBefore)
	membership("mixed", "jira", "ENG", 1, longBefore, open, longBefore)
	membership("mixed", "gitlab", "gitlab:ops", 0, longBefore, open, longBefore)

	people := []string{"two", "valid", "closed", "future", "reopened", "inactive", "mixed", "roster", "none"}
	// What the repository/user family writes for each person on each day.
	for _, rowDay := range []time.Time{day, twoDaysLater} {
		for _, person := range people {
			exec("insert user metrics of "+person, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, commits_count, loc_added, loc_deleted,
     prs_authored, prs_merged, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', 3, 40, 10, 1, 1, 50, 1, ?, ?)`,
				repo, rowDay, person+"@example.com", person+"@example.com", rowDay.Add(30*time.Hour), org)
		}
	}

	type answers struct {
		// The teams of each person's live points of the day, sorted.
		Points map[string][]string
		// The team of each person's newest row of the day.
		Rows map[string]string
		// The live points of the day: three maps for each (person, team).
		LivePoints uint64
	}
	read := func(asOf time.Time) answers {
		t.Helper()
		out := answers{Points: map[string][]string{}, Rows: map[string]string{}}
		live := teamkeytables.ICLandscapeRolling30d.LiveRow("")
		points, err := conn.Query(ctx, `SELECT identity_id, groupUniqArray(toString(team_id)), count() FROM ic_landscape_rolling_30d FINAL
WHERE org_id = ? AND as_of_day = ? AND `+live+` GROUP BY identity_id`, org, asOf)
		if err != nil {
			t.Fatalf("read the points: %v", err)
		}
		defer points.Close()
		for points.Next() {
			var person string
			var teams []string
			var count uint64
			if err := points.Scan(&person, &teams, &count); err != nil {
				t.Fatal(err)
			}
			sort.Strings(teams)
			out.Points[person[:len(person)-len("@example.com")]] = teams
			out.LivePoints += count
		}
		if err := points.Err(); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, `SELECT author_email, toString(team_id) FROM (
    SELECT * FROM user_metrics_daily ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND day = ?`, org, asOf)
		if err != nil {
			t.Fatalf("read the rows: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var person, team string
			if err := rows.Scan(&person, &team); err != nil {
				t.Fatal(err)
			}
			out.Rows[person[:len(person)-len("@example.com")]] = team
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	run := func(target time.Time) answers {
		t.Helper()
		if _, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: target}); err != nil {
			t.Fatalf("ic_finalize of %s: %v", target.Format("2006-01-02"), err)
		}
		return read(target)
	}

	unassigned := []string{"unassigned"}
	want := answers{
		Points: map[string][]string{
			"two": {"github:platform", "jira:ENG"}, "valid": {"github:platform"},
			"closed": unassigned, "future": unassigned, "reopened": unassigned, "inactive": unassigned,
			"mixed": {"gitlab:ops"}, "roster": unassigned, "none": unassigned,
		},
		Rows: map[string]string{
			// One row for a person: the primary membership of the two.
			"two": "jira:ENG", "valid": "github:platform",
			"closed": "unassigned", "future": "unassigned", "reopened": "unassigned", "inactive": "unassigned",
			"mixed": "gitlab:ops", "roster": "unassigned", "none": "unassigned",
		},
		// 10 (person, team) pairs, three maps each.
		LivePoints: 30,
	}
	if got := run(day); !reflect.DeepEqual(got, want) {
		t.Errorf("the day:\n got  %+v\n want %+v", got, want)
	}
	if got := run(day); !reflect.DeepEqual(got, want) {
		t.Errorf("the day, run again:\n got  %+v\n want %+v", got, want)
	}

	// Two days later the future membership is valid; nothing else changed.
	want.Points["future"], want.Rows["future"] = []string{"jira:ENG"}, "jira:ENG"
	if got := run(twoDaysLater); !reflect.DeepEqual(got, want) {
		t.Errorf("two days later:\n got  %+v\n want %+v", got, want)
	}
	// The earlier day is not changed by the later run.
	want.Points["future"], want.Rows["future"] = unassigned, "unassigned"
	if got := read(day); !reflect.DeepEqual(got, want) {
		t.Errorf("the day, after the later run:\n got  %+v\n want %+v", got, want)
	}
}
