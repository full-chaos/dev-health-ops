//go:build integration

package chquery

import (
	"sort"
	"testing"
	"time"
)

// The repositories of a team are read from the rows of user_metrics_daily that
// name the team. A key whose NEWEST row (of the team) is a retraction row is no
// longer a row of the team, and its repository is left out; every other key
// keeps the reading of the reference (CHAOS-9135).
func TestTheRepositoriesOfATeamLeaveOutAKeyWhoseNewestRowIsARetraction(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	older, newer := day.Add(10*time.Hour), day.Add(11*time.Hour)
	const (
		measured  = "00000000-0000-4000-8000-0000000093f1"
		retracted = "00000000-0000-4000-8000-0000000093f2"
		returned  = "00000000-0000-4000-8000-0000000093f4"
	)
	store := func(repo, person, team string, marker uint8, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, team_id, team_name, computed_at, retracted)
 VALUES (?, ?, ?, ?, ?, ?, 'Name', ?, ?)`, orgAlpha, repo, day, person, person, team, at, marker); err != nil {
			t.Fatal(err)
		}
	}
	store(measured, "a@example.com", "team-a", 0, newer)
	store(retracted, "b@example.com", "team-a", 0, older)
	store(retracted, "b@example.com", "team-a", 1, newer) // the row of zeros carries the team
	store(returned, "d@example.com", "team-a", 1, older)
	store(returned, "d@example.com", "team-a", 0, newer)

	got, err := reader.ResolveRepoIDsForTeams(ctx, []string{"team-a"}, orgAlpha)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{measured, returned}
	if len(got) != len(want) {
		t.Fatalf("repositories = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("repositories = %v, want %v", got, want)
		}
	}
}
