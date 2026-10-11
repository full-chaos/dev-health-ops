package providersync

import (
	"context"
	"testing"
	"time"
)

// CHAOS-9204: a snapshot never closes an open row written by a run that read the provider AFTER this one
// (updated_at later than the read), whatever the reason it would close it: a surplus open row of a member
// the run returns, a rename, or a departure. One case set, run for every writer (GitHub, GitLab, Linear).
func testNewerRowIsNeverClosed[R any](
	t *testing.T, writer MembershipSnapshotWriter[R], kind func([]string) SnapshotKind[MembershipSnapshotRow],
	row func(team, member string, id *string, from time.Time) R,
) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	older, newer := at.Add(-2*time.Hour), at.Add(time.Hour)
	text := func(value string) *string { return &value }
	open := func(member string, id *string, from, updated time.Time) openMembership {
		return openMembership{OrgID: "o", Provider: writer.Provider, TeamID: "t", MemberID: member, ProviderUserID: id, ValidFrom: from, UpdatedAt: updated, Source: writer.Source}
	}
	snapshot := kind([]string{"t"}).Snapshot(ScopeProof{stated: true}, ProveSnapshot(SnapshotTerm{Holds: true, Reason: "read_returned"}), AbsenceByCloseWriter[MembershipSnapshotRow]())
	// plan returns every row the writer writes (the returned members first, then the closes) and the outcome.
	plan := func(openRows []openMembership, returned ...R) ([]R, MembershipSnapshotOutcome) {
		t.Helper()
		rows, outcome, err := writer.planSnapshot(context.Background(), "o", openRows, returned, returned, at, nil, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		return rows, outcome
	}
	closedOf := func(rows []R, written int) int { return len(rows) - written }
	t.Run("a surplus open row of a returned member that is older than the read is retired", func(t *testing.T) {
		all, outcome := plan([]openMembership{open("m", nil, older, older), open("m", nil, older.Add(time.Minute), older)}, row("t", "m", nil, at))
		if closedOf(all, 1) != 1 || outcome.DuplicatesRetired != 1 {
			t.Fatalf("closed = %d, DuplicatesRetired = %d, want 1 and 1", closedOf(all, 1), outcome.DuplicatesRetired)
		}
	})
	t.Run("a surplus open row of a returned member that is NEWER than the read is left open and counted", func(t *testing.T) {
		openRows := []openMembership{open("m", nil, older, older), open("m", nil, older.Add(time.Minute), newer)}
		all, outcome := plan(openRows, row("t", "m", nil, at))
		if closedOf(all, 1) != 0 || outcome.DuplicatesRetired != 0 || outcome.Skipped[membershipSkipNewerRow] != 1 {
			t.Fatalf("closed = %d, DuplicatesRetired = %d, skipped = %v, want 0, 0 and one %s", closedOf(all, 1), outcome.DuplicatesRetired, outcome.Skipped, membershipSkipNewerRow)
		}
	})
	t.Run("an older surplus row is left open too when ANOTHER row of the same member is newer (the fact is left whole)", func(t *testing.T) {
		openRows := []openMembership{open("m", nil, older, older), open("m", nil, older.Add(time.Minute), older), open("m", nil, older.Add(2*time.Minute), newer)}
		all, outcome := plan(openRows, row("t", "m", nil, at))
		if closedOf(all, 1) != 0 || outcome.DuplicatesRetired != 0 || outcome.Skipped[membershipSkipNewerRow] != 1 {
			t.Fatalf("closed = %d, DuplicatesRetired = %d, skipped = %v, want 0, 0 and one %s", closedOf(all, 1), outcome.DuplicatesRetired, outcome.Skipped, membershipSkipNewerRow)
		}
	})
	t.Run("two newer surplus rows of one member count as one skipped member", func(t *testing.T) {
		openRows := []openMembership{open("m", nil, older, older), open("m", nil, older.Add(time.Minute), newer), open("m", nil, older.Add(2*time.Minute), newer)}
		all, outcome := plan(openRows, row("t", "m", nil, at))
		if closedOf(all, 1) != 0 || outcome.Skipped[membershipSkipNewerRow] != 1 {
			t.Fatalf("closed = %d, skipped = %v, want 0 and one %s", closedOf(all, 1), outcome.Skipped, membershipSkipNewerRow)
		}
	})
	t.Run("a rename never closes a member id that holds a row newer than the read", func(t *testing.T) {
		openRows := []openMembership{open("old", text("7"), older, older), open("old", nil, older.Add(time.Minute), newer)}
		all, outcome := plan(openRows, row("t", "new", text("7"), at))
		if closedOf(all, 1) != 0 || outcome.Renamed != 0 {
			t.Fatalf("closed = %d, Renamed = %d, want 0 and 0: the old member id holds a newer row", closedOf(all, 1), outcome.Renamed)
		}
	})
	t.Run("a departure of a member with a newer row is skipped", func(t *testing.T) {
		openRows := []openMembership{open("gone", nil, older, newer)}
		all, outcome := plan(openRows)
		if len(all) != 0 || outcome.Skipped[membershipSkipNewerRow] != 1 {
			t.Fatalf("closed = %d, skipped = %v, want 0 and one %s", len(all), outcome.Skipped, membershipSkipNewerRow)
		}
	})
}

func TestNewerRowIsNeverClosedGitHub(t *testing.T) {
	testNewerRowIsNeverClosed(t, githubMembershipWriter, GitHubTeamMembershipKind,
		func(team, member string, id *string, from time.Time) githubMembershipRow {
			return githubMembershipRow{OrgID: "o", Provider: "github", TeamID: team, MemberID: member, ProviderUserID: id, ValidFrom: from, UpdatedAt: from}
		})
}

func TestNewerRowIsNeverClosedGitLab(t *testing.T) {
	testNewerRowIsNeverClosed(t, gitlabMembershipWriter, GitLabTeamMembershipKind,
		func(team, member string, id *string, from time.Time) gitlabTeamCatalogMembershipRow {
			return gitlabTeamCatalogMembershipRow{OrgID: "o", Provider: gitlabTeamCatalogProvider, TeamID: team, MemberID: member, ProviderUserID: id, ValidFrom: from, UpdatedAt: from}
		})
}

func TestNewerRowIsNeverClosedLinear(t *testing.T) {
	testNewerRowIsNeverClosed(t, linearMembershipWriter, LinearTeamMembershipKind,
		func(team, member string, id *string, from time.Time) linearReferenceMembershipRow {
			return linearReferenceMembershipRow{OrgID: "o", Provider: "linear", TeamID: team, MemberID: member, ProviderUserID: id, ValidFrom: from, UpdatedAt: from}
		})
}
