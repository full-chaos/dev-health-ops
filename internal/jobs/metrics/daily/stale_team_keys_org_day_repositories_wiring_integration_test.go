//go:build integration

package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A retractor with no read of the organization's repositories cannot prove
// that a repository outside a run is gone. For a run of the whole organization
// it fails closed and writes nothing; a run of some repositories does not need
// the read.
func TestTheEndOfAnOrganizationRunFailsClosedWithNoRepositoryRead(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000007e0005"
	day := sharedScopeDay
	stored := day.Add(30 * time.Hour)
	if err := conn.Exec(ctx, `INSERT INTO team_metrics_daily
    (org_id, day, team_id, team_name, repo_id, commits_count, after_hours_commits_count, weekend_commits_count, computed_at)
    VALUES (?, ?, 'ENG', 'ENG', ?, 7, 1, 0, ?)`, org, day, orgDayRepoGone.String(), stored); err != nil {
		t.Fatal(err)
	}
	retractor, err := NewRunStaleKeyRetractor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if retractor.presentRepositories == nil {
		t.Fatal("the constructor wires no read of the organization's repositories")
	}
	retractor.nowUTC = func() time.Time { return stored.Add(time.Hour) }
	retractor.presentRepositories = nil
	listed := []RepositoryID{RepositoryID(sharedScopeRepoAPI.String())}
	written, err := retractor.RetractStaleKeys(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: listed})
	if !errors.Is(err, ErrOrganizationRepositoriesNotRead) || written != 0 {
		t.Errorf("a run of the whole organization with no read wired: %d row(s) and error %v, want no row and ErrOrganizationRepositoriesNotRead", written, err)
	}
	var held float64
	if err := conn.QueryRow(ctx, `SELECT toFloat64(sum(commits_count)) FROM team_metrics_daily FINAL WHERE org_id = ? AND day = ?`, org, day).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 7 {
		t.Errorf("team_metrics_daily holds %v after the refused step, want the stored 7", held)
	}
	if _, err := retractor.RetractStaleKeys(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: false, DiscoveredRepoIDs: listed}); err != nil {
		t.Errorf("a run of one repository needs no read of the organization's repositories and gives %v", err)
	}
}
