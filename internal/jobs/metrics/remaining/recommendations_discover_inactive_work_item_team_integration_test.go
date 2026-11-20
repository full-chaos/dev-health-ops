//go:build integration

package remaining

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestDiscoverTeamIDsDropsInactiveWorkItemTeams(t *testing.T) {
	const (
		org           = "org-discover-inactive-work-item-teams"
		activeTeam    = "team-active-work-items"
		inactiveTeam  = "team-inactive-work-items"
		retiredTeam   = "team-retired-after-active"
		inactiveOwner = "team-inactive-repo-owner"
		inactiveBoth  = "team-inactive-in-two-sets"
		ownerRepo     = "a4444444-4444-4444-4444-444444444444"
		bothRepo      = "a5555555-5555-5555-5555-555555555555"
	)
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	older := mustTimestamp(t, "2026-01-01 00:00:00")
	newer := mustTimestamp(t, "2026-02-01 00:00:00")

	team := func(id string, active uint8, at time.Time) {
		exec(`INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
			VALUES (?, ?, ?, 'github', ?, ?, ?)`, id, id, org, active, at, at)
	}
	team(activeTeam, 1, older)
	team(inactiveTeam, 0, older)
	team(retiredTeam, 1, older)
	team(retiredTeam, 0, newer)
	team(inactiveOwner, 0, older)
	team(inactiveBoth, 0, older)

	workItems := func(teamID string) {
		exec(`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
			 story_points_completed, computed_at, org_id)
			VALUES (?, 'github', 'acme/x', ?, '', 1, 1, 0, 0, 1, 0, 10, 0, 0, ?, ?)`,
			today.AddDate(0, 0, -1), teamID, today, org)
	}
	for _, id := range []string{activeTeam, inactiveTeam, retiredTeam, inactiveBoth} {
		workItems(id)
	}

	owns := func(teamID, repoID, name string) {
		exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
			VALUES (?, ?, ?, ?, ?, 'github')`, repoID, name, older, older, org)
		exec(`INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type,
			 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
			org, teamID, repoID, name, older, older)
	}
	owns(inactiveOwner, ownerRepo, "acme/inactive-owner")
	owns(inactiveBoth, bothRepo, "acme/inactive-both")

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	got, err := executor.DiscoverTeamIDs(ctx, org, today)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if want := []string{activeTeam}; !reflect.DeepEqual(got, want) {
		t.Errorf("discovered %v, want %v (an inactive team is not evaluated, whichever set lists it)", got, want)
	}
}

// inactiveReadConn counts the reads of the inactive team ids and runs hook
// after each one (the owner set reads it first; the union read is the last).
type inactiveReadConn struct {
	driver.Conn
	reads int
	hook  func(read int) error
}

func (conn *inactiveReadConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if !strings.Contains(query, "GROUP BY id HAVING") {
		return conn.Conn.Query(ctx, query, args...)
	}
	conn.reads++
	read := conn.reads
	rows, err := conn.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if conn.hook != nil {
		if hookErr := conn.hook(read); hookErr != nil {
			_ = rows.Close()
			return nil, hookErr
		}
	}
	return rows, nil
}

func startDiscoverFixture(t *testing.T, org string) (context.Context, driver.Conn) {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	return ctx, openLoaderClickHouse(t, ctx, dsn)
}

func seedTeamWithWorkItems(t *testing.T, ctx context.Context, conn driver.Conn, org, teamID string) {
	t.Helper()
	at := mustTimestamp(t, "2026-01-01 00:00:00")
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, ?, ?, 'github', 1, ?, ?)`, teamID, teamID, org, at, at); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
		 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
		 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
		 story_points_completed, computed_at, org_id)
		VALUES (?, 'github', 'acme/x', ?, '', 1, 1, 0, 0, 1, 0, 10, 0, 0, ?, ?)`,
		today.AddDate(0, 0, -1), teamID, today, org); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverTeamIDsFailsWhenTheInactiveReadFails(t *testing.T) {
	const org = "org-discover-inactive-read-error"
	ctx, conn := startDiscoverFixture(t, org)
	seedTeamWithWorkItems(t, ctx, conn, org, "team-a")
	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	counter := &inactiveReadConn{Conn: conn}
	executor.conn = counter
	if got, err := executor.DiscoverTeamIDs(ctx, org, time.Now().UTC()); err != nil || !reflect.DeepEqual(got, []string{"team-a"}) {
		t.Fatalf("control discover = %v, %v", got, err)
	}
	last := counter.reads
	if last < 2 {
		t.Fatalf("inactive reads = %d, want the owner read(s) and the union read", last)
	}

	wantErr := errors.New("inactive read refused")
	stub := &inactiveReadConn{Conn: conn, hook: func(read int) error {
		if read == last {
			return wantErr
		}
		return nil
	}}
	executor.conn = stub
	got, err := executor.DiscoverTeamIDs(ctx, org, time.Now().UTC())
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("discover = %v, %v; want the error of the last inactive read (the union read)", got, err)
	}
}

func TestDiscoverTeamIDsDropsATeamThatRetiresBetweenTheSetReads(t *testing.T) {
	const org = "org-discover-inactive-retires-between"
	ctx, conn := startDiscoverFixture(t, org)
	seedTeamWithWorkItems(t, ctx, conn, org, "team-stays")
	seedTeamWithWorkItems(t, ctx, conn, org, "team-retires")
	stub := &inactiveReadConn{Conn: conn, hook: func(read int) error {
		if read != 1 {
			return nil
		}
		later := mustTimestamp(t, "2026-02-01 00:00:00")
		return conn.Exec(ctx, `INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
			VALUES ('team-retires', 'team-retires', ?, 'github', 0, ?, ?)`, org, later, later)
	}}
	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	executor.conn = stub
	got, err := executor.DiscoverTeamIDs(ctx, org, time.Now().UTC())
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if want := []string{"team-stays"}; !reflect.DeepEqual(got, want) {
		t.Errorf("discovered %v, want %v (a team retired after the first read is dropped by the last)", got, want)
	}
}

// The project owner read runs after the repository owner set and before the
// union's activity read, so a stub of it changes the teams table between them.
func TestDiscoverTeamIDsKeepsARepoOnlyOwnerRestoredBeforeTheUnionRead(t *testing.T) {
	const (
		org      = "org-discover-repo-owner-restored"
		owner    = "team-repo-owner-restored"
		repoID   = "a6666666-6666-6666-6666-666666666666"
		repoName = "acme/restored-owner"
	)
	ctx, conn := startDiscoverFixture(t, org)
	older := mustTimestamp(t, "2026-01-01 00:00:00")
	retired := mustTimestamp(t, "2026-02-01 00:00:00")
	restored := mustTimestamp(t, "2026-03-01 00:00:00")
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}
	exec(`INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, ?, ?, 'github', 0, ?, ?)`, owner, owner, org, retired, retired)
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES (?, ?, ?, ?, ?, 'github')`, repoID, repoName, older, older, org)
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
		org, owner, repoID, repoName, older, older)

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	executor.loadProjectOwners = func(context.Context, string, time.Time) ([]teamattribution.GithubWorkItemDerivationProjectFact, error) {
		return nil, nil
	}
	if got, err := executor.DiscoverTeamIDs(ctx, org, time.Now().UTC()); err != nil || len(got) != 0 {
		t.Fatalf("control discover = %v, %v; want no team (the owner is retired at every read)", got, err)
	}

	restores := 0
	executor.loadProjectOwners = func(context.Context, string, time.Time) ([]teamattribution.GithubWorkItemDerivationProjectFact, error) {
		restores++
		return nil, conn.Exec(ctx, `INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
			VALUES (?, ?, ?, 'github', 1, ?, ?)`, owner, owner, org, restored, restored)
	}
	got, err := executor.DiscoverTeamIDs(ctx, org, time.Now().UTC())
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if restores != 1 {
		t.Fatalf("project owner reads = %d, want 1 (the restore ran between the two sets)", restores)
	}
	if want := []string{owner}; !reflect.DeepEqual(got, want) {
		t.Errorf("discovered %v, want %v (a repo-only owner that is active at the union read is evaluated)", got, want)
	}
}
