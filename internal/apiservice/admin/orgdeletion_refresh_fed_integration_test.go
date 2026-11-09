//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The ownership-group tables (migration 147) are filled by a refreshable view
// from projects, teams and team_project_ownership. These cells hold org
// deletion to the rule that no row of a deleted organization is left in them,
// whatever a refresh does while the organization's rows are being deleted.

const groupsRefreshViewName = "project_team_ownership_groups_refresh"

type refreshFedVenue struct {
	ctx             context.Context
	conn            driver.Conn
	goBase          string
	super           string
	target, control uuid.UUID
}

func startRefreshFedVenue(t *testing.T) *refreshFedVenue {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-deletion-refresh-fed-32"
	v := &refreshFedVenue{ctx: ctx, target: uuid.New(), control: uuid.New()}
	superID := uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for _, org := range []uuid.UUID{v.target, v.control} {
				if _, err := adminPool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "venue-groupsdel-"+org.String()[:8]); err != nil {
					t.Fatalf("seed organization: %v", err)
				}
			}
			if _, err := adminPool.Exec(ctx, `INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-groupsdel-super@example.com', true, true, true, 0, now(), now())`, superID); err != nil {
				t.Fatalf("seed user: %v", err)
			}
			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "venue-groupsdel-super@example.com", "is_superuser": true},
			}
		},
	})
	chURI := venue.AdminClickHouseURI(t, venue.GoClickHouseDB)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(chURI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	v.conn = conn
	v.goBase, _ = startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.ClickHouseDSN = chURI
	})
	v.super = venue.Tokens["super"]
	return v
}

func (v *refreshFedVenue) exec(t *testing.T, statement string, args ...any) {
	t.Helper()
	if err := v.conn.Exec(v.ctx, statement, args...); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// seedSources writes one project, one active team and one open native
// ownership of the project by the team: one ownership group.
func (v *refreshFedVenue) seedSources(t *testing.T, org uuid.UUID) {
	t.Helper()
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	v.exec(t, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at, last_synced) VALUES ('P1', ?, 'github', 'K1', 'P1', 1, 'started', '', ?, ?)`,
		org.String(), at, at)
	v.exec(t, `INSERT INTO teams (id, team_uuid, name, updated_at, last_synced, org_id, provider, is_active) VALUES ('T1', generateUUIDv4(), 'T1', ?, ?, ?, 'github', 1)`,
		at, at, org.String())
	v.exec(t, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at, last_synced) VALUES (?, 'github', 'T1', 'P1', 'K1', 'native', ?, NULL, ?, ?)`,
		org.String(), at, at, at)
}

// refresh runs one refresh of the view and waits for it.
func refreshGroups(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, "SYSTEM REFRESH VIEW "+groupsRefreshViewName); err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	if err := conn.Exec(ctx, "SYSTEM WAIT VIEW "+groupsRefreshViewName); err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	return nil
}

// plantGroupRow appends one gone-marked group row of org to the log; the view
// copies it into the current table.
func plantGroupRow(ctx context.Context, conn driver.Conn, org uuid.UUID) error {
	return conn.Exec(ctx, `INSERT INTO project_team_ownership_groups_log (org_id, row_key, project_id, team_id, source_name, provider, content_hash, stored_at, is_deleted) VALUES (?, 'github:P1:T1:native', 'P1', 'T1', 'native', 'github', 1, now64(6), 1)`, org.String())
}

func (v *refreshFedVenue) rows(t *testing.T, table string, org uuid.UUID) uint64 {
	t.Helper()
	var count uint64
	if err := v.conn.QueryRow(v.ctx, "SELECT count() FROM `"+table+"` WHERE org_id = ?", org.String()).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func (v *refreshFedVenue) liveGroups(t *testing.T, org uuid.UUID) uint64 {
	t.Helper()
	var count uint64
	if err := v.conn.QueryRow(v.ctx, "SELECT count() FROM project_team_ownership_groups_current FINAL WHERE org_id = ? AND is_deleted = 0", org.String()).Scan(&count); err != nil {
		t.Fatalf("count live groups: %v", err)
	}
	return count
}

func (v *refreshFedVenue) deleteTarget(t *testing.T) venueoracle.Response {
	t.Helper()
	return venueoracle.Do(t, v.goBase, venueoracle.Request{
		Name: "delete org", Method: "DELETE",
		Path:    "/api/v1/admin/orgs/" + v.target.String(),
		Headers: map[string]string{"Authorization": "Bearer " + v.super},
	})
}

func (v *refreshFedVenue) requireNoTargetGroupRows(t *testing.T) {
	t.Helper()
	for _, table := range []string{"project_team_ownership_groups_log", "project_team_ownership_groups_current"} {
		if got := v.rows(t, table, v.target); got != 0 {
			t.Errorf("%s holds %d row(s) of the deleted organization, want 0", table, got)
		}
	}
}

// hookRecord collects what the delete hook saw; the hook runs on the server's
// goroutine, so it records errors instead of failing the test there.
type hookRecord struct {
	mu     sync.Mutex
	passes map[int]bool
	errs   []string
}

func (r *hookRecord) pass(pass int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.passes == nil {
		r.passes = map[int]bool{}
	}
	r.passes[pass] = true
}

func (r *hookRecord) fail(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *hookRecord) seen() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int
	for pass := range r.passes {
		out = append(out, pass)
	}
	sort.Ints(out)
	return out
}

func (r *hookRecord) check(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) > 0 {
		t.Fatalf("delete hook:\n  %s", strings.Join(r.errs, "\n  "))
	}
}

// A refresh after every table delete of the purge: while a source row of the
// organization is left, a refresh builds its groups again, and after the last
// source row goes it marks them gone. Whatever it wrote, the deleted
// organization holds no group row afterwards, and the control organization's
// groups are untouched.
func TestOrgDeletionLeavesNoGroupRowWhenARefreshRunsAfterEachTableDelete(t *testing.T) {
	v := startRefreshFedVenue(t)
	for _, org := range []uuid.UUID{v.target, v.control} {
		v.seedSources(t, org)
	}
	if err := refreshGroups(v.ctx, v.conn); err != nil {
		t.Fatal(err)
	}
	for _, org := range []uuid.UUID{v.target, v.control} {
		if got := v.liveGroups(t, org); got == 0 {
			t.Fatalf("seed: the refresh built no group of organization %s", org)
		}
	}
	controlBefore := v.liveGroups(t, v.control)

	record := &hookRecord{}
	restore := admin.SetOrgDeletionDeleteHook(func(ctx context.Context, table string, pass int) {
		record.pass(pass)
		if err := refreshGroups(ctx, v.conn); err != nil {
			record.fail("after the delete of %s (pass %d): %v", table, pass, err)
		}
	})
	t.Cleanup(restore)

	response := v.deleteTarget(t)
	restore()
	record.check(t)
	if response.Status != http.StatusOK {
		t.Fatalf("delete org: status %d, want 200\nbody: %s", response.Status, response.Body)
	}
	v.requireNoTargetGroupRows(t)
	if got := v.liveGroups(t, v.control); got != controlBefore {
		t.Errorf("control organization: %d live group(s) after the delete, want %d (untouched)", got, controlBefore)
	}
}

// A refresh that read the current table before a delete of the group tables
// can append the organization's rows after that delete. A real refresh cannot
// be timed to read before and write after a synchronous delete, and a refresh
// started after the source deletes appends nothing (no source row and no live
// group row of the organization is left to mark gone), so the cell plants the
// row such a refresh appends, once, right after the second delete of the
// current table (pass 0 on a purge without a second delete). The read-back
// finds it and deletes again.
func TestOrgDeletionDeletesGroupRowsWrittenAfterTheSecondDelete(t *testing.T) {
	v := startRefreshFedVenue(t)
	if err := plantGroupRow(v.ctx, v.conn, v.target); err != nil {
		t.Fatal(err)
	}
	if got := v.rows(t, "project_team_ownership_groups_current", v.target); got != 1 {
		t.Fatalf("seed: the current table holds %d row(s) of the organization, want 1", got)
	}

	record := &hookRecord{}
	var once sync.Once
	restore := admin.SetOrgDeletionDeleteHook(func(ctx context.Context, table string, pass int) {
		if table != "project_team_ownership_groups_current" {
			return
		}
		record.pass(pass)
		if pass != 0 && pass != 2 {
			return
		}
		once.Do(func() {
			if err := plantGroupRow(ctx, v.conn, v.target); err != nil {
				record.fail("plant after pass %d: %v", pass, err)
			}
		})
	})
	t.Cleanup(restore)

	response := v.deleteTarget(t)
	restore()
	record.check(t)
	if response.Status != http.StatusOK {
		t.Fatalf("delete org: status %d, want 200\nbody: %s", response.Status, response.Body)
	}
	v.requireNoTargetGroupRows(t)
	if passes := record.seen(); len(passes) == 0 || passes[len(passes)-1] != 3 {
		t.Errorf("the current table was deleted on passes %v, want a last pass 3 (the read-back found the planted row)", passes)
	}
}

// Rows that come back after every delete are never left silently: after the
// last pass the delete reports the purge incomplete.
func TestOrgDeletionReportsGroupRowsThatKeepComingBack(t *testing.T) {
	v := startRefreshFedVenue(t)
	if err := plantGroupRow(v.ctx, v.conn, v.target); err != nil {
		t.Fatal(err)
	}

	record := &hookRecord{}
	restore := admin.SetOrgDeletionDeleteHook(func(ctx context.Context, table string, pass int) {
		if table != "project_team_ownership_groups_current" {
			return
		}
		record.pass(pass)
		if err := plantGroupRow(ctx, v.conn, v.target); err != nil {
			record.fail("plant after pass %d: %v", pass, err)
		}
	})
	t.Cleanup(restore)

	response := v.deleteTarget(t)
	restore()
	record.check(t)
	if response.Status != http.StatusInternalServerError {
		t.Fatalf("delete org: status %d, want 500 (the purge is incomplete)\nbody: %s", response.Status, response.Body)
	}
	if got, want := fmt.Sprint(record.seen()), "[1 2 3 4]"; got != want {
		t.Errorf("the current table was deleted on passes %s, want %s (one delete, then three passes)", got, want)
	}
}
