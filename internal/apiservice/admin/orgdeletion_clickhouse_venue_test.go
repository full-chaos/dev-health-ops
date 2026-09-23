//go:build integration

package admin_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgDeletionPurgesEveryDiscoveredClickHouseTable is CHAOS-6306
// approval condition 1's OTHER half (ruling 26): ground truth for this
// destructive port is the venue, not org_deletion.py's regex. It seeds a
// real, migrated ClickHouse (chschema.Apply, the actual migration chain)
// with rows for a target org and a control org in git_blame -- which also
// exercises git_blame_dirty_paths_mv's own MATERIALIZED VIEW trigger, the
// one case this route's live table discovery treats specially -- runs a
// REAL (non-dry-run) DELETE /orgs/{org_id} against the real Go route, then
// asks DiscoverClickHouseOrgTables (the exact function the route itself
// calls, never a second hand-authored table list) for every table it
// would purge and asserts the target org has ZERO rows in every one of
// them, while the control org's own rows survive.
func TestOrgDeletionPurgesEveryDiscoveredClickHouseTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-deletion-ch-flow-32-byte"

	chInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = chInstance.Close(context.Background()) })
	chschema.Apply(ctx, t, chInstance)

	targetOrgID := uuid.New()
	controlOrgID := uuid.New()
	superID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := adminPool.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for _, org := range []uuid.UUID{targetOrgID, controlOrgID} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "venue-chdel-"+org.String()[:8])
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-chdel-super@example.com', true, true, true, 0, now(), now())`, superID)
			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "venue-chdel-super@example.com", "is_superuser": true},
			}
		},
	})

	chConn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(chInstance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer chConn.Close()

	targetRepoID := uuid.New()
	controlRepoID := uuid.New()
	for _, row := range []struct {
		org, repo uuid.UUID
	}{
		{targetOrgID, targetRepoID},
		{controlOrgID, controlRepoID},
	} {
		if err := chConn.Exec(ctx,
			`INSERT INTO git_blame (org_id, repo_id, path, line_no, author_email, last_synced) VALUES (?, ?, 'main.go', 1, 'a@example.com', now64(3,'UTC'))`,
			row.org.String(), row.repo); err != nil {
			t.Fatalf("seed git_blame: %v", err)
		}
	}

	countGitBlame := func(orgID uuid.UUID) uint64 {
		t.Helper()
		var count uint64
		if err := chConn.QueryRow(ctx, `SELECT count() FROM git_blame WHERE org_id = ?`, orgID.String()).Scan(&count); err != nil {
			t.Fatalf("count git_blame: %v", err)
		}
		return count
	}
	countDirtyPaths := func(orgID uuid.UUID) uint64 {
		t.Helper()
		var count uint64
		if err := chConn.QueryRow(ctx, `SELECT count() FROM git_blame_dirty_paths WHERE org_id = ?`, orgID.String()).Scan(&count); err != nil {
			t.Fatalf("count git_blame_dirty_paths: %v", err)
		}
		return count
	}
	if got := countGitBlame(targetOrgID); got != 1 {
		t.Fatalf("seed: git_blame count for target org = %d, want 1", got)
	}
	if got := countDirtyPaths(targetOrgID); got != 1 {
		t.Fatalf("seed: the git_blame_dirty_paths_mv trigger did not fire -- git_blame_dirty_paths count for target org = %d, want 1", got)
	}

	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Probes.ClickHouseDSN = chInstance.URI
	})

	request := venueoracle.Request{
		Name: "delete org real (clickhouse ground truth)", Method: "DELETE",
		Path:    "/api/v1/admin/orgs/" + targetOrgID.String(),
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"]},
	}
	response := venueoracle.Do(t, goBase, request)
	if response.Status != 200 {
		t.Fatalf("delete org: status = %d, want 200\nbody: %s", response.Status, response.Body)
	}

	if got := countGitBlame(targetOrgID); got != 0 {
		t.Errorf("git_blame count for target org after delete = %d, want 0", got)
	}
	if got := countDirtyPaths(targetOrgID); got != 0 {
		t.Errorf("git_blame_dirty_paths count for target org after delete = %d, want 0 (the org-deletion route deletes this table directly by its own org_id column, not through the excluded git_blame_dirty_paths_mv)", got)
	}
	if got := countGitBlame(controlOrgID); got != 1 {
		t.Errorf("git_blame count for control org after delete = %d, want 1 (untouched)", got)
	}
	if got := countDirtyPaths(controlOrgID); got != 1 {
		t.Errorf("git_blame_dirty_paths count for control org after delete = %d, want 1 (untouched)", got)
	}

	// Ground truth, not a sample: ask the route's own discovery function
	// for every table it purges and assert zero rows for the target org
	// in EVERY one of them, and that the control org still has SOME data
	// left standing in the wider database (proving the assertion loop
	// below is not vacuously true because nothing exists anywhere).
	tables, err := admin.DiscoverClickHouseOrgTables(ctx, chConn)
	if err != nil {
		t.Fatalf("DiscoverClickHouseOrgTables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("DiscoverClickHouseOrgTables returned zero tables -- the migration chain likely did not apply")
	}
	var nonZero []string
	for _, table := range tables {
		condition := "org_id = ?"
		var bind any = targetOrgID.String()
		if admin.IsUUIDColumnType(table.OrgIDType) {
			condition = "org_id = toUUID(?)"
		}
		var count uint64
		if err := chConn.QueryRow(ctx, "SELECT count() FROM `"+table.Name+"` WHERE "+condition, bind).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table.Name, err)
		}
		if count != 0 {
			nonZero = append(nonZero, table.Name)
		}
	}
	if len(nonZero) > 0 {
		t.Errorf("target org still has rows after delete in: %v", nonZero)
	}
	if got := countGitBlame(controlOrgID); got == 0 {
		t.Fatal("control org's own git_blame row is gone too -- the zero-rows-everywhere check above would be vacuous proof; the delete over-scoped")
	}
}
