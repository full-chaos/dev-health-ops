//go:build integration

package syncadmin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestSyncConfigDeleteVenueOracle sends the same DELETE sequence to the
// real Python api and the Go api, over two copies of one seeded database,
// and requires byte-identical answers and then identical rows, as raw
// text, in sync_configurations and in every table whose foreign key
// references it (found from the catalog, not listed by hand). The deletes
// reach: a planner config with its integration, scheduled jobs, schedule
// occurrences, backfill jobs and coverage projections; a legacy parent
// whose children include a grandchild and a child in another org, and a
// child over that grandchild (both refused: the Python ORM delete raises
// on any config with children); the grandchild itself; another org's
// config from both sides; an id spelling uuid.UUID() reads; and a second
// delete of a deleted config.
func TestSyncConfigDeleteVenueOracle(t *testing.T) {
	resetIDs(t)
	golden := venueoracle.OpenGolden(t, goldenSpec(t.Name()))
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-admin-reads-32b!"
	ids := newVenueIDs()
	grandchild, foreignParent, sameNameGitHub := newID(), newID(), newID()
	t.Setenv("HIDE_MIGRATED_CHILD_CONFIGS", " On ")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      golden.PythonRoot(t, root),
		JWTKey:    jwtKey,
		PythonEnv: []string{"HIDE_MIGRATED_CHILD_CONFIGS= On "},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedSyncAdmin(t, ctx, admin, ids)
			seedDeleteReach(t, ctx, admin, ids, grandchild, foreignParent, sameNameGitHub)
			return map[string]map[string]any{
				"adminA":  {"user_id": ids.adminA.String(), "email": "admin-a@example.com", "org_id": ids.orgA.String(), "role": "admin"},
				"memberA": {"user_id": ids.memberA.String(), "email": "member-a@example.com", "org_id": ids.orgA.String(), "role": "member"},
				"ownerB":  {"user_id": ids.ownerB.String(), "email": "owner-b@example.com", "org_id": ids.orgB.String(), "role": "owner"},
			}
		},
	})
	base := startGoServer(t, ctx, venue, jwtKey)

	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	a, b := bearer("adminA"), bearer("ownerB")
	del := func(name, id string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "DELETE", Path: "/api/v1/admin/sync-configs/" + id, Headers: headers}
	}
	requests := []venueoracle.Request{
		del("no token", ids.cfgPlanner.String(), nil),
		del("member", ids.cfgPlanner.String(), bearer("memberA")),
		del("other org's config", ids.cfgB.String(), a),
		del("unknown id", newID().String(), a),
		del("not a uuid", "not-a-uuid", a),
		del("planner config", ids.cfgPlanner.String(), a),
		del("planner config again", ids.cfgPlanner.String(), a),
		del("legacy parent with children", ids.cfgLegacyParent.String(), a),
		del("child with a child", ids.child1.String(), a),
		del("parent of another org's child only", foreignParent.String(), a),
		del("grandchild", grandchild.String(), a),
		del("one of two same-name configs", sameNameGitHub.String(), a),
		del("child after its child is gone", ids.child2.String(), a),
		del("upper-case braced id", "{"+strings.ToUpper(ids.cfgSingleRepo.String())+"}", a),
		del("sourced config", ids.childSourced.String(), a),
		del("owner of the other org", ids.cfgB.String(), b),
		{Name: "list after", Method: "GET", Path: "/api/v1/admin/sync-configs?include_migrated=true", Headers: a},
	}
	python := golden.Python(t, venue, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{Golden: golden})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	tables := strings.Split(venueoracle.TableRows(t, ctx, source, `SELECT string_agg(conrelid::regclass::text, ',' ORDER BY conrelid::regclass::text)
FROM pg_constraint WHERE contype = 'f' AND confrelid = 'sync_configurations'::regclass`), ",")
	for _, want := range []string{"backfill_jobs", "scheduled_jobs", "scheduled_sync_occurrences", "sync_configurations", "sync_coverage_projections"} {
		if !contains(tables, want) {
			t.Fatalf("referencing tables %v: %s missing", tables, want)
		}
	}
	for _, table := range tables {
		query := fmt.Sprintf(`SELECT coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '') FROM %s t`, table)
		goRows := venueoracle.TableRows(t, ctx, goDB, query)
		golden.CompareRows(t, "rows:"+table, func() string { return venueoracle.TableRows(t, ctx, source, query) }, goRows)
		t.Logf("%s: %s rows identical", table, venueoracle.TableRows(t, ctx, goDB, "SELECT count(*) FROM "+table))
	}
	// The deletes happened, and the refused ones did not: the deleted
	// configs and their cascaded rows are gone from the Go copy; a config
	// with children (the legacy parent, child1 over the grandchild) is
	// refused whole, so it and its children remain.
	remaining := venueoracle.TableRows(t, ctx, goDB, fmt.Sprintf(`SELECT
  (SELECT count(*) FROM sync_configurations WHERE id IN ('%s', '%s', '%s', '%s', '%s', '%s')),
  (SELECT count(*) FROM backfill_jobs WHERE sync_config_id = '%s'),
  (SELECT count(*) FROM sync_coverage_projections WHERE sync_config_id = '%s'),
  (SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = '%s'),
  (SELECT count(*) FROM scheduled_jobs WHERE id = '%s' AND sync_config_id IS NULL),
  (SELECT count(*) FROM sync_configurations WHERE id IN ('%s', '%s', '%s', '%s')
     OR (org_id = '%s' AND name = 'same-name' AND provider = 'gitlab'))`,
		ids.cfgPlanner, grandchild, ids.cfgSingleRepo, ids.childSourced, ids.cfgB, sameNameGitHub,
		ids.cfgPlanner, ids.cfgPlanner, ids.cfgPlanner, ids.jobSync,
		ids.cfgLegacyParent, ids.child1, foreignParent, ids.cfgInactive, ids.orgA))
	if remaining != "0 0 0 0 1 5" {
		t.Errorf("after the deletes (deleted configs, backfill, projections, occurrences, jobs set null, refused and untouched configs) = %s, want 0 0 0 0 1 5", remaining)
	}
	golden.Finish(t)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// seedDeleteReach adds the rows only a delete reaches: a grandchild under
// a legacy child, and schedule occurrences of the planner config.
func seedDeleteReach(t *testing.T, ctx context.Context, admin *pgxpool.Pool, ids venueIDs, grandchild, foreignParent, sameNameGitHub uuid.UUID) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
parent_id, created_at, updated_at) VALUES ($1, $2, 'grandchild', 'gitlab', '["git"]', '{}', true, false, $3,
'2026-01-01 00:00:30+00', '2026-01-01 00:00:30+00')`, grandchild, ids.orgA.String(), ids.child1)
	// An org A config whose only child is in org B: the Python children
	// relationship joins on parent_id alone.
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
created_at, updated_at) VALUES ($1, $2, 'foreign-parent', 'github', '["git"]', '{}', true, false,
'2026-01-01 00:00:31+00', '2026-01-01 00:00:31+00')`, foreignParent, ids.orgA.String())
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
parent_id, created_at, updated_at) VALUES ($1, $2, 'foreign-parent-child', 'github', '["git"]', '{}', true, false, $3,
'2026-01-01 00:00:32+00', '2026-01-01 00:00:32+00')`, newID(), ids.orgB.String(), foreignParent)
	// Two org A configs named alike, one per provider: the delete is by
	// (name, provider), so only the addressed one goes.
	for index, provider := range []string{"github", "gitlab"} {
		id := newID()
		if provider == "github" {
			id = sameNameGitHub
		}
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
created_at, updated_at) VALUES ($1, $2, 'same-name', $3, '["git"]', '{}', true, false, $4::timestamptz, $4::timestamptz)`,
			id, ids.orgA.String(), provider, fmt.Sprintf("2026-01-01 00:00:4%d+00", index))
	}
	for index, at := range []string{"2026-06-01 00:00:00+00", "2026-06-01 01:00:00+00"} {
		exec(`INSERT INTO scheduled_sync_occurrences (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id,
scheduled_for, reconcile_status, created_at) VALUES ($1, 'v1', $2, $3, $4, $5::timestamptz, 'pending', $5::timestamptz)`,
			fmt.Sprintf("occ-%d", index), ids.orgA.String(), ids.cfgPlanner, ids.jobSync, at)
	}
}
