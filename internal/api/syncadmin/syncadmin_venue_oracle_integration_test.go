//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueIDs are the seeded rows' ids, fixed per run so requests can name them.
type venueIDs struct {
	orgA, orgB, orgC                                            uuid.UUID
	adminA, memberA, ownerB, adminC, adminNoOrg, superNoOrg     uuid.UUID
	credA                                                       uuid.UUID
	intA, intEmpty, intB                                        uuid.UUID
	src1, src2, src3, src4, src5, src6, srcB                    uuid.UUID
	cfgPlanner, cfgLegacyParent, cfgSingleRepo, cfgWeird        uuid.UUID
	cfgDictTargets, cfgInactive, cfgNoSources, cfgCrossOrg      uuid.UUID
	child1, child2, child3, child4, child5, childSourced        uuid.UUID
	cfgB, cfgJobsBadList, cfgJobsInf                            uuid.UUID
	cfgBadTargetsInt, cfgBadTargetsNumber, cfgBadTargetsTrue    uuid.UUID
	cfgBadOptionsString, cfgBadOptionsIntKey, cfgBadOptionsTrue uuid.UUID
	cfgSurrogate                                                uuid.UUID
	jobSync, jobMetrics, jobB, jobBadList, jobInf               uuid.UUID
	runP1, runP2, runB, runBadResult, runNullResult             uuid.UUID
	runRich, runBadUnit, srcNoFull, runBadFlags                 uuid.UUID
	bfP1, bfNone, bfBad, bfB, bfEdge, bfOneDay, bfBackward      uuid.UUID
}

func newVenueIDs() venueIDs {
	var ids venueIDs
	for _, target := range []*uuid.UUID{
		&ids.orgA, &ids.orgB, &ids.orgC, &ids.adminA, &ids.memberA, &ids.ownerB, &ids.adminC, &ids.adminNoOrg,
		&ids.superNoOrg, &ids.credA, &ids.intA, &ids.intEmpty, &ids.intB, &ids.src1, &ids.src2, &ids.src3, &ids.src4,
		&ids.src5, &ids.src6, &ids.srcB, &ids.cfgPlanner, &ids.cfgLegacyParent, &ids.cfgSingleRepo, &ids.cfgWeird,
		&ids.cfgDictTargets, &ids.cfgInactive, &ids.cfgNoSources, &ids.cfgCrossOrg, &ids.child1, &ids.child2,
		&ids.child3, &ids.child4, &ids.child5, &ids.childSourced, &ids.cfgB, &ids.cfgJobsBadList, &ids.cfgJobsInf,
		&ids.cfgBadTargetsInt, &ids.cfgBadTargetsNumber, &ids.cfgBadTargetsTrue, &ids.cfgBadOptionsString,
		&ids.cfgBadOptionsIntKey, &ids.cfgBadOptionsTrue, &ids.cfgSurrogate, &ids.jobSync, &ids.jobMetrics, &ids.jobB, &ids.jobBadList,
		&ids.jobInf, &ids.runP1, &ids.runP2, &ids.runB, &ids.runBadResult, &ids.runNullResult,
		&ids.runRich, &ids.runBadUnit, &ids.srcNoFull, &ids.runBadFlags,
		&ids.bfP1, &ids.bfNone, &ids.bfBad, &ids.bfB, &ids.bfEdge, &ids.bfOneDay, &ids.bfBackward,
	} {
		*target = uuid.New()
	}
	return ids
}

// TestSyncAdminReadsVenueOracle sends every sync admin read route the same
// requests on the real Python api and the Go api, over two copies of one
// seeded database, and requires byte-identical answers. The seed covers the
// stored-shape domain each route converts (JSON column shapes Python's
// list()/dict()/int() accept or refuse, planner and legacy repository
// selections, planner job runs with and without a linked sync run, backfill
// jobs with every sync_run marker shape) and the auth and query domains.
func TestSyncAdminReadsVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-admin-reads-32b!"
	ids := newVenueIDs()
	// Both planes read HIDE_MIGRATED_CHILD_CONFIGS per request; the venue
	// runs with it on, so include_migrated decides the child rows.
	t.Setenv("HIDE_MIGRATED_CHILD_CONFIGS", " On ")
	// The run-units freshness judges lag against the HEAVY window cap both
	// planes resolve from these: a 5-day cap, a one-hour overlap.
	t.Setenv("SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS", "5")
	t.Setenv("SYNC_WATERMARK_OVERLAP", "3600")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    jwtKey,
		PythonEnv: []string{"HIDE_MIGRATED_CHILD_CONFIGS= On ", "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS=5", "SYNC_WATERMARK_OVERLAP=3600"},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedSyncAdmin(t, ctx, admin, ids)
			seedBackfillDiagnostics(t, ctx, venue, ids)
			return map[string]map[string]any{
				"adminA":     {"user_id": ids.adminA.String(), "email": "admin-a@example.com", "org_id": ids.orgA.String(), "role": "admin"},
				"memberA":    {"user_id": ids.memberA.String(), "email": "member-a@example.com", "org_id": ids.orgA.String(), "role": "member"},
				"ownerB":     {"user_id": ids.ownerB.String(), "email": "owner-b@example.com", "org_id": ids.orgB.String(), "role": "owner"},
				"adminC":     {"user_id": ids.adminC.String(), "email": "admin-c@example.com", "org_id": ids.orgC.String(), "role": "admin"},
				"adminNoOrg": {"user_id": ids.adminNoOrg.String(), "email": "admin-noorg@example.com", "org_id": "", "role": "admin"},
				"superNoOrg": {"user_id": ids.superNoOrg.String(), "email": "super-noorg@example.com", "org_id": "", "role": "member", "is_superuser": true},
			}
		},
	})
	base := startGoServer(t, ctx, venue, jwtKey)

	requests := syncAdminRequests(venue, ids)
	python := venue.ServePython(t, requests)
	inspectDiagnostics := inspectBackfillDiagnostics(t)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		// lag_seconds is now minus the watermark at request time, and the
		// two planes answer minutes apart: the digits are blanked on both.
		// catching_up and ticks_behind are compared (every seeded
		// watermark sits mid-tick or in the future), and the arithmetic is
		// pinned exactly by the live model oracle at a fixed now.
		Normalize: func(request venueoracle.Request, body string) string {
			if !strings.HasSuffix(strings.SplitN(request.Path, "?", 2)[0], "/units") {
				return body
			}
			return lagSeconds.ReplaceAllString(body, `"lag_seconds":"<volatile>"`)
		},
		// The seed must reach every freshness state it was built for, or a
		// SAME on the rich run could be two empty lists agreeing.
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			inspectDiagnostics(request, goResponse)
			if request.Name != "run units rich " {
				return
			}
			for _, want := range []string{
				`"catching_up_dataset_count":2,`, `"dataset_key":"commit-stats","cost_class":"heavy",`, `"catching_up":true,"ticks_behind":11,"window_cap_days":5`, `"ticks_behind":81,`, `"source_name":"acme/zero","dataset_key":"tests"`,
				`"dataset_key":"work-item-history","cost_class":"medium",`, `"lag_seconds":0,`,
				`"dataset_key":"work-item-labels","cost_class":"light","watermark_at":"`,
				`"dataset_key":"files","cost_class":"heavy","watermark_at":null,"lag_seconds":null`,
				`"retry_exhausted_unit_count":2,"budget_blocked_unit_count":1,`, `"unit_count":9,`,
			} {
				if !strings.Contains(goResponse.Body, want) {
					t.Errorf("rich run units body lacks %s:\n%s", want, goResponse.Body)
				}
			}
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)
}

func syncAdminRequests(venue *venueoracle.Venue, ids venueIDs) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	a, b, c := bearer("adminA"), bearer("ownerB"), bearer("adminC")
	get := func(name, path string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: "/api/v1/admin" + path, Headers: headers}
	}
	var requests []venueoracle.Request
	routes := []string{
		"/sync-configs/auto-import-capabilities", "/sync-targets", "/sync-configs",
		"/sync-configs/" + ids.cfgPlanner.String(), "/sync-configs/" + ids.cfgPlanner.String() + "/repositories",
		"/sync-configs/" + ids.cfgPlanner.String() + "/jobs", "/backfill-jobs", "/sync-runs/" + ids.runP1.String(),
		"/sync-configs/" + ids.cfgPlanner.String() + "/coverage", "/sync-runs/" + ids.runRich.String() + "/units",
		"/sync-configs/" + ids.cfgPlanner.String() + "/coverage", "/backfill-jobs/" + ids.bfP1.String(),
	}
	// The guard domain, on every route: no credential, a malformed and a
	// wrong-key credential, a member, an admin and a superuser without an
	// org claim, and a malformed query next to a refused caller (the
	// dependency answers before the query is validated).
	for _, path := range routes {
		requests = append(requests,
			get("no token "+path, path, nil),
			get("bad scheme "+path, path, map[string]string{"Authorization": "Basic abc"}),
			get("garbage token "+path, path, map[string]string{"Authorization": "Bearer not.a.jwt"}),
			get("member "+path, path, bearer("memberA")),
			get("admin without org "+path, path, bearer("adminNoOrg")),
			get("superuser without org "+path, path, bearer("superNoOrg")),
			get("member bad query "+path, path+"?limit=x&active_only=maybe", bearer("memberA")),
		)
	}

	requests = append(requests,
		get("capabilities", "/sync-configs/auto-import-capabilities", a),
		get("capabilities trailing slash", "/sync-configs/auto-import-capabilities/", a),
		get("sync targets feature closed", "/sync-targets", a),
		get("sync targets feature open", "/sync-targets", b),
		get("sync targets no override", "/sync-targets", c),
	)

	// list_sync_configs: every bool query shape, the three-error 422, a
	// repeated key (the last wins), and the HIDE_MIGRATED filter both ways.
	for _, query := range []string{
		"", "?active_only=true", "?active_only=FALSE", "?parent_only=1", "?parent_only=off", "?include_migrated=yes",
		"?include_migrated=true&parent_only=true", "?active_only=true&include_migrated=on",
		"?active_only=maybe", "?active_only=&parent_only=2&include_migrated=nope", "?active_only=false&active_only=true",
		"?active_only=%20true%20", "?unknown=1",
	} {
		requests = append(requests, get("list configs "+query, "/sync-configs"+query, a))
	}
	requests = append(requests,
		get("list configs other org", "/sync-configs", b),
		get("list configs unrenderable org", "/sync-configs", c),
		get("list configs unrenderable org include migrated", "/sync-configs?include_migrated=true", c),
		get("list configs trailing slash", "/sync-configs/", a),
	)

	configIDs := map[string]uuid.UUID{
		"planner": ids.cfgPlanner, "legacy parent": ids.cfgLegacyParent, "single repo": ids.cfgSingleRepo,
		"weird shapes": ids.cfgWeird, "dict targets": ids.cfgDictTargets, "inactive": ids.cfgInactive,
		"no sources": ids.cfgNoSources, "cross org integration": ids.cfgCrossOrg, "child 1": ids.child1,
		"child 5": ids.child5, "child with source": ids.childSourced,
	}
	badIDs := map[string]uuid.UUID{
		"targets int items": ids.cfgBadTargetsInt, "targets number": ids.cfgBadTargetsNumber,
		"targets true": ids.cfgBadTargetsTrue, "options string": ids.cfgBadOptionsString,
		"options int key": ids.cfgBadOptionsIntKey, "options true": ids.cfgBadOptionsTrue,
		// Renders in Go and in Python, and then neither can serialize it:
		// both answer the unhandled-exception 500 (the repository selection
		// never writes the options, so it answers 200).
		"options lone surrogate": ids.cfgSurrogate,
	}
	for _, name := range sortedKeys(configIDs) {
		id := configIDs[name]
		requests = append(requests,
			get("get config "+name, "/sync-configs/"+id.String(), a),
			get("repositories "+name, "/sync-configs/"+id.String()+"/repositories", a),
		)
	}
	for _, name := range sortedKeys(badIDs) {
		id := badIDs[name]
		requests = append(requests,
			get("get unrenderable config "+name, "/sync-configs/"+id.String(), c),
			get("repositories unrenderable config "+name, "/sync-configs/"+id.String()+"/repositories", c),
		)
	}
	// The config_id domain: Python's uuid.UUID() spellings, a non-UUID, an
	// unknown UUID, another org's config.
	upper := strings.ToUpper(ids.cfgPlanner.String())
	hex32 := strings.ReplaceAll(ids.cfgPlanner.String(), "-", "")
	for name, raw := range map[string]string{
		"uppercase": upper, "braces": "{" + ids.cfgPlanner.String() + "}", "urn": "urn:uuid:" + ids.cfgPlanner.String(),
		"hex32": hex32, "not a uuid": "nope", "unknown": uuid.NewString(), "other org": ids.cfgB.String(),
		"percent-encoded space": "%20" + ids.cfgPlanner.String(),
	} {
		requests = append(requests,
			get("get config id "+name, "/sync-configs/"+raw, a),
			get("repositories id "+name, "/sync-configs/"+raw+"/repositories", a),
			get("jobs id "+name, "/sync-configs/"+raw+"/jobs", a),
		)
	}

	// list_sync_config_jobs: the page domain and the job-run shapes.
	for _, query := range []string{
		"", "?limit=1", "?limit=200", "?limit=2&offset=1", "?offset=3", "?offset=100",
		"?limit=0", "?limit=201", "?offset=-1", "?limit=abc", "?limit=1.0", "?limit=1.5", "?limit=", "?limit=%201%20",
		"?limit=0&offset=-1", "?limit=0&limit=3", "?limit=1_0", "?offset=99999999999999999999",
	} {
		requests = append(requests, get("jobs planner "+query, "/sync-configs/"+ids.cfgPlanner.String()+"/jobs"+query, a))
	}
	requests = append(requests,
		get("jobs no scheduled job", "/sync-configs/"+ids.cfgSingleRepo.String()+"/jobs", a),
		get("jobs no scheduled job huge offset", "/sync-configs/"+ids.cfgSingleRepo.String()+"/jobs?offset=99999999999999999999", a),
		get("jobs unrenderable list result", "/sync-configs/"+ids.cfgJobsBadList.String()+"/jobs", a),
		get("jobs infinite items", "/sync-configs/"+ids.cfgJobsInf.String()+"/jobs", a),
		get("jobs bad limit unknown config", "/sync-configs/"+uuid.NewString()+"/jobs?limit=0", a),
	)

	// list_backfill_jobs: the page domain and every sync_run marker shape.
	for _, query := range []string{
		"", "?limit=1", "?limit=2&offset=2", "?offset=50", "?limit=0", "?limit=201", "?offset=-1", "?limit=x&offset=y",
		"?offset=99999999999999999999",
	} {
		requests = append(requests, get("backfill jobs "+query, "/backfill-jobs"+query, a))
	}
	requests = append(requests, get("backfill jobs other org", "/backfill-jobs", b), get("backfill jobs empty org", "/backfill-jobs", c))

	// get_backfill_job: every sync_run marker shape, the diagnostics windows
	// over the seeded ClickHouse rows, another org's job, an unknown id,
	// and id spellings uuid.UUID() reads or refuses (a refused one is the
	// api's unhandled 500).
	for name, id := range map[string]uuid.UUID{
		"linked run": ids.bfP1, "no run": ids.bfNone, "bad marker": ids.bfBad, "month edge": ids.bfEdge,
		"one day": ids.bfOneDay, "backward window": ids.bfBackward, "other org": ids.bfB, "unknown": uuid.New(),
	} {
		requests = append(requests, get("backfill job "+name, "/backfill-jobs/"+id.String(), a))
	}
	for _, spelling := range []string{
		strings.ToUpper(ids.bfP1.String()), "{" + ids.bfP1.String() + "}", "urn:uuid:" + ids.bfP1.String(),
		strings.ReplaceAll(ids.bfP1.String(), "-", ""), "not-a-uuid", "%20" + ids.bfP1.String(),
	} {
		requests = append(requests, get("backfill job spelling "+spelling, "/backfill-jobs/"+spelling, a))
	}
	requests = append(requests, get("backfill job as other org", "/backfill-jobs/"+ids.bfB.String(), b),
		get("backfill job member", "/backfill-jobs/"+ids.bfP1.String(), bearer("memberA")))

	// get_sync_run: found, another org's, unknown, spellings, a result the
	// response model refuses, a null result.
	for name, raw := range map[string]string{
		"planner run": ids.runP1.String(), "idle run": ids.runP2.String(), "other org": ids.runB.String(),
		"unknown": uuid.NewString(), "not a uuid": "zzz", "braces": "{" + ids.runP1.String() + "}",
		"uppercase": strings.ToUpper(ids.runP1.String()), "unrenderable result": ids.runBadResult.String(),
		"null result": ids.runNullResult.String(),
	} {
		requests = append(requests, get("sync run "+name, "/sync-runs/"+raw, a))
	}
	requests = append(requests, get("sync run trailing slash", "/sync-runs/"+ids.runP1.String()+"/", a))

	// get_sync_run_units: the rich run under every limit shape, other
	// runs, another org's run, unknown and malformed ids, a unit the
	// response model refuses, and a bad limit next to an unknown run (the
	// query is validated first).
	for _, query := range []string{"", "?limit=2", "?limit=0", "?limit=-1", "?limit=-100", "?limit=99999999999999999999",
		"?limit=x", "?limit=", "?limit=1.0", "?limit=2&limit=3", "?limit=%201%20"} {
		requests = append(requests, get("run units rich "+query, "/sync-runs/"+ids.runRich.String()+"/units"+query, a))
	}
	for name, raw := range map[string]string{
		"planner run": ids.runP1.String(), "idle run": ids.runP2.String(), "other org": ids.runB.String(),
		"unknown": uuid.NewString(), "not a uuid": "zzz", "uppercase": strings.ToUpper(ids.runRich.String()),
		"refused unit":             ids.runBadUnit.String(),
		"list-shaped family flags": ids.runBadFlags.String(),
	} {
		requests = append(requests, get("run units "+name, "/sync-runs/"+raw+"/units", a))
	}
	requests = append(requests,
		get("run units bad limit unknown run", "/sync-runs/"+uuid.NewString()+"/units?limit=x", a),
		get("run units other org caller", "/sync-runs/"+ids.runB.String()+"/units", b),
	)

	// get_sync_config_coverage: a fresh and a refreshing projection, a
	// payload stored as pairs, rows at another version, lookback or org
	// (pending), payloads the response model refuses, another org's config,
	// an unknown config and a path that is not a uuid.
	for name, raw := range map[string]string{
		"fresh": ids.cfgPlanner.String(), "refreshing": ids.cfgLegacyParent.String(), "pairs": ids.cfgWeird.String(),
		"offsets": ids.cfgJobsInf.String(), "old version": ids.cfgSingleRepo.String(),
		"other lookback": ids.cfgNoSources.String(), "row in other org": ids.cfgCrossOrg.String(),
		"refused status": ids.cfgInactive.String(), "null payload": ids.cfgDictTargets.String(),
		"no overall": ids.cfgJobsBadList.String(), "other org config": ids.cfgB.String(),
		"unknown": uuid.NewString(), "not a uuid": "zzz", "uppercase": strings.ToUpper(ids.cfgPlanner.String()),
		"basic and week boundaries": ids.child1.String(),
	} {
		requests = append(requests, get("coverage "+name, "/sync-configs/"+raw+"/coverage", a))
	}
	requests = append(requests, get("coverage other org caller", "/sync-configs/"+ids.cfgB.String()+"/coverage", b))
	return requests
}

var lagSeconds = regexp.MustCompile(`"lag_seconds":[0-9]+`)

func sortedKeys(values map[string]uuid.UUID) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// seedSyncAdmin writes the venue's rows. Timestamps are distinct and carry
// microseconds where an order or a rendering depends on them.
func seedSyncAdmin(t *testing.T, ctx context.Context, admin *pgxpool.Pool, ids venueIDs) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	for _, org := range []struct {
		id   uuid.UUID
		slug string
	}{{ids.orgA, "sync-a"}, {ids.orgB, "sync-b"}, {ids.orgC, "sync-c"}} {
		exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
VALUES ($1, $2, $2, '{}', 'community', true, now(), now())`, org.id, org.slug)
	}
	for _, user := range []struct {
		id    uuid.UUID
		email string
		super bool
	}{
		{ids.adminA, "admin-a@example.com", false}, {ids.memberA, "member-a@example.com", false},
		{ids.ownerB, "owner-b@example.com", false}, {ids.adminC, "admin-c@example.com", false},
		{ids.adminNoOrg, "admin-noorg@example.com", false}, {ids.superNoOrg, "super-noorg@example.com", true},
	} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, user.id, user.email, user.super)
	}
	for _, member := range []struct {
		org, user uuid.UUID
		role      string
	}{{ids.orgA, ids.adminA, "admin"}, {ids.orgA, ids.memberA, "member"}, {ids.orgB, ids.ownerB, "owner"}, {ids.orgC, ids.adminC, "admin"}} {
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, now(), now())`,
			uuid.New(), member.user, member.org, member.role)
	}
	// canonical_incident_ingestion is registered and enabled by the
	// migrations; an org override decides each org: A closed, B open, C
	// neither (the tier decides).
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, false, now(), now() FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, uuid.New(), ids.orgA)
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, true, now(), now() FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, uuid.New(), ids.orgB)

	exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', 'cred-a', true, now(), now())`, ids.credA, ids.orgA.String())
	integration := func(id uuid.UUID, org uuid.UUID, provider string, credential any) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}', true, now(), now())`, id, org.String(), provider, credential, "int-"+id.String()[:8])
	}
	integration(ids.intA, ids.orgA, "github", ids.credA)
	integration(ids.intEmpty, ids.orgA, "github", nil)
	integration(ids.intB, ids.orgB, "github", nil)

	source := func(id uuid.UUID, org uuid.UUID, integrationID uuid.UUID, provider, externalID, fullName string, enabled bool, metadata string) {
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
metadata, is_enabled, discovered_at, last_seen_at) VALUES ($1, $2, $3, $4, 'repository', $5, $5, $6, $7::json, $8, now(), now())`,
			id, org.String(), integrationID, provider, externalID, fullName, metadata, enabled)
	}
	planner := `{"planner_managed_sync_config_id": "` + ids.cfgPlanner.String() + `"}`
	source(ids.src1, ids.orgA, ids.intA, "github", "acme/one", "acme/one", true, planner)
	source(ids.src2, ids.orgA, ids.intA, "github", "acme/two", "acme/two", false, planner)
	source(ids.src3, ids.orgA, ids.intA, "github", "acme/three", "acme/three", true, `{"planner_managed_sync_config_id": "`+uuid.NewString()+`"}`)
	source(ids.src4, ids.orgA, ids.intA, "GitHub", "acme/four", "acme/four", true, planner)
	source(ids.src5, ids.orgA, ids.intA, "github", "acme/five", "acme/five", true, `[["planner_managed_sync_config_id", "`+ids.cfgPlanner.String()+`"]]`)
	source(ids.src6, ids.orgA, ids.intA, "github", "acme/six", "acme/six", true, `[[1, 2], "ab"]`)
	source(ids.srcB, ids.orgB, ids.intA, "github", "acme/b", "acme/b", true, planner)

	config := func(id uuid.UUID, org uuid.UUID, name, provider, targets, options string, active bool, parent, integrationID, sourceID any, lastSync string, success any, lastError any, created string) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
parent_id, integration_id, source_id, last_sync_at, last_sync_success, last_sync_error, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, $6::json, $7, $8, $9, $10, $11, NULLIF($12, '')::timestamptz, $13, $14, $15::timestamptz, $15::timestamptz + interval '1 hour')`,
			id, org.String(), name, provider, targets, options, active, integrationID != nil, parent, integrationID, sourceID,
			lastSync, success, lastError, created)
	}
	config(ids.cfgPlanner, ids.orgA, "planner", "github", `["git", "prs"]`,
		`{"owner": "acme", "all_repos": false, "ratio": 1.0, "big": 12345678901234567890, "tiny": 1e-7, "fx": 1e-05, "fx2": 1.5212603486793025e-05, "neg": -2.5e-08, "huge": 1e400, "e16": 1e16, "e22": 1e22, "nested": {"b": 1, "a": [1.5, "x", null, true]}, "dup": 1, "dup": 2, "uni": "café 😀"}`,
		true, nil, ids.intA, nil, "2026-03-04 05:06:07.891234+00", true, nil, "2026-01-01 00:00:00.000001+00")
	config(ids.cfgLegacyParent, ids.orgA, "legacy", "gitlab", `["git"]`, `{"group": "grp", "repo": "own"}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:01+00")
	config(ids.child1, ids.orgA, "child1", "gitlab", `["git"]`, `{"repo": "r1", "owner": "o1"}`, true, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:02+00")
	config(ids.child2, ids.orgA, "child2", "gitlab", `["git"]`, `{"project_id": 42}`, true, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:03+00")
	config(ids.child3, ids.orgA, "child3", "gitlab", `["git"]`, `{"repo": "a/b", "owner": "o"}`, true, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:04+00")
	config(ids.child4, ids.orgA, "child4", "gitlab", `["git"]`, `{"repo": "", "project_id": null}`, false, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:05+00")
	config(ids.child5, ids.orgA, "child5", "gitlab", `["git"]`, `{"repo": 0, "project_id": 7, "group": 1.5, "owner": ""}`, true, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:06+00")
	config(ids.cfgSingleRepo, ids.orgA, "single", "github", `[]`, `{"repo": "solo", "owner": "acme", "all_repos": 1}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:07+00")
	config(ids.childSourced, ids.orgA, "sourced", "github", `["git"]`, `{"repo": true, "owner": ["x", 1]}`, true, nil, ids.intA, ids.src1, "", nil, nil, "2026-01-01 00:00:08+00")
	config(ids.cfgWeird, ids.orgA, "weird", "linear", `"abc"`, `[["k", "v"], "xy", {"p": 1, "q": 2}, ["k", 3.5e-06]]`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:09+00")
	config(ids.cfgDictTargets, ids.orgA, "dict-targets", "jira", `{"z": 1, "y": 2}`, `null`, true, nil, nil, nil, "2026-02-02 02:02:02+00", false, "boom", "2026-01-01 00:00:10+00")
	config(ids.cfgInactive, ids.orgA, "inactive", "jira", `[]`, `{"repo": "lonely"}`, false, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:11+00")
	config(ids.cfgNoSources, ids.orgA, "no-sources", "github", `["git"]`, `{"repo": "x/y", "owner": "ignored"}`, true, nil, ids.intEmpty, nil, "", nil, nil, "2026-01-01 00:00:12+00")
	config(ids.cfgCrossOrg, ids.orgA, "cross-org", "github", `["git"]`, `{"repo": 5, "owner": 0, "group": "g"}`, true, nil, ids.intB, nil, "", nil, nil, "2026-01-01 00:00:13+00")
	config(ids.cfgJobsBadList, ids.orgA, "jobs-bad-list", "jira", `[]`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:14+00")
	config(ids.cfgJobsInf, ids.orgA, "jobs-inf", "jira", `[]`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:15+00")
	config(ids.cfgB, ids.orgB, "planner", "github", `["git"]`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:16+00")
	// A child in another org whose parent is org A's legacy parent: the
	// children count is keyed by parent id only.
	config(uuid.New(), ids.orgB, "foreign-child", "gitlab", `["git"]`, `{}`, true, ids.cfgLegacyParent, nil, nil, "", nil, nil, "2026-01-01 00:00:17+00")
	config(ids.cfgBadTargetsInt, ids.orgC, "bad-targets-int", "github", `["git", 1]`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:18+00")
	config(ids.cfgBadTargetsNumber, ids.orgC, "bad-targets-number", "github", `5`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:19+00")
	config(ids.cfgBadTargetsTrue, ids.orgC, "bad-targets-true", "github", `true`, `{}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:20+00")
	config(ids.cfgBadOptionsString, ids.orgC, "bad-options-string", "github", `[]`, `"abc"`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:21+00")
	config(ids.cfgBadOptionsIntKey, ids.orgC, "bad-options-int-key", "github", `[]`, `[[1, 2], ["repo", "r"], ["owner", "o"]]`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:22+00")
	config(ids.cfgBadOptionsTrue, ids.orgC, "bad-options-true", "github", `[]`, `true`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:23+00")
	config(ids.cfgSurrogate, ids.orgC, "surrogate", "github", `[]`, `{"x": "\ud800"}`, true, nil, nil, nil, "", nil, nil, "2026-01-01 00:00:24+00")

	scheduled := func(id, org, configID uuid.UUID, jobType string) {
		exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id,
status, is_running, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'github', '0 * * * *', 'UTC', '{}', $5, 1, false, 0, 0, now(), now())`,
			id, org.String(), jobType+"-"+id.String()[:8], jobType, configID)
	}
	scheduled(ids.jobSync, ids.orgA, ids.cfgPlanner, "sync")
	scheduled(ids.jobMetrics, ids.orgA, ids.cfgPlanner, "metrics")
	scheduled(ids.jobB, ids.orgB, ids.cfgPlanner, "sync")
	scheduled(ids.jobBadList, ids.orgA, ids.cfgJobsBadList, "sync")
	scheduled(ids.jobInf, ids.orgA, ids.cfgJobsInf, "sync")

	syncRun := func(id uuid.UUID, org uuid.UUID, status string, total int, started, completed, result, runError any) {
		exec(`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units,
started_at, completed_at, result, error, created_at)
VALUES ($1, $2, $3, 'manual', 'incremental', $4, $5, 1, 2, $6::timestamptz, $7::timestamptz, $8::json, $9, '2026-05-05 05:05:05.5+00')`,
			id, org.String(), ids.intA, status, total, started, completed, result, runError)
	}
	syncRun(ids.runP1, ids.orgA, "running", 5, "2026-05-01 10:00:00+00", "2026-05-01 10:00:09.999999+00", `{"keep": "b", "extra": 1}`, "")
	syncRun(ids.runP2, ids.orgA, "success", 0, "2026-05-02 10:00:10+00", "2026-05-02 10:00:00+00", nil, nil)
	syncRun(ids.runB, ids.orgB, "failed", 1, nil, nil, `{}`, "other")
	syncRun(ids.runBadResult, ids.orgA, "planned", 0, nil, nil, `[1]`, nil)
	syncRun(ids.runNullResult, ids.orgA, "planned", 0, nil, nil, `null`, nil)

	unit := func(runID, sourceID uuid.UUID, status string, since, before any, updated string, heartbeat any) {
		exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
since_at, before_at, status, attempts, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'github', 'commits', 'medium', 'incremental', $6::timestamptz, $7::timestamptz, $8, 0, $9::timestamptz, now(), $10::timestamptz)`,
			uuid.New(), ids.orgA.String(), runID, ids.intA, sourceID, since, before, status, heartbeat, updated)
	}
	unit(ids.runP1, ids.src1, "success", "2026-04-01 00:00:00+00", "2026-04-02 00:00:00+00", "2026-05-01 10:00:01+00", nil)
	unit(ids.runP1, ids.src3, "success", "2026-03-30 00:00:00.5+00", "2026-04-01 00:00:00+00", "2026-05-01 10:00:02+00", "2026-05-01 11:00:00+00")
	unit(ids.runP1, ids.src2, "failed", "2026-03-01 00:00:00+00", "2026-04-09 00:00:00+00", "2026-05-01 10:00:03+00", nil)
	unit(ids.runP1, ids.src1, "planned", nil, "2026-04-10 00:00:00+00", "2026-05-01 10:00:04+00", nil)

	source(ids.srcNoFull, ids.orgA, ids.intA, "github", "acme/zero", "", true, planner)
	syncRun(ids.runRich, ids.orgA, "running", 9, "2026-05-01 10:00:00+00", nil, `{}`, nil)
	syncRun(ids.runBadUnit, ids.orgA, "running", 1, nil, nil, `{}`, nil)
	richUnit := func(runID, sourceID uuid.UUID, dataset, cost, status string, available, duration, result, flags any) {
		exec(`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
since_at, before_at, status, attempts, available_at, duration_seconds, result, processor_flags, rate_limit_deferrals,
budget_deferrals, error, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'github', $6, $7, 'incremental', '2026-04-01 00:00:00+00', '2026-04-02 00:00:00.25+00', $8, 2,
$9::timestamptz, $10, $11::json, $12::json, 1, 3, 'e', '2026-05-01 10:00:00+00', '2026-05-01 10:30:00+00')`,
			uuid.New(), ids.orgA.String(), runID, ids.intA, sourceID, dataset, cost, status, available, duration, result, flags)
	}
	richUnit(ids.runRich, ids.src1, "commits", "medium", "success", nil, 30, `{"items": 1.5, "nested": {"b": [1e-7, null]}}`, nil)
	richUnit(ids.runRich, ids.src1, "files", "heavy", "failed", nil, 90, `{"error_category": "timeout", "retry_count": "2", "retry_surfaces": ["a"]}`, nil)
	richUnit(ids.runRich, ids.src2, "work-items", "medium", "retrying", "2027-01-01 00:00:00+00", nil,
		`{"error_category": "budget_deferred", "linear_page_count": 4.0}`,
		`{"family_dataset_work_item_labels": true, "family_dataset_work_item_history": true, "family_dataset_work_item_comments": false}`)
	richUnit(ids.runRich, ids.src2, "prs", "light", "retrying", "2026-12-01 00:00:00.5+00", nil,
		`{"retry_exhausted": true, "next_retry_at": "2026-01-01T00:00:00", "retry_reason": "lease"}`, nil)
	richUnit(ids.runRich, ids.src4, "blame", "heavy", "failed", nil, 90, `{"error_category": "worker_lost_retry_exhausted", "retry_exhausted": "yes"}`, nil)
	richUnit(ids.runRich, ids.src1, "repo-metadata", "light", "success", nil, 5, `{}`, nil)
	richUnit(ids.runRich, ids.src3, "commit-stats", "heavy", "pending", nil, nil, `[1]`, nil)
	richUnit(ids.runRich, ids.src5, "files", "heavy", "success", nil, 90, `null`, nil)
	richUnit(ids.runRich, ids.srcNoFull, "tests", "heavy", "retrying", nil, 1, `{"last_lease_expired_at": 1767225600}`, `[]`)
	richUnit(ids.runBadUnit, ids.src1, "commits", "medium", "failed", nil, nil, `{"retry_count": "x"}`, nil)
	// A family unit whose processor_flags is a truthy non-object:
	// _effective_dataset_keys raises in build_dataset_freshness.
	syncRun(ids.runBadFlags, ids.orgA, "running", 1, nil, nil, `{}`, nil)
	richUnit(ids.runBadFlags, ids.src1, "work-items", "medium", "success", nil, nil, `{}`, `["x"]`)

	// Watermarks, relative to seed time so every verdict holds for the
	// minutes between the planes: the net advance is 5 days less one hour
	// (428400 s), so a watermark 10.5 advances back is catching up with 11
	// ticks behind, stable for 59 hours either way.
	watermark := func(org uuid.UUID, repoID, sourceID, target, dataset, at string) {
		exec(`INSERT INTO sync_watermarks (id, org_id, repo_id, source_id, target, dataset_key, last_synced_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, `+at+`, now())`, uuid.New(), org.String(), repoID, sourceID, target, dataset)
	}
	watermark(ids.orgA, "r-commits", "acme/one", "t-commits", "commits", "now() - interval '30 days'")
	watermark(ids.orgA, "r-files-null", "acme/one", "t-files-null", "files", "NULL")
	watermark(ids.orgA, "acme/one", "x-files", "files", "x-files", "now() - interval '1 day'")
	watermark(ids.orgA, "acme/two", "x-wi", "work-items", "work-items", "now() - interval '3 days'")
	watermark(ids.orgA, "acme/two", "x-wih", "work-item-history", "x-wih", "now() + interval '10 days'")
	watermark(ids.orgA, "r-cs", "acme/three", "t-cs", "commit-stats", "now() - 10.5 * interval '428400 seconds'")
	watermark(ids.orgA, "r-blame", "acme/four", "t-blame", "blame", "now() - interval '2 days'")
	watermark(ids.orgA, "r-files5", "acme/five", "t-files5", "files", "now() - interval '400 days'")
	watermark(ids.orgB, "r-other", "acme/one", "t-other", "commits", "now() - interval '1 day'")

	jobRun := func(jobID uuid.UUID, status int, started, completed any, duration any, result any, runError any, created string) {
		exec(`INSERT INTO job_runs (id, job_id, status, started_at, completed_at, duration_seconds, result, error, triggered_by, created_at)
VALUES ($1, $2, $3, $4::timestamptz, $5::timestamptz, $6, $7::json, $8, 'manual', $9::timestamptz)`,
			uuid.New(), jobID, status, started, completed, duration, result, runError, created)
	}
	jobRun(ids.jobSync, 2, "2026-06-01 00:00:00+00", "2026-06-01 00:00:05+00", 5, `{"items_synced": "12", "rows": 99}`, nil, "2026-06-01 00:00:01+00")
	jobRun(ids.jobSync, 7, nil, nil, nil, `{"rows": 3.9}`, "e2", "2026-06-01 00:00:02+00")
	jobRun(ids.jobSync, 4, nil, nil, nil, `{"count": true}`, nil, "2026-06-01 00:00:03+00")
	jobRun(ids.jobSync, 3, nil, nil, nil, `{"items": "1.5", "ratio": 2.5e-06, "fx": 4e-05, "inf": -1e999}`, nil, "2026-06-01 00:00:04+00")
	jobRun(ids.jobSync, 1, nil, nil, nil, `{"rows_ingested": " 1_000 "}`, nil, "2026-06-01 00:00:05+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, `{"sync_run_id": "`+ids.runP1.String()+`", "planner_managed": true, "keep": "a", "items_synced": 3}`, "jerr", "2026-06-01 00:00:06+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, `{"sync_run_id": "`+ids.runB.String()+`"}`, nil, "2026-06-01 00:00:07+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, `{"sync_run_id": "not-a-uuid", "items": 1e20}`, nil, "2026-06-01 00:00:08+00")
	jobRun(ids.jobSync, 2, "2026-06-01 00:00:00+00", nil, 1, `{"sync_run_id": "{`+strings.ToUpper(ids.runP2.String())+`}"}`, "keep", "2026-06-01 00:00:09+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, nil, nil, "2026-06-01 00:00:10+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, `{"sync_run_id": null, "items_synced": []}`, nil, "2026-06-01 00:00:11+00")
	jobRun(ids.jobSync, 0, nil, nil, nil, `{}`, nil, "2026-06-01 00:00:12.000001+00")
	jobRun(ids.jobMetrics, 2, nil, nil, nil, `{}`, nil, "2026-06-01 00:00:13+00")
	jobRun(ids.jobB, 2, nil, nil, nil, `{}`, nil, "2026-06-01 00:00:14+00")
	jobRun(ids.jobBadList, 2, nil, nil, nil, `[1, 2]`, nil, "2026-06-01 00:00:15+00")
	jobRun(ids.jobInf, 2, nil, nil, nil, `{"items_synced": 1e400}`, nil, "2026-06-01 00:00:16+00")

	window := [2]string{"2026-01-01", "2026-01-31"}
	backfill := func(id, org uuid.UUID, task any, status string, total, completed, failed int, errorMessage any, created string) {
		exec(`INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks,
completed_chunks, failed_chunks, error_message, started_at, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $11::date, $12::date, $6, $7, $8, $9, '2026-02-01 00:00:00+00', NULL, $10::timestamptz, $10::timestamptz + interval '1 second')`,
			id, org.String(), ids.cfgPlanner, task, status, total, completed, failed, errorMessage, created, window[0], window[1])
	}
	backfill(ids.bfP1, ids.orgA, "sync_run:"+ids.runP1.String(), "pending", 0, 0, 0, nil, "2026-07-01 00:00:01+00")
	backfill(ids.bfNone, ids.orgA, nil, "running", 3, 1, 0, nil, "2026-07-01 00:00:02+00")
	backfill(ids.bfBad, ids.orgA, "x sync_run:not-a-uuid", "failed", 2, 0, 2, "bad", "2026-07-01 00:00:03+00")
	backfill(uuid.New(), ids.orgA, "sync_run:", "pending", 0, 0, 0, nil, "2026-07-01 00:00:04+00")
	backfill(uuid.New(), ids.orgA, "a:sync_run:"+ids.runB.String(), "pending", 1, 1, 0, nil, "2026-07-01 00:00:05+00")
	backfill(uuid.New(), ids.orgA, "sync_run:{"+ids.runP2.String()+"}", "pending", 4, 0, 0, "old", "2026-07-01 00:00:06+00")
	backfill(uuid.New(), ids.orgA, "sync_run:"+uuid.NewString()+"sync_run:"+ids.runP1.String(), "done", 7, 7, 0, nil, "2026-07-01 00:00:07.123456+00")
	// progress_pct in the ranges where json.dumps and pydantic-core write
	// a float differently (this route has no response model: json.dumps).
	backfill(uuid.New(), ids.orgA, nil, "running", 2000000000, 1, 0, nil, "2026-07-01 00:00:08+00")
	backfill(uuid.New(), ids.orgA, nil, "running", 9000000, 1, 0, nil, "2026-07-01 00:00:09+00")
	backfill(ids.bfB, ids.orgB, nil, "pending", 0, 0, 0, nil, "2026-07-01 00:00:10+00")
	// The detail route's diagnostics windows: across a month end, one day,
	// and an end before the start (no days).
	window = [2]string{"2026-01-30", "2026-02-02"}
	backfill(ids.bfEdge, ids.orgA, nil, "running", 1, 0, 0, nil, "2026-06-30 00:00:01+00")
	window = [2]string{"2026-01-15", "2026-01-15"}
	backfill(ids.bfOneDay, ids.orgA, nil, "running", 1, 0, 0, nil, "2026-06-30 00:00:02+00")
	window = [2]string{"2026-01-15", "2026-01-14"}
	backfill(ids.bfBackward, ids.orgA, nil, "running", 1, 0, 0, nil, "2026-06-30 00:00:03+00")

	projection := func(org uuid.UUID, configID uuid.UUID, lookback, version int, invalidated bool, payload string) {
		exec(`INSERT INTO sync_coverage_projections (id, org_id, sync_config_id, history_lookback_days, projection_version,
generated_at, source_updated_at, backfill_updated_at, invalidated_at, payload, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '2026-09-01 10:00:00+00', NULL, NULL, CASE WHEN $6 THEN now() END, $7::json, now(), now())`,
			uuid.New(), org.String(), configID, lookback, version, invalidated, payload)
	}
	projection(ids.orgA, ids.cfgPlanner, 3650, 2, false, coveragePayload(ids.cfgPlanner.String(), "planner", ""))
	projection(ids.orgA, ids.cfgLegacyParent, 3650, 2, true, coveragePayload(ids.cfgLegacyParent.String(), "legacy", `, "projection_refreshing": false`))
	projection(ids.orgA, ids.cfgWeird, 3650, 2, false, coveragePairs(ids.cfgWeird.String()))
	projection(ids.orgA, ids.cfgJobsInf, 3650, 2, false, strings.NewReplacer(
		`"2026-01-01T00:00:00+00:00"`, `"2026-01-01T05:30:00+05:30"`,
		`"2026-08-31T00:00:00+00:00"`, `"2026-08-31"`,
		`"history_lookback_days": 3650`, `"history_lookback_days": "3650"`,
		`"gap_count": 1`, `"gap_count": 1.0`,
		`"is_truncated": false`, `"is_truncated": "off"`,
	).Replace(coveragePayload(ids.cfgJobsInf.String(), "planner", `, "extra": {"ignored": [1e400]}`)))
	projection(ids.orgA, ids.cfgSingleRepo, 3650, 1, false, coveragePayload(ids.cfgSingleRepo.String(), "planner", ""))
	projection(ids.orgA, ids.cfgNoSources, 30, 2, false, coveragePayload(ids.cfgNoSources.String(), "planner", ""))
	projection(ids.orgB, ids.cfgCrossOrg, 3650, 2, false, coveragePayload(ids.cfgCrossOrg.String(), "planner", ""))
	projection(ids.orgA, ids.cfgInactive, 3650, 2, false, strings.Replace(
		coveragePayload(ids.cfgInactive.String(), "planner", ""), `"health": "gaps"`, `"health": "bogus"`, 1))
	projection(ids.orgA, ids.cfgDictTargets, 3650, 2, false, `null`)
	projection(ids.orgA, ids.cfgJobsBadList, 3650, 2, false, `{"config_id": "x", "projection_version": 2}`)
	projection(ids.orgB, ids.cfgB, 3650, 2, false, coveragePayload(ids.cfgB.String(), "planner", ""))
	// Backfill boundaries in forms only datetime.fromisoformat reads: a
	// basic date and an ISO week date (the digits of the first read as
	// Unix seconds would answer 1970).
	projection(ids.orgA, ids.child1, 3650, 2, false, strings.Replace(coveragePayload(ids.child1.String(), "legacy", ""),
		`"since": "2026-01-01", "before": "2026-01-02T00:00:00"`, `"since": "20260101", "before": "2026-W01-5"`, 1))
}

// coveragePayload is a projection payload in the projector's shape; extra
// is appended inside the top-level object.
func coveragePayload(configID, basis, extra string) string {
	return `{"config_id": "` + configID + `", "provider": "github", "generated_at": "2026-09-01T10:00:00.123456+00:00",
"data_basis": "` + basis + `", "history_lookback_days": 3650, "truncated_before": "2016-09-03T10:00:00+00:00",
"coverage_since": "2026-01-01T00:00:00+00:00", "coverage_through": null, "is_truncated": false,
"truncation_reason": null, "projection_version": 2, "projection_complete": true,
"overall": {"health": "gaps", "latest_successful_run_at": "2026-08-31T00:00:00+00:00", "latest_covered_through": null,
 "next_scheduled_run_at": null, "gap_count": 1, "stale_dataset_count": 0, "failed_range_count": 0},
"datasets": [{"dataset_key": "git.commits", "status": "gaps", "covered_through": "2026-08-31T00:00:00+00:00",
 "requested_ranges": [{"since": "2026-01-01T00:00:00+00:00", "before": "2026-01-02T00:00:00+00:00", "source_ids": ["s1"], "run_ids": []}],
 "gaps": [{"since": "2026-01-01T00:00:00+00:00", "before": "2026-01-02T00:00:00+00:00"}]}],
"sources": [{"source_id": "s1", "source_name": "org/repo", "status": "not_enabled", "covered_through": null, "gap_count": 1, "failed_range_count": 0}],
"backfill_windows": [{"since": "2026-01-01", "before": "2026-01-02T00:00:00", "source_ids": ["s1"], "dataset_keys": ["git.commits"], "reasons": ["gap", "failed"]}]` + extra + `}`
}

// coveragePairs is a payload stored as a list of [key, value] pairs, which
// dict() reads.
func coveragePairs(configID string) string {
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(coveragePayload(configID, "legacy", "")), &object); err != nil {
		panic(err)
	}
	pairs := make([]string, 0, len(object))
	for _, key := range sortedRawKeys(object) {
		pairs = append(pairs, `["`+key+`", `+string(object[key])+`]`)
	}
	return "[" + strings.Join(pairs, ", ") + "]"
}

func sortedRawKeys(values map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// startGoServer runs the real Go api (apiservice.Routes and NewServer with
// the scope middlewares, as configure() wires them) on the venue's Go copy.
func startGoServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue, jwtKey string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatalf("go pool: %v", err)
	}
	t.Cleanup(pool.Close)
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	// The api's own ClickHouse login (clickhouse.APIPosture), as production
	// wires API_CLICKHOUSE_URI; Python's plane reads the same rows through
	// its CLICKHOUSE_URI.
	clickHouse, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.GoAPIClickHouseURI(t)))
	if err != nil {
		t.Fatalf("go clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, logger)}, logger)
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, routes, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// repoRoot walks up from this file to the directory holding src/dev_health_ops.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, err := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); err == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}

// seedBackfillDiagnostics writes the same ClickHouse rows into both
// planes' databases for get_backfill_job's diagnostics: repos per day with
// a repeated repo and a recomputed row, another org's and out-of-window
// days, and compounding-risk rows with a recomputed (argMax) row, a team
// scope row, unknown severity, and each missing input.
func seedBackfillDiagnostics(t *testing.T, ctx context.Context, venue *venueoracle.Venue, ids venueIDs) {
	t.Helper()
	orgA, orgB := ids.orgA.String(), ids.orgB.String()
	repo1, repo2 := uuid.NewString(), uuid.NewString()
	statements := []string{
		`INSERT INTO repo_metrics_daily (org_id, repo_id, day, computed_at) VALUES
('` + orgA + `', '` + repo1 + `', '2026-01-01', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo1 + `', '2026-01-01', '2026-02-02 00:00:00'),
('` + orgA + `', '` + repo2 + `', '2026-01-01', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo2 + `', '2026-01-31', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo1 + `', '2026-02-01', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo1 + `', '2026-01-15', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo1 + `', '2025-12-31', '2026-02-01 00:00:00'),
('` + orgB + `', '` + repo1 + `', '2026-01-02', '2026-02-01 00:00:00')`,
		`INSERT INTO repo_complexity_daily (org_id, repo_id, day, computed_at) VALUES
('` + orgA + `', '` + repo1 + `', '2026-01-02', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo2 + `', '2026-01-30', '2026-02-01 00:00:00'),
('` + orgA + `', '` + repo2 + `', '2026-02-02', '2026-02-01 00:00:00'),
('` + orgB + `', '` + repo2 + `', '2026-01-02', '2026-02-01 00:00:00')`,
		`INSERT INTO compounding_risk_daily (org_id, day, scope, scope_id, compounding_risk, severity, rework_churn,
complexity_delta, single_owner_ratio, ownership_gini, review_latency_p90h, w_churn, w_complexity, w_ownership, w_review,
threshold_elevated, threshold_high, computed_at) VALUES
('` + orgA + `', '2026-01-01', 'repo', 'r1', NULL, 'unknown', NULL, 1, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00'),
('` + orgA + `', '2026-01-01', 'repo', 'r1', 0.5, 'elevated', 1, 1, 0.1, NULL, 2, 0, 0, 0, 0, 0, 0, '2026-02-02 00:00:00'),
('` + orgA + `', '2026-01-01', 'repo', 'r2', NULL, 'unknown', NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00'),
('` + orgA + `', '2026-01-01', 'team', 't1', NULL, 'unknown', NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00'),
('` + orgA + `', '2026-01-15', 'repo', 'r1', 0.9, 'high', 1, NULL, NULL, 0.3, 5, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00'),
('` + orgA + `', '2026-02-01', 'repo', 'r1', NULL, 'unknown', NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00'),
('` + orgB + `', '2026-01-01', 'repo', 'r1', NULL, 'unknown', NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, '2026-02-01 00:00:00')`,
	}
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatalf("seed clickhouse %s: %v", database, err)
		}
		for _, statement := range statements {
			if err := conn.Exec(ctx, statement); err != nil {
				_ = conn.Close()
				t.Fatalf("seed clickhouse %s: %v\n%s", database, err, statement)
			}
		}
		_ = conn.Close()
	}
}

// inspectBackfillDiagnostics checks that the seeded ClickHouse rows reach
// the detail responses, so SAME is not two planes agreeing on zeros:
// each window's aggregate (the per-day sum after DISTINCT repo_id, the
// latest repo-scope compounding-risk row, org and window filters) and its
// day count.
func inspectBackfillDiagnostics(t *testing.T) func(venueoracle.Request, venueoracle.Response) {
	want := map[string]string{
		"backfill job linked run": `31 {"repo_metrics_rows":4,"repo_complexity_rows":1,"compounding_risk_rows":3,"compounding_risk_non_null_rows":2,` +
			`"compounding_risk_unknown_rows":1,"reason_counts":{"missing_rework_churn":1,"missing_complexity_delta":2,"missing_review_latency":1,"missing_ownership_signal":1}}`,
		"backfill job month edge": `4 {"repo_metrics_rows":2,"repo_complexity_rows":2,"compounding_risk_rows":1,"compounding_risk_non_null_rows":0,` +
			`"compounding_risk_unknown_rows":1,"reason_counts":{"missing_rework_churn":1,"missing_complexity_delta":1,"missing_review_latency":1,"missing_ownership_signal":1}}`,
		"backfill job one day": `1 {"repo_metrics_rows":1,"repo_complexity_rows":0,"compounding_risk_rows":1,"compounding_risk_non_null_rows":1,` +
			`"compounding_risk_unknown_rows":0,"reason_counts":{"missing_rework_churn":0,"missing_complexity_delta":1,"missing_review_latency":0,"missing_ownership_signal":0}}`,
		"backfill job backward window": `0 {"repo_metrics_rows":0,"repo_complexity_rows":0,"compounding_risk_rows":0,"compounding_risk_non_null_rows":0,` +
			`"compounding_risk_unknown_rows":0,"reason_counts":{"missing_rework_churn":0,"missing_complexity_delta":0,"missing_review_latency":0,"missing_ownership_signal":0}}`,
	}
	seen := 0
	t.Cleanup(func() {
		if seen != len(want) {
			t.Errorf("inspected %d of %d backfill diagnostics responses", seen, len(want))
		}
	})
	return func(request venueoracle.Request, response venueoracle.Response) {
		expected, ok := want[request.Name]
		if !ok {
			return
		}
		seen++
		var body struct {
			Diagnostics struct {
				Aggregate json.RawMessage   `json:"aggregate"`
				PerDay    []json.RawMessage `json:"per_day"`
			} `json:"metrics_diagnostics"`
		}
		if err := json.Unmarshal([]byte(response.Body), &body); err != nil {
			t.Errorf("%s: %v: %s", request.Name, err, response.Body)
			return
		}
		if got := fmt.Sprintf("%d %s", len(body.Diagnostics.PerDay), body.Diagnostics.Aggregate); got != expected {
			t.Errorf("%s: diagnostics\n got  %s\n want %s", request.Name, got, expected)
		}
	}
}
