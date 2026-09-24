//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    jwtKey,
		PythonEnv: []string{"HIDE_MIGRATED_CHILD_CONFIGS= On "},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedSyncAdmin(t, ctx, admin, ids)
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
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{})
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
		"/sync-configs/" + ids.cfgPlanner.String() + "/coverage",
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
	} {
		requests = append(requests, get("coverage "+name, "/sync-configs/"+raw+"/coverage", a))
	}
	requests = append(requests, get("coverage other org caller", "/sync-configs/"+ids.cfgB.String()+"/coverage", b))
	return requests
}

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

	backfill := func(org uuid.UUID, task any, status string, total, completed, failed int, errorMessage any, created string) {
		exec(`INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks,
completed_chunks, failed_chunks, error_message, started_at, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '2026-01-01', '2026-01-31', $6, $7, $8, $9, '2026-02-01 00:00:00+00', NULL, $10::timestamptz, $10::timestamptz + interval '1 second')`,
			uuid.New(), org.String(), ids.cfgPlanner, task, status, total, completed, failed, errorMessage, created)
	}
	backfill(ids.orgA, "sync_run:"+ids.runP1.String(), "pending", 0, 0, 0, nil, "2026-07-01 00:00:01+00")
	backfill(ids.orgA, nil, "running", 3, 1, 0, nil, "2026-07-01 00:00:02+00")
	backfill(ids.orgA, "x sync_run:not-a-uuid", "failed", 2, 0, 2, "bad", "2026-07-01 00:00:03+00")
	backfill(ids.orgA, "sync_run:", "pending", 0, 0, 0, nil, "2026-07-01 00:00:04+00")
	backfill(ids.orgA, "a:sync_run:"+ids.runB.String(), "pending", 1, 1, 0, nil, "2026-07-01 00:00:05+00")
	backfill(ids.orgA, "sync_run:{"+ids.runP2.String()+"}", "pending", 4, 0, 0, "old", "2026-07-01 00:00:06+00")
	backfill(ids.orgA, "sync_run:"+uuid.NewString()+"sync_run:"+ids.runP1.String(), "done", 7, 7, 0, nil, "2026-07-01 00:00:07.123456+00")
	// progress_pct in the ranges where json.dumps and pydantic-core write
	// a float differently (this route has no response model: json.dumps).
	backfill(ids.orgA, nil, "running", 2000000000, 1, 0, nil, "2026-07-01 00:00:08+00")
	backfill(ids.orgA, nil, "running", 9000000, 1, 0, nil, "2026-07-01 00:00:09+00")
	backfill(ids.orgB, nil, "pending", 0, 0, 0, nil, "2026-07-01 00:00:10+00")

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
	routes := apiservice.Routes(apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger)}, logger)
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
