//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	updateVenueKey       = "venue-oracle-update-sync-config-encryption-key-32!"
	updateVenuePinnedNow = "2026-09-25T08:15:30.654321+00:00"
)

type updateVenueIDs struct {
	orgA, orgB, orgC, adminA, memberA, ownerB, adminC     uuid.UUID
	credJira                                              uuid.UUID
	intGH, intGH2, intJira, intJiraOff, intPD             uuid.UUID
	cfgGH, cfgGated, cfgJira, cfgJiraOff, cfgPD, cfgChild uuid.UUID
	cfgLegacy, cfgLegacyChild1, cfgLegacyChild2, cfgNoInt uuid.UUID
	cfgB, cfgGHNoDataset, intGHNoDataset                  uuid.UUID
	cfgGHCanon, intGHCanon, cfgNoop                       uuid.UUID
}

func newUpdateVenueIDs() updateVenueIDs {
	var ids updateVenueIDs
	for _, target := range []*uuid.UUID{&ids.orgA, &ids.orgB, &ids.orgC, &ids.adminA, &ids.memberA, &ids.ownerB, &ids.adminC, &ids.credJira,
		&ids.intGH, &ids.intGH2, &ids.intJira, &ids.intJiraOff, &ids.intPD, &ids.cfgGH, &ids.cfgGated, &ids.cfgJira,
		&ids.cfgJiraOff, &ids.cfgPD, &ids.cfgChild, &ids.cfgLegacy, &ids.cfgLegacyChild1, &ids.cfgLegacyChild2, &ids.cfgNoInt,
		&ids.cfgB, &ids.cfgGHNoDataset, &ids.intGHNoDataset, &ids.cfgGHCanon, &ids.intGHCanon, &ids.cfgNoop} {
		*target = newID()
	}
	return ids
}

// TestSyncConfigUpdateVenueOracle sends one PATCH
// /api/v1/admin/sync-configs/{config_id} sequence to the real Python api
// and the Go api over two copies of one seeded database, on one pinned
// clock (croniter's time() included) and one fake Jira, and requires the
// same answers and then the same rows in sync_configurations, integrations,
// integration_datasets, integration_sources, scheduled_jobs and
// sync_coverage_projections (the projections' database-clock timestamps
// compared as invalidated or not).
//
// Named divergence (lead ruling on the cron domain): a refused cron's 422
// text is compared by status only. Named ordering difference: Python runs
// Jira discovery inside the request's transaction, before the sync job
// upsert and the coverage invalidation; Go runs it after the update
// commits. The stored end state is the same, so the row diff cannot see
// the order; only a reader between the two commits could.
func TestSyncConfigUpdateVenueOracle(t *testing.T) {
	resetIDs(t)
	golden := venueoracle.OpenGolden(t, goldenSpec(t.Name()))
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-config-update!"
	ids := newUpdateVenueIDs()
	jira, certFile := fakeJira(t)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   golden.PythonRoot(t, root),
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + updateVenueKey,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
			"REQUESTS_CA_BUNDLE=" + certFile,
			"VENUE_PINNED_NOW=" + updateVenuePinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.api.admin.routers.sync,dev_health_ops.models.settings," +
				"dev_health_ops.models.integrations,dev_health_ops.sync.discovery",
			"VENUE_PINNED_TIME_MODULES=croniter.croniter",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedUpdateVenue(t, ctx, admin, venue, ids, "https://"+jira.Listener.Addr().String())
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":  token(ids.adminA, ids.orgA, "update-admin-a@example.com", "admin"),
				"memberA": token(ids.memberA, ids.orgA, "update-member-a@example.com", "member"),
				"ownerB":  token(ids.ownerB, ids.orgB, "update-owner-b@example.com", "owner"),
				"adminC":  token(ids.adminC, ids.orgC, "update-admin-c@example.com", "admin"),
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, updateVenuePinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(updateVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	jiraClient := jira.Client()
	jiraClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := startGoServerWith(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return pinned }
		deps.Decryptor = decryptor
		deps.SyncJiraHTTP = jiraClient
	})

	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	a, b, c := bearer("adminA"), bearer("ownerB"), bearer("adminC")
	patch := func(name string, id uuid.UUID, body string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "PATCH", Path: "/api/v1/admin/sync-configs/" + id.String(), Headers: headers,
			Body: venueoracle.B64(body)}
	}
	requests := []venueoracle.Request{
		{Name: "no token", Method: "PATCH", Path: "/api/v1/admin/sync-configs/" + ids.cfgGH.String(),
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{}`)},
		patch("member", ids.cfgGH, `{}`, bearer("memberA")),
		patch("invalid json", ids.cfgGH, `{"sync_targets":`, a),
		patch("not an object", ids.cfgGH, `[]`, a),
		patch("wrong field types", ids.cfgGH, `{"sync_targets":"git","sync_options":[],"is_active":"maybe","schedule_cron":1,"timezone":[],"initial_sync_depth":"abc"}`, a),
		patch("unknown config", newID(), `{}`, a),
		{Name: "config id not a uuid", Method: "PATCH", Path: "/api/v1/admin/sync-configs/not-a-uuid", Headers: a, Body: venueoracle.B64(`{}`)},
		patch("other org's config", ids.cfgB, `{}`, a),
		patch("empty body", ids.cfgGH, `{}`, a),
		patch("gated new targets", ids.cfgGH, `{"sync_targets":["git","incidents"]}`, a),
		patch("gated stored targets", ids.cfgGated, `{"is_active":false}`, a),
		patch("github targets", ids.cfgGH, `{"sync_targets":["git","prs","work-items"]}`, a),
		patch("github runtime options", ids.cfgGH, `{"sync_options":{"fetch_comments":false,"comments_limit":7}}`, a),
		patch("github schedule", ids.cfgGH, `{"schedule_cron":"0 */6 * * *","timezone":"Europe/Paris","initial_sync_depth":999999}`, a),
		patch("github schedule cleared", ids.cfgGH, `{"schedule_cron":null,"timezone":null}`, a),
		patch("github stale nested schedule", ids.cfgGH, `{"sync_options":{"schedule_cron":"0 1 * * *"},"schedule_cron":null}`, a),
		patch("github no work-items dataset", ids.cfgGHNoDataset, `{"sync_options":{"fetch_milestones":false}}`, a),
		patch("cron refused", ids.cfgGH, `{"schedule_cron":"60 * * * *"}`, a),
		patch("timezone refused", ids.cfgGH, `{"schedule_cron":"0 * * * *","timezone":"Mars/Base"}`, a),
		patch("nested cron refused", ids.cfgGH, `{"sync_options":{"schedule_cron":"0 * * * *","timezone":"Nope/Zone"}}`, a),
		patch("malformed auto-import", ids.cfgGH, `{"sync_options":{"auto_import_teams":"yes"}}`, a),
		patch("unsupported auto-import", ids.cfgGH, `{"sync_options":{"auto_import_projects":true}}`, a),
		patch("lax is_active", ids.cfgGH, `{"is_active":"false"}`, a),
		patch("reactivate", ids.cfgGH, `{"is_active":1}`, a),
		patch("source-scoped child targets", ids.cfgChild, `{"sync_targets":["cicd"]}`, a),
		patch("github integration already canonical", ids.cfgGHCanon, `{}`, a),
		patch("no-op body", ids.cfgNoop, `{"is_active":true}`, a),
		patch("pagerduty mappings", ids.cfgPD, `{"sync_options":{"service_repository_mappings":{"svc":["acme/r"]}}}`, c),
		patch("pagerduty mappings not a dict", ids.cfgPD, `{"sync_options":{"service_repository_mappings":["x"]}}`, c),
		patch("pagerduty targets refused by the planner", ids.cfgPD, `{"sync_targets":["operational","incidents"]}`, c),
		patch("legacy parent cascade", ids.cfgLegacy, `{"sync_targets":["git"],"is_active":false,"schedule_cron":"0 3 * * *","initial_sync_depth":null}`, a),
		patch("no integration", ids.cfgNoInt, `{"sync_options":{"x":1}}`, a),
		patch("jira scope change", ids.cfgJira, `{"sync_options":{"project_key":"ACM"}}`, a),
		patch("jira reactivate", ids.cfgJiraOff, `{"is_active":true}`, a),
		patch("community schedule", ids.cfgB, `{"schedule_cron":"0 0 * * *"}`, b),
		{Name: "github after", Method: "GET", Path: "/api/v1/admin/sync-configs/" + ids.cfgGH.String(), Headers: a},
	}
	statusOnly := map[string]bool{"cron refused": true}
	python := golden.Python(t, venue, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Golden: golden,
		Normalize: func(request venueoracle.Request, body string) string {
			body = fakeJiraAddress.ReplaceAllString(body, "127.0.0.1:<port>")
			if statusOnly[request.Name] {
				return "<status only: named divergence>"
			}
			return body
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	orgs := fmt.Sprintf("'%s','%s','%s'", ids.orgA, ids.orgB, ids.orgC)
	queries := createVenueRowQueries(orgs, "true")
	queries["sync_coverage_projections"] = `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', p.org_id, c.name,
  p.payload::text, CASE WHEN p.invalidated_at IS NULL THEN 'valid' ELSE 'invalidated' END,
  CASE WHEN p.updated_at > '2026-06-01' THEN 'moved' ELSE p.updated_at::text END) AS r
  FROM sync_coverage_projections p JOIN sync_configurations c ON c.id = p.sync_config_id WHERE p.org_id IN (` + orgs + `)) AS rows`
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integrations", "sync_configurations", "integration_sources", "integration_datasets", "scheduled_jobs",
		"sync_coverage_projections"} {
		goRows := fakeJiraAddress.ReplaceAllString(venueoracle.TableRows(t, ctx, goDB, queries[table]), "127.0.0.1:<port>")
		golden.CompareRows(t, "rows:"+table, func() string {
			return fakeJiraAddress.ReplaceAllString(venueoracle.TableRows(t, ctx, source, queries[table]), "127.0.0.1:<port>")
		}, goRows)
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened on the Go plane: configs on the pinned clock,
	// discovered Jira projects, invalidated projections, a written
	// integration config, cascaded children.
	state := venueoracle.TableRows(t, ctx, goDB, fmt.Sprintf(`SELECT
  (SELECT count(*) FROM sync_configurations WHERE updated_at = '%s'),
  (SELECT count(*) FROM integration_sources WHERE integration_id IN ('%s','%s')),
  (SELECT count(*) FROM sync_coverage_projections WHERE invalidated_at IS NOT NULL),
  (SELECT count(*) FROM integrations WHERE updated_at = '%s'),
  (SELECT count(*) FROM sync_configurations WHERE parent_id = '%s' AND NOT is_active)`,
		updateVenuePinnedNow, ids.intJira, ids.intJiraOff, updateVenuePinnedNow, ids.cfgLegacy))
	if fields := strings.Fields(state); len(fields) != 5 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" || fields[3] == "0" || fields[4] == "0" {
		t.Errorf("writes not observed (pinned configs, discovered sources, invalidated projections, written integration, cascaded children) = %s", state)
	}
	t.Logf("write state: %s", state)
	golden.Finish(t)
}

// seedUpdateVenue writes org A (enterprise, the canonical incident feature
// off) and org B (community), their users, org A's Jira credential
// (encrypted by the Python plane, pointing at the fake Jira), and the
// configs the sequence edits, with their integrations, datasets, sync
// jobs and coverage projections.
func seedUpdateVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, ids updateVenueIDs, jiraURL string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'update-a', 'update-a', '{}', 'enterprise', true, $3, $3), ($2, 'update-b', 'update-b', '{}', 'community', true, $3, $3),
($4, 'update-c', 'update-c', '{}', 'enterprise', true, $3, $3)`, ids.orgA, ids.orgB, at, ids.orgC)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{ids.adminA, ids.orgA, "update-admin-a@example.com", "admin"}, {ids.memberA, ids.orgA, "update-member-a@example.com", "member"},
		{ids.ownerB, ids.orgB, "update-owner-b@example.com", "owner"}, {ids.adminC, ids.orgC, "update-admin-c@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			newID(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, false, $3, $3 FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, newID(), ids.orgA, at)

	encoded, _ := json.Marshal(map[string]any{"email": "venue@example.com", "api_token": "good-token", "base_url": jiraURL})
	raw := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	var ciphertext string
	if err := json.Unmarshal(raw[0], &ciphertext); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'jira', 'jira good', true, $3, '{}', $4, $4)`, ids.credJira, ids.orgA.String(), ciphertext, at)

	integration := func(id, org uuid.UUID, provider, name, config string, credential any) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, true, $7, $7)`, id, org.String(), provider, credential, name, config, at)
	}
	integration(ids.intGH, ids.orgA, "github", "gh", `{"owner": "acme", "fetch_comments": true}`, nil)
	integration(ids.intGH2, ids.orgA, "github", "gh gated", `{}`, nil)
	integration(ids.intGHNoDataset, ids.orgA, "github", "gh no dataset", `{}`, nil)
	integration(ids.intGHCanon, ids.orgA, "github", "gh canonical", `{"fetch_comments": true, "fetch_milestones": true, "comments_limit": 500}`, nil)
	integration(ids.intJira, ids.orgA, "jira", "jira", `{}`, ids.credJira)
	integration(ids.intJiraOff, ids.orgA, "jira", "jira off", `{}`, ids.credJira)
	integration(ids.intPD, ids.orgC, "pagerduty", "pd", `{}`, nil)

	config := func(id, org uuid.UUID, name, provider, targets, options string, active bool, integrationID, parentID, sourceID any, planner bool) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, parent_id, source_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, $6::json, $7, $8, $9, $10, $11, $12, $12)`,
			id, org.String(), name, provider, targets, options, active, planner, integrationID, parentID, sourceID, at)
	}
	config(ids.cfgGH, ids.orgA, "gh", "github", `["git"]`, `{"owner": "acme", "all_repos": true}`, true, ids.intGH, nil, nil, true)
	config(ids.cfgGated, ids.orgA, "gh gated", "github", `["git", "Incidents"]`, `{}`, true, ids.intGH2, nil, nil, true)
	config(ids.cfgGHNoDataset, ids.orgA, "gh no dataset", "GitHub", `["git"]`, `{"all_repos": true}`, true, ids.intGHNoDataset, nil, nil, true)
	config(ids.cfgJira, ids.orgA, "jira", "jira", `["work-items"]`, `{"project_key": "AAPRFE"}`, true, ids.intJira, nil, nil, true)
	config(ids.cfgJiraOff, ids.orgA, "jira off", "Jira", `["work-items"]`, `{"project_key": "AESH"}`, false, ids.intJiraOff, nil, nil, true)
	config(ids.cfgPD, ids.orgC, "pd", "pagerduty", `["operational"]`, `{}`, true, ids.intPD, nil, nil, true)
	config(ids.cfgLegacy, ids.orgA, "legacy", "github", `["git", "prs"]`, `{"owner": "acme", "initial_sync_depth": 30}`, true, nil, nil, nil, false)
	config(ids.cfgLegacyChild1, ids.orgA, "legacy child 1", "github", `["git", "prs"]`, `{"repo": "acme/a", "initial_sync_depth": 30}`, true, nil, ids.cfgLegacy, nil, false)
	config(ids.cfgLegacyChild2, ids.orgA, "legacy child 2", "github", `["prs"]`, `{"repo": "acme/b"}`, true, nil, ids.cfgLegacy, nil, false)
	config(ids.cfgNoInt, ids.orgA, "no integration", "linear", `["work-items"]`, `{}`, true, nil, nil, nil, false)
	config(ids.cfgNoop, ids.orgA, "no-op", "linear", `["work-items"]`, `{"team_id": "t"}`, true, nil, nil, nil, false)
	config(ids.cfgGHCanon, ids.orgA, "gh canonical", "github", `["git"]`, `{"all_repos": true}`, true, ids.intGHCanon, nil, nil, true)
	config(ids.cfgB, ids.orgB, "b", "github", `["git"]`, `{"all_repos": true}`, true, nil, nil, nil, false)
	source := newID()
	exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
is_enabled, discovered_at, last_seen_at) VALUES ($1, $2, $3, 'github', 'repository', 'acme/r', 'r', 'acme/r', '{}', true, $4, $4)`,
		source, ids.orgA.String(), ids.intGH, at)
	config(ids.cfgChild, ids.orgA, "gh child", "github", `["git"]`, `{}`, true, ids.intGH, nil, source, false)

	dataset := func(integrationID uuid.UUID, key string, enabled bool, options string) {
		exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, $5, $6::json)`,
			newID(), ids.orgA.String(), integrationID, key, enabled, options)
	}
	for _, key := range []string{"repo-metadata", "commits", "commit-stats", "files", "blame"} {
		dataset(ids.intGH, key, true, `{}`)
	}
	dataset(ids.intGH, "prs", false, `{}`)
	dataset(ids.intGH, "work-items", false, `{"fetch_comments": false, "legacy": 1}`)
	exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, 'services', true, $4::json)`,
		newID(), ids.orgC.String(), ids.intPD, `{"legacy_targets": ["operational"]}`)

	job := func(configID uuid.UUID, provider, cron string, status int) {
		exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status,
is_running, run_count, failure_count, created_at, updated_at) VALUES ($1, $2, $3, 'sync', $4, $5, 'UTC', $6::json, $7, $8, false, 0, 0, $9, $9)`,
			newID(), ids.orgA.String(), "sync-config-"+configID.String(), provider, cron,
			fmt.Sprintf(`{"provider": "%s", "sync_config_id": "%s"}`, provider, configID), configID, status, at)
	}
	job(ids.cfgGH, "github", "0 * * * *", 1)
	job(ids.cfgLegacy, "github", "0 * * * *", 1)
	job(ids.cfgLegacyChild1, "github", "0 * * * *", 1)

	projection := func(org, configID uuid.UUID) {
		exec(`INSERT INTO sync_coverage_projections (id, org_id, sync_config_id, history_lookback_days, projection_version,
generated_at, source_updated_at, backfill_updated_at, invalidated_at, payload, created_at, updated_at)
VALUES ($1, $2, $3, 3650, 2, $4, $4, $4, NULL, '{}', $4, $4)`, newID(), org.String(), configID, at)
	}
	projection(ids.orgA, ids.cfgGH)
	projection(ids.orgA, ids.cfgNoInt)
	projection(ids.orgA, ids.cfgLegacy)
	projection(ids.orgC, ids.cfgPD)
}
