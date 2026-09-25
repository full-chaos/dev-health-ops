//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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
	createVenueKey       = "venue-oracle-create-sync-config-encryption-key-32!"
	createVenuePinnedNow = "2026-09-24T12:34:56.123456+00:00"
)

type createVenueIDs struct {
	orgA, orgB, orgC, orgD, orgE                    uuid.UUID
	adminA, memberA, ownerB, adminC, adminD, adminE uuid.UUID
	credJira, credJiraBad, credJiraInactive         uuid.UUID
	credPD, credPDBad                               uuid.UUID
	intC                                            uuid.UUID
}

func newCreateVenueIDs() createVenueIDs {
	var ids createVenueIDs
	for _, target := range []*uuid.UUID{&ids.orgA, &ids.orgB, &ids.orgC, &ids.orgD, &ids.orgE, &ids.adminE, &ids.adminA, &ids.memberA, &ids.ownerB,
		&ids.adminC, &ids.adminD, &ids.credJira, &ids.credJiraBad, &ids.credJiraInactive, &ids.credPD, &ids.credPDBad, &ids.intC} {
		*target = uuid.New()
	}
	return ids
}

// fakeJira serves the recorded project list (testdata/jira_recorded, see
// its README) as /rest/api/3/project/search windows over TLS, the only
// scheme Python's Jira client uses. A request carrying the bad
// credential's token gets Jira's 401.
func fakeJira(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	var projects []json.RawMessage
	files, err := filepath.Glob(filepath.Join("testdata", "jira_recorded", "search_*.json"))
	if err != nil || len(files) != 6 {
		t.Fatalf("recorded jira pages: %v (%d)", err, len(files))
	}
	sort.Slice(files, func(i, j int) bool {
		start := func(name string) int {
			n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(name), "search_"), ".json"))
			return n
		}
		return start(files[i]) < start(files[j])
	})
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var page struct {
			Values []json.RawMessage `json:"values"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		projects = append(projects, page.Values...)
	}
	badAuth := "Basic " + basicToken("venue@example.com", "bad-token")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		if r.Header.Get("Authorization") == badAuth {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"errorMessages":["Client must be authenticated to access this resource."],"errors":{}}`))
			return
		}
		if r.URL.Path != "/rest/api/3/project/search" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"errorMessages":["Not found"]}`))
			return
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		size, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
		if size <= 0 {
			size = 50
		}
		end := start + size
		if end > len(projects) {
			end = len(projects)
		}
		values := []json.RawMessage{}
		if start < len(projects) {
			values = projects[start:end]
		}
		isLast := end >= len(projects)
		page := map[string]any{
			"self":       fmt.Sprintf("https://%s/rest/api/3/project/search?maxResults=%d&startAt=%d", r.Host, size, start),
			"maxResults": size, "startAt": start, "total": len(projects), "isLast": isLast, "values": values,
		}
		if !isLast {
			page["nextPage"] = fmt.Sprintf("https://%s/rest/api/3/project/search?maxResults=%d&startAt=%d", r.Host, size, end)
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(server.Close)
	certFile := filepath.Join(t.TempDir(), "fake-jira.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return server, certFile
}

func basicToken(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}

// TestSyncConfigCreateVenueOracle sends POST /api/v1/admin/sync-configs to
// the real Python api and the Go api over two copies of one seeded
// database, on one pinned clock (croniter's time() included) and one fake
// Jira, and requires the same answers and then the same rows in
// integrations, sync_configurations, integration_sources,
// integration_datasets and scheduled_jobs. Row ids (uuid4 on each plane)
// are replaced by the names they point at.
//
// Named divergences (lead ruling on the cron domain): a refused cron's 422
// text is compared by status only, and an expression only croniter accepts
// (org D, excluded from the row diff) is created by Python and refused by
// Go, asserted as such.
func TestSyncConfigCreateVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-config-create!"
	ids := newCreateVenueIDs()
	jira, certFile := fakeJira(t)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + createVenueKey,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
			"REQUESTS_CA_BUNDLE=" + certFile,
			"VENUE_PINNED_NOW=" + createVenuePinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.api.admin.routers.sync,dev_health_ops.models.settings," +
				"dev_health_ops.models.integrations,dev_health_ops.sync.pagerduty_repair,dev_health_ops.sync.discovery",
			"VENUE_PINNED_TIME_MODULES=croniter.croniter",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedCreateVenue(t, ctx, admin, venue, ids, "https://"+jira.Listener.Addr().String())
			token := func(user, org uuid.UUID, email, role string) map[string]any {
				return map[string]any{"user_id": user.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"adminA":  token(ids.adminA, ids.orgA, "create-admin-a@example.com", "admin"),
				"memberA": token(ids.memberA, ids.orgA, "create-member-a@example.com", "member"),
				"ownerB":  token(ids.ownerB, ids.orgB, "create-owner-b@example.com", "owner"),
				"adminC":  token(ids.adminC, ids.orgC, "create-admin-c@example.com", "admin"),
				"adminD":  token(ids.adminD, ids.orgD, "create-admin-d@example.com", "admin"),
				"adminE":  token(ids.adminE, ids.orgE, "create-admin-e@example.com", "admin"),
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, createVenuePinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(createVenueKey), "")
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
	a, b, c, d, e := bearer("adminA"), bearer("ownerB"), bearer("adminC"), bearer("adminD"), bearer("adminE")
	post := func(name, body string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/admin/sync-configs", Headers: headers, Body: venueoracle.B64(body)}
	}
	jiraCred, jiraBad, jiraInactive := ids.credJira.String(), ids.credJiraBad.String(), ids.credJiraInactive.String()
	pdCred, pdBad := ids.credPD.String(), ids.credPDBad.String()
	requests := []venueoracle.Request{
		{Name: "no token", Method: "POST", Path: "/api/v1/admin/sync-configs",
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"name":"x","provider":"jira"}`)},
		post("member", `{"name":"x","provider":"jira"}`, bearer("memberA")),
		post("invalid json", `{"name":`, a),
		post("not an object", `["x"]`, a),
		post("name and provider missing", `{}`, a),
		post("name empty", `{"name":"","provider":"jira"}`, a),
		post("wrong field types", `{"name":"x","provider":"jira","sync_targets":"work-items","sync_options":[],"credential_id":5,"schedule_cron":1,"timezone":[],"initial_sync_depth":"abc"}`, a),
		post("null defaulted fields", `{"name":"x","provider":"jira","sync_targets":null,"sync_options":null}`, a),
		post("gated target", `{"name":"gated","provider":"jira","sync_targets":["incidents"]}`, a),
		post("github without selection", `{"name":"gh none","provider":"github","sync_targets":["git"]}`, a),
		post("github all repos, scheduled", `{"name":"gh all","provider":"github","sync_targets":["git","prs","work-items"],"sync_options":{"all_repos":true},"schedule_cron":"0 */6 * * *","timezone":"Europe/Paris","initial_sync_depth":30}`, a),
		post("github malformed work-item option", `{"name":"gh bad option","provider":"github","sync_targets":["work-items"],"sync_options":{"all_repos":true,"fetch_comments":"no"}}`, a),
		post("gitlab all repos", `{"name":"gl all","provider":"GitLab","sync_targets":["git","cicd"],"sync_options":{"all_repos":true,"group":"acme"}}`, a),
		post("jira explicit project", `{"name":"jira eng","provider":"jira","sync_targets":["work-items"],"sync_options":{"project_key":"ENG","full_name":"Engineering"}}`, a),
		post("jira explicit project, same name", `{"name":"jira eng","provider":"jira","sync_targets":["work-items"],"sync_options":{"project_key":"OPS"}}`, a),
		post("jira discovered", `{"name":"jira all","provider":"jira","credential_id":"`+jiraCred+`","sync_targets":["work-items"],"schedule_cron":"0 0 * * 1"}`, a),
		post("jira discovery refused", `{"name":"jira bad cred","provider":"jira","credential_id":"`+jiraBad+`","sync_targets":["work-items"]}`, a),
		post("jira inactive credential", `{"name":"jira inactive cred","provider":"jira","credential_id":"`+jiraInactive+`","sync_targets":["work-items"]}`, a),
		post("jira no credential", `{"name":"jira no cred","provider":"jira","sync_targets":["work-items"]}`, a),
		post("jira whitespace project", `{"name":"jira blank","provider":"Jira","sync_targets":["work-items"],"sync_options":{"project_key":"   "}}`, a),
		post("linear org-wide", `{"name":"linear all","provider":"linear","sync_targets":["work-items"],"sync_options":{"auto_import_projects":true}}`, a),
		post("linear team", `{"name":"linear team","provider":"linear","sync_targets":["work-items"],"sync_options":{"team_id":7}}`, a),
		post("launchdarkly", `{"name":"ld","provider":"launchdarkly","sync_targets":["feature-flags"],"sync_options":{"project_key":"default"}}`, a),
		post("pagerduty usable credential", `{"name":"pd","provider":"pagerduty","credential_id":"`+pdCred+`","sync_targets":["operational"],"sync_options":{"service_repository_mappings":{"svc":["repo"]}}}`, e),
		post("pagerduty unusable credential", `{"name":"pd bad","provider":"pagerduty","credential_id":"`+pdBad+`","sync_targets":["operational"]}`, e),
		post("pagerduty not operational", `{"name":"pd wrong","provider":"pagerduty","sync_targets":["incidents","operational"]}`, e),
		post("credential id not a uuid", `{"name":"bad cred id","provider":"jira","credential_id":"nope","sync_targets":["work-items"],"sync_options":{"project_key":"X"}}`, a),
		post("empty credential id", `{"name":"empty cred id","provider":"jira","credential_id":"","sync_targets":["work-items"],"sync_options":{"project_key":"Y"}}`, a),
		post("malformed auto-import flags", `{"name":"ai bad","provider":"jira","sync_options":{"auto_import_teams":"yes","auto_import_members":null}}`, a),
		post("unsupported auto-import", `{"name":"ai unsupported","provider":"github","sync_options":{"all_repos":true,"auto_import_projects":true}}`, a),
		post("initial depth, string in options", `{"name":"depth str","provider":"jira","sync_options":{"project_key":"D1","initial_sync_depth":"20"}}`, a),
		post("initial depth, float in options", `{"name":"depth float","provider":"jira","sync_options":{"project_key":"D2","initial_sync_depth":29.9}}`, a),
		post("initial depth, list in options", `{"name":"depth list","provider":"jira","sync_options":{"project_key":"D3","initial_sync_depth":[1]}}`, a),
		post("cron below the minimum", `{"name":"fast","provider":"jira","schedule_cron":"*/5 * * * *","sync_options":{"project_key":"F"}}`, a),
		post("cron refused by both", `{"name":"bad cron","provider":"jira","schedule_cron":"60 * * * *","sync_options":{"project_key":"C"}}`, a),
		post("cron not a string", `{"name":"cron int","provider":"jira","sync_options":{"project_key":"C2","schedule_cron":5}}`, a),
		post("timezone refused", `{"name":"bad tz","provider":"jira","schedule_cron":"0 * * * *","timezone":"Mars/Base","sync_options":{"project_key":"T"}}`, a),
		post("timezone not a string", `{"name":"tz int","provider":"jira","sync_options":{"project_key":"T2","schedule_cron":"0 * * * *","timezone":5}}`, a),
		post("timezone without schedule", `{"name":"tz only","provider":"jira","timezone":"Mars/Base","sync_options":{"project_key":"T3"}}`, a),
		post("community schedule", `{"name":"b sched","provider":"jira","schedule_cron":"0 0 * * *","sync_options":{"project_key":"B"}}`, b),
		post("community backfill over", `{"name":"b depth","provider":"jira","initial_sync_depth":31,"sync_options":{"project_key":"B2"}}`, b),
		post("community under the repo limit", `{"name":"b ok","provider":"jira","initial_sync_depth":30,"sync_options":{"project_key":"B3"}}`, b),
		post("team at the repo limit", `{"name":"c gh","provider":"github","sync_options":{"all_repos":true}}`, c),
		post("team at the limit, unscoped jira", `{"name":"c jira","provider":"jira","sync_targets":["work-items"]}`, c),
		post("team schedule below its minimum", `{"name":"c hourly","provider":"jira","schedule_cron":"0 * * * *","sync_options":{"project_key":"C3"}}`, c),
		{Name: "list after", Method: "GET", Path: "/api/v1/admin/sync-configs", Headers: a},
	}
	statusOnly := map[string]bool{"cron refused by both": true, "cron not a string": true}
	idPattern := regexp.MustCompile(`"id":"[0-9a-f-]{36}"`)
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			if statusOnly[request.Name] {
				return "<status only: named divergence>"
			}
			if request.Method == "POST" || request.Name == "list after" {
				return idPattern.ReplaceAllString(body, `"id":"<uuid4>"`)
			}
			return body
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	// Named divergence: croniter-only syntax, org D. Python creates, Go refuses.
	divergent := []venueoracle.Request{
		post("croniter-only alias", `{"name":"d alias","provider":"jira","schedule_cron":"@hourly","sync_options":{"project_key":"DA"}}`, d),
		post("croniter-only seconds field", `{"name":"d seconds","provider":"jira","schedule_cron":"0 0 * * * 0","sync_options":{"project_key":"DS"}}`, d),
	}
	pythonDivergent := venue.ServePython(t, divergent)
	for index, request := range divergent {
		goResponse := venueoracle.Do(t, base, request)
		if pythonDivergent[index].Status != http.StatusCreated || goResponse.Status != http.StatusUnprocessableEntity {
			t.Errorf("%s: python %d, go %d %s; want the documented 201 / 422", request.Name, pythonDivergent[index].Status, goResponse.Status, goResponse.Body)
		}
	}

	orgs := fmt.Sprintf("'%s','%s','%s','%s'", ids.orgA, ids.orgB, ids.orgC, ids.orgE)
	queries := map[string]string{
		"integrations": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', org_id, provider,
  coalesce(credential_id::text, '<null>'), name, config::text, is_active, coalesce(schedule_cron, '<null>'),
  coalesce(timezone, '<null>'), created_at, updated_at) AS r FROM integrations WHERE org_id IN (` + orgs + `)) AS rows`,
		"sync_configurations": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', c.org_id, c.name, c.provider,
  c.sync_targets::text, c.sync_options::text, c.is_active, c.planner_managed, coalesce(c.last_sync_at::text, '<null>'),
  coalesce(c.last_sync_success::text, '<null>'), coalesce(c.last_sync_error, '<null>'), coalesce(c.last_sync_stats::text, '<null>'),
  c.created_at, c.updated_at, coalesce(p.name, '<null>'), coalesce(i.name, '<null>'), coalesce(c.source_id::text, '<null>')) AS r
  FROM sync_configurations c LEFT JOIN sync_configurations p ON p.id = c.parent_id LEFT JOIN integrations i ON i.id = c.integration_id
  WHERE c.org_id IN (` + orgs + `)) AS rows`,
		"integration_sources": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', s.org_id, i.name, s.provider,
  s.source_type, s.external_id, s.name, s.full_name, CASE WHEN c.id IS NULL THEN s.metadata::text
  ELSE replace(s.metadata::text, c.id::text, 'CFG:' || c.name) END, s.is_enabled, s.discovered_at, s.last_seen_at,
  coalesce(s.last_sync_at::text, '<null>'), coalesce(s.last_sync_success::text, '<null>'), coalesce(s.last_sync_error, '<null>')) AS r
  FROM integration_sources s JOIN integrations i ON i.id = s.integration_id
  LEFT JOIN sync_configurations c ON c.integration_id = s.integration_id AND c.parent_id IS NULL AND c.planner_managed
  WHERE s.org_id IN (` + orgs + `)) AS rows`,
		"integration_datasets": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', d.org_id, i.name, d.dataset_key,
  d.is_enabled, d.options::text, coalesce(d.unavailable_reason, '<null>'), coalesce(d.unavailable_since::text, '<null>'),
  coalesce(d.unavailable_last_seen_at::text, '<null>')) AS r
  FROM integration_datasets d JOIN integrations i ON i.id = d.integration_id WHERE d.org_id IN (` + orgs + `)) AS rows`,
		"scheduled_jobs": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', j.org_id,
  replace(j.name, c.id::text, 'CFG:' || c.name), j.job_type, j.provider, j.schedule_cron, j.timezone,
  replace(j.job_config::text, c.id::text, 'CFG:' || c.name), c.name, j.status, j.is_running, j.run_count, j.failure_count,
  coalesce(j.last_run_at::text, '<null>'), coalesce(j.next_run_at::text, '<null>'), j.created_at, j.updated_at) AS r
  FROM scheduled_jobs j JOIN sync_configurations c ON c.id = j.sync_config_id WHERE j.org_id IN (` + orgs + `)) AS rows`,
	}
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integrations", "sync_configurations", "integration_sources", "integration_datasets", "scheduled_jobs"} {
		pythonRows, goRows := venueoracle.TableRows(t, ctx, source, queries[table]), venueoracle.TableRows(t, ctx, goDB, queries[table])
		if pythonRows != goRows {
			t.Errorf("%s rows differ:\n python %s\n go     %s", table, pythonRows, goRows)
		}
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened on the Go plane: discovered Jira projects, the
	// PagerDuty account source, stamped PagerDuty config, pinned rows.
	state := venueoracle.TableRows(t, ctx, goDB, fmt.Sprintf(`SELECT
  (SELECT count(*) FROM integration_sources s JOIN integrations i ON i.id = s.integration_id WHERE i.name = 'jira all'),
  (SELECT count(*) FROM integration_sources WHERE source_type = 'account'),
  (SELECT count(*) FROM sync_configurations WHERE name = 'pd bad' AND NOT is_active),
  (SELECT count(*) FROM sync_configurations WHERE created_at = '%s')`, createVenuePinnedNow))
	if fields := strings.Fields(state); len(fields) != 4 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" || fields[3] == "0" {
		t.Errorf("writes not observed (discovered jira sources, pagerduty account source, stamped pagerduty config, pinned configs) = %s", state)
	}
	t.Logf("write state: %s", state)
}

// seedCreateVenue writes the orgs (A enterprise with the canonical incident
// feature switched off, B community, C team at its repo limit, D
// enterprise for the named divergence, E enterprise with the feature on
// for the PagerDuty operational target), their users and memberships, and
// org A's Jira and org E's PagerDuty credentials (Jira encrypted by the Python
// plane's encrypt_value, its base URL the fake Jira).
func seedCreateVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, ids createVenueIDs, jiraURL string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'create-a', 'create-a', '{}', 'enterprise', true, $5, $5), ($2, 'create-b', 'create-b', '{}', 'community', true, $5, $5),
($3, 'create-c', 'create-c', '{}', 'team', true, $5, $5), ($4, 'create-d', 'create-d', '{}', 'enterprise', true, $5, $5),
($6, 'create-e', 'create-e', '{}', 'enterprise', true, $5, $5)`,
		ids.orgA, ids.orgB, ids.orgC, ids.orgD, at, ids.orgE)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{ids.adminA, ids.orgA, "create-admin-a@example.com", "admin"}, {ids.memberA, ids.orgA, "create-member-a@example.com", "member"},
		{ids.ownerB, ids.orgB, "create-owner-b@example.com", "owner"}, {ids.adminC, ids.orgC, "create-admin-c@example.com", "admin"},
		{ids.adminD, ids.orgD, "create-admin-d@example.com", "admin"}, {ids.adminE, ids.orgE, "create-admin-e@example.com", "admin"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, false, $3, $3 FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, uuid.New(), ids.orgA, at)

	calls := []venueoracle.PythonCall{}
	for _, payload := range []map[string]any{
		{"email": "venue@example.com", "api_token": "good-token", "base_url": jiraURL},
		{"email": "venue@example.com", "api_token": "bad-token", "base_url": jiraURL},
	} {
		encoded, _ := json.Marshal(payload)
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	}
	var ciphertexts []string
	for _, raw := range venue.CallPython(t, calls...) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		ciphertexts = append(ciphertexts, text)
	}
	credential := func(id uuid.UUID, provider, name string, active bool, ciphertext any, config string) {
		org := ids.orgA
		if provider == "pagerduty" {
			org = ids.orgE
		}
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $8)`, id, org.String(), provider, name, active, ciphertext, config, at)
	}
	credential(ids.credJira, "jira", "jira good", true, ciphertexts[0], `{}`)
	credential(ids.credJiraBad, "jira", "jira bad", true, ciphertexts[1], `{}`)
	credential(ids.credJiraInactive, "jira", "jira inactive", false, ciphertexts[0], `{}`)
	credential(ids.credPD, "pagerduty", "pd good", true, nil, `{"account_id": " acct-1 ", "subdomain": "venue"}`)
	credential(ids.credPDBad, "pagerduty", "pd bad", true, nil, `{"subdomain": "venue"}`)

	// Org C (team: max_repos 10) at its limit: one planner parent with ten
	// enabled sources.
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', 'c existing', '{}', true, $3, $3)`, ids.intC, ids.orgC.String(), at)
	configC := uuid.New()
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, created_at, updated_at) VALUES ($1, $2, 'c existing', 'github', '["git"]', '{"all_repos": true}', true, true, $3, $4, $4)`,
		configC, ids.orgC.String(), ids.intC, at)
	for n := 0; n < 10; n++ {
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
is_enabled, discovered_at, last_seen_at) VALUES ($1, $2, $3, 'github', 'repo', $4, $4, $4, $5::json, true, $6, $6)`,
			uuid.New(), ids.orgC.String(), ids.intC, fmt.Sprintf("acme/r%d", n),
			fmt.Sprintf(`{"planner_managed_sync_config_id": "%s"}`, configC), at)
	}
}
