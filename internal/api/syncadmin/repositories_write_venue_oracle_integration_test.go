//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

// repositoriesVenueKey is the SETTINGS_ENCRYPTION_KEY both planes share:
// the Python plane's encrypt_value writes the seeded credentials with it.
const repositoriesVenueKey = "venue-sync-repositories-settings-encryption-key"

// repositoriesPinnedNow is the instant both planes read as now: the Python
// modules that write timestamps through datetime.now (the router, and the
// models' defaults and onupdate hooks) and the Go api's injected clock.
const repositoriesPinnedNow = "2026-09-01T12:34:56.123456+00:00"

// invalidGitLabToken is the fixed test string the invalid_token recording
// was captured with: not a credential.
const invalidGitLabToken = "invalid-venue-test-token-not-a-credential"

// recordedGitLab is one response recorded from gitlab.com
// (testdata/gitlab_recorded, see its README): status, the headers the
// listing reads, and the raw body.
type recordedGitLab struct {
	status  int
	headers [][2]string
	body    []byte
}

func loadRecordedGitLab(t *testing.T, name string) recordedGitLab {
	t.Helper()
	dir := filepath.Join("testdata", "gitlab_recorded")
	read := func(suffix string) []byte {
		data, err := os.ReadFile(filepath.Join(dir, name+suffix))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var out recordedGitLab
	if _, err := fmt.Sscanf(strings.TrimSpace(string(read(".status"))), "%d", &out.status); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(read(".headers"))), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			out.headers = append(out.headers, [2]string{key, strings.TrimSpace(value)})
		}
	}
	out.body = read(".body")
	return out
}

// fakeGitLab serves the recorded gitlab.com responses to both planes, by
// group, page and token: the two-page gitlab-examples/maven group, the
// empty gitlab-examples/ops group, 404 for any other group, 401 for the
// recorded invalid token. dupgroup is the maven recording's first page with
// its second project renamed to the first one's name (a modified
// recording: gitlab.com has no public group with two same-name projects to
// record), for the ambiguous-name answer.
func fakeGitLab(t *testing.T) *httptest.Server {
	t.Helper()
	page1, page2 := loadRecordedGitLab(t, "maven_page1"), loadRecordedGitLab(t, "maven_page2")
	empty, missing, invalid := loadRecordedGitLab(t, "empty_group"), loadRecordedGitLab(t, "missing_group"), loadRecordedGitLab(t, "invalid_token")
	var projects []map[string]any
	if err := json.Unmarshal(page1.body, &projects); err != nil || len(projects) < 2 {
		t.Fatalf("recorded page: %v", err)
	}
	projects[1]["name"] = projects[0]["name"]
	dupBody, err := json.Marshal(projects)
	if err != nil {
		t.Fatal(err)
	}
	dup := recordedGitLab{status: 200, headers: [][2]string{{"content-type", "application/json"}, {"x-next-page", ""}}, body: dupBody}
	serve := func(w http.ResponseWriter, response recordedGitLab) {
		for _, header := range response.headers {
			w.Header().Add(header[0], header[1])
		}
		w.WriteHeader(response.status)
		_, _ = w.Write(response.body)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		group, _ := url.PathUnescape(strings.TrimSuffix(strings.TrimPrefix(r.URL.EscapedPath(), "/api/v4/groups/"), "/projects"))
		if r.Header.Get("PRIVATE-TOKEN") == invalidGitLabToken {
			serve(w, invalid)
			return
		}
		switch {
		case group == "gitlab-examples/maven" && r.URL.Query().Get("page") == "2":
			serve(w, page2)
		case group == "gitlab-examples/maven":
			serve(w, page1)
		case group == "gitlab-examples/ops":
			serve(w, empty)
		case group == "dupgroup":
			serve(w, dup)
		default:
			serve(w, missing)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

type repositoriesIDs struct {
	orgA, orgB, adminA, memberA, ownerB                                        uuid.UUID
	credGL, credNoToken, credConfigURL, credBroken, credInvalid                uuid.UUID
	intGH, intGH2, intGH3, intGH4, intGL, intGL2, intGL3, intGL4, intGL5, intB uuid.UUID
	intJira, intGL6, intGH5                                                    uuid.UUID
	cfgGH, cfgGL, cfgGLNoCred, cfgGLNoToken, cfgGLConfigURL, cfgGLBroken       uuid.UUID
	cfgGLInvalidToken, cfgInfinity                                             uuid.UUID
	cfgLegacy, cfgJira, cfgGated, cfgBadTargets, cfgPairs, cfgEmptyName, cfgB  uuid.UUID
	seededSources                                                              []uuid.UUID
}

func newRepositoriesIDs() repositoriesIDs {
	var ids repositoriesIDs
	for _, target := range []*uuid.UUID{&ids.orgA, &ids.orgB, &ids.adminA, &ids.memberA, &ids.ownerB, &ids.credGL, &ids.credNoToken,
		&ids.credConfigURL, &ids.credBroken, &ids.intGH, &ids.intGH2, &ids.intGH3, &ids.intGH4, &ids.intGL, &ids.intGL2, &ids.intGL3,
		&ids.intGL4, &ids.intGL5, &ids.intB, &ids.intJira, &ids.cfgGH, &ids.cfgGL, &ids.cfgGLNoCred, &ids.cfgGLNoToken, &ids.cfgGLConfigURL,
		&ids.cfgGLBroken, &ids.cfgLegacy, &ids.cfgJira, &ids.cfgGated, &ids.cfgBadTargets, &ids.cfgPairs, &ids.cfgEmptyName, &ids.cfgB, &ids.credInvalid, &ids.intGL6, &ids.cfgGLInvalidToken, &ids.intGH5, &ids.cfgInfinity} {
		*target = uuid.New()
	}
	return ids
}

// TestSyncConfigRepositoriesVenueOracle sends one PUT
// /sync-configs/{id}/repositories sequence to the real Python api and the
// Go api, over two copies of one seeded database, both reading the pinned
// clock and one fake GitLab, and requires byte-identical answers, then
// identical rows (raw text) in integration_sources, sync_configurations and
// sync_coverage_projections. Only the ids of newly created source rows and
// the projections' database-clock invalidated_at and updated_at (both
// now() of each plane's own transaction) are compared by shape: set past
// the seed on both planes, or unchanged on both.
func TestSyncConfigRepositoriesVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-admin-reads-32b!"
	ids := newRepositoriesIDs()
	gitlab := fakeGitLab(t)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + repositoriesVenueKey,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
			"VENUE_PINNED_NOW=" + repositoriesPinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.api.admin.routers.sync,dev_health_ops.models.settings,dev_health_ops.models.integrations",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedRepositories(t, ctx, admin, venue, &ids, gitlab.URL)
			return map[string]map[string]any{
				"adminA":  {"user_id": ids.adminA.String(), "email": "repo-admin-a@example.com", "org_id": ids.orgA.String(), "role": "admin"},
				"memberA": {"user_id": ids.memberA.String(), "email": "repo-member-a@example.com", "org_id": ids.orgA.String(), "role": "member"},
				"ownerB":  {"user_id": ids.ownerB.String(), "email": "repo-owner-b@example.com", "org_id": ids.orgB.String(), "role": "owner"},
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, repositoriesPinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(repositoriesVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	base := startGoServerWith(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Now = func() time.Time { return pinned }
		deps.Decryptor = decryptor
	})

	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	a, b := bearer("adminA"), bearer("ownerB")
	put := func(name string, id uuid.UUID, body string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "PUT", Path: "/api/v1/admin/sync-configs/" + id.String() + "/repositories",
			Headers: headers, Body: venueoracle.B64(body)}
	}
	requests := []venueoracle.Request{
		{Name: "no token", Method: "PUT", Path: "/api/v1/admin/sync-configs/" + ids.cfgGH.String() + "/repositories",
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"owner":"acme"}`)},
		put("member", ids.cfgGH, `{"owner":"acme"}`, bearer("memberA")),
		put("invalid json", ids.cfgGH, `{"owner":`, a),
		put("not an object", ids.cfgGH, `["acme"]`, a),
		put("owner missing", ids.cfgGH, `{}`, a),
		put("owner empty", ids.cfgGH, `{"owner":""}`, a),
		put("repos not a list", ids.cfgGH, `{"owner":"acme","repos":"a"}`, a),
		put("repos item not a str", ids.cfgGH, `{"owner":"acme","repos":["a",1]}`, a),
		put("unknown config", uuid.New(), `{"owner":"acme"}`, a),
		{Name: "config id not a uuid", Method: "PUT", Path: "/api/v1/admin/sync-configs/not-a-uuid/repositories", Headers: a, Body: venueoracle.B64(`{"owner":"acme"}`)},
		put("jira config", ids.cfgJira, `{"owner":"acme","repos":["x"]}`, a),
		put("legacy config", ids.cfgLegacy, `{"owner":"acme","repos":["x"]}`, a),
		put("gated targets", ids.cfgGated, `{"owner":"acme","repos":["x"]}`, a),
		put("non-str target", ids.cfgBadTargets, `{"owner":"acme","repos":["x"]}`, a),
		put("empty config name", ids.cfgEmptyName, `{"owner":"acme","repos":["x"]}`, a),
		put("github select", ids.cfgGH, `{"owner":"acme","repos":["one","acme/three","new-org/four","one"]}`, a),
		put("github narrow", ids.cfgGH, `{"owner":"acme","repos":["acme/one"]}`, a),
		put("github same again", ids.cfgGH, `{"owner":"acme","repos":["acme/one"]}`, a),
		put("github new owner, none", ids.cfgGH, `{"owner":"other","repos":[]}`, a),
		put("github duplicate new", ids.cfgGH, `{"owner":"acme","repos":["dup","dup"]}`, a),
		put("stored pairs options", ids.cfgPairs, `{"owner":"old","repos":["a"]}`, a),
		put("stored infinite option, unchanged", ids.cfgInfinity, `{"owner":"acme","repos":[]}`, a),
		put("stored infinite option, new owner", ids.cfgInfinity, `{"owner":"other","repos":[]}`, a),
		put("gitlab names, path, ids", ids.cfgGL, `{"owner":"gitlab-examples/maven","repos":["simple-maven-dep","Simple Maven Example","gitlab-examples/maven/simple-maven-app","999"," 12 "]}`, a),
		put("gitlab unknown name", ids.cfgGL, `{"owner":"gitlab-examples/maven","repos":["nope","simple-maven-dep"]}`, a),
		put("gitlab ambiguous name", ids.cfgGL, `{"owner":"dupgroup","repos":["Simple Maven Example"]}`, a),
		put("gitlab empty group", ids.cfgGL, `{"owner":"gitlab-examples/ops","repos":["anything"]}`, a),
		put("gitlab missing group", ids.cfgGL, `{"owner":"gitlab-examples/no-such-group-for-venue-test","repos":["x"]}`, a),
		put("gitlab ids with failed listing", ids.cfgGL, `{"owner":"gitlab-examples/no-such-group-for-venue-test","repos":["31","32"]}`, a),
		put("gitlab invalid token", ids.cfgGLInvalidToken, `{"owner":"gitlab-examples/maven","repos":["simple-maven-dep"]}`, a),
		put("gitlab none", ids.cfgGL, `{"owner":"gitlab-examples/maven","repos":[]}`, a),
		put("gitlab no credential, name", ids.cfgGLNoCred, `{"owner":"gitlab-examples/maven","repos":["simple-maven-dep"]}`, a),
		put("gitlab no credential, ids", ids.cfgGLNoCred, `{"owner":"gitlab-examples/maven","repos":["5"," 6 "]}`, a),
		put("gitlab no token, name", ids.cfgGLNoToken, `{"owner":"gitlab-examples/maven","repos":["simple-maven-dep"]}`, a),
		put("gitlab no token, id", ids.cfgGLNoToken, `{"owner":"gitlab-examples/maven","repos":["7"]}`, a),
		put("gitlab url from config", ids.cfgGLConfigURL, `{"owner":"gitlab-examples/maven","repos":["simple-maven-app","3467553"]}`, a),
		put("gitlab credential not decryptable", ids.cfgGLBroken, `{"owner":"gitlab-examples/maven","repos":["simple-maven-dep"]}`, a),
		put("over the repo limit", ids.cfgB, `{"owner":"acme","repos":["a","b","c","d","e"]}`, b),
		put("at the repo limit", ids.cfgB, `{"owner":"acme","repos":["a","b","c"]}`, b),
		{Name: "github after", Method: "GET", Path: "/api/v1/admin/sync-configs/" + ids.cfgGH.String() + "/repositories", Headers: a},
		{Name: "gitlab after", Method: "GET", Path: "/api/v1/admin/sync-configs/" + ids.cfgGL.String() + "/repositories", Headers: a},
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	seeded := make([]string, 0, len(ids.seededSources))
	for _, id := range ids.seededSources {
		seeded = append(seeded, "'"+id.String()+"'")
	}
	queries := map[string]string{
		"integration_sources": `SELECT coalesce(string_agg(row_text, E'\n' ORDER BY row_text), '') FROM (
SELECT concat_ws(' | ', CASE WHEN id IN (` + strings.Join(seeded, ", ") + `) THEN id::text ELSE 'new:' || (id IS NOT NULL)::text END,
  org_id, integration_id, provider, source_type, external_id, name, full_name, metadata::text, is_enabled,
  discovered_at, last_seen_at, coalesce(last_sync_at::text, '<null>'), coalesce(last_sync_success::text, '<null>'),
  coalesce(last_sync_error, '<null>')) AS row_text FROM integration_sources) AS rows`,
		"sync_configurations": `SELECT coalesce(string_agg(t::text, E'\n' ORDER BY t::text), '') FROM sync_configurations t`,
		"sync_coverage_projections": `SELECT coalesce(string_agg(row_text, E'\n' ORDER BY row_text), '') FROM (
SELECT concat_ws(' | ', id, org_id, sync_config_id, payload::text, CASE WHEN invalidated_at IS NULL THEN 'valid'
  WHEN invalidated_at > '2026-06-01' THEN 'invalidated' ELSE invalidated_at::text END, created_at,
  CASE WHEN updated_at > '2026-06-01' THEN 'moved' ELSE updated_at::text END) AS row_text
FROM sync_coverage_projections) AS rows`,
	}
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integration_sources", "sync_configurations", "sync_coverage_projections"} {
		python, goRows := venueoracle.TableRows(t, ctx, source, queries[table]), venueoracle.TableRows(t, ctx, goDB, queries[table])
		if python != goRows {
			t.Errorf("%s rows differ:\n python %s\n go     %s", table, python, goRows)
		}
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened: the pinned instant reached the rows, a coverage
	// projection was invalidated, and rows were created.
	state := venueoracle.TableRows(t, ctx, goDB, fmt.Sprintf(`SELECT
  (SELECT count(*) FROM integration_sources WHERE last_seen_at = '%s'),
  (SELECT count(*) FROM integration_sources WHERE id NOT IN (%s)),
  (SELECT count(*) FROM sync_coverage_projections WHERE invalidated_at IS NOT NULL),
  (SELECT count(*) FROM sync_configurations WHERE updated_at = '%s')`,
		repositoriesPinnedNow, strings.Join(seeded, ", "), repositoriesPinnedNow))
	if fields := strings.Fields(state); len(fields) != 4 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" || fields[3] == "0" {
		t.Errorf("writes not observed (pinned last_seen_at, created sources, invalidated projections, pinned updated_at) = %s", state)
	}
	t.Logf("write state: %s", state)
}

// seedRepositories writes the orgs, credentials (encrypted by the Python
// plane's encrypt_value), integrations, configs, planner sources and
// coverage projections the repository-selection writes reach.
func seedRepositories(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, ids *repositoriesIDs, gitlabURL string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'repo-a', 'repo-a', '{}', 'enterprise', true, $3, $3), ($2, 'repo-b', 'repo-b', '{}', 'community', true, $3, $3)`, ids.orgA, ids.orgB, at)
	for _, user := range []struct {
		id    uuid.UUID
		email string
	}{{ids.adminA, "repo-admin-a@example.com"}, {ids.memberA, "repo-member-a@example.com"}, {ids.ownerB, "repo-owner-b@example.com"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
	}
	for _, member := range []struct {
		org, user uuid.UUID
		role      string
	}{{ids.orgA, ids.adminA, "admin"}, {ids.orgA, ids.memberA, "member"}, {ids.orgB, ids.ownerB, "owner"}} {
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), member.user, member.org, member.role, at)
	}
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, false, $3, $3 FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, uuid.New(), ids.orgA, at)

	secretsOf := []map[string]any{
		{"token": "tok-a", "url": gitlabURL},
		{"url": gitlabURL},
		{"token": "tok-c"},
		{"token": invalidGitLabToken, "url": gitlabURL},
	}
	var calls []venueoracle.PythonCall
	for _, payload := range secretsOf {
		encoded, _ := json.Marshal(payload)
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	}
	ciphertexts := venue.CallPython(t, calls...)
	credential := func(id uuid.UUID, name string, ciphertext any, config string) {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'gitlab', $3, true, $4, $5::json, $6, $6)`, id, ids.orgA.String(), name, ciphertext, config, at)
	}
	var texts []string
	for _, raw := range ciphertexts {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		texts = append(texts, text)
	}
	credential(ids.credGL, "gl", texts[0], `{}`)
	credential(ids.credNoToken, "gl-no-token", texts[1], `{}`)
	credential(ids.credConfigURL, "gl-config-url", texts[2], `{"url": "`+gitlabURL+`"}`)
	credential(ids.credInvalid, "gl-invalid-token", texts[3], `{}`)
	credential(ids.credBroken, "gl-broken", base64.StdEncoding.EncodeToString([]byte("not a fernet token")), `{}`)

	integration := func(id, org uuid.UUID, provider string, credentialID any) {
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}', true, $6, $6)`, id, org.String(), provider, credentialID, "int-"+id.String()[:8], at)
	}
	for _, spec := range []struct {
		id         uuid.UUID
		provider   string
		credential any
	}{{ids.intGH, "github", nil}, {ids.intGH2, "github", nil}, {ids.intGH3, "github", nil}, {ids.intGH4, "github", nil},
		{ids.intGL, "gitlab", ids.credGL}, {ids.intGL2, "gitlab", nil}, {ids.intGL3, "gitlab", ids.credNoToken},
		{ids.intGL4, "gitlab", ids.credConfigURL}, {ids.intGL5, "gitlab", ids.credBroken}, {ids.intJira, "jira", nil},
		{ids.intGL6, "gitlab", ids.credInvalid}, {ids.intGH5, "github", nil}} {
		integration(spec.id, ids.orgA, spec.provider, spec.credential)
	}
	integration(ids.intB, ids.orgB, "github", nil)

	config := func(id, org uuid.UUID, name, provider, targets, options string, integrationID any) {
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, $6::json, true, $7, $8, $9, $9)`,
			id, org.String(), name, provider, targets, options, integrationID != nil, integrationID, at)
	}
	config(ids.cfgGH, ids.orgA, "gh", "github", `["git", "prs"]`, `{"owner": "acme", "all_repos": true, "x": 1}`, ids.intGH)
	config(ids.cfgGL, ids.orgA, "gl", "gitlab", `["git"]`, `{"group": "acme", "gitlab_url": ""}`, ids.intGL)
	config(ids.cfgGLNoCred, ids.orgA, "gl-no-cred", "gitlab", `["git"]`, `{}`, ids.intGL2)
	config(ids.cfgGLNoToken, ids.orgA, "gl-no-token", "gitlab", `["git"]`, `{"group": "acme"}`, ids.intGL3)
	config(ids.cfgGLConfigURL, ids.orgA, "gl-config-url", "GitLab", `["git"]`, `{"owner": "acme"}`, ids.intGL4)
	config(ids.cfgGLBroken, ids.orgA, "gl-broken", "gitlab", `["git"]`, `{}`, ids.intGL5)
	config(ids.cfgGLInvalidToken, ids.orgA, "gl-invalid-token", "gitlab", `["git"]`, `{}`, ids.intGL6)
	// 1e400 is valid JSON that decodes to inf on both planes: an unchanged
	// selection must read the options as equal (inf == inf) and write nothing.
	config(ids.cfgInfinity, ids.orgA, "infinity", "github", `["git"]`, `{"owner": "acme", "limit": 1e400}`, ids.intGH5)
	config(ids.cfgLegacy, ids.orgA, "legacy", "github", `["git"]`, `{"owner": "acme"}`, nil)
	config(ids.cfgJira, ids.orgA, "jira", "jira", `["work-items"]`, `{}`, ids.intJira)
	config(ids.cfgGated, ids.orgA, "gated", "github", `["git", "Incidents"]`, `{}`, ids.intGH2)
	config(ids.cfgBadTargets, ids.orgA, "bad-targets", "github", `["git", 1]`, `{}`, ids.intGH2)
	config(ids.cfgPairs, ids.orgA, "pairs", "github", `["git"]`, `[["owner", "old"], ["all_repos", true]]`, ids.intGH3)
	config(ids.cfgEmptyName, ids.orgA, "", "github", `["git"]`, `{"owner": "acme"}`, ids.intGH4)
	config(ids.cfgB, ids.orgB, "b", "github", `["git"]`, `{"owner": "acme"}`, ids.intB)

	source := func(org, integrationID uuid.UUID, provider, sourceType, externalID, name, fullName, metadata string, enabled bool) {
		id := uuid.New()
		ids.seededSources = append(ids.seededSources, id)
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name,
metadata, is_enabled, discovered_at, last_seen_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json, $10, $11, $11)`,
			id, org.String(), integrationID, provider, sourceType, externalID, name, fullName, metadata, enabled, at)
	}
	gh := `{"planner_managed_sync_config_id": "` + ids.cfgGH.String() + `", "owner": "acme"}`
	// Equal to the metadata the write computes, in another key order: Python
	// compares dicts, not text, so it leaves this text as stored.
	source(ids.orgA, ids.intGH, "github", "repository", "acme/one", "one", "acme/one",
		`{"owner":"acme","planner_managed_sync_config_id":"`+ids.cfgGH.String()+`"}`, true)
	source(ids.orgA, ids.intGH, "github", "repository", "acme/two", "two", "acme/two", gh, false)
	source(ids.orgA, ids.intGH, "github", "repository", "acme/three", "3", "acme/three",
		`{"owner":"acme","planner_managed_sync_config_id":"`+ids.cfgGH.String()+`","extra":[1.0,true]}`, true)
	source(ids.orgA, ids.intGH, "github", "repository", "acme/other", "other", "acme/other",
		`{"planner_managed_sync_config_id": "`+uuid.NewString()+`"}`, true)
	source(ids.orgA, ids.intGH, "GitHub", "repository", "acme/case", "case", "acme/case", gh, true)
	gl := func(path string) string {
		return `{"path_with_namespace": "` + path + `", "planner_managed_sync_config_id": "` + ids.cfgGL.String() + `"}`
	}
	source(ids.orgA, ids.intGL, "gitlab", "project", "3467553", "simple-maven-dep", "gitlab-examples/maven/simple-maven-dep",
		gl("gitlab-examples/maven/simple-maven-dep"), true)
	source(ids.orgA, ids.intGL, "gitlab", "project", "99", "old", "gitlab-examples/maven/old", gl("gitlab-examples/maven/old"), true)
	b := `{"planner_managed_sync_config_id": "` + ids.cfgB.String() + `", "owner": "acme"}`
	for _, name := range []string{"x", "y", "z"} {
		source(ids.orgB, ids.intB, "github", "repository", "acme/"+name, name, "acme/"+name, b, true)
	}

	projection := func(org, configID uuid.UUID) {
		exec(`INSERT INTO sync_coverage_projections (id, org_id, sync_config_id, history_lookback_days, projection_version,
generated_at, source_updated_at, backfill_updated_at, invalidated_at, payload, created_at, updated_at)
VALUES ($1, $2, $3, 3650, 2, $4, $4, $4, NULL, '{}', $4, $4)`, uuid.New(), org.String(), configID, at)
	}
	projection(ids.orgA, ids.cfgGH)
	projection(ids.orgA, ids.cfgGL)
	projection(ids.orgA, ids.cfgLegacy)
}
