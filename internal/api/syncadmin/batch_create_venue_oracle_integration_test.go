//go:build integration

package syncadmin_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// batchVenueKey is the SETTINGS_ENCRYPTION_KEY both planes share.
const batchVenueKey = "venue-sync-batch-create-settings-encryption-key"

// batchPinnedNow is the instant both planes read as now.
const batchPinnedNow = "2026-09-24T12:34:56.123456+00:00"

type batchVenueIDs struct {
	orgA, orgB, adminA, memberA, ownerB                         uuid.UUID
	credGL, credNoToken, credConfigURL, credBroken, credInvalid uuid.UUID
}

func newBatchVenueIDs() batchVenueIDs {
	var ids batchVenueIDs
	for _, target := range []*uuid.UUID{&ids.orgA, &ids.orgB, &ids.adminA, &ids.memberA, &ids.ownerB,
		&ids.credGL, &ids.credNoToken, &ids.credConfigURL, &ids.credBroken, &ids.credInvalid} {
		*target = uuid.New()
	}
	return ids
}

// TestSyncConfigBatchCreateVenueOracle sends POST
// /api/v1/admin/sync-configs/batch to the real Python api and the Go api
// over two copies of one seeded database, on one pinned clock and one fake
// GitLab serving recorded gitlab.com responses, and requires the same
// answers and then the same rows in integrations, sync_configurations,
// integration_sources, integration_datasets and scheduled_jobs. Row ids
// (uuid4 on each plane) are replaced by the names they point at.
func TestSyncConfigBatchCreateVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-sync-config-batch!"
	ids := newBatchVenueIDs()
	gitlab := fakeGitLab(t)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"SETTINGS_ENCRYPTION_KEY=" + batchVenueKey,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
			"VENUE_PINNED_NOW=" + batchPinnedNow,
			"VENUE_PINNED_NOW_MODULES=dev_health_ops.api.admin.routers.sync,dev_health_ops.models.settings," +
				"dev_health_ops.models.integrations,dev_health_ops.sync.pagerduty_repair",
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seedBatchVenue(t, ctx, admin, venue, ids, gitlab.URL)
			return map[string]map[string]any{
				"adminA":  {"user_id": ids.adminA.String(), "email": "batch-admin-a@example.com", "org_id": ids.orgA.String(), "role": "admin"},
				"memberA": {"user_id": ids.memberA.String(), "email": "batch-member-a@example.com", "org_id": ids.orgA.String(), "role": "member"},
				"ownerB":  {"user_id": ids.ownerB.String(), "email": "batch-owner-b@example.com", "org_id": ids.orgB.String(), "role": "owner"},
			}
		},
	})
	pinned, err := time.Parse(time.RFC3339Nano, batchPinnedNow)
	if err != nil {
		t.Fatal(err)
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(batchVenueKey), "")
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
	post := func(name, body string, headers map[string]string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/admin/sync-configs/batch", Headers: headers, Body: venueoracle.B64(body)}
	}
	gl, noToken, configURL, broken, invalid := ids.credGL.String(), ids.credNoToken.String(), ids.credConfigURL.String(),
		ids.credBroken.String(), ids.credInvalid.String()
	gitlabBody := func(name, credential, options, repos string) string {
		cred := ""
		if credential != "" {
			cred = `"credential_id":"` + credential + `",`
		}
		return `{"name":"` + name + `","provider":"gitlab",` + cred + `"sync_targets":["git","prs"],"sync_options":` + options + `,"repos":` + repos + `}`
	}
	maven := `{"group":"gitlab-examples/maven"}`
	requests := []venueoracle.Request{
		{Name: "no token", Method: "POST", Path: "/api/v1/admin/sync-configs/batch",
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"name":"x","provider":"github"}`)},
		post("member", `{"name":"x","provider":"github"}`, bearer("memberA")),
		post("invalid json", `{"name":`, a),
		post("not an object", `["x"]`, a),
		post("name and provider missing", `{}`, a),
		post("name empty", `{"name":"","provider":"github"}`, a),
		post("wrong field types", `{"name":"x","provider":"github","sync_targets":"git","sync_options":[],"repos":"a","credential_id":5,"schedule_cron":1,"timezone":[],"initial_sync_depth":"abc"}`, a),
		post("repos item not a str", `{"name":"x","provider":"github","repos":["a",1]}`, a),
		post("null defaulted fields", `{"name":"gh nulls","provider":"github","sync_targets":null,"sync_options":null,"repos":null}`, a),
		post("gated target", `{"name":"gated","provider":"github","sync_targets":["incidents"],"repos":["a"]}`, a),
		post("github repos", `{"name":"gh batch","provider":"GitHub","sync_targets":["git","prs","work-items"],"sync_options":{"owner":"acme","repo":"dropped","x":1},"repos":["one","acme/two","other-org/three"]}`, a),
		post("github no owner", `{"name":"gh no owner","provider":"github","sync_targets":["git"],"repos":["solo"]}`, a),
		post("github group as owner", `{"name":"gh group","provider":"github","sync_targets":["git"],"sync_options":{"group":"grp"},"repos":["g1"]}`, a),
		post("github no repos", `{"name":"gh empty","provider":"github","sync_targets":["git"],"sync_options":{"owner":"acme"},"repos":[]}`, a),
		post("github scheduled", `{"name":"gh cron","provider":"github","sync_targets":["git"],"sync_options":{"owner":"acme"},"repos":["c1"],"schedule_cron":"0 */6 * * *","timezone":"Europe/Paris","initial_sync_depth":30}`, a),
		post("github duplicate repo", `{"name":"gh dup","provider":"github","sync_targets":["git"],"sync_options":{"owner":"acme"},"repos":["dup","dup"]}`, a),
		post("jira repos", `{"name":"jira batch","provider":"jira","sync_targets":["work-items"],"sync_options":{"owner":"ENG"},"repos":["ENG","OPS"]}`, a),
		post("linear repos", `{"name":"linear batch","provider":"linear","sync_targets":["work-items"],"repos":["team-1"]}`, a),
		post("malformed auto-import", `{"name":"ai bad","provider":"github","sync_options":{"auto_import_teams":"yes"},"repos":["a"]}`, a),
		post("unsupported auto-import", `{"name":"ai unsupported","provider":"GitHub","sync_options":{"auto_import_projects":true},"repos":["a"]}`, a),
		post("gitlab names, path, ids", gitlabBody("gl batch", gl, maven, `["simple-maven-dep","Simple Maven Example","gitlab-examples/maven/simple-maven-app","999"," 12 "]`), a),
		post("gitlab unknown name", gitlabBody("gl unknown", gl, maven, `["nope","simple-maven-dep"]`), a),
		post("gitlab ambiguous name", gitlabBody("gl ambiguous", gl, `{"group":"dupgroup"}`, `["Simple Maven Example"]`), a),
		post("gitlab empty group", gitlabBody("gl empty group", gl, `{"group":"gitlab-examples/ops"}`, `["anything"]`), a),
		post("gitlab missing group", gitlabBody("gl missing group", gl, `{"owner":"gitlab-examples/no-such-group-for-venue-test"}`, `["x"]`), a),
		post("gitlab ids with failed listing", gitlabBody("gl ids no listing", gl, `{"owner":"gitlab-examples/no-such-group-for-venue-test"}`, `["31","32"]`), a),
		post("gitlab invalid token", gitlabBody("gl invalid token", invalid, maven, `["simple-maven-dep"]`), a),
		post("gitlab no credential, name", gitlabBody("gl no cred name", "", maven, `["simple-maven-dep"]`), a),
		post("gitlab no credential, ids", gitlabBody("gl no cred ids", "", maven, `["5"," 6 "]`), a),
		post("gitlab no group, name", gitlabBody("gl no group", gl, `{}`, `["simple-maven-dep"]`), a),
		post("gitlab no group, ids", gitlabBody("gl no group ids", gl, `{}`, `["41"]`), a),
		post("gitlab no token, name", gitlabBody("gl no token", noToken, maven, `["simple-maven-dep"]`), a),
		post("gitlab no token, id", gitlabBody("gl no token id", noToken, maven, `["7"]`), a),
		post("gitlab url from credential config", gitlabBody("gl config url", configURL, `{"owner":"gitlab-examples/maven"}`, `["simple-maven-app","3467553"]`), a),
		post("gitlab url from options", gitlabBody("gl option url", "", `{"group":"acme","gitlab_url":" https://gitlab.example.test "}`, `["8"]`), a),
		post("gitlab credential not decryptable", gitlabBody("gl broken", broken, maven, `["simple-maven-dep"]`), a),
		post("gitlab unknown credential", gitlabBody("gl unknown cred", uuid.NewString(), maven, `["simple-maven-dep"]`), a),
		post("gitlab no repos", gitlabBody("gl none", gl, maven, `[]`), a),
		post("gitlab credential id not a uuid", gitlabBody("gl bad cred id", "nope", maven, `["simple-maven-dep"]`), a),
		post("gitlab empty credential id, ids", `{"name":"gl empty cred id","provider":"gitlab","credential_id":"","sync_targets":["git"],"sync_options":{"group":"gitlab-examples/maven"},"repos":["9"]}`, a),
		post("gitlab duplicate names", gitlabBody("gl dup names", gl, maven, `["simple-maven-dep","simple-maven-dep"]`), a),
		post("gitlab name and its id", gitlabBody("gl name and id", gl, maven, `["simple-maven-dep","3467553"]`), a),
		post("pagerduty batch", `{"name":"pd batch","provider":"pagerduty","sync_targets":["operational"],"repos":["svc"]}`, a),
		post("initial depth float", `{"name":"gh depth float","provider":"github","sync_options":{"owner":"acme"},"repos":["f1"],"initial_sync_depth":29.9}`, a),
		post("initial depth integral float", `{"name":"gh depth whole","provider":"github","sync_options":{"owner":"acme"},"repos":["f2"],"initial_sync_depth":30.0}`, a),
		post("gitlab page above int32", gitlabBody("gl overflow", gl, `{"group":"overflow"}`, `["target"]`), a),
		post("over the repo limit", `{"name":"b over","provider":"github","sync_options":{"owner":"acme"},"repos":["a","b","c","d","e"]}`, b),
		post("at the repo limit", `{"name":"b at","provider":"github","sync_options":{"owner":"acme"},"repos":["a","b","c"]}`, b),
		post("past the limit after", `{"name":"b past","provider":"github","sync_options":{"owner":"acme"},"repos":["z"]}`, b),
		{Name: "list after", Method: "GET", Path: "/api/v1/admin/sync-configs", Headers: a},
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			return uuidPattern.ReplaceAllString(body, `"id":"<uuid4>"`)
		},
	})
	t.Logf("receipt (%d requests):\n%s", len(requests), receipt)

	orgs := fmt.Sprintf("'%s','%s'", ids.orgA, ids.orgB)
	queries := createVenueRowQueries(orgs, "true")
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integrations", "sync_configurations", "integration_sources", "integration_datasets", "scheduled_jobs"} {
		pythonRows, goRows := venueoracle.TableRows(t, ctx, source, queries[table]), venueoracle.TableRows(t, ctx, goDB, queries[table])
		if pythonRows != goRows {
			t.Errorf("%s rows differ:\n python %s\n go     %s", table, pythonRows, goRows)
		}
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened on the Go plane: GitLab sources resolved through
	// the listing, a stamped self-hosted gitlab_url, configs on the pinned
	// clock.
	state := venueoracle.TableRows(t, ctx, goDB, fmt.Sprintf(`SELECT
  (SELECT count(*) FROM integration_sources WHERE provider = 'gitlab' AND metadata::text LIKE '%%gitlab-examples/maven/%%'),
  (SELECT count(*) FROM sync_configurations WHERE sync_options::text LIKE '%%"gitlab_url"%%'),
  (SELECT count(*) FROM sync_configurations WHERE created_at = '%s')`, batchPinnedNow))
	if fields := strings.Fields(state); len(fields) != 3 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" {
		t.Errorf("writes not observed (resolved gitlab sources, stamped gitlab_url, pinned configs) = %s", state)
	}
	t.Logf("write state: %s", state)

	// Named divergence (CHAOS-6719): Python's batch create runs no backfill
	// or schedule check, so it stores an out-of-range cron, an unknown
	// timezone or an over-tier depth on an active config,
	// and answers 201. The Go batch create runs the single create's checks
	// first and answers exactly as the Go single create does for the same
	// body, persisting nothing. These run after the row comparison because
	// the Python plane writes rows the Go plane (by design) does not.
	divergent := []struct {
		name, body string
		headers    map[string]string
		status     int
	}{
		{"out-of-range cron", `{"name":"gh cron 60","provider":"github","sync_options":{"owner":"acme","all_repos":true},"repos":["c60"],"schedule_cron":"60 * * * *"}`, a, 422},
		{"croniter-only cron and unknown timezone", `{"name":"gh sched","provider":"github","sync_targets":["git"],"sync_options":{"owner":"acme","all_repos":true},"repos":["s1"],"schedule_cron":"@hourly","timezone":"Mars/Base","initial_sync_depth":"9999"}`, a, 422},
		{"unknown timezone", `{"name":"gh tz","provider":"github","sync_options":{"owner":"acme","all_repos":true},"repos":["t1"],"schedule_cron":"0 * * * *","timezone":"Mars/Base"}`, a, 422},
	}
	batchRequests := make([]venueoracle.Request, len(divergent))
	for index, c := range divergent {
		batchRequests[index] = post("divergent: "+c.name, c.body, c.headers)
	}
	pythonDivergent := venue.ServePython(t, batchRequests)
	for index, c := range divergent {
		if pythonDivergent[index].Status != http.StatusCreated {
			t.Errorf("%s: python batch status %d, want 201 (the divergence this pins)", c.name, pythonDivergent[index].Status)
		}
		batch := venueoracle.Do(t, base, batchRequests[index])
		single := venueoracle.Do(t, base, venueoracle.Request{Name: "single: " + c.name, Method: "POST",
			Path: "/api/v1/admin/sync-configs", Headers: c.headers, Body: venueoracle.B64(c.body)})
		if batch.Status != c.status || batch.Status != single.Status || batch.Body != single.Body {
			t.Errorf("%s: go batch %d %s, go single create %d %s, want both %d and identical", c.name,
				batch.Status, batch.Body, single.Status, single.Body, c.status)
		}
		var persisted int
		name := strings.SplitN(strings.TrimPrefix(c.body, `{"name":"`), `"`, 2)[0]
		if err := pgxQueryInt(ctx, goDB, `SELECT count(*) FROM sync_configurations WHERE name = $1`, name, &persisted); err != nil {
			t.Fatal(err)
		}
		if persisted != 0 {
			t.Errorf("%s: go persisted %d sync configurations named %q, want none", c.name, persisted, name)
		}
		t.Logf("divergent %s: python=%d go=%d (single create %d), go persisted nothing", c.name, pythonDivergent[index].Status, batch.Status, single.Status)
	}
}

func pgxQueryInt(ctx context.Context, uri, sql string, arg any, out *int) error {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return conn.QueryRow(ctx, sql, arg).Scan(out)
}

// seedBatchVenue writes org A (enterprise, the canonical incident feature
// off) and org B (community), their users, and org A's GitLab credentials,
// encrypted by the Python plane's encrypt_value, pointing at the fake
// GitLab.
func seedBatchVenue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, ids batchVenueIDs, gitlabURL string) {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES
($1, 'batch-a', 'batch-a', '{}', 'enterprise', true, $3, $3), ($2, 'batch-b', 'batch-b', '{}', 'community', true, $3, $3)`, ids.orgA, ids.orgB, at)
	for _, user := range []struct {
		id, org uuid.UUID
		email   string
		role    string
	}{{ids.adminA, ids.orgA, "batch-admin-a@example.com", "admin"}, {ids.memberA, ids.orgA, "batch-member-a@example.com", "member"},
		{ids.ownerB, ids.orgB, "batch-owner-b@example.com", "owner"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, $3, $3)`, user.id, user.email, at)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			uuid.New(), user.id, user.org, user.role, at)
	}
	exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, created_at, updated_at)
SELECT $1, $2, id, false, $3, $3 FROM feature_flags WHERE key = 'canonical_incident_ingestion'`, uuid.New(), ids.orgA, at)

	var calls []venueoracle.PythonCall
	for _, payload := range []map[string]any{
		{"token": "tok-a", "url": gitlabURL},
		{"url": gitlabURL},
		{"token": "tok-c"},
		{"token": invalidGitLabToken, "url": gitlabURL},
	} {
		encoded, _ := json.Marshal(payload)
		calls = append(calls, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}})
	}
	var texts []string
	for _, raw := range venue.CallPython(t, calls...) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		texts = append(texts, text)
	}
	credential := func(id uuid.UUID, name string, ciphertext any, config string) {
		exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'gitlab', $3, true, $4, $5::json, $6, $6)`, id, ids.orgA.String(), name, ciphertext, config, at)
	}
	credential(ids.credGL, "gl", texts[0], `{}`)
	credential(ids.credNoToken, "gl-no-token", texts[1], `{}`)
	credential(ids.credConfigURL, "gl-config-url", texts[2], `{"url": "`+gitlabURL+`"}`)
	credential(ids.credInvalid, "gl-invalid-token", texts[3], `{}`)
	credential(ids.credBroken, "gl-broken", base64.StdEncoding.EncodeToString([]byte("not a fernet token")), `{}`)
}
