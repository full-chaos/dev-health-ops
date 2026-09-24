//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// credentialsVenueKey is the one SETTINGS_ENCRYPTION_KEY both planes share:
// Python seeds and reads ciphertext with it, Go writes with it.
const credentialsVenueKey = "venue-credentials-settings-encryption-key"

var credentialIDPattern = regexp.MustCompile(`"id":"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"`)

// credentialSeedRow is one row written before any request, ciphertext by the
// Python plane's own encrypt_value.
type credentialSeedRow struct {
	org                    uuid.UUID
	provider, name         string
	secrets                map[string]any
	config                 string // json text, "" = NULL
	isActive               bool
	lastTestSuccess        *bool
	lastTestError          string
	withoutEncryptedSecret bool
}

func credentialSeedRows(f venueFixture) []credentialSeedRow {
	ok := true
	return []credentialSeedRow{
		{org: f.orgA, provider: "github", name: "seeded", secrets: map[string]any{"token": "ghp_seed"}, config: `{"org": "acme", "zeta": 1, "alpha": [1, 2]}`, isActive: true, lastTestSuccess: &ok},
		{org: f.orgA, provider: "gitlab", name: "inactive", secrets: map[string]any{"token": "gl_seed"}, config: `{"url": "https://gitlab.example.test"}`, isActive: false, lastTestError: "boom"},
		{org: f.orgA, provider: "gitlab", name: "inactive2", secrets: map[string]any{"token": "gl_two"}, config: `{"url": "https://gl.example.test"}`, isActive: false},
		{org: f.orgA, provider: "gitlab", name: "noop-a", secrets: map[string]any{"token": "gl_noop"}, config: `{"a": 1}`, isActive: false},
		{org: f.orgA, provider: "github", name: "noop-b", secrets: map[string]any{"token": "gh_noop"}, config: `{"x": 1, "y": 2}`, isActive: true},
		{org: f.orgA, provider: "github", name: "eq-create", secrets: map[string]any{"token": "gh_eq"}, config: `{"org": "acme", "zeta": 1, "alpha": [1, 2]}`, isActive: true},
		{org: f.orgA, provider: "jira", name: "null-config", secrets: map[string]any{"email": "a@example.test", "api_token": "t"}, config: "", isActive: true},
		{org: f.orgA, provider: "jira", name: "null-config2", secrets: map[string]any{"email": "b@example.test", "api_token": "t"}, config: "", isActive: true},
		{org: f.orgA, provider: "github", name: "repos", secrets: map[string]any{"token": "gh_repos"}, config: `{"org": "shadowed"}`, isActive: true},
		// 1e400 is valid JSON (Postgres json keeps it) that decodes to inf.
		{org: f.orgA, provider: "github", name: "infinite", secrets: map[string]any{"token": "gh_inf"}, config: `{"limit": 1e400, "floor": -1e400}`, isActive: true},
		{org: f.orgA, provider: "pagerduty", name: "default", secrets: map[string]any{"auth_mode": "api_token", "api_token": "x", "subdomain": "s", "region": "us"}, config: `{"auth_mode": "api_token"}`, isActive: true},
		{org: f.orgB, provider: "github", name: "other-org", secrets: map[string]any{"token": "ghp_other"}, config: `{}`, isActive: true},
	}
}

func seedCredentials(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue, f venueFixture) {
	t.Helper()
	rows := credentialSeedRows(f)
	calls := make([]venueoracle.PythonCall, len(rows))
	for i, row := range rows {
		encoded, err := json.Marshal(row.secrets)
		if err != nil {
			t.Fatal(err)
		}
		calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{string(encoded)}}
	}
	results := venue.CallPython(t, calls...)
	for i, row := range rows {
		var ciphertext string
		if err := json.Unmarshal(results[i], &ciphertext); err != nil {
			t.Fatalf("decode ciphertext: %v", err)
		}
		var config any
		if row.config != "" {
			config = row.config
		}
		var lastError any
		if row.lastTestError != "" {
			lastError = row.lastTestError
		}
		if _, err := admin.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, last_test_at, last_test_success, last_test_error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::json, CASE WHEN $8::boolean IS NULL THEN NULL ELSE '2026-09-01T10:00:00Z'::timestamptz END, $8, $9, '2026-09-01T09:00:00Z', '2026-09-01T09:00:00Z')`,
			uuid.New(), row.org.String(), row.provider, row.name, row.isActive, ciphertext, config, row.lastTestSuccess, lastError); err != nil {
			t.Fatalf("seed credential %s/%s: %v", row.provider, row.name, err)
		}
	}
}

func credentialRequests(f venueFixture, tokens map[string]string) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + tokens[name]}
	}
	jsonH := func(name string) map[string]string {
		h := bearer(name)
		h["Content-Type"] = "application/json"
		return h
	}
	var out []venueoracle.Request
	add := func(name, method, path string, headers map[string]string, body *string) {
		out = append(out, venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers, Body: body})
	}
	base := "/api/v1/admin/credentials"
	one := func(provider, name string) string { return base + "/" + provider + "/" + name }

	add("list: anonymous", "GET", base, nil, nil)
	add("list: member", "GET", base, bearer("member"), nil)
	add("list: admin", "GET", base, bearer("admin"), nil)
	add("list: active_only", "GET", base+"?active_only=true", bearer("admin"), nil)
	add("list: active_only lax yes", "GET", base+"?active_only=yes", bearer("admin"), nil)
	add("list: active_only invalid", "GET", base+"?active_only=maybe", bearer("admin"), nil)
	add("list: repeated active_only takes the last", "GET", base+"?active_only=false&active_only=1", bearer("admin"), nil)
	add("list: provider filter", "GET", base+"?provider=gitlab", bearer("admin"), nil)
	add("list: provider filter wins over active_only", "GET", base+"?provider=gitlab&active_only=true", bearer("admin"), nil)
	add("list: empty provider is no filter", "GET", base+"?provider=", bearer("admin"), nil)
	add("list: unknown provider", "GET", base+"?provider=nope", bearer("admin"), nil)
	add("list: other org sees its own", "GET", base, bearer("superuser"), nil)
	add("list: PUT is not a route", "PUT", base, jsonH("admin"), b64(`{}`))
	add("list: DELETE on the collection is not a route", "DELETE", base, bearer("admin"), nil)

	add("get: anonymous", "GET", one("github", "seeded"), nil, nil)
	add("get: member", "GET", one("github", "seeded"), bearer("member"), nil)
	add("get: seeded", "GET", one("github", "seeded"), bearer("admin"), nil)
	add("get: null config", "GET", one("jira", "null-config"), bearer("admin"), nil)
	add("get: last test error", "GET", one("gitlab", "inactive"), bearer("admin"), nil)
	add("get: pagerduty row", "GET", one("pagerduty", "default"), bearer("admin"), nil)
	add("get: missing", "GET", one("github", "missing"), bearer("admin"), nil)
	add("get: other org row is not visible", "GET", one("github", "other-org"), bearer("admin"), nil)
	add("get: percent-encoded name", "GET", one("github", "a%20b"), bearer("admin"), nil)
	// /credentials/{credential_id}/repos wins the same-shaped path in Python:
	// a segment that is no credential id of the org is a plain not-found, so
	// a credential NAMED "repos" is unreadable through this route.
	add("get: a credential named repos is shadowed by the repos route", "GET", one("github", "repos"), bearer("admin"), nil)
	add("get: repos route with an unknown uuid", "GET", one("11111111-1111-4111-8111-111111111111", "repos"), bearer("admin"), nil)
	add("get: repos route as a non-admin", "GET", one("11111111-1111-4111-8111-111111111112", "repos"), bearer("member"), nil)
	add("patch: a credential named repos is patched like any other", "PATCH", one("github", "repos"), jsonH("admin"), b64(`{"config":{"org":"patched"}}`))
	add("get: PUT is not a route", "PUT", one("github", "seeded"), jsonH("admin"), b64(`{}`))

	add("create: anonymous bad json", "POST", base, jsonH("member"), b64(`{`))
	add("create: anonymous", "POST", base, map[string]string{"Content-Type": "application/json"}, b64(`{"provider":"github","credentials":{}}`))
	add("create: member", "POST", base, jsonH("member"), b64(`{"provider":"github","credentials":{"token":"x"}}`))
	add("create: body not an object", "POST", base, jsonH("admin"), b64(`[1]`))
	add("create: empty object", "POST", base, jsonH("admin"), b64(`{}`))
	add("create: empty provider and name", "POST", base, jsonH("admin"), b64(`{"provider":"","name":"","credentials":{}}`))
	add("create: wrong types", "POST", base, jsonH("admin"), b64(`{"provider":5,"name":null,"credentials":[],"config":"x"}`))
	add("create: credentials null", "POST", base, jsonH("admin"), b64(`{"provider":"github","credentials":null}`))
	add("create: pagerduty is refused", "POST", base, jsonH("admin"), b64(`{"provider":"pagerduty","credentials":{"auth_mode":"api_token"}}`))
	add("create: pagerduty validated before it is refused", "POST", base, jsonH("admin"), b64(`{"provider":"pagerduty"}`))
	add("create: github camelCase keys", "POST", base, jsonH("admin"), b64(`{"provider":"github","name":"camel","credentials":{"appId":"1","privateKey":"pem","installationId":"2","baseUrl":"https://ghe.example.test","extra":true},"config":{"org":"acme"}}`))
	add("create: jira camelCase keys", "POST", base, jsonH("admin"), b64(`{"provider":"jira","credentials":{"email":"e@x.test","apiToken":"t","baseUrl":"https://j.example.test"}}`))
	add("create: provider match is case-insensitive for the key map", "POST", base, jsonH("admin"), b64(`{"provider":"Linear","name":"cased","credentials":{"apiKey":"k"}}`))
	add("create: duplicate camelCase and snake_case key", "POST", base, jsonH("admin"), b64(`{"provider":"gitlab","name":"dup","credentials":{"token":"a","baseUrl":"u1","base_url":"u2"}}`))
	add("create: unknown provider stored as given", "POST", base, jsonH("admin"), b64(`{"provider":"custom","name":"c1","credentials":{"a":1,"b":[1,2.5,null,true],"c":{"d":"e"}}}`))
	add("create: non-ascii and escapes", "POST", base, jsonH("admin"), b64(`{"provider":"custom","name":"unicode é 日本","credentials":{"note":"héllo \"q\" \\ \n é 😀","k é":"v"},"config":{"label":"é","n":1.0,"big":12345678901234567890}}`))
	add("create: config null and default name", "POST", base, jsonH("admin"), b64(`{"provider":"custom","credentials":{"x":1},"config":null}`))
	add("create: empty config", "POST", base, jsonH("admin"), b64(`{"provider":"custom","name":"empty-config","credentials":{"x":1},"config":{}}`))
	add("create: replace seeded keeps config when none is given", "POST", base, jsonH("admin"), b64(`{"provider":"github","name":"seeded","credentials":{"token":"ghp_new"}}`))
	add("create: replace reactivates", "POST", base, jsonH("admin"), b64(`{"provider":"gitlab","name":"inactive","credentials":{"token":"gl_new"}}`))
	add("create: replace with an equal config keeps stored key order", "POST", base, jsonH("admin"), b64(`{"provider":"github","name":"seeded","credentials":{"token":"ghp_again"},"config":{"alpha":[1,2],"org":"acme","zeta":1.0}}`))
	add("create: replace with a different config", "POST", base, jsonH("admin"), b64(`{"provider":"github","name":"seeded","credentials":{"token":"ghp_third"},"config":{"org":"acme2"}}`))
	add("create: replace a NULL config with an empty one", "POST", base, jsonH("admin"), b64(`{"provider":"jira","name":"null-config","credentials":{"email":"a@example.test","api_token":"t2"},"config":{}}`))
	add("create: same name in another org is separate", "POST", base, jsonH("superuser"), b64(`{"provider":"github","name":"seeded","credentials":{"token":"other"}}`))
	add("get: created", "GET", one("github", "camel"), bearer("admin"), nil)
	add("get: created unicode name", "GET", one("custom", "unicode%20%C3%A9%20%E6%97%A5%E6%9C%AC"), bearer("admin"), nil)
	add("list: after creates", "GET", base, bearer("admin"), nil)

	add("patch: anonymous bad json", "PATCH", one("github", "seeded"), jsonH("member"), b64(`{`))
	add("patch: anonymous", "PATCH", one("github", "seeded"), map[string]string{"Content-Type": "application/json"}, b64(`{}`))
	add("patch: member", "PATCH", one("github", "seeded"), jsonH("member"), b64(`{"is_active":false}`))
	add("patch: body not an object", "PATCH", one("github", "seeded"), jsonH("admin"), b64(`"x"`))
	add("patch: wrong types", "PATCH", one("github", "seeded"), jsonH("admin"), b64(`{"credentials":[],"config":"x","is_active":{}}`))
	add("patch: is_active invalid", "PATCH", one("github", "seeded"), jsonH("admin"), b64(`{"is_active":"maybe"}`))
	add("patch: pagerduty is refused before the lookup", "PATCH", one("pagerduty", "nope"), jsonH("admin"), b64(`{"is_active":false}`))
	add("patch: pagerduty existing is refused", "PATCH", one("pagerduty", "default"), jsonH("admin"), b64(`{}`))
	add("patch: missing", "PATCH", one("github", "missing"), jsonH("admin"), b64(`{}`))
	add("patch: other org row is not visible", "PATCH", one("github", "other-org"), jsonH("admin"), b64(`{"is_active":false}`))
	add("patch: empty body changes nothing", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{}`))
	add("patch: nulls change nothing", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"credentials":null,"config":null,"is_active":null}`))
	add("patch: same is_active changes nothing", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"is_active":false}`))
	add("patch: equal config keeps the stored text", "PATCH", one("github", "camel"), jsonH("admin"), b64(`{"config":{"org":"acme"}}`))
	add("patch: config replaced", "PATCH", one("github", "camel"), jsonH("admin"), b64(`{"config":{"org":"acme","zeta":true,"1":1}}`))
	add("patch: bool config value equals int", "PATCH", one("github", "camel"), jsonH("admin"), b64(`{"config":{"org":"acme","zeta":1,"1":true}}`))
	add("patch: config on a NULL config", "PATCH", one("jira", "null-config2"), jsonH("admin"), b64(`{"config":{"a":1}}`))
	add("patch: is_active lax string", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"is_active":"yes"}`))
	add("patch: is_active false", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"is_active":0}`))
	add("patch: config and is_active", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"config":{"url":"https://gl2.example.test"},"is_active":true}`))
	add("patch: credentials only keeps config and flag", "PATCH", one("gitlab", "inactive"), jsonH("admin"), b64(`{"credentials":{"token":"gl_patched","baseUrl":"x"}}`))
	add("patch: credentials with config", "PATCH", one("github", "camel"), jsonH("admin"), b64(`{"credentials":{"token":"t2"},"config":{"org":"acme3"}}`))
	add("patch: credentials with is_active", "PATCH", one("github", "camel"), jsonH("admin"), b64(`{"credentials":{"appId":"9"},"is_active":false}`))
	add("patch: credentials on a NULL config row", "PATCH", one("jira", "null-config"), jsonH("admin"), b64(`{"credentials":{"email":"n@example.test","api_token":"t3"}}`))
	add("patch: PUT is not a route", "PUT", one("github", "camel"), jsonH("admin"), b64(`{}`))
	// Rows no earlier request touched: an assignment that changes nothing
	// writes nothing (updated_at stays put, the stored text keeps its own
	// key order), and a flag the request leaves out keeps its stored value.
	add("patch: credentials only leaves an inactive row inactive", "PATCH", one("gitlab", "inactive2"), jsonH("admin"), b64(`{"credentials":{"token":"z"}}`))
	add("patch: same is_active on an untouched row", "PATCH", one("gitlab", "noop-a"), jsonH("admin"), b64(`{"is_active":false}`))
	add("patch: equal config on an untouched row", "PATCH", one("github", "noop-b"), jsonH("admin"), b64(`{"config":{"y":2,"x":1.0}}`))
	// inf == inf: an equal config holding infinities writes nothing.
	add("patch: equal config with infinities", "PATCH", one("github", "infinite"), jsonH("admin"), b64(`{"config":{"floor":-1e400,"limit":1e400}}`))
	add("patch: credentials with an equal infinite config", "PATCH", one("github", "infinite"), jsonH("admin"), b64(`{"credentials":{"token":"gh_inf2"},"config":{"limit":1e400,"floor":-1e400}}`))
	add("get: an equal config leaves the stored order", "GET", one("github", "noop-b"), bearer("admin"), nil)
	add("create: equal config replace on an untouched row", "POST", base, jsonH("admin"), b64(`{"provider":"github","name":"eq-create","credentials":{"token":"gh_eq2"},"config":{"alpha":[1,2],"org":"acme","zeta":1.0}}`))
	add("get: an equal config on create leaves the stored order", "GET", one("github", "eq-create"), bearer("admin"), nil)
	add("get: after patches", "GET", one("github", "camel"), bearer("admin"), nil)
	add("list: after patches", "GET", base, bearer("admin"), nil)
	return out
}

// TestVenueOracleCredentialAdmin is the credential admin's differential:
// both planes answer every request the same, and the rows they leave
// compare equal after decryption -- every ciphertext, whichever plane wrote
// it, opens with the PYTHON plane's own decrypt_value, and the Go reader
// agrees with it.
func TestVenueOracleCredentialAdmin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	var seed venueFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"SETTINGS_ENCRYPTION_KEY=" + credentialsVenueKey},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			seed = venueSeed(t, ctx, admin)
			seedCredentials(t, ctx, admin, venue, seed)
			return seed.tokenSpecs()
		},
	})
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins:    []string{"http://localhost:3000"},
		SettingsEncryptionKey: secrets.NewValue(credentialsVenueKey),
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	requests := credentialRequests(seed, venue.Tokens)
	receipt := venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string {
			body = timestampFieldPattern.ReplaceAllString(body, `"$1":"<time>"`)
			body = strings.ReplaceAll(body, `"last_test_at":"2026-09-01T10:00:00Z"`, `"last_test_at":"<time>"`)
			return credentialIDPattern.ReplaceAllString(body, `"id":"<uuid>"`)
		},
	})

	// DELETE on a credential is deliberately not served here (its PagerDuty
	// disconnect is ported with the PagerDuty admin, and the path stays on
	// the Python plane until then): Go answers 405. This pins the gap so a
	// later change to it is a decision, not an accident.
	deleteResponse := venueoracle.Do(t, base, venueoracle.Request{Name: "delete: not served by Go", Method: "DELETE",
		Path: "/api/v1/admin/credentials/gitlab/noop-a", Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}})
	if deleteResponse.Status != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /credentials/{provider}/{name} on the Go plane answered %d, want 405", deleteResponse.Status)
	}

	// /credentials/{id}/repos of a credential the org holds is repo listing,
	// which this plane does not serve: 501, not a credential read.
	goPool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	var heldID string
	if err := goPool.QueryRow(ctx, `SELECT id::text FROM integration_credentials WHERE provider = 'github' AND name = 'eq-create'`).Scan(&heldID); err != nil {
		t.Fatal(err)
	}
	goPool.Close()
	repos := venueoracle.Do(t, base, venueoracle.Request{Name: "repos of a held credential", Method: "GET",
		Path: "/api/v1/admin/credentials/" + heldID + "/repos", Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}})
	if repos.Status != http.StatusNotImplemented || !strings.Contains(repos.Body, "not served by this API plane") {
		t.Errorf("repos of a held credential answered %d %s, want 501", repos.Status, repos.Body)
	}

	// Rows: everything but the generated id and the write timestamps, plus
	// the decrypted payload of each row from the Python plane's decrypt_value.
	planes := []struct{ name, uri string }{{"python", venue.AdminURI(t, venue.SourceDB)}, {"go", venue.AdminURI(t, venue.GoDB)}}
	type stored struct{ org, provider, name, text string }
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(credentialsVenueKey), "")
	if err != nil {
		t.Fatal(err)
	}
	rendered := map[string]string{}
	for _, plane := range planes {
		pool, err := pgxpool.New(ctx, plane.uri)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := pool.Query(ctx, `SELECT org_id, provider, name, is_active, coalesce(config::text, '<null>'), last_test_success, coalesce(last_test_error, '<null>'),
			last_test_at IS NOT NULL, updated_at > created_at, coalesce(credentials_encrypted, '')
			FROM integration_credentials ORDER BY org_id, provider, name`)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		var ciphertexts []string
		var keys []stored
		for rows.Next() {
			var org, provider, name, config, lastError, ciphertext string
			var active, hasTest, updated bool
			var lastSuccess *bool
			if err := rows.Scan(&org, &provider, &name, &active, &config, &lastSuccess, &lastError, &hasTest, &updated, &ciphertext); err != nil {
				t.Fatal(err)
			}
			success := "<null>"
			if lastSuccess != nil {
				success = fmt.Sprint(*lastSuccess)
			}
			lines = append(lines, fmt.Sprintf("%s|%s|%s|active=%v|config=%s|test_success=%s|test_error=%s|tested=%v|touched=%v", org, provider, name, active, config, success, lastError, hasTest, updated))
			ciphertexts = append(ciphertexts, ciphertext)
			keys = append(keys, stored{org, provider, name, ""})
		}
		rows.Close()
		pool.Close()
		calls := make([]venueoracle.PythonCall, len(ciphertexts))
		for i, ciphertext := range ciphertexts {
			calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{ciphertext}}
		}
		results := venue.CallPython(t, calls...)
		for i := range lines {
			var plaintext string
			if ciphertexts[i] == "" {
				plaintext = "<none>"
			} else {
				if err := json.Unmarshal(results[i], &plaintext); err != nil {
					t.Fatalf("%s: decrypt %s/%s with Python: %v (%s)", plane.name, keys[i].provider, keys[i].name, err, results[i])
				}
				// The Go reader must agree with Python's decrypt_value.
				goPlain, err := decryptor.Decrypt(secrets.NewValue(ciphertexts[i]))
				if err != nil || string(goPlain) != plaintext {
					t.Errorf("%s: Go reader disagrees with decrypt_value for %s/%s: %v %q vs %q", plane.name, keys[i].provider, keys[i].name, err, goPlain, plaintext)
				}
			}
			lines[i] += "|secret=" + plaintext
		}
		rendered[plane.name] = strings.Join(lines, "\n")
	}
	same := rendered["python"] == rendered["go"] && rendered["go"] != ""
	if !same {
		t.Errorf("integration_credentials rows differ after the writes:\n python:\n%s\n go:\n%s", rendered["python"], rendered["go"])
	}
	t.Log("\n" + receipt + "integration_credentials rows (decrypted): " + venueoracle.Mark(same) + "\n")
}
