//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const settingsVenueEncryptionKey = "venue-settings-admin-fernet-key-32-bytes!"

// TestSettingsRoutesVenueOracle is the venue-oracle proof for the generic
// settings admin routes. The real Python api and the real Go api answer the
// same requests against two copies of one database; responses are compared
// byte for byte, and the settings rows the writes touched are compared after
// (an encrypted value by decrypting each plane's ciphertext with the shared
// key through the Python api's own decrypt, since Fernet output is random).
func TestSettingsRoutesVenueOracle(t *testing.T) {
	runSettingsOracle(t, true)
}

// TestSettingsRoutesWithoutEncryptionKeyVenueOracle runs the same routes on
// planes that hold no SETTINGS_ENCRYPTION_KEY: encrypting or decrypting then
// raises in Python, an unhandled 500.
func TestSettingsRoutesWithoutEncryptionKeyVenueOracle(t *testing.T) {
	runSettingsOracle(t, false)
}

func runSettingsOracle(t *testing.T, withKey bool) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-settings-flow-32-bytes!"

	orgA, orgB := uuid.New(), uuid.New()
	adminA, adminB, memberA, superID := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	var pythonEnv []string
	if withKey {
		pythonEnv = []string{"SETTINGS_ENCRYPTION_KEY=" + settingsVenueEncryptionKey}
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:      root,
		JWTKey:    jwtKey,
		PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
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
			}{{orgA, "set-a"}, {orgB, "set-b"}} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org.id, org.slug)
			}
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminA, "set-admina@example.com", false)
			user(adminB, "set-adminb@example.com", false)
			user(memberA, "set-membera@example.com", false)
			user(superID, "set-super@example.com", true)

			ciphertext := ""
			if withKey {
				results := v.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"seed secret"}})
				if err := json.Unmarshal(results[0], &ciphertext); err != nil {
					t.Fatalf("decode encrypt_value result: %v", err)
				}
			} else {
				ciphertext = "v1:not-decryptable-here"
			}
			row := func(org uuid.UUID, category, key string, value any, encrypted bool, desc any) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, '2026-02-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')`,
					uuid.New(), org.String(), category, key, value, encrypted, desc)
			}
			row(orgA, "general", "site_name", "Acme", false, "the name")
			row(orgA, "general", "empty_value", "", false, nil)
			row(orgA, "general", "null_value", nil, false, "no value")
			row(orgA, "general", "unicode", "Café 東京", false, nil)
			row(orgA, "github", "token", ciphertext, true, "a secret")
			row(orgA, "notifications", "bad_cipher", "garbage-not-fernet", true, nil)
			row(orgA, "notifications", "legacy_v2", "v2:abc", true, nil)
			row(orgA, "notifications", "empty_encrypted", "", true, nil)
			row(orgB, "general", "other_org", "b", false, nil)
			return map[string]map[string]any{
				"adminA":  {"user_id": adminA.String(), "email": "set-admina@example.com", "org_id": orgA.String(), "role": "admin"},
				"adminB":  {"user_id": adminB.String(), "email": "set-adminb@example.com", "org_id": orgB.String(), "role": "admin"},
				"memberA": {"user_id": memberA.String(), "email": "set-membera@example.com", "org_id": orgA.String(), "role": "member"},
				"super":   {"user_id": superID.String(), "email": "set-super@example.com", "is_superuser": true},
			}
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const admin = "/api/v1/admin"
	get := func(name, path, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: admin + path, Headers: auth(token)}
	}
	send := func(name, method, path, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: method, Path: admin + path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}
	plain := func(name, method, path, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: method, Path: admin + path, Headers: auth(token)}
	}

	requests := []venueoracle.Request{
		// ---- categories and listing ---------------------------------------
		get("categories", "/settings/categories", "adminA"),
		get("categories superuser without org", "/settings/categories", "super"),
		get("categories member refused", "/settings/categories", "memberA"),
		{Name: "categories unauthenticated", Method: "GET", Path: admin + "/settings/categories"},
		plain("categories post is 405", "POST", "/settings/categories", "adminA"),
		plain("categories delete is 405", "DELETE", "/settings/categories", "adminA"),
		send("categories put is 405", "PUT", "/settings/categories", "adminA", `{}`),
		get("list with settings is a server error", "/settings/general", "adminA"),
		get("list encrypted category", "/settings/github", "adminA"),
		get("list notifications", "/settings/notifications", "adminA"),
		get("list empty category", "/settings/jira", "adminA"),
		get("list unknown category", "/settings/nope", "adminA"),
		get("list category with space", "/settings/a%20b", "adminA"),
		get("list other org category", "/settings/general", "adminB"),
		get("list llm refused", "/settings/llm", "adminA"),
		get("list llm_budget refused", "/settings/llm_budget", "adminA"),
		get("list ask_dev refused", "/settings/ask_dev", "adminA"),
		get("list superuser without org", "/settings/general", "super"),
		get("list member refused", "/settings/general", "memberA"),
		{Name: "list unauthenticated", Method: "GET", Path: admin + "/settings/general"},
		// ---- get one --------------------------------------------------------
		get("get plain", "/settings/general/site_name", "adminA"),
		get("get empty value", "/settings/general/empty_value", "adminA"),
		get("get null value", "/settings/general/null_value", "adminA"),
		get("get unicode", "/settings/general/unicode", "adminA"),
		get("get encrypted", "/settings/github/token", "adminA"),
		get("get bad cipher", "/settings/notifications/bad_cipher", "adminA"),
		get("get legacy version cipher", "/settings/notifications/legacy_v2", "adminA"),
		get("get empty encrypted", "/settings/notifications/empty_encrypted", "adminA"),
		get("get unknown key", "/settings/general/nope", "adminA"),
		get("get unknown category", "/settings/nope/site_name", "adminA"),
		get("get other org key", "/settings/general/other_org", "adminA"),
		get("get llm refused", "/settings/llm/api_key", "adminA"),
		get("get ask_dev refused", "/settings/ask_dev/x", "adminA"),
		get("get superuser without org", "/settings/general/site_name", "super"),
		get("get member refused", "/settings/general/site_name", "memberA"),
		plain("get path post is 405", "POST", "/settings/general/site_name", "adminA"),
		plain("settings get is 405", "GET", "/settings", "adminA"),
		// ---- put ------------------------------------------------------------
		// A no-op write on a seeded row must leave its updated_at alone, as
		// the ORM does when no attribute changed.
		send("W put same values on a seeded row", "PUT", "/settings/general/unicode", "adminA", `{"value":"Café 東京"}`),
		send("W put new plain", "PUT", "/settings/general/put_new", "adminA", `{"value":"v1"}`),
		send("W put replace plain", "PUT", "/settings/general/site_name", "adminA", `{"value":"Acme2","description":"renamed"}`),
		send("W put keeps description", "PUT", "/settings/general/site_name", "adminA", `{"value":"Acme3"}`),
		send("W put same values", "PUT", "/settings/general/site_name", "adminA", `{"value":"Acme3"}`),
		send("W put empty body", "PUT", "/settings/general/empty_body", "adminA", `{}`),
		send("W put null value", "PUT", "/settings/general/put_null", "adminA", `{"value":null}`),
		send("W put empty string", "PUT", "/settings/general/put_empty", "adminA", `{"value":""}`),
		send("W put unicode", "PUT", "/settings/general/put_unicode", "adminA", `{"value":"日本語 Café"}`),
		send("W put encrypt", "PUT", "/settings/general/put_secret", "adminA", `{"value":"top secret","encrypt":true}`),
		send("W put encrypt empty value", "PUT", "/settings/general/put_secret_empty", "adminA", `{"value":"","encrypt":true}`),
		send("W put encrypt null value", "PUT", "/settings/general/put_secret_null", "adminA", `{"encrypt":true}`),
		send("W put unencrypt", "PUT", "/settings/github/token", "adminA", `{"value":"now plain","encrypt":false}`),
		send("W put encrypt null flag", "PUT", "/settings/general/put_flag_null", "adminA", `{"value":"x","encrypt":null}`),
		send("W put encrypt yes", "PUT", "/settings/general/put_flag_yes", "adminA", `{"value":"y","encrypt":"yes"}`),
		send("W put encrypt bad flag", "PUT", "/settings/general/put_flag_bad", "adminA", `{"encrypt":"maybe"}`),
		send("W put value number", "PUT", "/settings/general/put_num", "adminA", `{"value":5}`),
		send("W put description number", "PUT", "/settings/general/put_desc", "adminA", `{"description":5}`),
		send("W put body list", "PUT", "/settings/general/put_list", "adminA", `[]`),
		send("W put body null", "PUT", "/settings/general/put_bnull", "adminA", `null`),
		send("W put invalid json", "PUT", "/settings/general/put_bad", "adminA", `{`),
		venueoracle.Request{Name: "W put non-json content type", Method: "PUT", Path: admin + "/settings/general/put_text",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["adminA"], "Content-Type": "text/plain"}, Body: venueoracle.B64("x")},
		send("W put llm refused", "PUT", "/settings/llm/api_key", "adminA", `{"value":"k"}`),
		send("W put llm_budget refused", "PUT", "/settings/llm_budget/limit", "adminA", `{}`),
		send("W put ask_dev refused", "PUT", "/settings/ask_dev/enabled", "adminA", `{}`),
		send("W put 422 beats category refusal", "PUT", "/settings/llm/api_key", "adminA", `{"value":5}`),
		send("W put category named categories", "PUT", "/settings/categories/x", "adminA", `{"value":"c"}`),
		send("W put unicode key and category", "PUT", "/settings/caf%C3%A9/cl%C3%A9", "adminA", `{"value":"u"}`),
		send("W put superuser without org", "PUT", "/settings/general/su", "super", `{"value":"s"}`),
		send("W put member refused", "PUT", "/settings/general/mem", "memberA", `{"value":"m"}`),
		send("W put other org writes its own", "PUT", "/settings/general/site_name", "adminB", `{"value":"B site"}`),
		// ---- post -----------------------------------------------------------
		send("W post minimal", "POST", "/settings", "adminA", `{"key":"post_min"}`),
		send("W post full", "POST", "/settings", "adminA", `{"key":"post_full","value":"v","category":"notifications","encrypt":true,"description":"d"}`),
		send("W post replaces existing", "POST", "/settings", "adminA", `{"key":"site_name","value":"Posted"}`),
		send("W post key empty", "POST", "/settings", "adminA", `{"key":""}`),
		send("W post key 255 chars", "POST", "/settings", "adminA", fmt.Sprintf(`{"key":%q}`, strings.Repeat("k", 255))),
		send("W post key 256 chars", "POST", "/settings", "adminA", fmt.Sprintf(`{"key":%q}`, strings.Repeat("k", 256))),
		send("W post key missing", "POST", "/settings", "adminA", `{}`),
		send("W post key null", "POST", "/settings", "adminA", `{"key":null}`),
		send("W post key number", "POST", "/settings", "adminA", `{"key":5}`),
		send("W post category null", "POST", "/settings", "adminA", `{"key":"c","category":null}`),
		send("W post category empty", "POST", "/settings", "adminA", `{"key":"post_cat_empty","category":""}`),
		send("W post encrypt null", "POST", "/settings", "adminA", `{"key":"post_enc_null","encrypt":null}`),
		send("W post encrypt string", "POST", "/settings", "adminA", `{"key":"post_enc_str","value":"z","encrypt":"true"}`),
		send("W post encrypt bad", "POST", "/settings", "adminA", `{"key":"post_enc_bad","encrypt":"perhaps"}`),
		send("W post value number", "POST", "/settings", "adminA", `{"key":"post_num","value":1}`),
		send("W post llm refused", "POST", "/settings", "adminA", `{"key":"k","category":"llm"}`),
		send("W post ask_dev refused", "POST", "/settings", "adminA", `{"key":"k","category":"ask_dev"}`),
		send("W post llm_budget refused", "POST", "/settings", "adminA", `{"key":"k","category":"llm_budget"}`),
		send("W post 422 beats category refusal", "POST", "/settings", "adminA", `{"category":"llm"}`),
		send("W post body list", "POST", "/settings", "adminA", `[]`),
		send("W post invalid json", "POST", "/settings", "adminA", `{`),
		send("W post member refused", "POST", "/settings", "memberA", `{"key":"m"}`),
		send("W post superuser without org", "POST", "/settings", "super", `{"key":"s"}`),
		{Name: "W post unauthenticated", Method: "POST", Path: admin + "/settings", Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"key":"u"}`)},
		// ---- reads after writes ---------------------------------------------
		get("W get after put", "/settings/general/put_new", "adminA"),
		get("W get encrypted after put", "/settings/general/put_secret", "adminA"),
		get("W get unencrypted after put", "/settings/github/token", "adminA"),
		get("W get post full", "/settings/notifications/post_full", "adminA"),
		get("W list after writes", "/settings/general", "adminA"),
		// ---- delete ---------------------------------------------------------
		plain("W delete", "DELETE", "/settings/general/put_new", "adminA"),
		plain("W delete again", "DELETE", "/settings/general/put_new", "adminA"),
		plain("W delete encrypted", "DELETE", "/settings/general/put_secret", "adminA"),
		plain("W delete unknown category", "DELETE", "/settings/nope/x", "adminA"),
		plain("W delete other org key", "DELETE", "/settings/general/other_org", "adminA"),
		plain("W delete llm refused", "DELETE", "/settings/llm/api_key", "adminA"),
		plain("W delete member refused", "DELETE", "/settings/general/site_name", "memberA"),
		plain("W delete superuser without org", "DELETE", "/settings/general/site_name", "super"),
		{Name: "W delete unauthenticated", Method: "DELETE", Path: admin + "/settings/general/site_name"},
		get("W get after deletes", "/settings/general/site_name", "adminA"),
	}

	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		if withKey {
			decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(settingsVenueEncryptionKey), "")
			if err != nil {
				t.Fatalf("build decryptor: %v", err)
			}
			deps.Decryptor = decryptor
		}
	})
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source == "" {
			t.Errorf("%s: the query matched no rows on the Python plane; the comparison proves nothing", name)
		}
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	// An encrypted value is compared as a version-prefixed token whose
	// plaintext is checked below; every other column is compared directly.
	compare("settings rows", `SELECT org_id, category, key, is_encrypted, (CASE WHEN is_encrypted AND value LIKE 'v1:%' THEN 'v1:<token>' ELSE value END),
	description, (updated_at > '2026-06-01')::text
FROM settings ORDER BY org_id, category, key`)

	if !withKey {
		return
	}
	ciphertexts := func(db string) map[string]string {
		out := map[string]string{}
		raw := venueoracle.TableRows(t, ctx, venue.AdminURI(t, db), `SELECT category || '/' || key || ' ' || value FROM settings WHERE is_encrypted AND value LIKE 'v1:%'`)
		for _, line := range strings.Split(raw, " | ") {
			name, token, ok := strings.Cut(line, " ")
			if ok {
				out[name] = token
			}
		}
		return out
	}
	source, goSide := ciphertexts(venue.SourceDB), ciphertexts(venue.GoDB)
	if len(source) == 0 || len(source) != len(goSide) {
		t.Fatalf("encrypted rows: python %d, go %d", len(source), len(goSide))
	}
	decrypt := func(token string) string {
		results := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{token}})
		var plaintext string
		if err := json.Unmarshal(results[0], &plaintext); err != nil {
			t.Fatalf("decode decrypt_value result: %v", err)
		}
		return plaintext
	}
	for name, token := range source {
		if decrypt(token) != decrypt(goSide[name]) {
			t.Errorf("encrypted setting %s decrypts differently: python %q, go %q", name, decrypt(token), decrypt(goSide[name]))
		}
	}
}
