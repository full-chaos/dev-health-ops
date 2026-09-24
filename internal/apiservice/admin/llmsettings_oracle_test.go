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

const llmVenueEncryptionKey = "venue-llm-settings-fernet-key-32-bytes!!"

// llmVenueOperatorMax is BYO_LLM_MAX_BUDGET_MICRO_USD on both planes.
const llmVenueOperatorMax = "10000000"

// TestLLMSettingsRoutesVenueOracle is the venue-oracle proof for the BYO LLM
// settings routes (GET/PUT/DELETE /llm-settings): the real Python api and the
// real Go api answer the same requests against two copies of one database, the
// responses are compared byte for byte, and the settings rows the writes
// touched are compared after.
func TestLLMSettingsRoutesVenueOracle(t *testing.T) {
	runLLMSettingsOracle(t, true)
}

// TestLLMSettingsRoutesWithoutEncryptionKeyVenueOracle runs the same routes on
// planes with no SETTINGS_ENCRYPTION_KEY: encrypting or decrypting raises in
// Python, an unhandled 500.
func TestLLMSettingsRoutesWithoutEncryptionKeyVenueOracle(t *testing.T) {
	runLLMSettingsOracle(t, false)
}

func runLLMSettingsOracle(t *testing.T, withKey bool) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-llm-settings-32-bytes!"
	t.Setenv("BYO_LLM_MAX_BUDGET_MICRO_USD", llmVenueOperatorMax)

	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()
	type orgSpec struct {
		id        uuid.UUID
		slug      string
		tier      string
		license   string // enterprise licence tier, "" = none
		overrides string // limits_override JSON
	}
	orgs := map[string]*orgSpec{}
	for _, spec := range []orgSpec{
		{slug: "read", tier: "team"},
		{slug: "write", tier: "team"},
		{slug: "bad", tier: "team"},
		{slug: "empty", tier: "team"},
		{slug: "community", tier: "community"},
		{slug: "off", tier: "team"},
		{slug: "down", tier: "community"},
		{slug: "lic", tier: "team", license: "enterprise", overrides: `{"byo_llm_budget_micro_usd": 5000000}`},
		{slug: "licbad", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": "abc"}`},
		{slug: "licfloat", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": 5.0}`},
		{slug: "lichigh", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": 999999999}`},
		{slug: "licstr", tier: "team", license: "team", overrides: `{"byo_llm_budget_micro_usd": " 7_000 "}`},
		{slug: "licnone", tier: "team", license: "team", overrides: `{"other": 1}`},
		{slug: "licnull", tier: "team", license: "team", overrides: `null`},
		{slug: "licbadtier", tier: "enterprise", license: "platinum"},
	} {
		spec := spec
		spec.id = uuid.New()
		orgs[spec.slug] = &spec
	}
	ghostOrg := uuid.New()

	var pythonEnv = []string{"BYO_LLM_MAX_BUDGET_MICRO_USD=" + llmVenueOperatorMax}
	if withKey {
		pythonEnv = append(pythonEnv, "SETTINGS_ENCRYPTION_KEY="+llmVenueEncryptionKey)
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
			for _, org := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, now(), now())`, org.id, "llm-"+org.slug, org.tier)
				if org.license != "" {
					overrides := org.overrides
					if overrides == "" {
						overrides = "{}"
					}
					exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, limits_override, created_at, updated_at)
VALUES ($1, $2, $3, true, 'saas', 'stripe', $4::json, now(), now())`, uuid.New(), org.id, org.license, overrides)
				}
			}
			exec(`UPDATE feature_flags SET created_at = '2020-01-01T00:00:00+00:00', updated_at = '2020-01-01T00:00:00+00:00'`)
			exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'byo_llm'), false, NULL, NULL, 'kill switch', NULL, now(), now())`,
				uuid.New(), orgs["off"].id)
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminID, "llm-admin@example.com", false)
			user(memberID, "llm-member@example.com", false)
			user(superID, "llm-super@example.com", true)

			encrypt := func(plain string) string {
				if !withKey {
					return "v1:not-decryptable-here"
				}
				results := v.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{plain}})
				var token string
				if err := json.Unmarshal(results[0], &token); err != nil {
					t.Fatalf("decode encrypt_value result: %v", err)
				}
				return token
			}
			row := func(org, category, key string, value any, encrypted bool, desc any) {
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, '2026-02-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')`,
					uuid.New(), orgs[org].id.String(), category, key, value, encrypted, desc)
			}
			row("read", "llm", "provider", "openai", false, "seed provider")
			row("read", "llm", "model", "gpt-5", false, nil)
			row("read", "llm", "api_key", encrypt("sk-seededsecretkey1234"), true, "seed key")
			row("read", "llm", "base_url", "https://gateway.example.invalid/v1", false, nil)
			row("read", "llm", "concurrency", "8", false, nil)
			row("read", "llm_budget", "limit_micro_usd", "250000", false, "seed budget")
			row("read", "general", "unrelated", "kept", false, nil)
			row("bad", "llm", "provider", "openai", false, nil)
			row("bad", "llm", "concurrency", "abc", false, nil)
			row("bad", "llm", "api_key", "garbage-not-fernet", true, nil)
			row("down", "llm", "provider", "openai", false, nil)
			row("down", "llm", "api_key", encrypt("sk-downgraded-secret"), true, nil)
			row("empty", "general", "unrelated", "kept", false, nil)
			row("lic", "llm", "provider", "openai", false, nil)
			row("lic", "llm", "concurrency", "", false, nil)
			row("lic", "llm", "api_key", "", true, nil)
			row("lic", "llm", "base_url", "", false, nil)
			row("lic", "llm", "model", "", false, nil)

			tokens := map[string]map[string]any{
				"member":  {"user_id": memberID.String(), "email": "llm-member@example.com", "org_id": orgs["read"].id.String(), "role": "member"},
				"super":   {"user_id": superID.String(), "email": "llm-super@example.com", "is_superuser": true},
				"notuuid": {"user_id": adminID.String(), "email": "llm-admin@example.com", "org_id": "not-a-uuid", "role": "admin"},
				"ghost":   {"user_id": adminID.String(), "email": "llm-admin@example.com", "org_id": ghostOrg.String(), "role": "admin"},
				"impersonating": {
					"user_id": adminID.String(), "email": "llm-admin@example.com", "org_id": orgs["write"].id.String(),
					"role": "admin", "impersonating_user_id": superID.String(),
				},
			}
			for slug, org := range orgs {
				tokens[slug] = map[string]any{"user_id": adminID.String(), "email": "llm-admin@example.com", "org_id": org.id.String(), "role": "admin"}
			}
			return tokens
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const path = "/api/v1/admin/llm-settings"
	get := func(name, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: path, Headers: auth(token)}
	}
	del := func(name, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "DELETE", Path: path, Headers: auth(token)}
	}
	put := func(name, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "PUT", Path: path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}
	long := strings.Repeat("k", 300)

	requests := []venueoracle.Request{
		// ---- GET ---------------------------------------------------------------
		get("get seeded", "read"),
		get("get empty org", "empty"),
		get("get bad rows is a server error", "bad"),
		get("get empty-string rows", "lic"),
		get("get community refused", "community"),
		get("get kill switch refused", "off"),
		get("get downgraded refused", "down"),
		get("get invalid licence tier", "licbadtier"),
		get("get org id not a uuid", "notuuid"),
		get("get org missing", "ghost"),
		get("get superuser without org", "super"),
		get("get member refused", "member"),
		{Name: "get unauthenticated", Method: "GET", Path: path},
		{Name: "post is 405", Method: "POST", Path: path, Headers: jsonAuth("read"), Body: venueoracle.B64(`{}`)},
		{Name: "patch is 405", Method: "PATCH", Path: path, Headers: jsonAuth("read"), Body: venueoracle.B64(`{}`)},
		// ---- PUT: fields ---------------------------------------------------------
		put("W put provider only", "write", `{"provider":"OpenAI"}`),
		put("W put every field", "write", `{"provider":"  Anthropic  ","model":"m1","api_key":"sk-abcdefghijklmnop","base_url":"https://example.invalid/v1","concurrency":4}`),
		get("W get after every field", "write"),
		put("W put keeps key when omitted", "write", `{"provider":"anthropic","model":"m2"}`),
		put("W put short key", "write", `{"provider":"openai","api_key":"sk-1234"}`),
		put("W put 8 char key", "write", `{"provider":"openai","api_key":"12345678"}`),
		put("W put 9 char key", "write", `{"provider":"openai","api_key":"123456789"}`),
		put("W put unicode key", "write", `{"provider":"openai","api_key":"ключ-секрет-😀-ключ"}`),
		put("W put empty key", "write", `{"provider":"openai","api_key":""}`),
		put("W put null key keeps", "write", `{"provider":"openai","api_key":null}`),
		put("W put long key", "write", fmt.Sprintf(`{"provider":"openai","api_key":%q}`, long)),
		put("W put whitespace provider", "write", `{"provider":"   "}`),
		put("W put dotted capital I provider", "write", `{"provider":"İSTANBUL"}`),
		put("W put final sigma provider", "write", `{"provider":"ΑΣ"}`),
		put("W put tab provider", "write", "{\"provider\":\"\\tOpenAI\\n\"}"),
		put("W put model null clears", "write", `{"provider":"openai","model":null}`),
		put("W put model unicode", "write", `{"provider":"openai","model":"模型-é"}`),
		put("W put concurrency 1", "write", `{"provider":"openai","concurrency":1}`),
		put("W put concurrency 32", "write", `{"provider":"openai","concurrency":32}`),
		put("W put concurrency 0", "write", `{"provider":"openai","concurrency":0}`),
		put("W put concurrency 33", "write", `{"provider":"openai","concurrency":33}`),
		put("W put concurrency string", "write", `{"provider":"openai","concurrency":"7"}`),
		put("W put concurrency float", "write", `{"provider":"openai","concurrency":6.0}`),
		put("W put concurrency fraction", "write", `{"provider":"openai","concurrency":6.5}`),
		put("W put concurrency bool", "write", `{"provider":"openai","concurrency":true}`),
		put("W put concurrency null keeps", "write", `{"provider":"openai","concurrency":null}`),
		put("W put provider empty", "write", `{"provider":""}`),
		put("W put provider missing", "write", `{}`),
		put("W put provider null", "write", `{"provider":null}`),
		put("W put provider number", "write", `{"provider":5}`),
		put("W put model number", "write", `{"provider":"openai","model":5}`),
		put("W put several errors", "write", `{"provider":"","concurrency":0,"budget_limit_micro_usd":-1}`),
		// ---- PUT: base_url -------------------------------------------------------
		// Named limit (CHAOS-6502): base_url cases whose urlsplit error text or
		// outcome differs between the planes (port out of range or not a
		// number, an unbalanced or invalid IPv6 bracket, a percent sign in
		// the host, an over-long host label) are not in this list; the
		// shared validator owns them.
		put("W base_url empty", "write", `{"provider":"openai","base_url":""}`),
		put("W base_url null", "write", `{"provider":"openai","base_url":null}`),
		put("W base_url http", "write", `{"provider":"openai","base_url":"http://example.invalid/v1"}`),
		put("W base_url ftp", "write", `{"provider":"openai","base_url":"ftp://example.invalid/"}`),
		put("W base_url userinfo", "write", `{"provider":"openai","base_url":"https://u:p@example.invalid/"}`),
		put("W base_url private ip", "write", `{"provider":"openai","base_url":"https://10.0.0.1/v1"}`),
		put("W base_url loopback v6", "write", `{"provider":"openai","base_url":"https://[::1]/v1"}`),
		put("W base_url public literal", "write", `{"provider":"openai","base_url":"https://8.8.8.8/v1"}`),
		put("W base_url space", "write", `{"provider":"openai","base_url":"https://exa mple.invalid/"}`),
		put("W base_url control", "write", `{"provider":"openai","base_url":"https://example.invalid/\u0001"}`),
		put("W base_url port ok", "write", `{"provider":"openai","base_url":"https://example.invalid:8443/v1"}`),
		put("W base_url empty port", "write", `{"provider":"openai","base_url":"https://example.invalid:/v1"}`),
		put("W base_url unicode host", "write", `{"provider":"openai","base_url":"https://例え.jp/v1"}`),
		put("W base_url zone id", "write", `{"provider":"openai","base_url":"https://[fe80::1%25eth0]/v1"}`),
		put("W base_url uppercase scheme", "write", `{"provider":"openai","base_url":"HTTPS://Example.Invalid/v1"}`),
		put("W base_url scheme only", "write", `{"provider":"openai","base_url":"https:"}`),
		put("W base_url ipv4 mapped ipv6", "write", `{"provider":"openai","base_url":"https://[::ffff:10.0.0.1]/v1"}`),
		put("W base_url no host", "write", `{"provider":"openai","base_url":"https:///v1"}`),
		put("W base_url not a url", "write", `{"provider":"openai","base_url":"not a url"}`),
		put("W base_url bad refusal keeps rows", "write", `{"provider":"changed","model":"changed","base_url":"http://x.invalid"}`),
		get("W get after refusal", "write"),
		// ---- PUT: budget ---------------------------------------------------------
		put("W budget zero", "write", `{"provider":"openai","budget_limit_micro_usd":0}`),
		put("W budget at the operator ceiling", "write", `{"provider":"openai","budget_limit_micro_usd":10000000}`),
		put("W budget over the ceiling rolls back", "write", `{"provider":"rolled-back","model":"rolled-back","api_key":"rolled-back-key","concurrency":9,"budget_limit_micro_usd":10000001}`),
		get("W get after rollback", "write"),
		put("W budget huge", "write", `{"provider":"openai","budget_limit_micro_usd":99999999999999999999999}`),
		put("W budget negative", "write", `{"provider":"openai","budget_limit_micro_usd":-1}`),
		put("W budget string", "write", `{"provider":"openai","budget_limit_micro_usd":"5"}`),
		put("W budget float whole", "write", `{"provider":"openai","budget_limit_micro_usd":5.0}`),
		put("W budget float fraction", "write", `{"provider":"openai","budget_limit_micro_usd":5.5}`),
		put("W budget bool", "write", `{"provider":"openai","budget_limit_micro_usd":true}`),
		put("W budget exponent", "write", `{"provider":"openai","budget_limit_micro_usd":1e3}`),
		put("W budget null", "write", `{"provider":"openai","budget_limit_micro_usd":null}`),
		put("W budget replaces", "write", `{"provider":"openai","budget_limit_micro_usd":1234}`),
		put("W budget same value", "write", `{"provider":"openai","budget_limit_micro_usd":1234}`),
		put("W budget impersonated refused", "impersonating", `{"provider":"openai","budget_limit_micro_usd":1}`),
		put("W budget impersonated bad url still refused first", "impersonating", `{"provider":"openai","base_url":"http://x.invalid","budget_limit_micro_usd":1}`),
		put("W impersonated without budget", "impersonating", `{"provider":"openai","model":"imp"}`),
		put("W budget licence ceiling", "lic", `{"provider":"openai","budget_limit_micro_usd":5000000}`),
		put("W budget over licence ceiling", "lic", `{"provider":"openai","budget_limit_micro_usd":5000001}`),
		put("W budget licence text ceiling", "licstr", `{"provider":"openai","budget_limit_micro_usd":7000}`),
		put("W budget over licence text ceiling", "licstr", `{"provider":"openai","budget_limit_micro_usd":7001}`),
		put("W budget invalid licence ceiling", "licbad", `{"provider":"openai","budget_limit_micro_usd":1}`),
		put("W budget zero under invalid licence ceiling", "licbad", `{"provider":"openai","budget_limit_micro_usd":0}`),
		put("W budget float licence ceiling", "licfloat", `{"provider":"openai","budget_limit_micro_usd":1}`),
		put("W budget licence above operator", "lichigh", `{"provider":"openai","budget_limit_micro_usd":10000001}`),
		put("W budget licence above operator ok", "lichigh", `{"provider":"openai","budget_limit_micro_usd":10000000}`),
		put("W budget licence without the key", "licnone", `{"provider":"openai","budget_limit_micro_usd":10000000}`),
		put("W budget licence null overrides", "licnull", `{"provider":"openai","budget_limit_micro_usd":10000000}`),
		// ---- PUT: gates and bodies -----------------------------------------------
		put("W put community refused", "community", `{"provider":"openai"}`),
		put("W put kill switch refused", "off", `{"provider":"openai"}`),
		put("W put downgraded refused", "down", `{"provider":"openai"}`),
		put("W put invalid licence tier", "licbadtier", `{"provider":"openai"}`),
		put("W put org not a uuid", "notuuid", `{"provider":"openai"}`),
		put("W put org missing", "ghost", `{"provider":"openai"}`),
		put("W put 422 beats the gate", "community", `{"provider":""}`),
		put("W put 422 beats org check", "notuuid", `{}`),
		put("W put superuser without org", "super", `{"provider":"openai"}`),
		put("W put member refused", "member", `{"provider":"openai"}`),
		put("W put body list", "write", `[]`),
		put("W put body null", "write", `null`),
		put("W put invalid json", "write", `{`),
		{Name: "W put non-json content type", Method: "PUT", Path: path,
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["write"], "Content-Type": "text/plain"}, Body: venueoracle.B64("x")},
		{Name: "W put unauthenticated", Method: "PUT", Path: path, Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"provider":"openai"}`)},
		put("W put over bad rows rolls back", "bad", `{"provider":"changed","model":"changed"}`),
		put("W put over empty-string rows", "lic", `{"provider":"openai","model":"m","api_key":"sk-emptied-and-refilled"}`),
		get("W get after empty-string rows", "lic"),
		// ---- reads after writes --------------------------------------------------
		get("W get final write org", "write"),
		get("W get final read org", "read"),
		// ---- DELETE --------------------------------------------------------------
		del("W delete seeded", "read"),
		del("W delete again", "read"),
		get("W get after delete", "read"),
		del("W delete downgraded still works", "down"),
		del("W delete community without rows", "community"),
		del("W delete kill switch without rows", "off"),
		del("W delete empty org", "empty"),
		del("W delete org not a uuid", "notuuid"),
		del("W delete org missing", "ghost"),
		del("W delete superuser without org", "super"),
		del("W delete member refused", "member"),
		{Name: "W delete unauthenticated", Method: "DELETE", Path: path},
		del("W delete bad rows", "bad"),
		del("W delete write org", "write"),
	}

	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		if withKey {
			decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmVenueEncryptionKey), "")
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
	compare("settings rows", `SELECT org_id, category, key, is_encrypted, (CASE WHEN is_encrypted AND value LIKE 'v1:%' THEN 'v1:<token>' ELSE value END),
	description, (updated_at > '2026-06-01')::text
FROM settings ORDER BY org_id, category, key`)
}
