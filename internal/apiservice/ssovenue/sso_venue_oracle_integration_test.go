//go:build integration

package ssovenue_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// jwtKey is JWT_SECRET_KEY on both planes. It is assembled at run time so
// the source holds no credential-shaped literal.
var jwtKey = "sso-venue-" + strings.Repeat("0123456789abcdef", 2)

// Fixed ids, so rows and answers name the same subjects on both planes.
const (
	orgA         = "a0000000-0000-4000-8000-0000000000a1"
	orgB         = "b0000000-0000-4000-8000-0000000000b1"
	owner        = "11000000-0000-4000-8000-000000000001"
	admin        = "11000000-0000-4000-8000-000000000002"
	member       = "11000000-0000-4000-8000-000000000003"
	ownerB       = "11000000-0000-4000-8000-000000000004"
	samlID       = "22000000-0000-4000-8000-000000000001"
	oidcID       = "22000000-0000-4000-8000-000000000002"
	oauthID      = "22000000-0000-4000-8000-000000000003"
	otherOrgID   = "22000000-0000-4000-8000-000000000004"
	unknownID    = "22000000-0000-4000-8000-0000000000ff"
	badConfigID  = "22000000-0000-4000-8000-000000000005"
	badDomainsID = "22000000-0000-4000-8000-000000000006"
)

// certificate is over 50 characters, with non-ASCII ones before the cut, so
// the response's truncation counts code points.
var certificate = "MIIC" + strings.Repeat("é", 10) + strings.Repeat("A", 60)

func seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	for _, org := range [][2]string{{orgA, "sso-org-a"}, {orgB, "sso-org-b"}} {
		exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, 'enterprise', 'stripe', true, '{}', now(), now())`, org[0], org[1])
	}
	for _, u := range [][2]string{{owner, "owner@sso.test"}, {admin, "admin@sso.test"}, {member, "member@sso.test"}, {ownerB, "ownerb@sso.test"}} {
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, u[0], u[1])
	}
	for _, m := range [][3]string{{owner, orgA, "owner"}, {admin, orgA, "admin"}, {member, orgA, "member"}, {ownerB, orgB, "owner"}} {
		exec(`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, now(), now(), now())`, m[0], m[1], m[2])
	}
	provider := func(id, org, name, protocol, status, config string, domains *string) {
		exec(`INSERT INTO sso_providers (id, org_id, name, protocol, status, is_default, allow_idp_initiated,
	auto_provision_users, default_role, config, encrypted_secrets, allowed_domains, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, false, true, true, 'member', $6::json, NULL, $7::json,
	'2026-08-01 00:00:00+00', '2026-08-01 00:00:00+00')`, id, org, name, protocol, status, config, domains)
	}
	domains := `["a.example", null, "b.example"]`
	provider(samlID, orgA, "Corp SAML", "saml", "pending_setup",
		`{"entity_id": "https://idp.example/e", "sso_url": "https://idp.example/sso", "certificate": "`+certificate+`", "attribute_mapping": {"email": "mail"}}`, &domains)
	provider(oidcID, orgA, "Corp OIDC", "oidc", "active",
		`{"client_id": "cid", "client_secret": "not-masked-in-the-row", "issuer": "https://issuer.example", "scopes": ["openid"]}`, nil)
	provider(oauthID, orgA, "GitHub", "oauth_github", "inactive", `null`, nil)
	provider(otherOrgID, orgB, "Other org", "saml", "active", `{}`, nil)
	// Stored values of the wrong JSON shape: _provider_to_response raises
	// TypeError after the route has committed the status.
	provider(badConfigID, orgA, "Bad config", "oidc", "inactive", `[]`, nil)
	badDomains := `{"a": 1}`
	provider(badDomainsID, orgA, "Bad domains", "oidc", "inactive", `{}`, &badDomains)

	tok := func(user, email, org, role string) map[string]any {
		out := map[string]any{"user_id": user, "email": email, "role": role}
		if org != "" {
			out["org_id"] = org
		}
		return out
	}
	return map[string]map[string]any{
		"owner":   tok(owner, "owner@sso.test", orgA, "owner"),
		"admin":   tok(admin, "admin@sso.test", orgA, "admin"),
		"member":  tok(member, "member@sso.test", orgA, "member"),
		"ownerB":  tok(ownerB, "ownerb@sso.test", orgB, "owner"),
		"noorg":   tok(owner, "owner@sso.test", "", "owner"),
		"badorg":  tok(owner, "owner@sso.test", "not-a-uuid", "owner"),
		"otherid": tok(owner, "owner@sso.test", orgB, "owner"),
	}
}

var updatedAtPattern = regexp.MustCompile(`"updated_at":"[^"]*"`)

// TestSSORoutesVenueOracle is the venue differential of the enterprise SSO
// routes: the REAL Python api (TestClient over dev_health_ops.api.main:app)
// and the REAL dho api route set answer the same requests against two
// copies of one seeded database, compared byte for byte (the stamped
// updated_at blanked); then the sso_providers rows are compared.
//
// Neither plane has a process license, so every gated route answers its
// authentication, its validation and then the feature_not_licensed 402;
// activate and deactivate are not gated and do their work.
func TestSSORoutesVenueOracle(t *testing.T) {
	ctx := context.Background()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{Root: venueRoot(), JWTKey: jwtKey,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Seed: seed})
	tok := venue.Tokens
	req := func(name, method, path, bearer, body string) venueoracle.Request {
		headers := map[string]string{}
		if bearer != "" {
			headers["Authorization"] = "Bearer " + tok[bearer]
		}
		var encoded *string
		if body != "" {
			headers["Content-Type"] = "application/json"
			encoded = venueoracle.B64(body)
		}
		return venueoracle.Request{Name: name, Method: method, Path: "/api/v1/auth" + path, Headers: headers, Body: encoded}
	}
	get, post, patch, del := http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete
	longName := strings.Repeat("n", 256)
	requests := []venueoracle.Request{
		// Provider management: authentication, validation, then the 402.
		req("list: no credential", get, "/sso/providers", "", ""),
		req("list: owner", get, "/sso/providers", "owner", ""),
		req("list: member", get, "/sso/providers", "member", ""),
		req("list: bad limit", get, "/sso/providers?limit=x&offset=1.5", "owner", ""),
		req("list: filters", get, "/sso/providers?protocol=saml&status=active&limit=5&offset=0", "owner", ""),
		req("create: no credential", post, "/sso/providers", "", `{"name": "x", "protocol": "saml"}`),
		req("create: malformed json, no credential", post, "/sso/providers", "", `{"name":`),
		req("create: missing fields", post, "/sso/providers", "owner", `{}`),
		req("create: body is a list", post, "/sso/providers", "owner", `[]`),
		req("create: empty name, bad protocol", post, "/sso/providers", "owner", `{"name": "", "protocol": "ldap"}`),
		req("create: name 256 chars", post, "/sso/providers", "owner", `{"name": "`+longName+`", "protocol": "oidc"}`),
		req("create: protocol with a newline", post, "/sso/providers", "owner", `{"name": "x", "protocol": "saml\n"}`),
		req("create: protocol lone surrogate", post, "/sso/providers", "owner", `{"name": "x", "protocol": "\ud800"}`),
		req("create: saml_config not an object", post, "/sso/providers", "owner", `{"name": "x", "protocol": "saml", "saml_config": "s"}`),
		req("create: saml_config fields", post, "/sso/providers", "owner",
			`{"name": "x", "protocol": "saml", "saml_config": {"entity_id": 1, "attribute_mapping": {"a": 1, "b": "ok"}}}`),
		req("create: oidc_config fields", post, "/sso/providers", "owner",
			`{"name": "x", "protocol": "oidc", "oidc_config": {"client_id": "c", "scopes": "openid", "claim_mapping": []}}`),
		req("create: bools and lists", post, "/sso/providers", "owner",
			`{"name": "x", "protocol": "saml", "is_default": "maybe", "allowed_domains": ["a", 1], "default_role": null}`),
		req("create: lax bool strings", post, "/sso/providers", "owner",
			`{"name": "x", "protocol": "saml", "is_default": "yes", "auto_provision_users": 0}`),
		req("create: valid, by a member (gate before role)", post, "/sso/providers", "member", `{"name": "x", "protocol": "oidc"}`),
		req("create: valid", post, "/sso/providers", "owner",
			`{"name": "x", "protocol": "saml", "saml_config": {"entity_id": "e", "sso_url": "u", "certificate": "c"}}`),
		req("get: no credential", get, "/sso/providers/"+samlID, "", ""),
		req("get: owner", get, "/sso/providers/"+samlID, "owner", ""),
		req("get: not a uuid", get, "/sso/providers/nope", "owner", ""),
		req("patch: bad fields", patch, "/sso/providers/"+samlID, "owner", `{"name": "`+longName+`", "is_default": [], "oidc_config": {}}`),
		req("patch: nulls", patch, "/sso/providers/"+samlID, "owner", `{"name": null, "saml_config": null, "allowed_domains": null}`),
		req("patch: no body", patch, "/sso/providers/"+samlID, "owner", ``),
		req("delete: owner", del, "/sso/providers/"+samlID, "owner", ""),
		req("delete: no credential", del, "/sso/providers/"+samlID, "", ""),
		req("oauth create: missing config", post, "/oauth/providers", "owner", `{"name": "g", "provider_type": "github"}`),
		req("oauth create: bad type and config", post, "/oauth/providers", "owner",
			`{"name": "g", "provider_type": "bitbucket", "oauth_config": [1]}`),
		req("oauth create: config fields", post, "/oauth/providers", "owner",
			`{"name": "g", "provider_type": "gitlab", "oauth_config": {"client_secret": 5, "scopes": [null]}}`),
		req("oauth create: valid", post, "/oauth/providers", "owner",
			`{"name": "g", "provider_type": "google", "oauth_config": {"client_id": "i", "client_secret": "s"}}`),
		req("oauth create: no credential", post, "/oauth/providers", "", `{}`),
		req("oauth create: GET is 405", get, "/oauth/providers", "owner", ""),
		req("oauth patch: bad", patch, "/oauth/providers/"+oauthID, "owner", `{"oauth_config": "x", "default_role": 1}`),
		req("oauth patch: valid", patch, "/oauth/providers/"+oauthID, "owner", `{"name": "renamed"}`),
		req("oauth patch: no credential", patch, "/oauth/providers/"+oauthID, "", `{}`),
		// The flows: public, validation then the 402.
		req("saml metadata", get, "/saml/"+samlID+"/metadata", "", ""),
		req("saml initiate: no body", post, "/saml/"+samlID+"/initiate", "", ``),
		req("saml initiate: relay_state int", post, "/saml/"+samlID+"/initiate", "", `{"relay_state": 1}`),
		req("saml initiate: valid", post, "/saml/"+samlID+"/initiate", "", `{}`),
		req("saml acs: missing", post, "/saml/"+samlID+"/acs", "", `{}`),
		req("saml acs: by name", post, "/saml/"+samlID+"/acs", "", `{"saml_response": "x"}`),
		req("saml acs: name wrong type", post, "/saml/"+samlID+"/acs", "", `{"saml_response": 1}`),
		req("saml acs: alias wins over name", post, "/saml/"+samlID+"/acs", "", `{"SAMLResponse": 1, "saml_response": "x"}`),
		req("saml acs: relay by alias wrong type", post, "/saml/"+samlID+"/acs", "", `{"SAMLResponse": "x", "RelayState": 1}`),
		req("saml acs: relay by name wrong type", post, "/saml/"+samlID+"/acs", "", `{"SAMLResponse": "x", "relay_state": 1}`),
		req("oidc authorize: bad pkce", post, "/oidc/"+oidcID+"/authorize", "", `{"use_pkce": "sometimes"}`),
		req("oidc authorize: valid", post, "/oidc/"+oidcID+"/authorize", "", `{}`),
		req("oidc callback: missing", post, "/oidc/"+oidcID+"/callback", "", `{"code_verifier": 1}`),
		req("oidc callback: valid", post, "/oidc/"+oidcID+"/callback", "", `{"code": "c", "state": "s"}`),
		req("oauth authorize: bad", post, "/oauth/"+oauthID+"/authorize", "", `{"redirect_uri": []}`),
		req("oauth authorize: valid", post, "/oauth/"+oauthID+"/authorize", "", `{}`),
		req("oauth callback: missing", post, "/oauth/"+oauthID+"/callback", "", `{}`),
		req("oauth callback: valid", post, "/oauth/"+oauthID+"/callback", "", `{"code": "c", "state": "s"}`),
		req("oauth by type: missing org_id", get, "/oauth/github/authorize", "", ""),
		req("oauth by type: valid", get, "/oauth/github/authorize?org_id="+orgA+"&redirect_uri=x", "", ""),
		// The overlapping two-segment /oauth paths, as Starlette resolves them.
		req("overlap: POST /oauth/providers/authorize", post, "/oauth/providers/authorize", "", `{}`),
		req("overlap: GET /oauth/providers/authorize", get, "/oauth/providers/authorize?org_id=o", "", ""),
		req("overlap: PATCH /oauth/providers/authorize", patch, "/oauth/providers/authorize", "", `{}`),
		req("overlap: PATCH /oauth/x/authorize", patch, "/oauth/x/authorize", "", `{}`),
		req("overlap: PATCH /oauth/x/callback", patch, "/oauth/x/callback", "", `{}`),
		req("overlap: GET /oauth/providers/callback", get, "/oauth/providers/callback", "", ""),
		req("overlap: DELETE /oauth/providers/x", del, "/oauth/providers/x", "", ""),
		req("overlap: HEAD /oauth/x/authorize", http.MethodHead, "/oauth/x/authorize", "", ""),
		req("overlap: GET /oauth/x/y", get, "/oauth/x/y", "", ""),
		req("overlap: PUT /oauth/providers/x", http.MethodPut, "/oauth/providers/x", "", ""),
		// Activate and deactivate: not gated.
		req("activate: no credential", post, "/sso/providers/"+samlID+"/activate", "", ""),
		req("activate: member", post, "/sso/providers/"+samlID+"/activate", "member", ""),
		req("activate: owner, saml (masking, truncation, null domains)", post, "/sso/providers/"+samlID+"/activate", "owner", ""),
		req("activate: again", post, "/sso/providers/"+samlID+"/activate", "owner", ""),
		req("deactivate: admin, oidc (client_secret masked)", post, "/sso/providers/"+oidcID+"/deactivate", "admin", ""),
		req("activate: oauth with a null config", post, "/sso/providers/"+oauthID+"/activate", "owner", ""),
		req("activate: another org's provider", post, "/sso/providers/"+otherOrgID+"/activate", "owner", ""),
		req("activate: unknown provider", post, "/sso/providers/"+unknownID+"/activate", "owner", ""),
		req("activate: stored config not an object (committed, then 500)", post, "/sso/providers/"+badConfigID+"/activate", "owner", ""),
		req("activate: stored allowed_domains not a list (committed, then 500)", post, "/sso/providers/"+badDomainsID+"/activate", "owner", ""),
		req("activate: token of the other org", post, "/sso/providers/"+samlID+"/activate", "otherid", ""),
		req("activate: token without an org", post, "/sso/providers/"+samlID+"/activate", "noorg", ""),
		req("activate: org claim not a uuid", post, "/sso/providers/"+samlID+"/activate", "badorg", ""),
		req("activate: provider id not a uuid", post, "/sso/providers/nope/activate", "owner", ""),
		req("activate: provider id in braces", post, "/sso/providers/{"+oidcID+"}/activate", "owner", ""),
		req("deactivate: GET is 405", get, "/sso/providers/"+samlID+"/deactivate", "owner", ""),
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, startGoAPI(t, ctx, venue), requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string {
			return updatedAtPattern.ReplaceAllString(body, `"updated_at":"<stamped>"`)
		},
	})
	rows := `SELECT id, org_id, name, protocol, status, is_default, allow_idp_initiated, auto_provision_users, default_role,
		config::text, coalesce(encrypted_secrets::text, '<null>'), coalesce(allowed_domains::text, '<null>'),
		created_at, updated_at > created_at FROM sso_providers ORDER BY id`
	pyRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), rows)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), rows)
	same := pyRows == goRows && pyRows != ""
	receipt += "sso_providers rows after the requests: " + venueoracle.Mark(same) + "\n"
	if !same {
		t.Errorf("sso_providers rows differ (or are empty):\n python %s\n go     %s", pyRows, goRows)
	}
	venueoracle.WriteProof(t)
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

// startGoAPI builds the dho api route set against the venue's Go copy as
// the api role.
func startGoAPI(t *testing.T, ctx context.Context, venue *venueoracle.Venue) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("DEV_HEALTH_VENUE_GO_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	deps := apiservice.Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger)}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// venueRoot is the repository root, where the venue finds the Python api.
func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
