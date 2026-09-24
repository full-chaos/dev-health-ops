//go:build integration

package admin_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestPagerDutyStatusAndPreflightVenueOracle is the venue-oracle proof for
// GET /integrations/pagerduty/status and POST .../preflight: the real
// Python api and the real Go api answer the same requests against two
// copies of one database, byte for byte.
func TestPagerDutyStatusAndPreflightVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-status-32-bytes!"

	type orgSpec struct {
		id   uuid.UUID
		slug string
	}
	orgs := map[string]*orgSpec{}
	for _, slug := range []string{"empty", "oauth", "oauth-inactive", "oauth-nometadata", "token", "creds", "custom-name", "unknownmode"} {
		orgs[slug] = &orgSpec{id: uuid.New(), slug: "pd-" + slug}
	}
	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
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
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org.id, org.slug)
			}
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminID, "pd-admin@example.com", false)
			user(memberID, "pd-member@example.com", false)
			user(superID, "pd-super@example.com", true)

			credential := func(orgSlug, name string, isActive bool, config string) uuid.UUID {
				id := uuid.New()
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, $4, NULL, $5::json, now(), now())`, id, orgs[orgSlug].id.String(), name, isActive, config)
				return id
			}
			oauthMetadata := func(orgSlug, name, bindingID string, expiresAt any, grantedScopes string, hasRefresh bool, accountID, accountDisplay any) {
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id, expires_at, granted_scopes, has_refresh_token, account_id, account_display)
VALUES ($1, 'pagerduty', $2, 'v1:not-decryptable-here', 1, now(), now(), $3, $4, $5::json, $6, $7, $8)`,
					orgs[orgSlug].id.String(), name, bindingID, expiresAt, grantedScopes, hasRefresh, accountID, accountDisplay)
			}

			credential("oauth", "default", true, `{"auth_mode":"oauth","region":"us","subdomain":"acme","account_id":"cfg-acct"}`)
			oauthMetadata("oauth", "default", "b1", "2026-05-01T00:00:00+00:00", `["incidents.read","services.read","incidents.read"]`, true, "acct-1", "Acme Co")

			credential("oauth-inactive", "default", false, `{"auth_mode":"oauth","region":"us","subdomain":"gone"}`)
			oauthMetadata("oauth-inactive", "default", "b2", nil, `[]`, false, nil, nil)

			credential("oauth-nometadata", "default", true, `{"auth_mode":"oauth","region":"eu","subdomain":"stale"}`)
			// no provider_oauth_credentials row: the OAuth row was deleted
			// but the descriptor was not (an inconsistent-but-reachable state).

			credential("token", "default", true, `{"auth_mode":"api_token","region":"us","subdomain":"tok"}`)

			credential("creds", "default", true, `{"auth_mode":"client_credentials","region":"eu","subdomain":"cc","account_id":"cc-acct"}`)

			credential("custom-name", "secondary", true, `{"auth_mode":"api_token","region":"us","subdomain":"named"}`)

			credential("unknownmode", "default", true, `{"auth_mode":"unknown_mode","region":"us","subdomain":"weird"}`)

			return map[string]map[string]any{
				"admin":            {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["oauth"].id.String(), "role": "admin"},
				"admin_inactive":   {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["oauth-inactive"].id.String(), "role": "admin"},
				"admin_nometadata": {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["oauth-nometadata"].id.String(), "role": "admin"},
				"admin_token":      {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["token"].id.String(), "role": "admin"},
				"admin_creds":      {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["creds"].id.String(), "role": "admin"},
				"admin_named":      {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["custom-name"].id.String(), "role": "admin"},
				"admin_unknown":    {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["unknownmode"].id.String(), "role": "admin"},
				"admin_empty":      {"user_id": adminID.String(), "email": "pd-admin@example.com", "org_id": orgs["empty"].id.String(), "role": "admin"},
				"member":           {"user_id": memberID.String(), "email": "pd-member@example.com", "org_id": orgs["oauth"].id.String(), "role": "member"},
				"super":            {"user_id": superID.String(), "email": "pd-super@example.com", "is_superuser": true},
			}
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const path = "/api/v1/admin/integrations/pagerduty"
	get := func(name, urlPath, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: urlPath, Headers: auth(token)}
	}
	preflight := func(name, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: path + "/preflight", Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}

	requests := []venueoracle.Request{
		// ---- status ----------------------------------------------------------
		get("status oauth connected", path+"/status", "admin"),
		get("status oauth inactive", path+"/status", "admin_inactive"),
		get("status oauth no metadata row", path+"/status", "admin_nometadata"),
		get("status api_token", path+"/status", "admin_token"),
		get("status client_credentials", path+"/status", "admin_creds"),
		get("status unrecognized auth_mode", path+"/status", "admin_unknown"),
		get("status no descriptor at all", path+"/status", "admin_empty"),
		get("status custom credential_name", path+"/status?credential_name=secondary", "admin_named"),
		get("status unknown credential_name on a connected org", path+"/status?credential_name=nope", "admin"),
		get("status empty credential_name query value", path+"/status?credential_name=", "admin_empty"),
		get("status member refused", path+"/status", "member"),
		get("status superuser without org", path+"/status", "super"),
		{Name: "status unauthenticated", Method: "GET", Path: path + "/status"},
		{Name: "status post is 405", Method: "POST", Path: path + "/status", Headers: jsonAuth("admin"), Body: venueoracle.B64(`{}`)},
		// ---- preflight ---------------------------------------------------------
		preflight("preflight oauth all granted", "admin", `{"enabled_datasets":["incidents","services"]}`),
		preflight("preflight oauth some missing", "admin", `{"enabled_datasets":["incidents","escalation-policies","teams"]}`),
		preflight("preflight oauth business-services family", "admin", `{"enabled_datasets":["business-services","incident-alerts"]}`),
		preflight("preflight oauth duplicate dataset", "admin", `{"enabled_datasets":["users","users"]}`),
		preflight("preflight oauth empty datasets", "admin", `{"enabled_datasets":[]}`),
		preflight("preflight oauth inactive still computes", "admin_inactive", `{"enabled_datasets":["incidents"]}`),
		preflight("preflight oauth no metadata row", "admin_nometadata", `{"enabled_datasets":["incidents"]}`),
		preflight("preflight api_token grantable bypasses scopes", "admin_token", `{"enabled_datasets":["incidents","on-calls"]}`),
		preflight("preflight client_credentials grantable bypasses scopes", "admin_creds", `{"enabled_datasets":["schedules"]}`),
		preflight("preflight unrecognized auth_mode not grantable", "admin_unknown", `{"enabled_datasets":["incidents"]}`),
		preflight("preflight no descriptor at all", "admin_empty", `{"enabled_datasets":["incidents"]}`),
		preflight("preflight custom credential_name", "admin_named", `{"credential_name":"secondary","enabled_datasets":["incidents"]}`),
		preflight("preflight unknown dataset", "admin", `{"enabled_datasets":["not-a-real-dataset"]}`),
		preflight("preflight mixed known and unknown datasets", "admin", `{"enabled_datasets":["incidents","bogus-one","also-bogus"]}`),
		preflight("preflight duplicate unknown datasets are named once", "admin", `{"enabled_datasets":["bogus-one","bogus-one","bogus-two","bogus-one"]}`),
		preflight("preflight unknown datasets are sorted by code point", "admin", `{"enabled_datasets":["zz","é","B","a","é"]}`),
		preflight("preflight credential_name whitespace only", "admin", `{"credential_name":"   ","enabled_datasets":["incidents"]}`),
		preflight("preflight credential_name empty string", "admin", `{"credential_name":"","enabled_datasets":["incidents"]}`),
		preflight("preflight credential_name padded", "admin", `{"credential_name":"  default  ","enabled_datasets":["incidents"]}`),
		preflight("preflight credential_name null", "admin", `{"credential_name":null,"enabled_datasets":["incidents"]}`),
		preflight("preflight credential_name number", "admin", `{"credential_name":5,"enabled_datasets":["incidents"]}`),
		preflight("preflight enabled_datasets missing", "admin", `{}`),
		preflight("preflight enabled_datasets null", "admin", `{"enabled_datasets":null}`),
		preflight("preflight enabled_datasets not a list", "admin", `{"enabled_datasets":"incidents"}`),
		preflight("preflight enabled_datasets element not a string", "admin", `{"enabled_datasets":["incidents",5]}`),
		preflight("preflight extra field", "admin", `{"enabled_datasets":["incidents"],"bogus":1}`),
		preflight("preflight several errors then extra", "admin", `{"credential_name":"","enabled_datasets":"nope","bogus":1}`),
		preflight("preflight body list", "admin", `[]`),
		preflight("preflight body null", "admin", `null`),
		preflight("preflight invalid json", "admin", `{`),
		{Name: "preflight non-json content type", Method: "POST", Path: path + "/preflight",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "text/plain"}, Body: venueoracle.B64("x")},
		{Name: "preflight unauthenticated", Method: "POST", Path: path + "/preflight", Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"enabled_datasets":["incidents"]}`)},
		preflight("preflight member refused", "member", `{"enabled_datasets":["incidents"]}`),
		preflight("preflight superuser without org", "super", `{"enabled_datasets":["incidents"]}`),
		get("preflight get is 405", path+"/preflight", "admin"),
	}

	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}
