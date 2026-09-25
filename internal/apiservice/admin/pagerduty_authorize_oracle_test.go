//go:build integration

package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	pagerDutyAuthorizeVenueEncryptionKey = "venue-pd-authorize-fernet-key-32-bytes!"
	pagerDutyAuthorizeVenueClientID      = "venue-pd-authorize-client-id"
	pagerDutyAuthorizeVenueRedirectURI   = "https://venue.example.com/pagerduty/callback"
)

// TestPagerDutyAuthorizeVenueOracle is the venue-oracle proof for POST
// .../authorize with everything configured: the URL's non-random content,
// the stored rows, and (the cross-plane property this route exists for)
// that a verifier one plane stored decrypts through the other plane's own
// decrypt and hashes to the code_challenge the URL carried.
func TestPagerDutyAuthorizeVenueOracle(t *testing.T) {
	runPagerDutyAuthorizeOracle(t, "full")
}

// TestPagerDutyAuthorizeWithoutClientIDVenueOracle: PAGER_DUTY_CLIENT_ID
// unset on both planes -- PagerDutyOAuthConfig.from_env() is None, a 400
// (after the feature gate).
func TestPagerDutyAuthorizeWithoutClientIDVenueOracle(t *testing.T) {
	runPagerDutyAuthorizeOracle(t, "noclientid")
}

// TestPagerDutyAuthorizeWithoutEncryptionKeyVenueOracle: no
// SETTINGS_ENCRYPTION_KEY -- encrypt_value raises inside the store's create,
// an unhandled 500 that must also roll back the expired-row cleanup.
func TestPagerDutyAuthorizeWithoutEncryptionKeyVenueOracle(t *testing.T) {
	runPagerDutyAuthorizeOracle(t, "nokey")
}

func runPagerDutyAuthorizeOracle(t *testing.T, mode string) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-authorize-32-by"

	orgs := map[string]uuid.UUID{}
	for _, slug := range []string{"a", "b", "off", "cleanup", "othercleanup"} {
		orgs[slug] = uuid.New()
	}
	adminID, memberID := uuid.New(), uuid.New()

	pythonEnv := []string{"PAGER_DUTY_REDIRECT_URI=" + pagerDutyAuthorizeVenueRedirectURI}
	if mode != "noclientid" {
		pythonEnv = append(pythonEnv, "PAGER_DUTY_CLIENT_ID="+pagerDutyAuthorizeVenueClientID)
	}
	if mode != "nokey" {
		pythonEnv = append(pythonEnv, "SETTINGS_ENCRYPTION_KEY="+pagerDutyAuthorizeVenueEncryptionKey)
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
			for slug, id := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "pd-auth-"+slug)
			}
			user := func(id uuid.UUID, email string) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, email)
			}
			user(adminID, "pd-auth-admin@example.com")
			user(memberID, "pd-auth-member@example.com")
			exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'canonical_incident_ingestion'), false, NULL, NULL, 'kill switch', NULL, now(), now())`,
				uuid.New(), orgs["off"])
			// An expired request for the org that authorizes below (must be
			// purged by that authorize) and one for an org that never does
			// (must survive: the purge is per org).
			for _, slug := range []string{"cleanup", "othercleanup"} {
				exec(`INSERT INTO pagerduty_oauth_authorization_requests (state_hash, org_id, code_verifier_encrypted, created_at, expires_at)
VALUES ($1, $2, 'v1:not-decryptable-here', now() - interval '2 hours', now() - interval '1 hour')`,
					hex.EncodeToString([]byte(slug + "-expired-state-hash-padding-to-64-chars-0123456789ab"))[:64], orgs[slug].String())
			}
			admin_ := func(slug string) map[string]any {
				return map[string]any{"user_id": adminID.String(), "email": "pd-auth-admin@example.com", "org_id": orgs[slug].String(), "role": "admin"}
			}
			return map[string]map[string]any{
				"admin_a":       admin_("a"),
				"admin_b":       admin_("b"),
				"admin_off":     admin_("off"),
				"admin_cleanup": admin_("cleanup"),
				"admin_badorg":  {"user_id": adminID.String(), "email": "pd-auth-admin@example.com", "org_id": "not-a-uuid", "role": "admin"},
				"admin_upper":   {"user_id": adminID.String(), "email": "pd-auth-admin@example.com", "org_id": strings.ToUpper(orgs["a"].String()), "role": "admin"},
				"member":        {"user_id": memberID.String(), "email": "pd-auth-member@example.com", "org_id": orgs["a"].String(), "role": "member"},
			}
		},
	})

	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const path = "/api/v1/admin/integrations/pagerduty/authorize"
	authorize := func(name, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: path, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}
	requests := []venueoracle.Request{
		authorize("W authorize", "admin_a", `{}`),
		authorize("W authorize again same org", "admin_a", `{}`),
		authorize("W authorize other org", "admin_b", `{}`),
		authorize("W authorize purges this org's expired row", "admin_cleanup", `{}`),
		authorize("W authorize uppercase org id claim", "admin_upper", `{}`),
		authorize("W authorize feature off refused", "admin_off", `{}`),
		authorize("W authorize malformed org id claim is 403", "admin_badorg", `{}`),
		authorize("W authorize extra field", "admin_a", `{"bogus":1}`),
		authorize("W authorize several extra fields in order", "admin_a", `{"z":1,"a":2}`),
		authorize("W authorize body list", "admin_a", `[]`),
		authorize("W authorize body null", "admin_a", `null`),
		authorize("W authorize empty body", "admin_a", ``),
		authorize("W authorize invalid json", "admin_a", `{`),
		{Name: "W authorize non-json content type", Method: "POST", Path: path,
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin_a"], "Content-Type": "text/plain"}, Body: venueoracle.B64("x")},
		{Name: "W authorize unauthenticated", Method: "POST", Path: path, Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{}`)},
		authorize("W authorize member refused", "member", `{}`),
		{Name: "authorize get is 405", Method: "GET", Path: path, Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin_a"]}},
	}

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyAuthorizeVenueEncryptionKey), "")
	if mode == "nokey" {
		decryptor = providerfoundation.FernetDecryptor{}
		err = nil
	}
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	clientID := pagerDutyAuthorizeVenueClientID
	if mode == "noclientid" {
		clientID = ""
	}

	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: clientID, RedirectURI: pagerDutyAuthorizeVenueRedirectURI}
	})
	goAnswers := map[string]string{}
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			return normalizeAuthorizeURL(t, body)
		},
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			if goResponse.Status == 200 && strings.Contains(request.Name, "authorize") {
				goAnswers[request.Name] = goResponse.Body
			}
		},
	})
	t.Log(receipt)

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	// The random state hash is excluded: per org, how many requests are
	// held, that each lives exactly 15 minutes (or is the seeded expired
	// row), and what kind of ciphertext it is.
	compare("authorization request rows", `SELECT org_id, count(*)::text,
	string_agg(EXTRACT(EPOCH FROM (expires_at - created_at))::text, ',' ORDER BY expires_at - created_at),
	string_agg(CASE WHEN code_verifier_encrypted LIKE 'v1:%' THEN 'v1' ELSE 'other' END, ',' ORDER BY code_verifier_encrypted LIKE 'v1:%')
FROM pagerduty_oauth_authorization_requests GROUP BY org_id ORDER BY org_id`)

	if mode != "full" {
		return
	}
	// The cross-plane PKCE property, for the answers each plane gave:
	// the stored verifier opens with Python's own decrypt_value, is 86
	// characters (secrets.token_urlsafe(64)), and its S256 hash is the
	// code_challenge the URL carried.
	checkPKCE := func(plane, dbName, body string) {
		t.Helper()
		var parsed struct {
			AuthorizeURL string `json:"authorize_url"`
		}
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Fatalf("%s: decode authorize answer: %v", plane, err)
		}
		u, err := url.Parse(parsed.AuthorizeURL)
		if err != nil {
			t.Fatalf("%s: parse authorize_url: %v", plane, err)
		}
		query := u.Query()
		for key, want := range map[string]string{
			"response_type": "code", "client_id": pagerDutyAuthorizeVenueClientID,
			"redirect_uri": pagerDutyAuthorizeVenueRedirectURI, "code_challenge_method": "S256",
			"scope": "escalation_policies.read incidents.read oncalls.read schedules.read services.read teams.read users.read",
		} {
			if query.Get(key) != want {
				t.Errorf("%s: %s = %q, want %q", plane, key, query.Get(key), want)
			}
		}
		state := query.Get("state")
		stateHash := sha256.Sum256([]byte(state))
		ciphertext := venueoracle.TableRows(t, ctx, venue.AdminURI(t, dbName),
			`SELECT code_verifier_encrypted FROM pagerduty_oauth_authorization_requests WHERE state_hash = '`+hex.EncodeToString(stateHash[:])+`'`)
		if ciphertext == "" {
			t.Errorf("%s: no stored request for the URL's state", plane)
			return
		}
		results := venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{ciphertext}})
		var verifier string
		if err := json.Unmarshal(results[0], &verifier); err != nil {
			t.Fatalf("%s: decode decrypt_value result: %v", plane, err)
		}
		if len(verifier) != 86 {
			t.Errorf("%s: verifier length %d, want 86", plane, len(verifier))
		}
		sum := sha256.Sum256([]byte(verifier))
		if got := strings.TrimRight(base64.URLEncoding.EncodeToString(sum[:]), "="); got != query.Get("code_challenge") {
			t.Errorf("%s: code_challenge %q does not match the stored verifier's S256 %q", plane, query.Get("code_challenge"), got)
		}
		if len(query.Get("nonce")) != 43 || len(state) != 43 {
			t.Errorf("%s: state/nonce lengths %d/%d, want 43 (token_urlsafe(32))", plane, len(state), len(query.Get("nonce")))
		}
	}
	for index, request := range requests {
		if request.Name != "W authorize" && request.Name != "W authorize other org" {
			continue
		}
		checkPKCE("python", venue.SourceDB, python[index].Body)
		body, ok := goAnswers[request.Name]
		if !ok {
			t.Errorf("go plane gave no 200 answer for %q", request.Name)
			continue
		}
		checkPKCE("go", venue.GoDB, body)
	}
}

// normalizeAuthorizeURL blanks the three random members of a 200 body's
// authorize_url (state, nonce, code_challenge) and puts the query in one
// canonical key order, so the parts that must agree between planes -- the
// endpoint, client id, redirect URI, scope, method -- are still compared.
// A body without an authorize_url string passes through unchanged.
func normalizeAuthorizeURL(t *testing.T, body string) string {
	t.Helper()
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return body
	}
	raw, present := object.Get("authorize_url")
	text, isString := raw.(string)
	if !present || !isString {
		return body
	}
	parsed, err := url.Parse(text)
	if err != nil {
		return body
	}
	query := parsed.Query()
	for _, key := range []string{"state", "nonce", "code_challenge"} {
		if query.Has(key) {
			query.Set(key, "<random>")
		}
	}
	parsed.RawQuery = query.Encode()
	object.Set("authorize_url", parsed.String())
	out, err := pyjson.Marshal(object)
	if err != nil {
		return body
	}
	return string(out)
}
