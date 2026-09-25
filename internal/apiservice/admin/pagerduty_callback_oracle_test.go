//go:build integration

package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
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
	pagerDutyCallbackVenueEncryptionKey = "venue-pd-callback-fernet-key-32-bytes!"
	pagerDutyCallbackVenueClientID      = "venue-pd-callback-client-id"
	pagerDutyCallbackVenueSecret        = "venue-pd-callback-client-secret"
	pagerDutyCallbackVenueRedirectURI   = "https://app.example.test/oauth/pagerduty/callback"
)

var pagerDutyAllReadScopes = "escalation_policies.read incidents.read oncalls.read schedules.read services.read teams.read users.read"

// fakePagerDuty is ONE upstream for both planes: the token endpoint, the
// regional REST reads and the revoke endpoint. It is stateless in what it
// answers (the code, client id, bearer token or api token picks the answer),
// so the Python batch and the Go batch see the same world; what differs
// between planes is only what each asks, which it records line by line.
type fakePagerDuty struct {
	mu         sync.Mutex
	lines      []string
	failRevoke map[string]bool
	// redirectTo is where a revoke of "rt-redirect" is redirected (307,
	// which replays the POST body): a plane that follows it delivers the
	// token to a third party, which captured counts.
	redirectTo string
	captured   int
}

type fakeAccount struct{ id, subdomain, name string }

var fakeAccounts = map[string]fakeAccount{
	"acme":    {"ACC1", "acme", "Acme Co"},
	"eu":      {"ACCEU", "acme-eu", "Acme EU"},
	"other":   {"ACC2", "other-co", "Other Co"},
	"float":   {"ACCF", "float-co", "Float Co"},
	"neg":     {"ACCN", "neg-co", "Neg Co"},
	"nodisp":  {"ACCD", "nodisp", ""},
	"htmlurl": {},
}

func (f *fakePagerDuty) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, fmt.Sprintf(format, args...))
}

func (f *fakePagerDuty) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.lines
	f.lines = nil
	return out
}

func sortedForm(values url.Values) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strings.Join(values[key], ","))
	}
	return strings.Join(parts, "&")
}

func (f *fakePagerDuty) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(body))
		f.record("POST /token %s", sortedForm(form))
		reply := func(status int, payload string) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, payload)
		}
		token := func(access, refresh, scope, expires string) string {
			out := `{"access_token":"` + access + `"`
			if refresh != "" {
				out += `,"refresh_token":"` + refresh + `"`
			}
			out += `,"scope":"` + scope + `","token_type":"bearer"`
			if expires != "" {
				out += `,"expires_in":` + expires
			}
			return out + `}`
		}
		truncated := func() {
			// Promise more bytes than are sent: the reader sees the body end early.
			w.Header().Set("Content-Length", "500")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `{"access_token":"at-tru`)
		}
		if form.Get("grant_type") == "client_credentials" {
			switch form.Get("client_id") {
			case "cc-truncated":
				truncated()
			case "cc-rejected":
				reply(401, `{"error":"invalid_client"}`)
			case "cc-badjson":
				reply(200, `not json`)
			case "cc-list":
				reply(200, `[]`)
			case "cc-noaccess":
				reply(200, `{"scope":"`+pagerDutyAllReadScopes+`"}`)
			case "cc-badscope":
				reply(200, `{"access_token":"at-cc-ok","scope":5}`)
			case "cc-missing":
				reply(200, token("at-cc-missing", "", "incidents.read services.read", ""))
			case "cc-other":
				reply(200, token("at-cc-other", "", pagerDutyAllReadScopes, ""))
			case "cc-eu":
				reply(200, token("at-cc-eu", "", pagerDutyAllReadScopes, ""))
			default:
				reply(200, token("at-cc-ok", "", pagerDutyAllReadScopes, ""))
			}
			return
		}
		switch form.Get("code") {
		case "c-truncated":
			truncated()
		case "c-truncated-403":
			w.Header().Set("Content-Length", "500")
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"err`)
		case "c-missing-refused":
			reply(200, token("at-missing-refused", "rt-missing-refused", "incidents.read services.read", "3600"))
		case "c-noaccount-refused":
			reply(200, token("at-noaccount", "rt-noaccount-refused", pagerDutyAllReadScopes, "3600"))
		case "c-corrupt-refused":
			reply(200, token("at-ok", "rt-corrupt-refused", pagerDutyAllReadScopes, "3600"))
		case "c-missing-redirect":
			reply(200, token("at-missing-redirect", "rt-redirect", "incidents.read", "3600"))
		case "c-rejected":
			reply(400, `{"error":"invalid_grant"}`)
		case "c-forbidden":
			reply(403, `{}`)
		case "c-server-error":
			reply(500, `{}`)
		case "c-redirect":
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(302)
		case "c-badjson":
			reply(200, `not json`)
		case "c-noaccess":
			reply(200, `{"token_type":"bearer"}`)
		case "c-badexpires":
			reply(200, token("at-ok", "rt-ok", pagerDutyAllReadScopes, `"abc"`))
		case "c-ok":
			reply(200, token("at-ok", "rt-ok", pagerDutyAllReadScopes, "3600"))
		case "c-ok2":
			reply(200, token("at-ok2", "rt-ok2", pagerDutyAllReadScopes, "3600"))
		case "c-ok3":
			reply(200, token("at-ok3", "rt-ok3", pagerDutyAllReadScopes, "3600"))
		case "c-norefresh":
			reply(200, token("at-norefresh", "", pagerDutyAllReadScopes, `"120"`))
		case "c-missing-scopes":
			reply(200, token("at-missing", "rt-missing", "incidents.read services.read", "3600"))
		case "c-eu":
			reply(200, token("at-eu", "rt-eu", pagerDutyAllReadScopes, "3600"))
		case "c-noaccount":
			reply(200, token("at-noaccount", "rt-noaccount", pagerDutyAllReadScopes, "3600"))
		case "c-htmlurl":
			reply(200, token("at-htmlurl", "rt-htmlurl", pagerDutyAllReadScopes, "3600"))
		case "c-nodisplay":
			reply(200, token("at-nodisp", "rt-nodisp", pagerDutyAllReadScopes, "3600"))
		case "c-expfloat":
			reply(200, token("at-float", "rt-float", pagerDutyAllReadScopes, "1.5"))
		case "c-expneg":
			reply(200, token("at-neg", "rt-neg", pagerDutyAllReadScopes, "-30"))
		case "c-readfails":
			reply(200, token("at-readfails", "rt-readfails", pagerDutyAllReadScopes, "3600"))
		default:
			reply(400, `{"error":"unknown_code"}`)
		}
	})
	mux.HandleFunc("POST /revoke", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(body))
		f.record("POST /revoke %s", sortedForm(form))
		f.mu.Lock()
		fail := f.failRevoke[form.Get("token")]
		redirectTo := f.redirectTo
		f.mu.Unlock()
		if form.Get("token") == "rt-redirect" {
			w.Header().Set("Location", redirectTo+"/captured")
			w.WriteHeader(307)
			return
		}
		if fail {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	})
	services := func(w http.ResponseWriter, r *http.Request) {
		region := r.PathValue("region")
		authorization := r.Header.Get("Authorization")
		f.record("GET /%s/services?%s auth=%q accept=%q", region, r.URL.RawQuery, authorization, r.Header.Get("Accept"))
		accountKey := map[string]string{
			"Bearer at-ok": "acme", "Bearer at-ok2": "acme", "Bearer at-ok3": "acme", "Bearer at-norefresh": "acme",
			"Bearer at-missing": "acme", "Bearer at-cc-ok": "acme", "Bearer at-cc-missing": "acme", "Token token=tok-acme": "acme",
			"Bearer at-eu": "eu", "Bearer at-cc-eu": "eu", "Bearer at-cc-other": "other", "Token token=tok-other": "other",
			"Bearer at-htmlurl": "htmlurl", "Token token=tok-html": "htmlurl", "Bearer at-nodisp": "nodisp",
			"Bearer at-float": "float", "Bearer at-neg": "neg", "Bearer at-noaccount": "noaccount",
			"Token token=tok-list": "notlist", "Token token=tok-noservices": "noservices",
		}[authorization]
		w.Header().Set("Content-Type", "application/json")
		respond := func(status int, payload string) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, payload)
		}
		switch {
		case accountKey == "" || (accountKey == "eu" && region == "us"):
			respond(401, `{"error":{"message":"Unauthorized"}}`)
		case accountKey == "noaccount":
			respond(200, `{"services":[]}`)
		case accountKey == "notlist":
			respond(200, `[]`)
		case accountKey == "noservices":
			respond(200, `{"services":"none"}`)
		case accountKey == "htmlurl":
			respond(200, `{"services":[{"id":"P1","html_url":"https://HtmlAcct.pagerduty.com/service-directory/P1"}]}`)
		case authorization == "Token token=tok-html2":
			respond(200, `{"services":[{"id":"P1","html_url":"https://api.pagerduty.com/services/P1"}]}`)
		default:
			account := fakeAccounts[accountKey]
			payload := `{"services":[{"id":"P1","account":{"id":"` + account.id + `","subdomain":"` + account.subdomain + `"`
			if account.name != "" {
				payload += `,"name":"` + account.name + `"`
			}
			respond(200, payload+`}}]}`)
		}
	}
	mux.HandleFunc("GET /{region}/services", services)
	return mux
}

type callbackOrg struct {
	slug   string
	gateOn bool
}

func TestPagerDutyCallbackAndManualVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-callback-32-b"

	fake := &fakePagerDuty{failRevoke: map[string]bool{"old-fail-rt": true, "rt-missing-refused": true, "rt-noaccount-refused": true, "rt-corrupt-refused": true}}
	upstream := httptest.NewServer(fake.handler())
	t.Cleanup(upstream.Close)
	capture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.captured++
		fake.mu.Unlock()
		w.WriteHeader(200)
	}))
	t.Cleanup(capture.Close)
	fake.redirectTo = capture.URL

	orgSlugs := []string{"fresh", "replacefail", "variants", "failures", "corrupt", "off", "cc", "ccoauth", "cccorrupt", "tok", "ccoff", "refmissing", "refnoacct", "refcorrupt", "refredirect"}
	orgs := map[string]uuid.UUID{}
	for _, slug := range orgSlugs {
		orgs[slug] = uuid.New()
	}
	adminID, memberID, superID := uuid.New(), uuid.New(), uuid.New()

	// state -> (org, verifier, expired)
	type stateSpec struct {
		org      string
		expired  bool
		verifier string
	}
	states := map[string]stateSpec{}
	addState := func(name, org string) string {
		states[name] = stateSpec{org: org, verifier: "verifier-for-" + name}
		return name
	}
	for _, spec := range []struct{ name, org string }{
		{"s-fresh-1", "fresh"}, {"s-fresh-2", "fresh"},
		{"s-rf-1", "replacefail"}, {"s-rf-2", "replacefail"},
		{"s-var-norefresh", "variants"}, {"s-var-eu", "variants"}, {"s-var-html", "variants"}, {"s-var-nodisp", "variants"},
		{"s-var-float", "variants"}, {"s-var-neg", "variants"}, {"s-var-other-org", "variants"},
		{"s-fail-missing", "failures"}, {"s-fail-noaccount", "failures"}, {"s-fail-readfails", "failures"}, {"s-fail-rejected", "failures"},
		{"s-fail-forbidden", "failures"}, {"s-fail-server", "failures"}, {"s-fail-redirect", "failures"}, {"s-fail-badjson", "failures"},
		{"s-fail-noaccess", "failures"}, {"s-fail-badexpires", "failures"}, {"s-fail-error", "failures"}, {"s-fail-nocode", "failures"},
		{"s-fail-emptycode", "failures"}, {"s-fail-truncated", "failures"}, {"s-fail-truncated403", "failures"}, {"s-fail-errorempty", "failures"}, {"s-fail-reuse", "failures"}, {"s-fail-extra", "failures"},
		{"s-corrupt", "corrupt"}, {"s-off", "off"},
		// A compensating revoke PagerDuty refuses (CHAOS-6631): the first state
		// of each org runs on both planes, the second only on the Go plane
		// once the fake accepts revokes again.
		{"s-refm-1", "refmissing"}, {"s-refm-2", "refmissing"}, {"s-refm-3", "refmissing"}, {"s-refn-1", "refnoacct"}, {"s-refn-2", "refnoacct"}, {"s-refn-3", "refnoacct"},
		{"s-refc-1", "refcorrupt"}, {"s-refc-2", "refcorrupt"}, {"s-refr-1", "refredirect"}, {"s-refr-2", "refredirect"},
	} {
		addState(spec.name, spec.org)
	}
	states["s-fail-expired"] = stateSpec{org: "failures", expired: true, verifier: "verifier-for-expired"}

	const oldOAuthPlain = `{"access_token":"old-at","refresh_token":"%s","expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"PAGER_DUTY_CLIENT_ID=" + pagerDutyCallbackVenueClientID,
			"PAGER_DUTY_SECRET=" + pagerDutyCallbackVenueSecret,
			"PAGER_DUTY_REDIRECT_URI=" + pagerDutyCallbackVenueRedirectURI,
			"SETTINGS_ENCRYPTION_KEY=" + pagerDutyCallbackVenueEncryptionKey,
			"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE=" + upstream.URL + "/revoke",
			"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE=" + upstream.URL + "/token",
			"VENUE_PAGERDUTY_API_BASE_OVERRIDE=" + upstream.URL,
		},
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
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "pd-cb-"+slug)
			}
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminID, "pd-cb-admin@example.com", false)
			user(memberID, "pd-cb-member@example.com", false)
			user(superID, "pd-cb-super@example.com", true)
			for _, slug := range []string{"off", "ccoff"} {
				exec(`INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, (SELECT id FROM feature_flags WHERE key = 'canonical_incident_ingestion'), false, NULL, NULL, 'kill switch', NULL, now(), now())`,
					uuid.New(), orgs[slug])
			}

			// Every ciphertext is written by Python's own encrypt_value.
			var plains []string
			index := map[string]int{}
			need := func(plain string) {
				if _, ok := index[plain]; !ok {
					index[plain] = len(plains)
					plains = append(plains, plain)
				}
			}
			names := make([]string, 0, len(states))
			for name := range states {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				need(states[name].verifier)
			}
			for _, refresh := range []string{"old-rt", "old-fail-rt", "old-cc-rt", "old-cc2-rt"} {
				need(fmt.Sprintf(oldOAuthPlain, refresh))
			}
			calls := make([]venueoracle.PythonCall, len(plains))
			for i, plain := range plains {
				calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{plain}}
			}
			results := v.CallPython(t, calls...)
			encrypted := func(plain string) string {
				var out string
				if err := json.Unmarshal(results[index[plain]], &out); err != nil {
					t.Fatalf("decode encrypt_value: %v", err)
				}
				return out
			}

			for _, name := range names {
				spec := states[name]
				digest := sha256.Sum256([]byte(name))
				expires := "now() + interval '15 minutes'"
				created := "now()"
				if spec.expired {
					expires, created = "now() - interval '1 hour'", "now() - interval '2 hours'"
				}
				exec(`INSERT INTO pagerduty_oauth_authorization_requests (state_hash, org_id, code_verifier_encrypted, created_at, expires_at)
VALUES ($1, $2, $3, `+created+`, `+expires+`)`, hex.EncodeToString(digest[:]), orgs[spec.org].String(), encrypted(spec.verifier))
			}
			oauthRow := func(org, credentialName, refresh string) {
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id, has_refresh_token, granted_scopes)
VALUES ($1, 'pagerduty', $2, $3, 4, now(), now(), 'seed-binding', true, '["incidents.read"]'::json)`,
					orgs[org].String(), credentialName, encrypted(fmt.Sprintf(oldOAuthPlain, refresh)))
			}
			integration := func(org, name string) {
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, false, NULL, '{"region": "us", "auth_mode": "oauth", "subdomain": "old-sub"}'::json, now(), now())`,
					uuid.New(), orgs[org].String(), name)
			}
			// Replacement: fresh org's first callback creates the row; the
			// replacefail org starts with a grant whose revoke the upstream
			// refuses, so its row stays pending.
			oauthRow("replacefail", "Acme Co", "old-fail-rt")
			integration("replacefail", "Acme Co")
			oauthRow("ccoauth", "default", "old-cc-rt")
			integration("ccoauth", "default")
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'Acme Co', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["corrupt"].String())
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'Acme Co', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["refcorrupt"].String())
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["cccorrupt"].String())
			integration("cccorrupt", "default")

			admin_ := func(slug string) map[string]any {
				return map[string]any{"user_id": adminID.String(), "email": "pd-cb-admin@example.com", "org_id": orgs[slug].String(), "role": "admin"}
			}
			out := map[string]map[string]any{
				"member": {"user_id": memberID.String(), "email": "pd-cb-member@example.com", "org_id": orgs["fresh"].String(), "role": "member"},
				"super":  {"user_id": superID.String(), "email": "pd-cb-super@example.com", "is_superuser": true},
			}
			for _, slug := range orgSlugs {
				out["admin_"+slug] = admin_(slug)
			}
			return out
		},
	})

	jsonAuth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	const prefix = "/api/v1/admin/integrations/pagerduty"
	post := func(name, route, token, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: prefix + route, Headers: jsonAuth(token), Body: venueoracle.B64(body)}
	}
	cb := func(name, token, state, code string) venueoracle.Request {
		return post("W callback "+name, "/callback", token, `{"state":"`+state+`","code":"`+code+`"}`)
	}

	requests := []venueoracle.Request{
		// ---- callback: connecting and replacing --------------------------------
		cb("fresh connect", "admin_fresh", "s-fresh-1", "c-ok"),
		cb("replace grant, old one revoked", "admin_fresh", "s-fresh-2", "c-ok2"),
		cb("replace grant, old revoke refused stays pending", "admin_replacefail", "s-rf-1", "c-ok"),
		cb("second replace retries the pending revoke", "admin_replacefail", "s-rf-2", "c-ok2"),
		cb("no refresh token, string expires_in", "admin_variants", "s-var-norefresh", "c-norefresh"),
		cb("eu region found after us refuses", "admin_variants", "s-var-eu", "c-eu"),
		cb("identity from the service html_url", "admin_variants", "s-var-html", "c-htmlurl"),
		cb("account without a name uses the subdomain", "admin_variants", "s-var-nodisp", "c-nodisplay"),
		cb("float expires_in means an hour", "admin_variants", "s-var-float", "c-expfloat"),
		cb("negative expires_in is not clamped", "admin_variants", "s-var-neg", "c-expneg"),
		cb("feature flag off does not gate the callback", "admin_off", "s-off", "c-ok"),
		cb("undecryptable existing grant, new grant revoked", "admin_corrupt", "s-corrupt", "c-ok"),
		// ---- callback: what PagerDuty or the state refuses ----------------------
		cb("scopes missing", "admin_failures", "s-fail-missing", "c-missing-scopes"),
		cb("no account for the token", "admin_failures", "s-fail-noaccount", "c-noaccount"),
		cb("live read refused", "admin_failures", "s-fail-readfails", "c-readfails"),
		cb("code rejected 400", "admin_failures", "s-fail-rejected", "c-rejected"),
		cb("code rejected 403", "admin_failures", "s-fail-forbidden", "c-forbidden"),
		cb("service 500", "admin_failures", "s-fail-server", "c-server-error"),
		cb("service redirect", "admin_failures", "s-fail-redirect", "c-redirect"),
		cb("token body cut short is a transport error", "admin_failures", "s-fail-truncated", "c-truncated"),
		cb("error status body cut short is a transport error", "admin_failures", "s-fail-truncated403", "c-truncated-403"),
		// A compensating revoke PagerDuty refuses (CHAOS-6631): the answers are
		// Python's on both planes; what the Go plane also keeps is asserted below.
		cb("compensating revoke refused after missing scopes", "admin_refmissing", "s-refm-1", "c-missing-refused"),
		cb("compensating revoke refused after no account", "admin_refnoacct", "s-refn-1", "c-noaccount-refused"),
		cb("compensating revoke refused after a failed local write", "admin_refcorrupt", "s-refc-1", "c-corrupt-refused"),
		cb("compensating revoke redirected is not followed", "admin_refredirect", "s-refr-1", "c-missing-redirect"),
		cb("token body not json", "admin_failures", "s-fail-badjson", "c-badjson"),
		cb("token body without access_token", "admin_failures", "s-fail-noaccess", "c-noaccess"),
		cb("expires_in not an integer", "admin_failures", "s-fail-badexpires", "c-badexpires"),
		post("W callback error from provider", "/callback", "admin_failures", `{"state":"s-fail-error","error":"access_denied","code":"c-ok"}`),
		post("W callback code missing", "/callback", "admin_failures", `{"state":"s-fail-nocode"}`),
		post("W callback code empty", "/callback", "admin_failures", `{"state":"s-fail-emptycode","code":""}`),
		post("W callback empty error does not count", "/callback", "admin_failures", `{"state":"s-fail-errorempty","error":"","code":"c-rejected"}`),
		cb("state used once", "admin_failures", "s-fail-reuse", "c-rejected"),
		cb("state used twice", "admin_failures", "s-fail-reuse", "c-rejected"),
		cb("state unknown", "admin_failures", "s-nope", "c-ok"),
		cb("state expired", "admin_failures", "s-fail-expired", "c-ok"),
		cb("state of another org", "admin_failures", "s-var-other-org", "c-ok"),
		post("W callback extra field", "/callback", "admin_failures", `{"state":"s-fail-extra","code":"c-rejected","x":1}`),
		post("W callback state missing", "/callback", "admin_failures", `{"code":"c-ok"}`),
		post("W callback state empty", "/callback", "admin_failures", `{"state":"","code":"c-ok"}`),
		post("W callback state number", "/callback", "admin_failures", `{"state":5,"code":5,"error":[]}`),
		post("W callback several errors and extras", "/callback", "admin_failures", `{"code":7,"z":1,"a":2}`),
		post("W callback body list", "/callback", "admin_failures", `[]`),
		post("W callback body null", "/callback", "admin_failures", `null`),
		post("W callback invalid json", "/callback", "admin_failures", `{`),
		{Name: "W callback unauthenticated", Method: "POST", Path: prefix + "/callback", Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"state":"a","code":"b"}`)},
		post("W callback member refused", "/callback", "member", `{"state":"a","code":"b"}`),
		post("W callback superuser without org", "/callback", "super", `{"state":"a","code":"b"}`),
		{Name: "callback get is 405", Method: "GET", Path: prefix + "/callback", Headers: jsonAuth("admin_failures")},
	}

	ccBody := func(clientID, subdomain string, extra string) string {
		return `{"client_id":"` + clientID + `","client_secret":"shh-` + clientID + `","subdomain":"` + subdomain + `"` + extra + `}`
	}
	tokBody := func(apiToken, subdomain string, extra string) string {
		return `{"api_token":"` + apiToken + `","subdomain":"` + subdomain + `"` + extra + `}`
	}
	requests = append(requests,
		// ---- client credentials -----------------------------------------------------
		post("W client-credentials connect", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", "")),
		post("W client-credentials replaces itself", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", "")),
		post("W client-credentials subdomain case-insensitive, padded", "/client-credentials", "admin_cc", ccBody("cc-ok", "  ACME ", `,"credential_name":"  padded name "`)),
		post("W client-credentials eu region", "/client-credentials", "admin_cc", ccBody("cc-eu", "acme-eu", `,"region":"eu","credential_name":"eu"`)),
		post("W client-credentials other account", "/client-credentials", "admin_cc", ccBody("cc-other", "acme", "")),
		post("W client-credentials rejected", "/client-credentials", "admin_cc", ccBody("cc-rejected", "acme", "")),
		post("W client-credentials token body cut short", "/client-credentials", "admin_cc", ccBody("cc-truncated", "acme", "")),
		post("W client-credentials token body not json", "/client-credentials", "admin_cc", ccBody("cc-badjson", "acme", "")),
		post("W client-credentials token body a list", "/client-credentials", "admin_cc", ccBody("cc-list", "acme", "")),
		post("W client-credentials token without access_token", "/client-credentials", "admin_cc", ccBody("cc-noaccess", "acme", "")),
		post("W client-credentials scope not a string", "/client-credentials", "admin_cc", ccBody("cc-badscope", "acme", "")),
		post("W client-credentials scopes missing", "/client-credentials", "admin_cc", ccBody("cc-missing", "acme", "")),
		post("W client-credentials retires an OAuth binding and revokes it", "/client-credentials", "admin_ccoauth", ccBody("cc-ok", "acme", "")),
		post("W client-credentials retires an unreadable binding without a revoke", "/client-credentials", "admin_cccorrupt", ccBody("cc-ok", "acme", "")),
		post("W client-credentials feature off refused", "/client-credentials", "admin_ccoff", ccBody("cc-ok", "acme", "")),
		post("W client-credentials invalid body wins over the feature gate", "/client-credentials", "admin_ccoff", `{}`),
		post("W client-credentials fields missing", "/client-credentials", "admin_cc", `{}`),
		post("W client-credentials fields empty", "/client-credentials", "admin_cc", `{"client_id":"","client_secret":"","subdomain":""}`),
		post("W client-credentials blank subdomain and name", "/client-credentials", "admin_cc", ccBody("cc-ok", "   ", `,"credential_name":"  "`)),
		post("W client-credentials region invalid", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", `,"region":"ap"`)),
		post("W client-credentials region null", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", `,"region":null`)),
		post("W client-credentials name null", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", `,"credential_name":null`)),
		post("W client-credentials types wrong", "/client-credentials", "admin_cc", `{"client_id":1,"client_secret":[],"subdomain":{},"region":3,"credential_name":true}`),
		post("W client-credentials extra fields", "/client-credentials", "admin_cc", ccBody("cc-ok", "acme", `,"z":1,"a":2`)),
		post("W client-credentials body list", "/client-credentials", "admin_cc", `[]`),
		post("W client-credentials member refused", "/client-credentials", "member", ccBody("cc-ok", "acme", "")),
		venueoracle.Request{Name: "W client-credentials unauthenticated", Method: "POST", Path: prefix + "/client-credentials", Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(ccBody("cc-ok", "acme", ""))},
		// ---- api token ----------------------------------------------------------------
		post("W api-token connect", "/api-token", "admin_tok", tokBody("tok-acme", "acme", "")),
		post("W api-token named credential, eu", "/api-token", "admin_tok", tokBody("tok-acme", "acme", `,"credential_name":"second","region":"eu"`)),
		post("W api-token identity from html_url", "/api-token", "admin_tok", tokBody("tok-html", "htmlacct", `,"credential_name":"html"`)),
		post("W api-token html_url on the api host has no subdomain", "/api-token", "admin_tok", tokBody("tok-html2", "acme", "")),
		post("W api-token other account", "/api-token", "admin_tok", tokBody("tok-other", "acme", "")),
		post("W api-token rejected", "/api-token", "admin_tok", tokBody("tok-bad", "acme", "")),
		post("W api-token services body a list", "/api-token", "admin_tok", tokBody("tok-list", "acme", "")),
		post("W api-token services not a list", "/api-token", "admin_tok", tokBody("tok-noservices", "acme", "")),
		post("W api-token fields missing", "/api-token", "admin_tok", `{}`),
		post("W api-token extra field", "/api-token", "admin_tok", tokBody("tok-acme", "acme", `,"client_id":"x"`)),
		post("W api-token blank subdomain", "/api-token", "admin_tok", tokBody("tok-acme", " ", "")),
		post("W api-token region invalid", "/api-token", "admin_tok", tokBody("tok-acme", "acme", `,"region":"US"`)),
		post("W api-token member refused", "/api-token", "member", tokBody("tok-acme", "acme", "")),
		post("W api-token superuser without org", "/api-token", "super", tokBody("tok-acme", "acme", "")),
		venueoracle.Request{Name: "api-token get is 405", Method: "GET", Path: prefix + "/api-token", Headers: jsonAuth("admin_tok")},
	)

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyCallbackVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	python := venue.ServePython(t, requests)
	pythonCalls := fake.take()
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{
			ClientID: pagerDutyCallbackVenueClientID, ClientSecret: pagerDutyCallbackVenueSecret, RedirectURI: pagerDutyCallbackVenueRedirectURI,
			RevokeURL: upstream.URL + "/revoke", TokenURL: upstream.URL + "/token", APIBaseOverride: upstream.URL,
		}
	})
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
	goCalls := fake.take()

	// What each plane asked PagerDuty, in order: the exchange's form, the
	// region and credentials of every live read, every revoke.
	if strings.Join(pythonCalls, "\n") != strings.Join(goCalls, "\n") {
		t.Errorf("upstream calls differ:\n python (%d):\n%s\n go (%d):\n%s", len(pythonCalls), strings.Join(pythonCalls, "\n"), len(goCalls), strings.Join(goCalls, "\n"))
	}
	// The redirected revoke must reach the redirect target on NEITHER plane.
	fake.mu.Lock()
	captured := fake.captured
	fake.mu.Unlock()
	if captured != 0 {
		t.Errorf("a revoke redirect was followed and delivered a token: %d request(s) reached the redirect target", captured)
	}
	if len(pythonCalls) < 60 {
		t.Errorf("the fake upstream saw only %d calls from the Python plane", len(pythonCalls))
	}

	compare := func(name, query string) {
		t.Helper()
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != goRows {
			t.Errorf("%s differs after the writes:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	// The states only the Go plane's follow-up callbacks consume are not part
	// of the two-plane comparison.
	var goOnlyStates []string
	for _, name := range []string{"s-refm-2", "s-refm-3", "s-refn-2", "s-refn-3", "s-refc-2", "s-refr-2"} {
		digest := sha256.Sum256([]byte(name))
		goOnlyStates = append(goOnlyStates, "'"+hex.EncodeToString(digest[:])+"'")
	}
	compare("authorization requests left", `SELECT org_id, state_hash FROM pagerduty_oauth_authorization_requests WHERE state_hash NOT IN (`+strings.Join(goOnlyStates, ",")+`) ORDER BY org_id, state_hash`)
	compare("integration_credentials rows (config as stored text)", `SELECT org_id, name, is_active, config::text, last_test_success::text, last_test_error, (last_test_at IS NULL)::text
FROM integration_credentials ORDER BY org_id, name`)
	compare("provider_oauth_credentials metadata", `SELECT org_id, credential_name, version, has_refresh_token, granted_scopes::text, account_id, account_display,
	(binding_id ~ '^[0-9a-f]{32}$')::text, round(EXTRACT(EPOCH FROM (expires_at - updated_at)) / 10) * 10
FROM provider_oauth_credentials ORDER BY org_id, credential_name`)
	compare("provider_oauth_revocations metadata", `SELECT org_id, credential_name, purpose, status, attempts, last_error, token_key_version
FROM provider_oauth_revocations WHERE purpose <> 'setup' ORDER BY org_id, credential_name, purpose, attempts`)

	// Ciphertexts differ by construction; what they hold must not. Each
	// plane's stored secrets are opened with Python's own decrypt_value and
	// compared as text (a binding id is random, so it is blanked; a token's
	// expires_at is checked to parse, not compared: it is now-relative and
	// its offset is compared through the metadata query above).
	binding := regexp.MustCompile(`"oauth_binding_id": "[0-9a-f]{32}"`)
	opened := func(dbName string) []string {
		t.Helper()
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		type secret struct{ label, key, ciphertext string }
		var secrets_ []secret
		for _, query := range []struct{ label, sql string }{
			{"credentials", `SELECT org_id || '/' || name, credentials_encrypted FROM integration_credentials WHERE credentials_encrypted IS NOT NULL ORDER BY org_id, name`},
			{"oauth token", `SELECT org_id || '/' || credential_name, token_encrypted FROM provider_oauth_credentials WHERE token_encrypted <> 'garbage-not-fernet' ORDER BY org_id, credential_name`},
			{"revocation", `SELECT org_id || '/' || credential_name || '/' || purpose || '/' || attempts, token_encrypted FROM provider_oauth_revocations WHERE purpose <> 'setup' ORDER BY org_id, credential_name, purpose, attempts`},
		} {
			rows, err := pool.Query(ctx, query.sql)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var item secret
				item.label = query.label
				if err := rows.Scan(&item.key, &item.ciphertext); err != nil {
					t.Fatal(err)
				}
				secrets_ = append(secrets_, item)
			}
			rows.Close()
		}
		calls := make([]venueoracle.PythonCall, len(secrets_))
		for i, item := range secrets_ {
			calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{item.ciphertext}}
		}
		results := venue.CallPython(t, calls...)
		out := make([]string, len(secrets_))
		for i, item := range secrets_ {
			var plain string
			if err := json.Unmarshal(results[i], &plain); err != nil {
				t.Fatalf("decode decrypt_value: %v", err)
			}
			switch item.label {
			case "credentials":
				plain = binding.ReplaceAllString(plain, `"oauth_binding_id": "<id>"`)
			case "oauth token":
				var tokens struct {
					AccessToken   string   `json:"access_token"`
					RefreshToken  *string  `json:"refresh_token"`
					ExpiresAt     string   `json:"expires_at"`
					GrantedScopes []string `json:"granted_scopes"`
				}
				if err := json.Unmarshal([]byte(plain), &tokens); err != nil {
					t.Fatalf("%s %s: stored tokens are not the expected JSON: %v (%s)", dbName, item.key, err, plain)
				}
				if _, err := time.Parse(time.RFC3339Nano, tokens.ExpiresAt); err != nil {
					t.Errorf("%s %s: expires_at %q does not parse: %v", dbName, item.key, tokens.ExpiresAt, err)
				}
				sort.Strings(tokens.GrantedScopes)
				refresh := "<none>"
				if tokens.RefreshToken != nil {
					refresh = *tokens.RefreshToken
				}
				plain = fmt.Sprintf("access=%s refresh=%s scopes=%s", tokens.AccessToken, refresh, strings.Join(tokens.GrantedScopes, ","))
			}
			out[i] = item.label + " " + item.key + " => " + plain
		}
		return out
	}
	pythonSecrets, goSecrets := opened(venue.SourceDB), opened(venue.GoDB)
	if strings.Join(pythonSecrets, "\n") != strings.Join(goSecrets, "\n") {
		t.Errorf("stored secrets differ:\n python:\n%s\n go:\n%s", strings.Join(pythonSecrets, "\n"), strings.Join(goSecrets, "\n"))
	}
	if len(pythonSecrets) < 20 {
		t.Errorf("only %d stored secrets to compare", len(pythonSecrets))
	}

	// ---- CHAOS-6631: what the Go plane keeps that Python drops ---------------
	// A compensating revoke PagerDuty refuses leaves the issued token live and
	// untracked in Python (no row, both planes used to match). The Go plane
	// keeps a pending 'setup' revocation for it and retries it at the next
	// callback of the org. The answers above are equal; this is the one
	// difference, asserted on each side so a change to either is a decision.
	refused := []struct{ slug, token string }{
		{"refmissing", "rt-missing-refused"}, {"refnoacct", "rt-noaccount-refused"}, {"refcorrupt", "rt-corrupt-refused"}, {"refredirect", "rt-redirect"},
	}
	setupRows := func(dbName string) map[string]string {
		t.Helper()
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		rows, err := pool.Query(ctx, `SELECT org_id, credential_name || '/' || status || '/' || attempts || '/' || coalesce(last_error, '<null>'), token_encrypted
FROM provider_oauth_revocations WHERE purpose = 'setup' ORDER BY org_id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		var ciphertexts []string
		var keys []string
		for rows.Next() {
			var org, summary, ciphertext string
			if err := rows.Scan(&org, &summary, &ciphertext); err != nil {
				t.Fatal(err)
			}
			slug := org
			for name, id := range orgs {
				if id.String() == org {
					slug = name
				}
			}
			keys = append(keys, slug)
			out[slug] = summary
			ciphertexts = append(ciphertexts, ciphertext)
		}
		if len(ciphertexts) == 0 {
			return out
		}
		calls := make([]venueoracle.PythonCall, len(ciphertexts))
		for i, ciphertext := range ciphertexts {
			calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{ciphertext}}
		}
		for i, raw := range venue.CallPython(t, calls...) {
			var plain string
			if err := json.Unmarshal(raw, &plain); err != nil {
				t.Fatal(err)
			}
			out[keys[i]] += "/" + plain
		}
		return out
	}
	if rows := setupRows(venue.SourceDB); len(rows) != 0 {
		t.Errorf("the Python plane kept setup revocations (%v); it used to keep none: this is a decision, not an accident", rows)
	}
	goRows := setupRows(venue.GoDB)
	for _, want := range refused {
		if got, wantRow := goRows[want.slug], "/pending/1/remote_revoke_failed/"+want.token; got != wantRow {
			t.Errorf("Go plane setup revocation for %s = %q, want %q", want.slug, got, wantRow)
		}
	}
	if len(goRows) != len(refused) {
		t.Errorf("Go plane kept %d setup revocations, want %d: %v", len(goRows), len(refused), goRows)
	}

	// PagerDuty accepts revokes again. Each org's next callback retries its
	// pending revoke first; the redirected one is refused again and stays.
	fake.mu.Lock()
	for _, token := range []string{"rt-missing-refused", "rt-noaccount-refused", "rt-corrupt-refused"} {
		delete(fake.failRevoke, token)
	}
	fake.mu.Unlock()
	fake.take()
	for _, followUp := range []struct {
		name, org, state string
		status           int
	}{
		{"refmissing", "admin_refmissing", "s-refm-2", 200}, {"refnoacct", "admin_refnoacct", "s-refn-2", 200},
		{"refcorrupt", "admin_refcorrupt", "s-refc-2", 500}, {"refredirect", "admin_refredirect", "s-refr-2", 200},
	} {
		response := venueoracle.Do(t, goBase, cb("follow-up "+followUp.name, followUp.org, followUp.state, "c-ok3"))
		if response.Status != followUp.status {
			t.Errorf("follow-up callback for %s answered %d %s, want %d", followUp.name, response.Status, response.Body, followUp.status)
		}
	}
	retried := strings.Join(fake.take(), "\n")
	for _, token := range []string{"rt-missing-refused", "rt-noaccount-refused", "rt-corrupt-refused", "rt-redirect"} {
		if !strings.Contains(retried, "POST /revoke") && strings.Contains(retried, "token="+token) {
			t.Errorf("the next callback did not retry the revoke of %s:\n%s", token, retried)
		}
	}
	after := setupRows(venue.GoDB)
	if len(after) != 1 || after["refredirect"] != "/pending/2/remote_revoke_failed/rt-redirect" {
		t.Errorf("after the retries the Go plane should keep only the redirected token's row (attempts 2): %v", after)
	}
	fake.mu.Lock()
	captured = fake.captured
	fake.mu.Unlock()
	if captured != 0 {
		t.Errorf("a retried revoke followed a redirect: %d request(s) reached the redirect target", captured)
	}

	// Which rows a callback retries: a refused revoke (attempts > 0) and a row
	// old enough that its callback died without an outcome, never a young row
	// whose callback may still be running (revoking it would revoke a grant
	// about to be stored).
	goPool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	defer goPool.Close()
	insertSetup := func(org, token string, attempts int, age string) {
		t.Helper()
		var sealed string
		if err := json.Unmarshal(venue.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{token}})[0], &sealed); err != nil {
			t.Fatal(err)
		}
		if _, err := goPool.Exec(ctx, `INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', '', 'setup', $3, 'v1', 'pending', $4, now() - $5::interval, now())`, uuid.New(), orgs[org].String(), sealed, attempts, age); err != nil {
			t.Fatal(err)
		}
	}
	insertSetup("refmissing", "rt-inflight", 0, "1 minute")
	insertSetup("refmissing", "rt-stale", 0, "16 minutes")
	insertSetup("refmissing", "rt-failed-young", 1, "0 seconds")
	fake.take()
	if response := venueoracle.Do(t, goBase, cb("follow-up drain selection", "admin_refmissing", "s-refm-3", "c-ok3")); response.Status != 200 {
		t.Errorf("drain-selection callback answered %d %s", response.Status, response.Body)
	}
	drained := strings.Join(fake.take(), "\n")
	for token, want := range map[string]bool{"rt-inflight": false, "rt-stale": true, "rt-failed-young": true} {
		if got := strings.Contains(drained, "token="+token); got != want {
			t.Errorf("callback retried the revoke of %s = %v, want %v:\n%s", token, got, want, drained)
		}
	}
	var inflight int
	if err := goPool.QueryRow(ctx, `SELECT count(*) FROM provider_oauth_revocations WHERE purpose = 'setup' AND org_id = $1`, orgs["refmissing"].String()).Scan(&inflight); err != nil || inflight != 1 {
		t.Errorf("after the drain the refmissing org should keep only the young row (%d rows, %v)", inflight, err)
	}

	// A disconnect retries the org's refused setup revokes too, and answers
	// as it always did.
	insertSetup("refnoacct", "rt-disc-failed", 1, "0 seconds")
	fake.take()
	disconnect := venueoracle.Do(t, goBase, post("follow-up disconnect", "/disconnect", "admin_refnoacct", `{"credential_name":"nothing-connected"}`))
	if disconnect.Status != http.StatusOK {
		t.Errorf("disconnect answered %d %s, want 200", disconnect.Status, disconnect.Body)
	}
	if calls := strings.Join(fake.take(), "\n"); !strings.Contains(calls, "token=rt-disc-failed") {
		t.Errorf("a disconnect did not retry the org's refused setup revoke:\n%s", calls)
	}

	// A token that cannot be recorded is revoked at once and the setup fails
	// loudly: the record is written before anything else touches the grant.
	if _, err := goPool.Exec(ctx, `CREATE FUNCTION pd_setup_record_refused() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'setup record refused'; END $$;
CREATE TRIGGER pd_setup_record_refused BEFORE INSERT ON provider_oauth_revocations FOR EACH ROW WHEN (NEW.purpose = 'setup') EXECUTE FUNCTION pd_setup_record_refused()`); err != nil {
		t.Fatal(err)
	}
	fake.take()
	unrecorded := venueoracle.Do(t, goBase, cb("follow-up record refused", "admin_refnoacct", "s-refn-3", "c-ok3"))
	if unrecorded.Status != http.StatusInternalServerError {
		t.Errorf("a callback whose setup record could not be written answered %d %s, want 500", unrecorded.Status, unrecorded.Body)
	}
	if calls := strings.Join(fake.take(), "\n"); !strings.Contains(calls, "POST /revoke") || !strings.Contains(calls, "token=rt-ok3") {
		t.Errorf("a token that could not be recorded was not revoked:\n%s", calls)
	}
	if _, err := goPool.Exec(ctx, `DROP TRIGGER pd_setup_record_refused ON provider_oauth_revocations; DROP FUNCTION pd_setup_record_refused()`); err != nil {
		t.Fatal(err)
	}
}
