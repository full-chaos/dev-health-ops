//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
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
	pagerDutyServicesVenueEncryptionKey = "venue-pd-services-fernet-key-32-bytes!"
	pagerDutyServicesVenueClientID      = "venue-pd-services-client-id"
	pagerDutyServicesVenueSecret        = "venue-pd-services-client-secret"
)

// fakeServicesPagerDuty is one upstream for both planes: the token endpoint
// and the regional /services reads. What it answers is picked by the request
// (the bearer or API token, the refresh token, the client id); it counts the
// attempts per token so a "fails once, then succeeds" token behaves the same
// for each plane once reset() is called between them, and records every call.
type fakeServicesPagerDuty struct {
	mu       sync.Mutex
	lines    []string
	attempts map[string]int
}

func (f *fakeServicesPagerDuty) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = map[string]int{}
	f.lines = nil
}

func (f *fakeServicesPagerDuty) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.lines
	f.lines = nil
	return out
}

func (f *fakeServicesPagerDuty) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lines = append(f.lines, fmt.Sprintf(format, args...))
}

func (f *fakeServicesPagerDuty) attempt(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts[key]++
	return f.attempts[key]
}

func serviceObject(id, name, status string) string {
	out := `{"id":"` + id + `","type":"service","summary":"` + name + `","html_url":"https://acme.pagerduty.com/service-directory/` + id + `","created_at":"2026-01-02T03:04:05Z"`
	if name != "" {
		out += `,"name":"` + name + `"`
	}
	if status != "" {
		out += `,"status":"` + status + `"`
	}
	return out + `,"escalation_policy":{"id":"EP1","type":"escalation_policy_reference"},"extra":{"a":1}}`
}

func (f *fakeServicesPagerDuty) handler() http.Handler {
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
		token := func(access, refresh, scope string) string {
			out := `{"access_token":"` + access + `"`
			if refresh != "" {
				out += `,"refresh_token":"` + refresh + `"`
			}
			if scope != "" {
				out += `,"scope":"` + scope + `"`
			}
			return out + `,"expires_in":3600}`
		}
		if form.Get("grant_type") == "client_credentials" {
			switch form.Get("client_id") {
			case "cc-rejected":
				reply(401, `{"error":"invalid_client"}`)
			case "cc-badbody":
				reply(200, `not json`)
			case "cc-server":
				reply(500, `{}`)
			default:
				reply(200, token("at-cc-ok", "", pagerDutyAllReadScopes))
			}
			return
		}
		switch form.Get("refresh_token") {
		case "rt-due":
			reply(200, token("at-renewed", "rt-renewed", pagerDutyAllReadScopes))
		case "rt-keep":
			reply(200, token("at-renewed-keep", "", ""))
		case "rt-rejected":
			reply(400, `{"error":"invalid_grant"}`)
		case "rt-server":
			reply(503, `{}`)
		case "rt-numeric":
			// Python str()s the values: a numeric token is the text "123".
			reply(200, `{"access_token":123,"refresh_token":456,"scope":7,"expires_in":3600}`)
		case "rt-badbody":
			reply(200, `{"token_type":"bearer"}`)
		default:
			reply(400, `{}`)
		}
	})
	mux.HandleFunc("GET /{region}/services", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		f.record("GET /%s/services?%s auth=%q accept=%q", r.PathValue("region"), r.URL.RawQuery, auth, r.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json")
		reply := func(status int, payload string) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, payload)
		}
		page := func(items []string, more bool) {
			reply(200, `{"services":[`+strings.Join(items, ",")+`],"limit":100,"offset":`+strconv.Itoa(offset)+`,"more":`+strconv.FormatBool(more)+`}`)
		}
		switch auth {
		case "Token token=tok-basic", "Bearer at-fresh", "Bearer at-renewed", "Bearer at-renewed-keep", "Bearer at-cc-ok", "Bearer 123":
			page([]string{serviceObject("P3", "zeta", "active"), serviceObject("P1", "Alpha", "disabled"), serviceObject("P2", "alpha", ""),
				`{"id":"P4","name":"  "}`, `{"id":"P5","name":"  Padded  ","status":"warning"}`, serviceObject("P0", "ÄNDERUNG", "active"), serviceObject("P6", "Zulu", "active")}, false)
		case "Token token=tok-pages":
			all := []string{serviceObject("PB", "beta", "active"), serviceObject("PA", "alpha", "active"), serviceObject("PD", "delta", "active"),
				serviceObject("PC", "charlie", "active"), serviceObject("PF", "foxtrot", "active"), serviceObject("PE", "echo", "active")}
			end := offset + 2
			if end > len(all) {
				end = len(all)
			}
			page(all[offset:end], end < len(all))
		case "Token token=tok-empty":
			page(nil, false)
		case "Token token=tok-noprogress":
			page(nil, true)
		case "Token token=tok-nomore":
			reply(200, `{"services":[]}`)
		case "Token token=tok-morestring":
			reply(200, `{"services":[],"more":"no"}`)
		case "Token token=tok-notobject":
			reply(200, `[]`)
		case "Token token=tok-nolist":
			reply(200, `{"more":false}`)
		case "Token token=tok-listnull":
			reply(200, `{"services":null,"more":false}`)
		case "Token token=tok-badjson":
			reply(200, `not json`)
		case "Token token=tok-noid":
			page([]string{`{"name":"x"}`}, false)
		case "Token token=tok-idnumber":
			page([]string{`{"id":5}`}, false)
		case "Token token=tok-badname":
			page([]string{`{"id":"P1","name":5}`}, false)
		case "Token token=tok-badstatus":
			page([]string{`{"id":"P1","status":["x"]}`}, false)
		case "Token token=tok-badtime":
			page([]string{`{"id":"P1","created_at":"soon"}`}, false)
		case "Token token=tok-goodtime":
			page([]string{`{"id":"P1","name":"n","created_at":1700000000,"updated_at":"2026-01-02"}`}, false)
		case "Token token=tok-badpolicy":
			page([]string{`{"id":"P1","escalation_policy":"EP"}`}, false)
		case "Token token=tok-badpolicyid":
			page([]string{`{"id":"P1","escalation_policy":{"type":"x"}}`}, false)
		case "Token token=tok-notaservice":
			page([]string{`"P1"`}, false)
		case "Token token=tok-401":
			reply(401, `{}`)
		case "Token token=tok-403":
			reply(403, `{}`)
		case "Token token=tok-404":
			reply(404, `{}`)
		case "Token token=tok-400":
			reply(400, `{}`)
		case "Token token=tok-302":
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(302)
		case "Token token=tok-500":
			reply(500, `{}`)
		case "Token token=tok-429":
			w.Header().Set("Retry-After", "0")
			reply(429, `{}`)
		case "Token token=tok-429-frac":
			w.Header().Set("Retry-After", "0.4")
			reply(429, `{}`)
		case "Token token=tok-429-once":
			if f.attempt(auth) == 1 {
				w.Header().Set("Retry-After", "1")
				reply(429, `{}`)
				return
			}
			page([]string{serviceObject("P1", "after-429", "active")}, false)
		case "Token token=tok-503-once":
			if f.attempt(auth) == 1 {
				reply(503, `{}`)
				return
			}
			page([]string{serviceObject("P1", "after-503", "active")}, false)
		case "Token token=tok-r401":
			if f.attempt(auth) == 1 {
				reply(503, `{}`)
				return
			}
			reply(401, `{}`)
		default:
			reply(401, `{}`)
		}
	})
	return mux
}

type servicesCredential struct {
	name    string
	payload string // "" = a NULL credentials_encrypted
	active  bool
	raw     string // stored as is (not encrypted) when set
}

func TestPagerDutyServicesVenueOracle(t *testing.T) {
	runPagerDutyServicesOracle(t, "full", "285af7feedb688b54525c857ecbd3f351d1def4c227614058919aec17e942de9")
}
func TestPagerDutyServicesWithoutOAuthAppVenueOracle(t *testing.T) {
	runPagerDutyServicesOracle(t, "noapp", "c5ea110b2e92451f590476a54732b4f950ff8c814ac8254b1d4cb54eb7cc8c5c")
}

func runPagerDutyServicesOracle(t *testing.T, mode, digest string) {
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, pagerDutyGolden("services_"+mode, t.Name(), digest))
	root := golden.PythonRoot(t, repoRoot(t))
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-services-32-b"

	fake := &fakeServicesPagerDuty{attempts: map[string]int{}}
	upstream := httptest.NewServer(fake.handler())
	t.Cleanup(upstream.Close)

	orgs := map[string]uuid.UUID{"main": uuid.MustParse(venueoracle.StableUUID("pd-svc-org-main")), "other": uuid.MustParse(venueoracle.StableUUID("pd-svc-org-other"))}
	adminID, memberID, superID := uuid.MustParse(venueoracle.StableUUID("pd-svc-admin")), uuid.MustParse(venueoracle.StableUUID("pd-svc-member")), uuid.MustParse(venueoracle.StableUUID("pd-svc-super"))

	api := func(mode, token string, extra string) string {
		return `{"auth_mode":"` + mode + `","subdomain":"acme","region":"us"` + extra + `}`
	}
	tokenCred := func(token string) string { return api("api_token", token, `,"api_token":"`+token+`"`) }
	oauthCred := func(name, binding string) string {
		return api("oauth", "", `,"oauth_credential_name":"`+name+`","oauth_binding_id":"`+binding+`"`)
	}
	creds := []servicesCredential{
		{name: "default", payload: tokenCred("tok-basic"), active: true},
		{name: "pages", payload: tokenCred("tok-pages"), active: true},
		{name: "empty", payload: tokenCred("tok-empty"), active: true},
		{name: "noprogress", payload: tokenCred("tok-noprogress"), active: true},
		{name: "nomore", payload: tokenCred("tok-nomore"), active: true},
		{name: "morestring", payload: tokenCred("tok-morestring"), active: true},
		{name: "notobject", payload: tokenCred("tok-notobject"), active: true},
		{name: "nolist", payload: tokenCred("tok-nolist"), active: true},
		{name: "listnull", payload: tokenCred("tok-listnull"), active: true},
		{name: "badjson", payload: tokenCred("tok-badjson"), active: true},
		{name: "noid", payload: tokenCred("tok-noid"), active: true},
		{name: "idnumber", payload: tokenCred("tok-idnumber"), active: true},
		{name: "badname", payload: tokenCred("tok-badname"), active: true},
		{name: "badstatus", payload: tokenCred("tok-badstatus"), active: true},
		{name: "badtime", payload: tokenCred("tok-badtime"), active: true},
		{name: "goodtime", payload: tokenCred("tok-goodtime"), active: true},
		{name: "badpolicy", payload: tokenCred("tok-badpolicy"), active: true},
		{name: "badpolicyid", payload: tokenCred("tok-badpolicyid"), active: true},
		{name: "notaservice", payload: tokenCred("tok-notaservice"), active: true},
		{name: "e401", payload: tokenCred("tok-401"), active: true},
		{name: "e403", payload: tokenCred("tok-403"), active: true},
		{name: "e404", payload: tokenCred("tok-404"), active: true},
		{name: "e400", payload: tokenCred("tok-400"), active: true},
		{name: "e302", payload: tokenCred("tok-302"), active: true},
		{name: "e500", payload: tokenCred("tok-500"), active: true},
		{name: "e429", payload: tokenCred("tok-429"), active: true},
		{name: "e429frac", payload: tokenCred("tok-429-frac"), active: true},
		{name: "r429", payload: tokenCred("tok-429-once"), active: true},
		{name: "r503", payload: tokenCred("tok-503-once"), active: true},
		{name: "r401", payload: tokenCred("tok-r401"), active: true},
		{name: "eu", payload: `{"auth_mode":"api_token","subdomain":"acme","region":"eu","api_token":"tok-basic"}`, active: true},
		{name: "  padded  ", payload: tokenCred("tok-basic"), active: true},
		{name: "inactive", payload: tokenCred("tok-basic"), active: false},
		{name: "nopayload", active: true},
		{name: "garbage", raw: "garbage-not-fernet", active: true},
		{name: "list", payload: `[]`, active: true},
		{name: "noauthmode", payload: `{"subdomain":"acme","region":"us","api_token":"tok-basic"}`, active: true},
		{name: "blankmode", payload: `{"auth_mode":"  ","region":"us"}`, active: true},
		{name: "unsupported", payload: `{"auth_mode":"magic","region":"us"}`, active: true},
		{name: "notoken", payload: `{"auth_mode":"api_token","region":"us"}`, active: true},
		{name: "noregion", payload: `{"auth_mode":"api_token","api_token":"tok-basic"}`, active: true},
		{name: "numericregion", payload: `{"auth_mode":"api_token","api_token":"tok-basic","region":5}`, active: true},
		{name: "ccok", payload: `{"auth_mode":"client_credentials","client_id":"cc-ok","client_secret":"shh","subdomain":"acme","region":"us"}`, active: true},
		{name: "ccrejected", payload: `{"auth_mode":"client_credentials","client_id":"cc-rejected","client_secret":"shh","subdomain":"acme","region":"us"}`, active: true},
		{name: "ccbadbody", payload: `{"auth_mode":"client_credentials","client_id":"cc-badbody","client_secret":"shh","subdomain":"acme","region":"us"}`, active: true},
		{name: "ccserver", payload: `{"auth_mode":"client_credentials","client_id":"cc-server","client_secret":"shh","subdomain":"acme","region":"eu"}`, active: true},
		{name: "ccnosecret", payload: `{"auth_mode":"client_credentials","client_id":"cc-ok","subdomain":"acme","region":"us"}`, active: true},
		{name: "ccnoregion", payload: `{"auth_mode":"client_credentials","client_id":"cc-ok","client_secret":"shh","subdomain":"acme"}`, active: true},
		{name: "oa-fresh", payload: oauthCred("oa-fresh", "b-fresh"), active: true},
		{name: "oa-window", payload: oauthCred("oa-window", "b-win"), active: true},
		{name: "oa-due", payload: oauthCred("oa-due", "b-due"), active: true},
		{name: "oa-keep", payload: oauthCred("oa-keep", "b-keep"), active: true},
		{name: "oa-numeric", payload: oauthCred("oa-numeric", "b-num"), active: true},
		{name: "oa-norefresh", payload: oauthCred("oa-norefresh", "b-nr"), active: true},
		{name: "oa-rejected", payload: oauthCred("oa-rejected", "b-rej"), active: true},
		{name: "oa-server", payload: oauthCred("oa-server", "b-srv"), active: true},
		{name: "oa-badbody", payload: oauthCred("oa-badbody", "b-bad"), active: true},
		{name: "oa-mismatch", payload: oauthCred("oa-mismatch", "b-other"), active: true},
		{name: "oa-norow", payload: oauthCred("oa-norow", "b-none"), active: true},
		{name: "oa-corrupt", payload: oauthCred("oa-corrupt", "b-corrupt"), active: true},
		{name: "oa-emptyjson", payload: oauthCred("oa-emptyjson", "b-empty"), active: true},
		{name: "oa-nofields", payload: `{"auth_mode":"oauth","subdomain":"acme","region":"us"}`, active: true},
		{name: "oa-nobinding", payload: `{"auth_mode":"oauth","oauth_credential_name":"oa-fresh","subdomain":"acme","region":"us"}`, active: true},
		{name: "other-org", payload: tokenCred("tok-basic"), active: true},
	}

	oldToken := func(access, refresh string, expires string) string {
		r := "null"
		if refresh != "" {
			r = `"` + refresh + `"`
		}
		return `{"access_token":"` + access + `","refresh_token":` + r + `,"expires_at":"` + expires + `","granted_scopes":["incidents.read","services.read"]}`
	}
	type oauthSeed struct{ name, binding, plaintext string }
	oauthRows := []oauthSeed{
		{"oa-fresh", "b-fresh", oldToken("at-fresh", "rt-fresh", "2099-01-01T00:00:00Z")},
		// Expires in four minutes: inside the five-minute renewal window when
		// the Python plane asks, and still valid when the Go plane does.
		{"oa-window", "b-win", oldToken("at-old-window", "rt-due", time.Now().Add(4*time.Minute).UTC().Format(time.RFC3339))},
		{"oa-due", "b-due", oldToken("at-old", "rt-due", "2000-01-01T00:00:00Z")},
		{"oa-keep", "b-keep", oldToken("at-old-keep", "rt-keep", "2000-01-01T00:00:00Z")},
		{"oa-numeric", "b-num", oldToken("at-old-num", "rt-numeric", "2000-01-01T00:00:00Z")},
		{"oa-norefresh", "b-nr", oldToken("at-nr", "", "2000-01-01T00:00:00Z")},
		{"oa-rejected", "b-rej", oldToken("at-rej", "rt-rejected", "2000-01-01T00:00:00Z")},
		{"oa-server", "b-srv", oldToken("at-srv", "rt-server", "2000-01-01T00:00:00Z")},
		{"oa-badbody", "b-bad", oldToken("at-bad", "rt-badbody", "2000-01-01T00:00:00Z")},
		{"oa-mismatch", "b-mismatch-actual", oldToken("at-fresh", "rt-x", "2099-01-01T00:00:00Z")},
		{"oa-emptyjson", "b-empty", `{}`},
	}

	pythonEnv := []string{
		"PAGER_DUTY_SECRET=" + pagerDutyServicesVenueSecret,
		"SETTINGS_ENCRYPTION_KEY=" + pagerDutyServicesVenueEncryptionKey,
		"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE=" + upstream.URL + "/token",
		"VENUE_PAGERDUTY_API_BASE_OVERRIDE=" + upstream.URL,
	}
	clientID := ""
	if mode == "full" {
		clientID = pagerDutyServicesVenueClientID
		pythonEnv = append(pythonEnv, "PAGER_DUTY_CLIENT_ID="+clientID)
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
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "pd-svc-"+slug)
			}
			user := func(id uuid.UUID, email string, super bool) {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, email, super)
			}
			user(adminID, "pd-svc-admin@example.com", false)
			user(memberID, "pd-svc-member@example.com", false)
			user(superID, "pd-svc-super@example.com", true)

			var plains []string
			index := map[string]int{}
			need := func(p string) {
				if _, ok := index[p]; !ok {
					index[p] = len(plains)
					plains = append(plains, p)
				}
			}
			for _, c := range creds {
				if c.payload != "" {
					need(c.payload)
				}
			}
			for _, o := range oauthRows {
				need(o.plaintext)
			}
			calls := make([]venueoracle.PythonCall, len(plains))
			for i, p := range plains {
				calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{p}}
			}
			results := v.CallPython(t, calls...)
			enc := func(p string) string {
				var out string
				if err := json.Unmarshal(results[index[p]], &out); err != nil {
					t.Fatalf("decode encrypt_value: %v", err)
				}
				return out
			}
			for _, c := range creds {
				var payload any
				switch {
				case c.raw != "":
					payload = c.raw
				case c.payload != "":
					payload = enc(c.payload)
				}
				org := orgs["main"]
				if c.name == "other-org" {
					org = orgs["other"]
				}
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, $4, $5, '{}'::json, now(), now())`, uuid.New(), org.String(), c.name, c.active, payload)
			}
			for _, o := range oauthRows {
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id, expires_at, granted_scopes, has_refresh_token)
VALUES ($1, 'pagerduty', $2, $3, 3, now(), now(), $4, '2000-01-01T00:00:00+00:00', '["incidents.read"]'::json, true)`, orgs["main"].String(), o.name, enc(o.plaintext), o.binding)
			}
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id, has_refresh_token)
VALUES ($1, 'pagerduty', 'oa-corrupt', 'garbage-not-fernet', 1, now(), now(), 'b-corrupt', false)`, orgs["main"].String())

			return map[string]map[string]any{
				"admin":  {"user_id": adminID.String(), "email": "pd-svc-admin@example.com", "org_id": orgs["main"].String(), "role": "admin"},
				"member": {"user_id": memberID.String(), "email": "pd-svc-member@example.com", "org_id": orgs["main"].String(), "role": "member"},
				"super":  {"user_id": superID.String(), "email": "pd-svc-super@example.com", "is_superuser": true},
			}
		},
	})

	auth := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	const path = "/api/v1/admin/integrations/pagerduty/services"
	svc := func(name, cred string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "GET", Path: path + "?credential_name=" + url.QueryEscape(cred), Headers: auth("admin")}
	}

	var requests []venueoracle.Request
	add := func(reqs ...venueoracle.Request) { requests = append(requests, reqs...) }
	add(
		svc("W services oauth token inside the renewal window", "oa-window"),
		venueoracle.Request{Name: "W services default credential", Method: "GET", Path: path, Headers: auth("admin")},
		svc("W services several pages", "pages"),
		svc("W services empty list", "empty"),
		svc("W services eu region", "eu"),
		svc("W services padded credential name", "  padded  "),
		svc("W services goodtime", "goodtime"),
		venueoracle.Request{Name: "W services credential_name empty", Method: "GET", Path: path + "?credential_name=", Headers: auth("admin")},
		venueoracle.Request{Name: "W services credential_name blank", Method: "GET", Path: path + "?credential_name=%20%20", Headers: auth("admin")},
		venueoracle.Request{Name: "W services credential_name repeated", Method: "GET", Path: path + "?credential_name=nope&credential_name=default", Headers: auth("admin")},
		svc("W services unknown credential", "nope"),
		svc("W services inactive credential", "inactive"),
		svc("W services credential without payload", "nopayload"),
		svc("W services undecryptable payload", "garbage"),
		svc("W services payload not an object", "list"),
		svc("W services other organisation's credential", "other-org"),
		svc("W services no auth_mode", "noauthmode"),
		svc("W services blank auth_mode", "blankmode"),
		svc("W services unsupported auth_mode", "unsupported"),
		svc("W services api_token missing", "notoken"),
		svc("W services region missing", "noregion"),
		svc("W services region not a string", "numericregion"),
		// ---- what PagerDuty answers -----------------------------------------------
		svc("W services 401", "e401"),
		svc("W services 403", "e403"),
		svc("W services 404 is a 502", "e404"),
		svc("W services 400 is a 502", "e400"),
		svc("W services redirect is a 502", "e302"),
		svc("W services 429 once then a list", "r429"),
		svc("W services 503 once then a list", "r503"),
		svc("W services 503 then 401", "r401"),
		svc("W services 500 after every retry", "e500"),
		svc("W services 429 after every retry", "e429"),
		svc("W services 429 with a fractional Retry-After", "e429frac"),
		svc("W services no progress", "noprogress"),
		svc("W services more missing", "nomore"),
		svc("W services more not a bool", "morestring"),
		svc("W services body not an object", "notobject"),
		svc("W services list missing", "nolist"),
		svc("W services list null", "listnull"),
		svc("W services body not json", "badjson"),
		svc("W services item without id", "noid"),
		svc("W services id a number", "idnumber"),
		svc("W services name a number", "badname"),
		svc("W services status a list", "badstatus"),
		svc("W services created_at not a datetime", "badtime"),
		svc("W services escalation_policy not an object", "badpolicy"),
		svc("W services escalation_policy without id", "badpolicyid"),
		svc("W services item not an object", "notaservice"),
		// ---- client credentials -------------------------------------------------------
		svc("W services client credentials", "ccok"),
		svc("W services client credentials rejected", "ccrejected"),
		svc("W services client credentials answer not json", "ccbadbody"),
		svc("W services client credentials server error", "ccserver"),
		svc("W services client credentials without a secret", "ccnosecret"),
		svc("W services client credentials without a region", "ccnoregion"),
		// ---- OAuth -----------------------------------------------------------------------------
		svc("W services oauth token still fresh", "oa-fresh"),
		svc("W services oauth token renewed and rotated", "oa-due"),
		svc("W services oauth renewed again is fresh", "oa-due"),
		svc("W services oauth renewal keeps refresh token and scopes", "oa-keep"),
		svc("W services oauth renewal answer with numeric values", "oa-numeric"),
		svc("W services oauth expired without a refresh token", "oa-norefresh"),
		svc("W services oauth refresh rejected", "oa-rejected"),
		svc("W services oauth refresh server error", "oa-server"),
		svc("W services oauth refresh answer malformed", "oa-badbody"),
		svc("W services oauth binding mismatch", "oa-mismatch"),
		svc("W services oauth row missing", "oa-norow"),
		svc("W services oauth token undecryptable", "oa-corrupt"),
		svc("W services oauth token payload empty object", "oa-emptyjson"),
		svc("W services oauth descriptor without its fields", "oa-nofields"),
		svc("W services oauth descriptor without a binding", "oa-nobinding"),
		// ---- callers ---------------------------------------------------------------------------------
		venueoracle.Request{Name: "W services member refused", Method: "GET", Path: path, Headers: auth("member")},
		venueoracle.Request{Name: "W services superuser without org", Method: "GET", Path: path, Headers: auth("super")},
		venueoracle.Request{Name: "W services unauthenticated", Method: "GET", Path: path},
		venueoracle.Request{Name: "services post is 405", Method: "POST", Path: path, Headers: auth("admin")},
	)

	if mode == "noapp" {
		// Without PAGER_DUTY_CLIENT_ID only the OAuth branch differs (a 409
		// before any row is read); keep those requests and a plain one.
		var kept []venueoracle.Request
		for _, request := range requests {
			if strings.Contains(request.Name, "oauth") || strings.Contains(request.Name, "default credential") || strings.Contains(request.Name, "client credentials") {
				kept = append(kept, request)
			}
		}
		requests = kept
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyServicesVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	python := golden.Python(t, venue, requests)
	// What the Python plane sent upstream is part of what its executed build
	// answered: frozen with the golden, compared with the Go plane's calls.
	pythonCallsText := golden.Rows(t, "upstream calls of the Python plane", func() string { return strings.Join(fake.take(), "\n") })
	fake.reset()
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{
			ClientID: clientID, ClientSecret: pagerDutyServicesVenueSecret,
			TokenURL: upstream.URL + "/token", APIBaseOverride: upstream.URL,
		}
	})
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{Golden: golden})
	t.Log(receipt)
	goCalls := fake.take()

	if pythonCallsText != strings.Join(goCalls, "\n") {
		t.Errorf("upstream calls differ:\n python:\n%s\n go (%d):\n%s", pythonCallsText, len(goCalls), strings.Join(goCalls, "\n"))
	}
	if pythonCalls := strings.Count(pythonCallsText, "\n") + 1; mode == "full" && pythonCalls < 50 {
		t.Errorf("the fake upstream saw only %d calls from the Python plane", pythonCalls)
	}
	_ = time.Second

	compare := func(name, query string) {
		t.Helper()
		source := golden.Rows(t, name, func() string {
			return venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		})
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != goRows {
			t.Errorf("%s differs after the requests:\n python: %s\n go:     %s", name, source, goRows)
		}
	}
	compare("provider_oauth_credentials metadata", `SELECT org_id, credential_name, version, has_refresh_token, granted_scopes::text, binding_id,
	CASE WHEN expires_at < '2001-01-01'::timestamptz THEN 'seeded expiry kept' ELSE (round(EXTRACT(EPOCH FROM (expires_at - updated_at)) / 10) * 10)::text END
FROM provider_oauth_credentials ORDER BY org_id, credential_name`)
	compare("integration_credentials untouched", `SELECT org_id, name, is_active, (credentials_encrypted IS NULL)::text, config::text FROM integration_credentials ORDER BY org_id, name`)

	// Stored OAuth tokens opened with Python's decrypt, expires_at checked
	// for shape only (it is now-relative).
	opened := func(dbName string) []string {
		t.Helper()
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		rows, err := pool.Query(ctx, `SELECT credential_name, token_encrypted FROM provider_oauth_credentials WHERE token_encrypted <> 'garbage-not-fernet' ORDER BY credential_name`)
		if err != nil {
			t.Fatal(err)
		}
		type item struct{ name, ciphertext string }
		var items []item
		for rows.Next() {
			var i item
			if err := rows.Scan(&i.name, &i.ciphertext); err != nil {
				t.Fatal(err)
			}
			items = append(items, i)
		}
		rows.Close()
		calls := make([]venueoracle.PythonCall, len(items))
		for i, it := range items {
			calls[i] = venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{it.ciphertext}}
		}
		results := venue.CallPython(t, calls...)
		out := make([]string, len(items))
		for i, it := range items {
			var plain string
			if err := json.Unmarshal(results[i], &plain); err != nil {
				t.Fatalf("decode decrypt_value: %v", err)
			}
			var tokens struct {
				AccessToken   string   `json:"access_token"`
				RefreshToken  *string  `json:"refresh_token"`
				ExpiresAt     string   `json:"expires_at"`
				GrantedScopes []string `json:"granted_scopes"`
			}
			if json.Unmarshal([]byte(plain), &tokens) != nil || tokens.ExpiresAt == "" {
				// A seeded payload that is not an OAuthTokens object (never rotated).
				out[i] = it.name + " => " + plain
				continue
			}
			if _, err := time.Parse(time.RFC3339Nano, tokens.ExpiresAt); err != nil {
				t.Errorf("%s %s: expires_at %q does not parse", dbName, it.name, tokens.ExpiresAt)
			}
			sort.Strings(tokens.GrantedScopes)
			refresh := "<none>"
			if tokens.RefreshToken != nil {
				refresh = *tokens.RefreshToken
			}
			out[i] = fmt.Sprintf("%s => access=%s refresh=%s scopes=%s", it.name, tokens.AccessToken, refresh, strings.Join(tokens.GrantedScopes, ","))
		}
		return out
	}
	pythonTokens := golden.Rows(t, "stored OAuth tokens opened", func() string { return strings.Join(opened(venue.SourceDB), "\n") })
	goTokens := strings.Join(opened(venue.GoDB), "\n")
	if pythonTokens != goTokens {
		t.Errorf("stored OAuth tokens differ:\n python:\n%s\n go:\n%s", pythonTokens, goTokens)
	}
	golden.Finish(t)
}
