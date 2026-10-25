package acr

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	wlIssuer  = "https://auth.example.test"
	wlSubject = "prn_AAAAAAAAAAAAAAAAAAAAAA"
	wlOther   = "prn_BBBBBBBBBBBBBBBBBBBBBB"
	wlKid     = "kid-acr-1"
)

var wlNow = time.Unix(1_800_000_000, 0)

type wlSigner struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
}

func newWLSigner(t *testing.T) wlSigner {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return wlSigner{pub: pub, priv: priv}
}

func (s wlSigner) sign(header, payload map[string]any) string {
	enc := base64.RawURLEncoding
	h, _ := json.Marshal(header)
	p, _ := json.Marshal(payload)
	input := enc.EncodeToString(h) + "." + enc.EncodeToString(p)
	return input + "." + enc.EncodeToString(ed25519.Sign(s.priv, []byte(input)))
}

func wlHeader() map[string]any {
	return map[string]any{"alg": "EdDSA", "typ": "workload+jwt", "kid": wlKid}
}

func wlClaims() map[string]any {
	return map[string]any{
		"iss": wlIssuer, "aud": "dev-health-api", "sub": wlSubject, "principal_type": "workload",
		"jti": "000000000000000000000001", "iat": wlNow.Unix() - 10, "nbf": wlNow.Unix() - 10, "exp": wlNow.Unix() + 240,
	}
}

func wlAuth(t *testing.T, signer wlSigner, audience string) *WorkloadAuth {
	t.Helper()
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: wlIssuer, Audience: audience, Keys: authverify.StaticKeys{wlKid: signer.pub},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &WorkloadAuth{Verifier: verifier, Subject: wlSubject, Now: func() time.Time { return wlNow }}
}

type wlRoute struct{ key, path string }

var wlRoutesUnderTest = []wlRoute{
	{routeHealth, healthPath},
	{routeEntitlement, entitlementPath + someOrg},
	{routeLLMConfig, "/api/v1/internal/acr/orgs/" + someOrg + "/llm-config"},
}

type wlHarness struct {
	handler http.Handler
	creds   *fakeCredentials
	llm     *fakeVerifier
	logs    *bytes.Buffer
}

func newWLHarness(t *testing.T, workload *WorkloadAuth) wlHarness {
	t.Helper()
	acrMetricReader()
	creds, llm, logs := credsFor(), &fakeVerifier{}, &bytes.Buffer{}
	creds.creds[entToken] = Credential{ID: "cred-ent", ServiceName: "acr", Scopes: []string{EntitlementScope}}
	mux := http.NewServeMux()
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	for _, route := range Routes(Deps{Store: &fakeStore{}, Auth: creds, LLMConfig: configured(), Credentials: llm, Logger: logger, Workload: workload}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return wlHarness{handler: mux, creds: creds, llm: llm, logs: logs}
}

func wlCounts(t *testing.T, name string) map[string]int64 {
	t.Helper()
	return counterByAttribute(t, name, func(p metricdata.DataPoint[int64]) (string, bool) {
		route, _ := p.Attributes.Value("route")
		reason, ok := p.Attributes.Value("reason")
		if !ok {
			return route.AsString(), true
		}
		return route.AsString() + "/" + reason.AsString(), true
	})
}

func wlDelta(before, after map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

func TestWorkloadTokenAcceptedOnAllThreeRoutes(t *testing.T) {
	signer := newWLSigner(t)
	token := signer.sign(wlHeader(), wlClaims())
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
			before := wlCounts(t, "devhealth_acr_workload_accepted_total")
			rec := do(h.handler, route.path, "Bearer "+token)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d %s, want 200", rec.Code, rec.Body.String())
			}
			if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_accepted_total")); !reflect.DeepEqual(got, map[string]int64{route.key: 1}) {
				t.Fatalf("accepted counter delta = %v", got)
			}
			if h.creds.findCalls != 0 || len(h.creds.used) != 0 {
				t.Fatalf("workload token touched the credential store: finds %d used %v", h.creds.findCalls, h.creds.used)
			}
			if route.key == routeEntitlement {
				if want := []auditRow{{"", someOrg, "success"}}; !reflect.DeepEqual(h.creds.audits, want) || !reflect.DeepEqual(h.creds.principals, []string{wlSubject}) {
					t.Fatalf("audits %+v principals %v, want %+v [%s]", h.creds.audits, h.creds.principals, want, wlSubject)
				}
			}
		})
	}
}

func TestWorkloadAuditCarriesThePrincipalOnEveryEntitlementOutcome(t *testing.T) {
	signer := newWLSigner(t)
	token := signer.sign(wlHeader(), wlClaims())
	for name, tc := range map[string]struct {
		store   EntitlementStore
		outcome string
	}{
		"unknown org": {&fakeStore{err: ErrOrgUnknown}, "organization_not_found"},
		"unavailable": {&fakeStore{err: ErrUnavailable}, "entitlement_lookup_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			creds := credsFor()
			mux := http.NewServeMux()
			for _, route := range Routes(Deps{Store: tc.store, Auth: creds, Workload: wlAuth(t, signer, "dev-health-api")}) {
				mux.Handle(route.Method+" "+route.Pattern, route.Handler)
			}
			do(mux, entitlementPath+someOrg, "Bearer "+token)
			if want := []auditRow{{"", someOrg, tc.outcome}}; !reflect.DeepEqual(creds.audits, want) || !reflect.DeepEqual(creds.principals, []string{wlSubject}) {
				t.Fatalf("audits %+v principals %v", creds.audits, creds.principals)
			}
		})
	}
}

func TestSvcAcrAuditHasNoPrincipal(t *testing.T) {
	h := newWLHarness(t, nil)
	if rec := do(h.handler, entitlementPath+someOrg, "Bearer "+entToken); rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if want := []auditRow{{"cred-ent", someOrg, "success"}}; !reflect.DeepEqual(h.creds.audits, want) || !reflect.DeepEqual(h.creds.principals, []string{""}) {
		t.Fatalf("audits %+v principals %v", h.creds.audits, h.creds.principals)
	}
}

type wlRefusal struct {
	name   string
	token  func(t *testing.T, s wlSigner) string
	status int
	reason string
}

func wlMutated(header, claims func(map[string]any)) func(*testing.T, wlSigner) string {
	return func(_ *testing.T, s wlSigner) string {
		h, c := wlHeader(), wlClaims()
		if header != nil {
			header(h)
		}
		if claims != nil {
			claims(c)
		}
		return s.sign(h, c)
	}
}

func wlRefusals() []wlRefusal {
	return []wlRefusal{
		{"scope claim", wlMutated(nil, func(c map[string]any) { c["scope"] = "entitlements:read" }), 401, ReasonContract},
		{"org_id claim", wlMutated(nil, func(c map[string]any) { c["org_id"] = someOrg }), 401, ReasonContract},
		{"sid claim", wlMutated(nil, func(c map[string]any) { c["sid"] = "ses_x" }), 401, ReasonContract},
		{"aud array", wlMutated(nil, func(c map[string]any) { c["aud"] = []string{"dev-health-api"} }), 401, ReasonContract},
		{"extra header member", wlMutated(func(h map[string]any) { h["jku"] = "https://x.example.test" }, nil), 401, ReasonContract},
		{"unknown kid", wlMutated(func(h map[string]any) { h["kid"] = "kid-unknown" }, nil), 401, ReasonUnknownKid},
		{"signed by another key", func(t *testing.T, _ wlSigner) string { return newWLSigner(t).sign(wlHeader(), wlClaims()) }, 401, ReasonSignature},
		{"wrong issuer", wlMutated(nil, func(c map[string]any) { c["iss"] = "https://other.example.test" }), 401, ReasonIssuer},
		{"aud auth-service", wlMutated(nil, func(c map[string]any) { c["aud"] = "auth-service" }), 401, ReasonAudience},
		{"aud query-api", wlMutated(nil, func(c map[string]any) { c["aud"] = "dev-health-query-api" }), 401, ReasonContract},
		{"not yet valid", wlMutated(nil, func(c map[string]any) {
			c["nbf"] = wlNow.Unix() + 60
			c["iat"] = wlNow.Unix() + 60
			c["exp"] = wlNow.Unix() + 120
		}), 401, ReasonNotYetValid},
		{"expired", wlMutated(nil, func(c map[string]any) { c["exp"] = wlNow.Unix() - 1 }), 401, ReasonExpired},
		{"other workload subject", wlMutated(nil, func(c map[string]any) { c["sub"] = wlOther }), 403, ReasonPrincipal},
	}
}

func TestWorkloadTokenRefusalTable(t *testing.T) {
	signer := newWLSigner(t)
	for _, route := range wlRoutesUnderTest {
		for _, tc := range wlRefusals() {
			t.Run(route.key+"/"+tc.name, func(t *testing.T) {
				h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
				before := wlCounts(t, "devhealth_acr_workload_refused_total")
				rec := do(h.handler, route.path, "Bearer "+tc.token(t, signer))
				if rec.Code != tc.status {
					t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
				}
				wantBody := `{"detail":"Unauthorized"}`
				if tc.status == 403 {
					wantBody = `{"detail":"Forbidden"}`
				}
				if strings.TrimSpace(rec.Body.String()) != wantBody {
					t.Fatalf("body = %s, want %s: no reason may reach the body", rec.Body.String(), wantBody)
				}
				if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); !reflect.DeepEqual(got, map[string]int64{route.key + "/" + tc.reason: 1}) {
					t.Fatalf("refused counter delta = %v, want %s/%s once", got, route.key, tc.reason)
				}
				if h.creds.findCalls != 0 || len(h.creds.used) != 0 || h.llm.gotToken != "" {
					t.Fatalf("a refused workload token reached a store: finds %d used %v verifier token %q", h.creds.findCalls, h.creds.used, h.llm.gotToken)
				}
				if route.key == routeEntitlement {
					outcome := outcomeWorkloadRefused
					if tc.status == 403 {
						outcome = outcomeWorkloadForbidden
					}
					if want := []auditRow{{"", someOrg, outcome}}; !reflect.DeepEqual(h.creds.audits, want) || !reflect.DeepEqual(h.creds.principals, []string{""}) {
						t.Fatalf("audits %+v principals %v, want %+v with a NULL principal", h.creds.audits, h.creds.principals, want)
					}
				}
			})
		}
	}
}

func TestOtherCredentialClassesNeverReachAStoreOrTheVerifier(t *testing.T) {
	signer := newWLSigner(t)
	userHeader := wlHeader()
	userHeader["typ"] = "at+jwt"
	hs256 := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + ".e30.c2ln"
	bearers := map[string]string{
		"user access token": "Bearer " + signer.sign(userHeader, wlClaims()),
		"fcacr":             "Bearer fcacr_0123456789abcdef",
		"hs256":             "Bearer " + hs256,
		"svc_worker":        "Bearer svc_worker_x-0123456789",
		"svc_authsess":      "Bearer svc_authsess_x-0123456789",
		"push key":          "Bearer dh_push_0123456789abcdef",
	}
	for _, route := range wlRoutesUnderTest[:2] {
		for name, bearer := range bearers {
			t.Run(route.key+"/"+name, func(t *testing.T) {
				h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
				before := wlCounts(t, "devhealth_acr_workload_refused_total")
				rec := do(h.handler, route.path, bearer)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
				if h.creds.findCalls != 0 || len(h.creds.used) != 0 {
					t.Fatalf("store touched: finds %d used %v", h.creds.findCalls, h.creds.used)
				}
				if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); len(got) != 0 {
					t.Fatalf("a non-workload token was counted as a workload refusal: %v", got)
				}
			})
		}
	}
}

func TestSvcAcrStillAcceptedDuringMigration(t *testing.T) {
	signer := newWLSigner(t)
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
			token := entToken
			if route.key == routeLLMConfig {
				token = testToken
			}
			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code != http.StatusOK {
				t.Fatalf("status = %d %s, want 200", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestWorkloadPathOffWhenUnconfigured(t *testing.T) {
	signer := newWLSigner(t)
	token := signer.sign(wlHeader(), wlClaims())
	configs := map[string]*WorkloadAuth{
		"nil":         nil,
		"no subject":  {Verifier: wlAuth(t, signer, "dev-health-api").Verifier},
		"no verifier": {Subject: wlSubject},
	}
	for name, workload := range configs {
		for _, route := range wlRoutesUnderTest {
			t.Run(name+"/"+route.key, func(t *testing.T) {
				h := newWLHarness(t, workload)
				before := wlCounts(t, "devhealth_acr_workload_refused_total")
				if rec := do(h.handler, route.path, "Bearer "+token); rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
				if got := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); !reflect.DeepEqual(got, map[string]int64{route.key + "/" + ReasonNotConfigured: 1}) {
					t.Fatalf("delta = %v, want not_configured once", got)
				}
			})
		}
	}
}

func TestWorkloadRefusalCountedAndLoggedByReason(t *testing.T) {
	signer := newWLSigner(t)
	for _, route := range wlRoutesUnderTest {
		for _, tc := range wlRefusals() {
			t.Run(route.key+"/"+tc.name, func(t *testing.T) {
				h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
				token := tc.token(t, signer)
				do(h.handler, route.path, "Bearer "+token)
				var found bool
				for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
					var record map[string]any
					if json.Unmarshal([]byte(line), &record) != nil || record["msg"] != "acr internal: workload token refused" {
						continue
					}
					found = true
					if record["reason"] != tc.reason || record["route"] != route.key {
						t.Fatalf("log record %v, want reason %s route %s", record, tc.reason, route.key)
					}
					if _, has := record["workload_subject"]; has != (tc.reason == ReasonPrincipal) {
						t.Fatalf("workload_subject logged = %v for reason %s", has, tc.reason)
					}
				}
				if !found {
					t.Fatalf("no refusal log record in %s", h.logs.String())
				}
				for _, secret := range strings.Split(token, ".") {
					if secret != "" && strings.Contains(h.logs.String(), secret) {
						t.Fatalf("a token segment reached the log: %q", secret)
					}
				}
			})
		}
	}
}

func TestSvcAcrCompatCounterCountsOnlySvcAcr(t *testing.T) {
	signer := newWLSigner(t)
	workloadToken := signer.sign(wlHeader(), wlClaims())
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
			svc := entToken
			if route.key == routeLLMConfig {
				svc = testToken
			}
			before := wlCounts(t, "devhealth_acr_svc_acr_compat_total")
			do(h.handler, route.path, "Bearer "+workloadToken)
			do(h.handler, route.path, "Bearer svc_acr_nobody-0123456789")
			do(h.handler, route.path, "")
			if got := wlDelta(before, wlCounts(t, "devhealth_acr_svc_acr_compat_total")); len(got) != 0 {
				t.Fatalf("compat counted a workload token or a refusal: %v", got)
			}
			do(h.handler, route.path, "Bearer "+svc)
			if got := wlDelta(before, wlCounts(t, "devhealth_acr_svc_acr_compat_total")); !reflect.DeepEqual(got, map[string]int64{route.key: 1}) {
				t.Fatalf("compat delta = %v, want one svc_acr_ use on %s", got, route.key)
			}
		})
	}
}

func TestWorkloadVerifierForTheWrongAudienceRefusesEveryToken(t *testing.T) {
	signer := newWLSigner(t)
	h := newWLHarness(t, wlAuth(t, signer, "auth-service"))
	for _, route := range wlRoutesUnderTest {
		if rec := do(h.handler, route.path, "Bearer "+signer.sign(wlHeader(), wlClaims())); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d: a verifier built for auth-service must refuse a dev-health-api token", route.key, rec.Code)
		}
	}
}

func TestWorkloadSeriesAreDeclaredAtZero(t *testing.T) {
	acrMetricReader()
	Routes(Deps{})
	seen := wlCounts(t, "devhealth_acr_workload_refused_total")
	for _, route := range wlRoutesUnderTest {
		for _, reason := range workloadReasons {
			if _, ok := seen[fmt.Sprintf("%s/%s", route.key, reason)]; !ok {
				t.Errorf("no zero series for %s/%s", route.key, reason)
			}
		}
	}
}
