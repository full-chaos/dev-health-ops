package acr

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const (
	healthPath      = "/api/v1/internal/acr/health"
	entitlementPath = "/api/v1/internal/acr/entitlements/"
	someOrg         = "6f1c2a52-6d0e-4a5e-9d57-3f4b8d1a7c11"
)

func authServer(t *testing.T, store EntitlementStore, creds CredentialStore, limiter *httpapi.KeyedLimiter) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, route := range Routes(Deps{Store: store, Auth: creds, Limiter: limiter, Logger: logger, Workload: testWorkload}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return mux
}

func do(h http.Handler, path, authorization string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return recorder
}

func credsFor() *fakeCredentials { return newFakeCredentials() }

// TestInternalAcrRoutesBearerMatrix: the acr-api workload token is the only
// accepted bearer. Every other header, a svc_acr_ service credential
// included, is the uniform 401 "Unauthorized" with outcome
// missing_or_malformed_token; the entitlement route audits each outcome with
// a NULL credential_id, health audits nothing.
func TestInternalAcrRoutesBearerMatrix(t *testing.T) {
	refused := &auditRow{"", someOrg, "missing_or_malformed_token"}
	cases := []struct {
		name   string
		header string
		status int
		audit  *auditRow
	}{
		{"no header", "", 401, refused},
		{"lowercase scheme", "bearer " + entToken, 401, refused},
		{"svc_worker_", "Bearer svc_worker_x-0123456789", 401, refused},
		{"svc_acr_ prefix only", "Bearer svc_acr_", 401, refused},
		{"svc_acr_ credential", "Bearer svc_acr_entitlements-token-0123456789", 401, refused},
		{"workload token", "Bearer " + entToken, 200, &auditRow{"", someOrg, "success"}},
	}
	for _, tc := range cases {
		t.Run("entitlement/"+tc.name, func(t *testing.T) {
			creds := credsFor()
			rec := do(authServer(t, &fakeStore{}, creds, nil), entitlementPath+someOrg, tc.header)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if tc.status == 401 && rec.Body.String() != `{"detail":"Unauthorized"}` {
				t.Fatalf("body = %s, want the uniform refusal", rec.Body.String())
			}
			if !reflect.DeepEqual(creds.audits, []auditRow{*tc.audit}) {
				t.Fatalf("audits = %+v, want [%+v]", creds.audits, *tc.audit)
			}
		})
		t.Run("health/"+tc.name, func(t *testing.T) {
			creds := credsFor()
			rec := do(authServer(t, &fakeStore{}, creds, nil), healthPath, tc.header)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if len(creds.audits) != 0 {
				t.Fatalf("health wrote audits %+v; health writes none", creds.audits)
			}
		})
	}
}

func TestInternalAcrEntitlementAuditsTheLookupOutcome(t *testing.T) {
	cases := []struct {
		name    string
		store   EntitlementStore
		status  int
		outcome string
	}{
		{"unknown org", &fakeStore{err: ErrOrgUnknown}, 404, "organization_not_found"},
		{"malformed org", &fakeStore{err: ErrOrgNotFound}, 404, "organization_not_found"},
		{"store unavailable", &fakeStore{err: ErrUnavailable}, 503, "entitlement_lookup_failed"},
		{"unclassified", &fakeStore{err: errBoom}, 500, "entitlement_lookup_failed"},
		{"no store", nil, 503, "entitlement_lookup_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			creds := credsFor()
			rec := do(authServer(t, tc.store, creds, nil), entitlementPath+someOrg, "Bearer "+entToken)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			want := []auditRow{{"", someOrg, tc.outcome}}
			if !reflect.DeepEqual(creds.audits, want) {
				t.Fatalf("audits = %+v, want %+v", creds.audits, want)
			}
		})
	}
}

func TestInternalAcrRoutesFailClosed(t *testing.T) {
	for name, path := range map[string]string{"entitlement": entitlementPath + someOrg, "health": healthPath} {
		t.Run(name+"/nil credential store", func(t *testing.T) {
			if rec := do(authServer(t, &fakeStore{}, nil, nil), path, "Bearer "+entToken); rec.Code != 503 {
				t.Fatalf("status = %d, want 503: an unconfigured route must never serve", rec.Code)
			}
		})
	}
	t.Run("entitlement/audit write fails", func(t *testing.T) {
		creds := credsFor()
		creds.auditErr = errBoom
		for _, header := range []string{"Bearer " + entToken, "", "Bearer svc_acr_nobody-0123456789"} {
			if rec := do(authServer(t, &fakeStore{}, creds, nil), entitlementPath+someOrg, header); rec.Code != 503 {
				t.Fatalf("header %q: status = %d %s, want 503: an unaudited answer must not be served", header, rec.Code, rec.Body.String())
			}
		}
	})
}

// TestInternalAcrRoutesShareOnePerAddressBudget: INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT
// is one bucket per client address across both routes and every org id, and a
// refused attempt writes no audit row.
func TestInternalAcrRoutesShareOnePerAddressBudget(t *testing.T) {
	limit := httpapi.Limit{ID: "test_internal_acr", Count: 3, Window: time.Minute}
	creds := credsFor()
	h := authServer(t, &fakeStore{}, creds, httpapi.NewKeyedLimiter(httpapi.NewMemoryCounters(nil), limit))
	paths := []string{healthPath, entitlementPath + someOrg, entitlementPath + "other-org"}
	for i, path := range paths {
		if rec := do(h, path, "Bearer "+entToken); rec.Code != 200 && rec.Code != 404 {
			t.Fatalf("attempt %d: status = %d", i, rec.Code)
		}
	}
	auditsBefore := len(creds.audits)
	rec := do(h, healthPath, "Bearer "+entToken)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th attempt = %d %s, want 429", rec.Code, rec.Body.String())
	}
	if want := `{"detail":"Too many authentication attempts"}`; rec.Body.String() != want {
		t.Fatalf("body = %s, want %s", rec.Body.String(), want)
	}
	if len(creds.audits) != auditsBefore {
		t.Fatal("a rate-limited attempt must not reach the audit")
	}
	other := httptest.NewRequest(http.MethodGet, healthPath, nil)
	other.Header.Set("Authorization", "Bearer "+entToken)
	other.RemoteAddr = "203.0.113.9:4000"
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, other)
	if recorder.Code != 200 {
		t.Fatalf("another address = %d, want 200: the budget is per address", recorder.Code)
	}
}
