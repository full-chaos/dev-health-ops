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
	for _, route := range Routes(Deps{Store: store, Auth: creds, Limiter: limiter, Logger: logger}) {
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

func credsFor() *fakeCredentials {
	past := time.Now().Add(-time.Hour)
	f := newFakeCredentials()
	f.creds["svc_acr_revoked-token-0123456789"] = Credential{ID: "cred-revoked", ServiceName: "acr", Scopes: []string{EntitlementScope}, RevokedAt: &past}
	f.creds["svc_acr_expired-token-0123456789"] = Credential{ID: "cred-expired", ServiceName: "acr", Scopes: []string{EntitlementScope}, ExpiresAt: &past}
	f.creds["svc_acr_other-svc-token-0123456789"] = Credential{ID: "cred-other", ServiceName: "worker-operator", Scopes: []string{EntitlementScope}}
	f.creds["svc_acr_llm-only-token-0123456789"] = Credential{ID: "cred-llm", ServiceName: "acr", Scopes: []string{"llm_config:read"}}
	return f
}

// TestInternalAcrRoutesBearerMatrix is api/internal/acr.py's auth order on
// both routes: 401 for a missing, malformed, unknown or inactive token, 403
// for a missing scope. The entitlement route audits each outcome and stamps
// last_used_at after validity and before the scope check; health does
// neither.
func TestInternalAcrRoutesBearerMatrix(t *testing.T) {
	cases := []struct {
		name       string
		header     string
		status     int
		audit      *auditRow
		usedBefore bool
	}{
		{"no header", "", 401, &auditRow{"", someOrg, "missing_or_malformed_token"}, false},
		{"lowercase scheme", "bearer " + entToken, 401, &auditRow{"", someOrg, "missing_or_malformed_token"}, false},
		{"wrong prefix", "Bearer svc_worker_x-0123456789", 401, &auditRow{"", someOrg, "missing_or_malformed_token"}, false},
		{"prefix only", "Bearer svc_acr_", 401, &auditRow{"", someOrg, "missing_or_malformed_token"}, false},
		{"unknown", "Bearer svc_acr_nobody-0123456789", 401, &auditRow{"", someOrg, "unknown_token"}, false},
		{"revoked", "Bearer svc_acr_revoked-token-0123456789", 401, &auditRow{"cred-revoked", someOrg, "inactive_token"}, false},
		{"expired", "Bearer svc_acr_expired-token-0123456789", 401, &auditRow{"cred-expired", someOrg, "inactive_token"}, false},
		{"other service", "Bearer svc_acr_other-svc-token-0123456789", 401, &auditRow{"cred-other", someOrg, "inactive_token"}, false},
		{"missing scope", "Bearer svc_acr_llm-only-token-0123456789", 403, &auditRow{"cred-llm", someOrg, "insufficient_scope"}, true},
		{"valid", "Bearer " + entToken, 200, &auditRow{"cred-ent", someOrg, "success"}, true},
	}
	for _, tc := range cases {
		t.Run("entitlement/"+tc.name, func(t *testing.T) {
			creds := credsFor()
			rec := do(authServer(t, &fakeStore{}, creds, nil), entitlementPath+someOrg, tc.header)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if !reflect.DeepEqual(creds.audits, []auditRow{*tc.audit}) {
				t.Fatalf("audits = %+v, want [%+v]", creds.audits, *tc.audit)
			}
			if got := len(creds.used) == 1; got != tc.usedBefore {
				t.Fatalf("last_used_at stamped = %v, want %v", got, tc.usedBefore)
			}
		})
		t.Run("health/"+tc.name, func(t *testing.T) {
			creds := credsFor()
			rec := do(authServer(t, &fakeStore{}, creds, nil), healthPath, tc.header)
			if rec.Code != tc.status {
				t.Fatalf("status = %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if len(creds.audits) != 0 || len(creds.used) != 0 {
				t.Fatalf("health wrote audits %+v used %v; Python health writes neither", creds.audits, creds.used)
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
			want := []auditRow{{"cred-ent", someOrg, tc.outcome}}
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
		t.Run(name+"/credential lookup fails", func(t *testing.T) {
			creds := credsFor()
			creds.findErr = errBoom
			rec := do(authServer(t, &fakeStore{}, creds, nil), path, "Bearer "+entToken)
			if rec.Code != 503 || len(creds.audits) != 0 {
				t.Fatalf("status = %d audits %+v, want 503 and none", rec.Code, creds.audits)
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
	t.Run("entitlement/last_used write fails", func(t *testing.T) {
		creds := credsFor()
		creds.markErr = errBoom
		if rec := do(authServer(t, &fakeStore{}, creds, nil), entitlementPath+someOrg, "Bearer "+entToken); rec.Code != 503 {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})
}

// TestInternalAcrRoutesShareOnePerAddressBudget: INTERNAL_ACR_AUTH_ATTEMPT_IP_LIMIT
// is one bucket per client address across both routes and every org id, and a
// refused attempt reaches neither the credential store nor the audit.
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
	findsBefore, auditsBefore := creds.findCalls, len(creds.audits)
	rec := do(h, healthPath, "Bearer "+entToken)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th attempt = %d %s, want 429", rec.Code, rec.Body.String())
	}
	if want := `{"detail":"Too many authentication attempts"}`; rec.Body.String() != want {
		t.Fatalf("body = %s, want %s", rec.Body.String(), want)
	}
	if creds.findCalls != findsBefore || len(creds.audits) != auditsBefore {
		t.Fatal("a rate-limited attempt must not reach the credential store or the audit")
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

func TestCredentialValidAtItsExpiryInstant(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	revoked := now.Add(-time.Second)
	for name, tc := range map[string]struct {
		cred Credential
		want bool
	}{
		"no expiry":     {Credential{}, true},
		"future":        {Credential{ExpiresAt: ptr(now.Add(time.Second))}, true},
		"exactly now":   {Credential{ExpiresAt: ptr(now)}, true},
		"one ns past":   {Credential{ExpiresAt: ptr(now.Add(-time.Nanosecond))}, false},
		"revoked":       {Credential{RevokedAt: &revoked}, false},
		"revoked+valid": {Credential{RevokedAt: &revoked, ExpiresAt: ptr(now.Add(time.Hour))}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := tc.cred.Valid(now); got != tc.want {
				t.Fatalf("Valid = %v, want %v", got, tc.want)
			}
		})
	}
}

func ptr(t time.Time) *time.Time { return &t }
