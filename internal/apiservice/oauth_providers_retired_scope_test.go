package apiservice

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// The OAuth provider patch route is retired, so nothing behind PATCH /api/v1/auth/oauth/{first}/{second} takes a
// token: its endpoint-profile row is public, and the scope layers never refuse a PATCH there. Through go-api's own
// server with the real scope rows, a token whose org claim the caller left, and a foreign X-Org-Id, get the
// dispatcher's answer like every other caller: 404 for a provider id, 405 Allow POST for a sign-in path.
func TestRetiredOAuthProviderPatchIsNotScoped(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	store.members[[2]uuid.UUID{probeUser, probeOrg}] = "owner"
	store.members[[2]uuid.UUID{probeMember, probeOtherOrg}] = "owner"
	adminSession := uuid.MustParse("00000000-0000-4000-8000-0000000000c4")
	store.sessions[adminSession] = policy.SessionRow{PrincipalID: probeAdminUser, LegacyTokenVersion: i64ptr(0)}
	auth, err := policy.NewAuthenticator(store, logger)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows: realRows(t), EdDSA: verifier, Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: logger,
		Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	delegated, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(delegated)
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	scope.DeclaredRows(rows)
	server, err := NewServer(apiTestConfig(), logger, Routes(Deps{Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(auth, logger), Auth: auth}, logger),
		scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	foreign := probeTargetOrg.String()
	callers := []struct{ name, token, headerOrg string }{
		{"anonymous", "", ""},
		{"owner", minter.mint(probeUser, nil), ""},
		{"other org owner", minter.mint(probeMember, map[string]any{"sid": sessionid.Wire(probeOtherSID), "org_id": probeOtherOrg.String()}), ""},
		{"platform admin", minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(adminSession)}), ""},
		{"owner, org claim it left", minter.mint(probeUser, map[string]any{"org_id": foreign}), ""},
		{"owner, foreign X-Org-Id", minter.mint(probeUser, nil), foreign},
		{"platform admin, foreign X-Org-Id", minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(adminSession)}), foreign},
	}
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
	for _, license := range []*licensing.ProcessLicense{nil, {Tier: "enterprise", Features: map[string]bool{"sso_saml": true}}} {
		licensing.SetProcessLicense(license)
		for _, step := range []struct {
			path, allow string
			status      int
			body        string
		}{
			{"/api/v1/auth/oauth/providers/22000000-0000-4000-8000-000000000003", "", http.StatusNotFound, `{"detail":"Not Found"}`},
			{"/api/v1/auth/oauth/providers/x", "", http.StatusNotFound, `{"detail":"Not Found"}`},
			{"/api/v1/auth/oauth/providers/authorize", "POST", http.StatusMethodNotAllowed, `{"detail":"Method Not Allowed"}`},
			{"/api/v1/auth/oauth/providers/callback", "POST", http.StatusMethodNotAllowed, `{"detail":"Method Not Allowed"}`},
		} {
			for _, caller := range callers {
				request := httptest.NewRequest(http.MethodPatch, step.path, strings.NewReader(`{"name":"renamed"}`))
				request.Header.Set("Content-Type", "application/json")
				if caller.token != "" {
					request.Header.Set("Authorization", "Bearer "+caller.token)
				}
				if caller.headerOrg != "" {
					request.Header.Set("X-Org-Id", caller.headerOrg)
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				body, _ := io.ReadAll(recorder.Body)
				if recorder.Code != step.status || string(body) != step.body || recorder.Header().Get("Allow") != step.allow {
					t.Errorf("license %v, %s: PATCH %s = %d Allow %q %s, want %d Allow %q %s", license, caller.name, step.path,
						recorder.Code, recorder.Header().Get("Allow"), body, step.status, step.allow, step.body)
				}
			}
		}
	}
}
