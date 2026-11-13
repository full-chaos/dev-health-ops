package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// What POST /api/v1/auth/sso/providers answers an impersonating caller, on today's go-api, with and without the process
// licence for sso_saml and with the legacy impersonation refusal on and off. The route is a stub that answers 500 once the
// licence is held (internal/api/sso/sso.go gated), so these answers decide whether the refusal comes first.

const ssoProviderCreateBody = `{"name":"x","protocol":"oidc"}`

func postSSOProvider(handler http.Handler, token string) cellAnswer {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/sso/providers", strings.NewReader(ssoProviderCreateBody))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

func withSSOLicence(t *testing.T, held bool) {
	t.Helper()
	if held {
		licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{"sso_saml": true}})
	} else {
		licensing.SetProcessLicense(nil)
	}
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
}

func TestSSOProviderCreateAnswersAnImpersonatingCaller(t *testing.T) {
	for _, tc := range []struct {
		name         string
		refuseLegacy bool
		licence      bool
		wantStatus   int
		wantBodyHas  string
		wantNoMarker bool
	}{
		{name: "a refusal on, licence held", refuseLegacy: true, licence: true, wantStatus: http.StatusForbidden, wantBodyHas: impersonationRefusalBody, wantNoMarker: true},
		{name: "b refusal on, no licence", refuseLegacy: true, licence: false, wantStatus: http.StatusForbidden, wantBodyHas: impersonationRefusalBody, wantNoMarker: true},
		{name: "c refusal off, licence held (control: the stub)", refuseLegacy: false, licence: true, wantStatus: http.StatusInternalServerError, wantBodyHas: `Internal Server Error`},
		{name: "d refusal off, no licence (control: the 402)", refuseLegacy: false, licence: false, wantStatus: http.StatusPaymentRequired, wantBodyHas: `feature_not_licensed`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withSSOLicence(t, tc.licence)
			handler, minter := adminConfigServerScoped(t, nil, withLegacyImpersonationCaller, realRoutes, refuseLegacyCaller(tc.refuseLegacy))
			token := minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
			got := postSSOProvider(handler, token)
			t.Logf("ANSWER\t%s\t%d %s\tX-Impersonating=%q", tc.name, got.status, strings.TrimSpace(got.body), got.header.Get("X-Impersonating"))
			if got.status != tc.wantStatus || !strings.Contains(got.body, tc.wantBodyHas) {
				t.Errorf("%s: %d %s, want %d holding %s", tc.name, got.status, got.body, tc.wantStatus, tc.wantBodyHas)
			}
			if tc.wantNoMarker && got.header.Get("X-Impersonating") != "" {
				t.Errorf("%s: X-Impersonating %q on a refused request", tc.name, got.header.Get("X-Impersonating"))
			}
		})
	}
}

// The same refusal for every SSO provider write with the switch off (the stub and the direct UPDATE paths): an impersonating caller
// is refused with the impersonation text before the licence and before any handler, licensed and not. The switch-on twin is
// TestAnImpersonatedSSOWriteIsRefusedBeforeGatedAndCallsNothing.
func TestEverySSOProviderWriteRefusesAnImpersonatingCallerWithTheSwitchOff(t *testing.T) {
	writes := []string{"POST /api/v1/auth/sso/providers", "PATCH /api/v1/auth/sso/providers/{provider_id}", "DELETE /api/v1/auth/sso/providers/{provider_id}",
		"POST /api/v1/auth/sso/providers/{provider_id}/activate", "POST /api/v1/auth/sso/providers/{provider_id}/deactivate"}
	for _, licensed := range []bool{true, false} {
		withSSOLicence(t, licensed)
		handler, minter := adminConfigServerScoped(t, nil, withLegacyImpersonationCaller, realRoutes, refuseLegacyCaller(true))
		token := minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
		for _, key := range writes {
			got := ssoDo(handler, key, token)
			if got.status != http.StatusForbidden || !strings.Contains(got.body, impersonationRefusalBody) || got.header.Get("X-Impersonating") != "" {
				t.Errorf("licensed=%v %s: %d %s X-Impersonating=%q, want the 403 %s", licensed, key, got.status, got.body, got.header.Get("X-Impersonating"), impersonationRefusalBody)
			}
		}
	}
}
