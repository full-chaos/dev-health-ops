package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// platformAdminRig is the routed api listener as newMatrixRig builds it (real
// rows, real scope layers, the real route guard on each protected row) with
// probeUser a superuser or not.
func platformAdminRig(t *testing.T, superuser bool) (http.Handler, *eddsaMinter) {
	t.Helper()
	counts := &lookupCounts{}
	minter := newEdDSAMinter(t)
	probe := newProbeStore()
	row := probe.users[probeUser]
	row.state.IsSuperuser = superuser
	probe.users[probeUser] = row
	store := matrixStore{probeStore: probe, counts: counts}
	auth, err := policy.NewAuthenticator(store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: matrixKeys{inner: minter.keys, counts: counts}})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows: realRows(t), EdDSA: eddsa,
		Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(),
		Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(matrixValidator{inner: routedAccessVerify{validator}, counts: counts}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	scopeRows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(scopeRows)
	guard := policy.NewGuard(auth, quietLogger())
	var routes []httpapi.Route
	for _, row := range apiRows(t) {
		var handler http.Handler = http.HandlerFunc(probeEcho)
		if row["classification"] == "protected" && matrixAcceptsAccessToken(row) {
			handler = guard.Wrap(policy.Authenticated, handler)
		}
		routes = append(routes, httpapi.Route{Method: row["method"].(string), Pattern: row["route"].(string), Handler: handler})
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler(), minter
}

// legacyPlatformAdminProbeBody is the HS256 path's answer on each row below for a superuser probe with no membership in
// probeTargetOrg (role member, the users row's profile, token version 3): the executed legacy run on main 98c7d4f56,
// log 13154-legacy-rec-98c7d4f5.log sha256 8ba2f74cdd64 (LEGACY-REC superuser=true lines, 200 on each row).
const legacyPlatformAdminProbeBody = `{"org":"00000000-0000-4000-8000-0000000000b3","user":{"email":"user@example.test","full_name":"Probe User","is_superuser":true,"org_id":"00000000-0000-4000-8000-0000000000b3","role":"member","token_version":3,"user_id":"00000000-0000-4000-8000-0000000000a1","username":"user"}}` + "\n"

// Product rule 1 (D6773): on org-scoped admin rows a platform admin whose
// token names an org it holds no membership row in gets, with a user access
// token, the HS256 path's answer byte for byte. A caller without the platform
// action keeps the routed refusal.
func TestProductRule1PlatformAdminWithoutMembershipAnswersAsLegacy(t *testing.T) {
	rows := []string{"/api/v1/admin/audit-logs", "/api/v1/admin/credentials", "/api/v1/admin/backfill-jobs"}
	for _, superuser := range []bool{true, false} {
		routed, minter := platformAdminRig(t, superuser)
		eddsa := "Bearer " + minter.mint(probeUser, map[string]any{"org_id": probeTargetOrg.String()})
		for _, path := range rows {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", eddsa)
			recorder := httptest.NewRecorder()
			routed.ServeHTTP(recorder, request)
			gotStatus, gotBody := recorder.Code, recorder.Body.String()
			if superuser {
				if gotStatus != http.StatusOK || gotBody != legacyPlatformAdminProbeBody {
					t.Fatalf("%s superuser: user access token %d %s, HS256 200 %s", path, gotStatus, gotBody, legacyPlatformAdminProbeBody)
				}
				continue
			}
			if gotStatus != http.StatusUnauthorized || strings.TrimSpace(gotBody) != genericRefusal {
				t.Fatalf("%s non-member without the platform action: %d %s, want the routed 401", path, gotStatus, gotBody)
			}
		}
	}
}
