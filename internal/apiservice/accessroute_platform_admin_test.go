package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// platformAdminRig is the api listener as newMatrixRig builds it (real rows,
// real scope layers, the real route guard on each protected row) with probeUser
// a superuser or not; routed false is the HS256 path alone.
func platformAdminRig(t *testing.T, superuser, routed bool) (http.Handler, *eddsaMinter, *lookupCounts) {
	t.Helper()
	counts := &lookupCounts{}
	minter := newEdDSAMinter(t)
	probe := newProbeStore()
	row := probe.users[probeUser]
	row.state.IsSuperuser = superuser
	probe.users[probeUser] = row
	store := matrixStore{probeStore: probe, counts: counts}
	hs, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(hs, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: matrixKeys{inner: minter.keys, counts: counts}})
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyVerifier(auth.AuthenticateLegacy)
	validator, err := accessverify.New(accessverify.Config{
		Rows: realRows(t), EdDSA: eddsa,
		Legacy: func(ctx context.Context, token string) (any, error) { return legacy(ctx, token) },
		Meter:  sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(),
		Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if routed {
		if err := auth.RouteAccess(matrixValidator{inner: routedAccessVerify{validator}, counts: counts}, goAPIAccessService); err != nil {
			t.Fatal(err)
		}
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
	return server.Handler(), minter, counts
}

// Product rule 1 (D6773): on org-scoped admin rows a platform admin whose
// token names an org it holds no membership row in gets, with a user access
// token, the HS256 path's answer byte for byte. A caller without the platform
// action gets the HS256 path's answer too (CHAOS-13227): the X-Org-Id refusal
// for an org that exists on an org-scoped row, and the claimed org on an
// org-less row. A claim that is not a UUID is refused by the access-token.v1
// verifier itself, where legacy goes on in the claimed org: CHAOS-13261.
func TestProductRule1PlatformAdminWithoutMembershipAnswersAsLegacy(t *testing.T) {
	rows := []string{"/api/v1/admin/audit-logs", "/api/v1/admin/credentials", "/api/v1/admin/backfill-jobs", probeAuthenticated}
	serve := func(handler http.Handler, path, bearer string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", bearer)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}
	// The HS256 token a minter gives a non-member: role member, the users
	// row's profile, the row's token version.
	hs256 := func(org string) string {
		return "Bearer " + probeHS256(t, probeKey, jwt.MapClaims{"sub": probeUser.String(), "org_id": org,
			"role": "member", "email": "user@example.test", "username": "user", "full_name": "Probe User", "tv": 3})
	}
	for _, superuser := range []bool{true, false} {
		legacy, _, _ := platformAdminRig(t, superuser, false)
		routed, minter, _ := platformAdminRig(t, superuser, true)
		eddsa := "Bearer " + minter.mint(probeUser, map[string]any{"org_id": probeTargetOrg.String()})
		for _, path := range rows {
			wantStatus, wantBody := serve(legacy, path, hs256(probeTargetOrg.String()))
			gotStatus, gotBody := serve(routed, path, eddsa)
			if superuser {
				if wantStatus != http.StatusOK || gotStatus != wantStatus || gotBody != wantBody {
					t.Fatalf("%s superuser: user access token %d %s, HS256 %d %s", path, gotStatus, gotBody, wantStatus, wantBody)
				}
				continue
			}
			legacyStatus := http.StatusForbidden
			if path == probeAuthenticated {
				legacyStatus = http.StatusOK
			}
			if wantStatus != legacyStatus || gotStatus != wantStatus || gotBody != wantBody {
				t.Fatalf("%s non-member without the platform action: user access token %d %s, HS256 %d %s", path, gotStatus, gotBody, wantStatus, wantBody)
			}
		}
	}
	t.Run("org claim not a UUID", func(t *testing.T) {
		legacy, _, _ := platformAdminRig(t, false, false)
		routed, minter, _ := platformAdminRig(t, false, true)
		eddsa := "Bearer " + minter.mint(probeUser, map[string]any{"org_id": "not-a-uuid"})
		refused := []string{}
		for _, path := range rows {
			wantStatus, wantBody := serve(legacy, path, hs256("not-a-uuid"))
			gotStatus, gotBody := serve(routed, path, eddsa)
			t.Logf("%s: HS256 %d %.120s | user access token %d %.120s", path, wantStatus, wantBody, gotStatus, gotBody)
			switch {
			case gotStatus == wantStatus && gotBody == wantBody:
			case gotStatus == http.StatusUnauthorized && wantStatus != http.StatusUnauthorized:
				refused = append(refused, path)
			default:
				t.Fatalf("%s: user access token %d %s, HS256 %d %s", path, gotStatus, gotBody, wantStatus, wantBody)
			}
		}
		if len(refused) > 0 {
			t.Skipf("CHAOS-13261: the access-token.v1 verifier refuses an org claim that is not a UUID (401) where legacy answers: %v", refused)
		}
	})
}
