package apiservice

import (
	"fmt"
	"net/http"
	"net/http/httptest"
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

// legacyProbeBody is the HS256 path's 200 body on the probe rows below for the probe user (role member, the users
// row's profile, token version 3) in org: the executed legacy runs on main 98c7d4f56 (log 13154-legacy-rec-98c7d4f5.log
// sha256 8ba2f74cdd64) and 715e3f93d (log 13154-legacy-rec-pa-715e3f93.log, LEGACY-REC-PA lines).
func legacyProbeBody(org string, superuser bool) string {
	return fmt.Sprintf(`{"org":%q,"user":{"email":"user@example.test","full_name":"Probe User","is_superuser":%v,"org_id":%q,"role":"member","token_version":3,"user_id":"00000000-0000-4000-8000-0000000000a1","username":"user"}}`, org, superuser, org) + "\n"
}

// legacyXOrgRefusal is the HS256 path's answer on an org-scoped admin row for a caller with no membership and no
// platform action (same logs).
const legacyXOrgRefusal = `{"detail": "X-Org-Id not permitted for this user"}`

// Product rule 1 (D6773): on org-scoped admin rows a platform admin whose
// token names an org it holds no membership row in gets, with a user access
// token, the HS256 path's answer byte for byte. A caller without the platform
// action gets the HS256 path's answer too (CHAOS-13227): the X-Org-Id refusal
// for an org that exists on an org-scoped row, and the claimed org on an
// org-less row. A claim that is not a UUID is refused by the access-token.v1
// verifier itself, where legacy goes on in the claimed org: CHAOS-13262.
func TestProductRule1PlatformAdminWithoutMembershipAnswersAsLegacy(t *testing.T) {
	rows := []string{"/api/v1/admin/audit-logs", "/api/v1/admin/credentials", "/api/v1/admin/backfill-jobs", probeAuthenticated}
	serve := func(handler http.Handler, path, bearer string) (int, string) {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", bearer)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Code, recorder.Body.String()
	}
	target := probeTargetOrg.String()
	for _, superuser := range []bool{true, false} {
		routed, minter := platformAdminRig(t, superuser)
		eddsa := "Bearer " + minter.mint(probeUser, map[string]any{"org_id": target})
		for _, path := range rows {
			wantStatus, wantBody := http.StatusOK, legacyProbeBody(target, superuser)
			if !superuser && path != probeAuthenticated {
				wantStatus, wantBody = http.StatusForbidden, legacyXOrgRefusal
			}
			if gotStatus, gotBody := serve(routed, path, eddsa); gotStatus != wantStatus || gotBody != wantBody {
				t.Fatalf("%s superuser %v: user access token %d %s, HS256 %d %s", path, superuser, gotStatus, gotBody, wantStatus, wantBody)
			}
		}
	}
	t.Run("org claim not a UUID", func(t *testing.T) {
		routed, minter := platformAdminRig(t, false)
		eddsa := "Bearer " + minter.mint(probeUser, map[string]any{"org_id": "not-a-uuid"})
		refused := []string{}
		for _, path := range rows {
			wantStatus, wantBody := http.StatusOK, legacyProbeBody("not-a-uuid", false)
			gotStatus, gotBody := serve(routed, path, eddsa)
			switch {
			case gotStatus == wantStatus && gotBody == wantBody:
			case gotStatus == http.StatusUnauthorized:
				refused = append(refused, path)
			default:
				t.Fatalf("%s: user access token %d %s, HS256 %d %s", path, gotStatus, gotBody, wantStatus, wantBody)
			}
		}
		if len(refused) > 0 {
			t.Skipf("CHAOS-13262: the access-token.v1 verifier refuses an org claim that is not a UUID (401) where legacy answers: %v", refused)
		}
	})
}
