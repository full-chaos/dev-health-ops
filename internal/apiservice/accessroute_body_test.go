package apiservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// TestIssuerSetBodyFirstRoutesKeepTheRouteProfile: on the real session routes
// that decode the body before authenticating (BodyFirst), the token is still
// validated against the route's own row: an HS256 token on the legacy
// /auth/validate and /auth/logout rows is accepted there, never refused for
// want of a profile.
func TestIssuerSetBodyFirstRoutesKeepTheRouteProfile(t *testing.T) {
	verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, newProbeStore(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	validator, err := accessverify.New(accessverify.Config{Rows: realRows(t), Legacy: legacyVerifier(auth.AuthenticateLegacy),
		Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer}
	scope := policy.NewScope(auth, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	token := probeHS256(t, probeKey, map[string]any{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3})

	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/validate", strings.NewReader(`{"token":"`+token+`"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"valid":true`) {
		t.Errorf("validate: %d %s, want valid:true", recorder.Code, recorder.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", strings.NewReader(`{"refresh_token":"r"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	server.Handler().ServeHTTP(httptest.NewRecorder(), request)

	counters := (&routedFixture{reader: reader}).counters(t)
	for _, profile := range []string{"POST /api/v1/auth/validate", "POST /api/v1/auth/logout"} {
		if counters[accessverify.AcceptedMetric+"|class=ops_access_token_hs256,profile="+profile+",service="+goAPIAccessService] == 0 {
			t.Errorf("%s: no legacy acceptance on its own row", profile)
		}
	}
	if n := counters[accessverify.RefusedMetric+"|profile=unknown,reason=no_profile,service=unknown"]; n != 0 {
		t.Errorf("%d validations ran without a route profile", n)
	}
}
