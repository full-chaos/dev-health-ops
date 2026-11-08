package authapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

func installLogoutCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := accessverify.NewLogoutCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), AccessService)
	if err != nil {
		t.Fatal(err)
	}
	SetLogoutAccepted(counter)
	t.Cleanup(func() { SetLogoutAccepted(nil) })
	return reader
}

func legacyLogoutTotal(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != accessverify.LegacyLogoutAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				service, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				if service.AsString() == AccessService && class.AsString() == accessverify.ClassLegacyRefresh {
					total += point.Value
				}
			}
		}
	}
	return total
}

// dropLegacyKey leaves the fixture's routes as they stand once the legacy key
// is gone: the Issuer they hold has no HS256 verifier.
func dropLegacyKey(f *compatFixture) { f.issuer.verifier = nil }

func logoutBody(token string) string { return `{"refresh_token":"` + token + `"}` }

func assertLogoutDone(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s, want 200", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), textLogoutDone) {
		t.Fatalf("body %s, want the logout message", response.Body)
	}
}

func TestCompatLogoutKeySetVerifiedLegacyTokenRevokesAndCounts(t *testing.T) {
	f := newCompatFixture(t)
	logouts := installLogoutCounter(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(legacyToken(t, f, f.org.String(), testSecret)), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.revokedLegacy) != 1 || f.sessions.revokedLegacy[0].PrincipalID != f.principal {
		t.Fatalf("revoked lineages %v, want the token's one", f.sessions.revokedLegacy)
	}
	if got := legacyLogoutTotal(t, logouts); got != 1 {
		t.Fatalf("logout counter %d, want 1", got)
	}
}

func TestCompatLogoutKeySetForeignKeyTokenRevokesNothingAndCountsNothing(t *testing.T) {
	f := newCompatFixture(t)
	logouts := installLogoutCounter(t)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(legacyToken(t, f, f.org.String(), strings.Repeat("k", len(testSecret)+1))), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.revokedLegacy) != 0 {
		t.Fatalf("revoked lineages %v from a token another key signed", f.sessions.revokedLegacy)
	}
	if got := legacyLogoutTotal(t, logouts); got != 0 {
		t.Fatalf("logout counter %d, want 0", got)
	}
}

func TestCompatLogoutWithoutLegacyKeyAnswers200AndRevokesNothing(t *testing.T) {
	f := newCompatFixture(t)
	logouts := installLogoutCounter(t)
	token := legacyToken(t, f, f.org.String(), testSecret)
	dropLegacyKey(f)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody(token), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.revokedLegacy) != 0 {
		t.Fatalf("revoked lineages %v from a token no key verified", f.sessions.revokedLegacy)
	}
	if got := legacyLogoutTotal(t, logouts); got != 0 {
		t.Fatalf("logout counter %d, want 0", got)
	}
}

func TestCompatLogoutWithoutLegacyKeyStillRevokesAnOpaqueCredential(t *testing.T) {
	f := newCompatFixture(t)
	dropLegacyKey(f)
	response := f.do(http.MethodPost, PatternCompatLogout, logoutBody("opaque-credential"), nil)
	assertLogoutDone(t, response)
	if len(f.sessions.byCredential) != 1 || f.sessions.byCredential[0] != "opaque-credential" {
		t.Fatalf("revoked credentials %v, want the opaque one", f.sessions.byCredential)
	}
}

func TestVerifyLegacyRefreshWithoutLegacyKeyIsRefused(t *testing.T) {
	f := newCompatFixture(t)
	token := legacyToken(t, f, f.org.String(), testSecret)
	dropLegacyKey(f)
	legacy, err := f.issuer.VerifyLegacyRefresh(token)
	if !errors.Is(err, errLegacyKeyAbsent) || legacy.JTI.Reveal() != "" {
		t.Fatalf("VerifyLegacyRefresh = %+v, %v, want errLegacyKeyAbsent and no claims", legacy, err)
	}
}
