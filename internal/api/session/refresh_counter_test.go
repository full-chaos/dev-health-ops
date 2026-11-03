package session

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

const (
	refreshCounterSecret   = "refresh-counter-test-key-0123456789abcdef"
	refreshCounterIssuer   = "dev-health-ops"
	refreshCounterAudience = "dev-health-api"
)

func refreshAcceptedTotal(t *testing.T, reader *sdkmetric.ManualReader, service string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != accessverify.RefreshAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				got, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				if got.AsString() == service && class.AsString() == accessverify.ClassLegacyRefresh {
					total += point.Value
				}
			}
		}
	}
	return total
}

func TestRefreshClaimsRefuseWhatTheRouteRefusesAndCountNothing(t *testing.T) {
	verifier, err := edgetoken.New(refreshCounterSecret, refreshCounterIssuer, refreshCounterAudience)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(refreshCounterSecret, refreshCounterIssuer, refreshCounterAudience)
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	counter, err := accessverify.NewRefreshCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	h := handlers{Deps: Deps{Verifier: verifier, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	SetRefreshAccepted(counter)
	t.Cleanup(func() { SetRefreshAccepted(nil) })
	now := time.Now()
	refresh, err := signer.Refresh(edgetoken.RefreshClaims{UserID: "11111111-1111-1111-1111-111111111111"}, now, "jti-1")
	if err != nil {
		t.Fatal(err)
	}
	access, err := signer.Access(edgetoken.AccessClaims{UserID: "11111111-1111-1111-1111-111111111111"}, now, "jti-2")
	if err != nil {
		t.Fatal(err)
	}
	count := func(token string) (ok bool, status int) {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
		w := httptest.NewRecorder()
		_, _, _, ok = h.refreshClaims(w, r, token)
		return ok, w.Code
	}
	if ok, _ := count(refresh); !ok {
		t.Fatal("a valid refresh token was refused")
	}
	if got := refreshAcceptedTotal(t, reader, "dev-health-ops-api"); got != 0 {
		t.Fatalf("reading a refresh token's claims moved the counter to %d, want 0 (the route counts at its 200)", got)
	}
	for name, token := range map[string]string{"an access token": access, "garbage": "a.b.c"} {
		if ok, _ := count(token); ok {
			t.Fatalf("%s was accepted as a refresh token", name)
		}
	}
	if got := refreshAcceptedTotal(t, reader, "dev-health-ops-api"); got != 0 {
		t.Fatalf("a refused token moved the counter to %d, want 0", got)
	}
}
