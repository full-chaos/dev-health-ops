package session

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

func TestRefreshWithoutAJTIIsRefusedAndNotCounted(t *testing.T) {
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
	SetRefreshAccepted(counter)
	t.Cleanup(func() { SetRefreshAccepted(nil) })
	h := handlers{Deps: Deps{Verifier: verifier, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	token, err := signer.Refresh(edgetoken.RefreshClaims{UserID: "11111111-1111-1111-1111-111111111111"}, time.Now(), "")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
	r = r.WithContext(context.WithValue(r.Context(), refreshInputKey{}, token))
	w := httptest.NewRecorder()
	h.refresh(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d: %s, want 401", w.Code, w.Body)
	}
	if got := refreshAcceptedTotal(t, reader, "dev-health-ops-api"); got != 0 {
		t.Fatalf("a refresh token without a jti moved the counter to %d, want 0", got)
	}
}
