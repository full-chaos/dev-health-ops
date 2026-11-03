//go:build integration

package session_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

func refreshCounterValue(t *testing.T, reader *sdkmetric.ManualReader) int64 {
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
				service, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				if service.AsString() == "dev-health-ops-api" && class.AsString() == accessverify.ClassLegacyRefresh {
					total += point.Value
				}
			}
		}
	}
	return total
}

// The refresh counter moves only when the route answers 200: a reused token and
// a token of a deactivated user are verified by the route's JWT check but refused.
func TestRefreshCounterCountsOnlyTheRefreshesTheRouteAccepts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStack(t, ctx)
	reader := sdkmetric.NewManualReader()
	counter, err := accessverify.NewRefreshCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	session.SetRefreshAccepted(counter)
	t.Cleanup(func() { session.SetRefreshAccepted(nil) })
	post := func(path, body string) (int, string) {
		return changeRequest(t, ctx, st, http.MethodPost, path, "", body)
	}
	login := func(email string) string {
		status, body := post("/api/v1/auth/login", `{"email": "`+email+`", "password": "`+sessionscenario.Password+`"}`)
		if status != http.StatusOK {
			t.Fatalf("login %s: %d %s", email, status, body)
		}
		return sessionscenario.Field(body, "refresh_token")
	}
	refresh := func(token string) int {
		status, _ := post("/api/v1/auth/refresh", `{"refresh_token": "`+token+`"}`)
		return status
	}
	first := login("alice@example.com")
	if got := refreshCounterValue(t, reader); got != 0 {
		t.Fatalf("a login moved the counter to %d, want 0", got)
	}
	if status := refresh(first); status != http.StatusOK {
		t.Fatalf("valid refresh: %d, want 200", status)
	}
	if got := refreshCounterValue(t, reader); got != 1 {
		t.Fatalf("after one accepted refresh the counter is %d, want 1", got)
	}

	if _, err := st.pool.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = revoked_at - interval '1 hour' WHERE revoked_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
	if status := refresh(first); status != http.StatusUnauthorized {
		t.Fatalf("reused refresh token: %d, want 401", status)
	}
	if got := refreshCounterValue(t, reader); got != 1 {
		t.Fatalf("a reused refresh token moved the counter to %d, want 1", got)
	}

	bob := login("bob@example.com")
	if _, err := st.pool.Exec(ctx, `UPDATE users SET is_active = false WHERE id = $1::uuid`, changeBob); err != nil {
		t.Fatal(err)
	}
	if status := refresh(bob); status == http.StatusOK {
		t.Fatalf("refresh of a deactivated user: %d, want a refusal", status)
	}
	if got := refreshCounterValue(t, reader); got != 1 {
		t.Fatalf("a refresh of a deactivated user moved the counter to %d, want 1", got)
	}
}
