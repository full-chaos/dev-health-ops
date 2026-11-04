package authapi

import (
	"context"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const lagMetric = "auth.session.revocation_to_refusal_seconds"

func lagPoints(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var out []metricdata.HistogramDataPoint[float64]
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == lagMetric {
				out = append(out, h.DataPoints...)
			}
		}
	}
	return out
}

func wantLag(t *testing.T, reader *sdkmetric.ManualReader, count uint64, sum float64) {
	t.Helper()
	points := lagPoints(t, reader)
	var gotCount uint64
	var gotSum float64
	for _, p := range points {
		gotCount += p.Count
		gotSum += p.Sum
		if p.Attributes.Len() != 0 {
			t.Fatalf("the lag histogram carries %d attributes, want none", p.Attributes.Len())
		}
	}
	if gotCount != count || gotSum != sum {
		t.Fatalf("lag observations = %d sum %v, want %d sum %v", gotCount, gotSum, count, sum)
	}
}

func sessionRouteWithMeter(t *testing.T) (*fixture, *sdkmetric.ManualReader, time.Time) {
	t.Helper()
	f := newFixture(t)
	reader, provider := accessMeter()
	t0 := time.Now().UTC()
	f.issuer.now = func() time.Time { return t0 }
	routes, err := Routes(Deps{Sessions: f.sessions, Users: f.users, Issuer: f.issuer, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
	return f, reader, t0
}

func (f *fixture) setRecord(mutate func(*session.Record)) {
	r := f.sessions.records[f.record.ID]
	mutate(&r)
	f.sessions.records[f.record.ID] = r
}

func TestARevokedSessionRefusalRecordsItsLag(t *testing.T) {
	f, reader, t0 := sessionRouteWithMeter(t)
	at := t0.Add(-5 * time.Second)
	f.setRecord(func(r *session.Record) { r.RevokedAt = &at })
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_revoked")
	wantLag(t, reader, 1, 5)
}

func TestARevokedAtAheadOfTheClockRecordsZero(t *testing.T) {
	f, reader, t0 := sessionRouteWithMeter(t)
	at := t0.Add(10 * time.Second)
	f.setRecord(func(r *session.Record) { r.RevokedAt = &at })
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_revoked")
	wantLag(t, reader, 1, 0)
}

func TestARefusalOlderThanTheWindowRecordsNothing(t *testing.T) {
	f, reader, t0 := sessionRouteWithMeter(t)
	for _, age := range map[string]time.Duration{"just over": time.Hour + time.Second, "two hours": 2 * time.Hour} {
		at := t0.Add(-age)
		f.setRecord(func(r *session.Record) { r.RevokedAt = &at })
		expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_revoked")
		wantLag(t, reader, 0, 0)
	}
	at := t0.Add(-time.Hour)
	f.setRecord(func(r *session.Record) { r.RevokedAt = &at })
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_revoked")
	wantLag(t, reader, 1, 3600)
}

func TestRefusalsThatAreNotARevokeRecordNoLag(t *testing.T) {
	f, reader, t0 := sessionRouteWithMeter(t)
	f.setRecord(func(r *session.Record) { r.ExpiresAt = t0.Add(-time.Second) })
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_expired")
	delete(f.sessions.records, f.record.ID)
	expectError(t, f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())), http.StatusUnauthorized, "session_revoked")
	wantLag(t, reader, 0, 0)
}

func TestALiveSessionRecordsNoLag(t *testing.T) {
	f, reader, _ := sessionRouteWithMeter(t)
	if r := f.do(http.MethodGet, PatternCurrent, "", f.bearer(f.grant())); r.Code != http.StatusOK {
		t.Fatalf("live session = %d %s", r.Code, r.Body)
	}
	wantLag(t, reader, 0, 0)
}

func TestValidateRefusalOfARevokedSessionRecordsItsLag(t *testing.T) {
	f := newValidateFixture(t, false)
	reader, provider := accessMeter()
	f.meter = provider.Meter("test")
	t0 := time.Now().UTC()
	f.issuer.now = func() time.Time { return t0 }
	f.build()
	live := f.post(f.server, tokenBody(f.hs256Token(nil)), nil)
	if live.Code != http.StatusOK {
		t.Fatalf("live validate = %d %s", live.Code, live.Body)
	}
	wantLag(t, reader, 0, 0)
	at := t0.Add(-7 * time.Second)
	f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, LegacyTokenVersion: f.record.LegacyTokenVersion}
	record := f.record
	record.RevokedAt = &at
	f.sessions.records[f.record.ID] = record
	f.post(f.server, tokenBody(f.hs256Token(nil)), nil)
	wantLag(t, reader, 1, 7)
}

func TestANilRevokedAtRecordsNoLag(t *testing.T) {
	reader, provider := accessMeter()
	rec, err := newRecorder(slog.New(slog.DiscardHandler), provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	rec.observeRevocationLag(context.Background(), nil, time.Now())
	wantLag(t, reader, 0, 0)
	at := time.Now().Add(-2 * time.Second)
	rec.observeRevocationLag(context.Background(), &at, at.Add(2*time.Second))
	wantLag(t, reader, 1, 2)
}

func TestValidateRefusalOfASessionRevokedInItsOwnRowRecordsItsLag(t *testing.T) {
	for _, tc := range []struct {
		name  string
		eddsa bool
		token func(*validateFixture) string
	}{
		{"HS256", false, func(f *validateFixture) string { return f.hs256Token(nil) }},
		{"EdDSA", true, func(f *validateFixture) string { return f.eddsaToken("dev-health-api") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newValidateFixture(t, tc.eddsa)
			reader, provider := accessMeter()
			f.meter = provider.Meter("test")
			t0 := time.Now().UTC()
			f.issuer.now = func() time.Time { return t0 }
			f.build()
			at := t0.Add(-7 * time.Second)
			f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, Revoked: true, LegacyTokenVersion: f.record.LegacyTokenVersion}
			record := f.record
			record.RevokedAt = &at
			f.sessions.records[f.record.ID] = record
			response := f.post(f.server, tokenBody(tc.token(f)), nil)
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"valid":false`) {
				t.Fatalf("validate = %d %s", response.Code, response.Body)
			}
			wantLag(t, reader, 1, 7)
		})
	}
}

func TestValidateRefusalOfARevokedRowThatIsNotTheTokenOwnersRecordsNoLag(t *testing.T) {
	f := newValidateFixture(t, false)
	reader, provider := accessMeter()
	f.meter = provider.Meter("test")
	f.build()
	at := time.Now().Add(-7 * time.Second)
	f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: uuid.New(), Revoked: true, LegacyTokenVersion: f.record.LegacyTokenVersion}
	record := f.record
	record.RevokedAt = &at
	f.sessions.records[f.record.ID] = record
	f.post(f.server, tokenBody(f.hs256Token(nil)), nil)
	wantLag(t, reader, 0, 0)
}

func TestValidateRefusalOfARevokedRowWithNoAuthServiceSessionRecordsNoLag(t *testing.T) {
	f := newValidateFixture(t, false)
	reader, provider := accessMeter()
	f.meter = provider.Meter("test")
	f.build()
	f.rows.rows[f.record.ID] = policy.SessionRow{PrincipalID: f.principal, Revoked: true, LegacyTokenVersion: f.record.LegacyTokenVersion}
	delete(f.sessions.records, f.record.ID)
	f.post(f.server, tokenBody(f.hs256Token(nil)), nil)
	wantLag(t, reader, 0, 0)
}
