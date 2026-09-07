package principal

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestVerify_RejectionsAreLoggedAndCountedByReason is CHAOS-5443's proof:
// before this fix, Verify's rejection path returned an error with NO log
// line and NO per-reason counter at all -- a kid mismatch (edge minted
// with one kid, JWKS rotated to another) was indistinguishable from a bad
// signature, a wrong audience, or an expired token from outside the
// process, which is what took 40 minutes to diagnose live. This drives
// one real request per reason class through the REAL verifier (a
// freshly-generated Ed25519 JWKS + a signed token, never a checked-in
// key), plus one request that must verify cleanly, and proves both
// deliverables at once: a WARN log record naming
// reason/kid/iss/aud/remote_addr/request_id (via a real
// slog.JSONHandler, never a mock), and real
// dev_health_query_api_envelope_rejected_total{reason} data points read
// back through a real sdkmetric.ManualReader -- matching
// operatingreview_test.go's "a real OTel SDK ManualReader ... not a bare
// var-is-non-nil check" standard.
//
// All 8 requests share ONE MeterProvider/ManualReader for the whole test:
// this package's counters are process-global singletons created once at
// package load against whatever the ambient (delegating) MeterProvider
// was at that time, and go.opentelemetry.io/otel's global package only
// resolves that delegation against the FIRST real provider a process
// ever installs via otel.SetMeterProvider -- every later SetMeterProvider
// call only affects instruments created afterwards, not ones already
// resolved (see cmd/query-api/internal/analytics/main_test.go's own doc
// comment for the same fact from the other direction). A per-subtest
// provider swap was tried first and silently produced a "not found" for
// every case after the very first, for exactly this reason -- fixed by
// installing one provider before all 8 calls and collecting once after.
func TestVerify_RejectionsAreLoggedAndCountedByReason(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	_, otherPriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate other key: %v", err)
	}
	jwksPath := writeJWKS(t, pub, testKID)
	v := mustVerifier(t, jwksPath, testIssuer, testAudience)

	const remoteAddr = "203.0.113.7:51902"
	const requestID = "req-5443-abc123"

	cases := []struct {
		name       string
		wantReason string
		wantKID    string
		wantIss    string
		wantAud    string
		token      func() string
	}{
		{
			name:       "unknown_kid",
			wantReason: "unknown_kid",
			wantKID:    "edge-minted-kid-not-in-jwks",
			wantIss:    testIssuer,
			wantAud:    testAudience,
			token: func() string {
				return signEnvelope(t, priv, "edge-minted-kid-not-in-jwks", baseClaims())
			},
		},
		{
			name:       "bad_signature",
			wantReason: "bad_signature",
			wantKID:    testKID,
			wantIss:    testIssuer,
			wantAud:    testAudience,
			token: func() string {
				// Right kid, WRONG key -- proves signature verification,
				// not just kid lookup, drives this classification.
				return signEnvelope(t, otherPriv, testKID, baseClaims())
			},
		},
		{
			name:       "audience",
			wantReason: "audience",
			wantKID:    testKID,
			wantIss:    testIssuer,
			wantAud:    "some-other-service",
			token: func() string {
				claims := baseClaims()
				claims.Audience = jwt.ClaimStrings{"some-other-service"}
				return signEnvelope(t, priv, testKID, claims)
			},
		},
		{
			name:       "issuer",
			wantReason: "issuer",
			wantKID:    testKID,
			wantIss:    "not-the-real-edge",
			wantAud:    testAudience,
			token: func() string {
				claims := baseClaims()
				claims.Issuer = "not-the-real-edge"
				return signEnvelope(t, priv, testKID, claims)
			},
		},
		{
			name:       "expired",
			wantReason: "expired",
			wantKID:    testKID,
			wantIss:    testIssuer,
			wantAud:    testAudience,
			token: func() string {
				claims := baseClaims()
				past := time.Now().Add(-2 * time.Minute)
				claims.IssuedAt = jwt.NewNumericDate(past)
				claims.ExpiresAt = jwt.NewNumericDate(past.Add(30 * time.Second))
				return signEnvelope(t, priv, testKID, claims)
			},
		},
		{
			name:       "not_yet_valid",
			wantReason: "not_yet_valid",
			wantKID:    testKID,
			wantIss:    testIssuer,
			wantAud:    testAudience,
			token: func() string {
				claims := baseClaims()
				claims.NotBefore = jwt.NewNumericDate(time.Now().Add(2 * time.Minute))
				return signEnvelope(t, priv, testKID, claims)
			},
		},
		{
			name:       "malformed",
			wantReason: "malformed",
			wantKID:    "",
			wantIss:    "",
			wantAud:    "",
			token: func() string {
				return "not-a-jwt-at-all"
			},
		},
	}

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(prevProvider)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			prevLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
			defer slog.SetDefault(prevLogger)

			ctx := WithRequestMeta(context.Background(), remoteAddr, requestID)
			token := tc.token()
			if _, err := v.Verify(ctx, token); err == nil {
				t.Fatalf("Verify: expected a rejection for %s, got nil error", tc.name)
			}

			rec := findLogRecord(t, logBuf.Bytes(), "query_api.envelope.rejected")
			if lvl, _ := rec["level"].(string); lvl != "WARN" {
				t.Errorf("log level = %q, want WARN", lvl)
			}
			if got, _ := rec["reason"].(string); got != tc.wantReason {
				t.Errorf("log reason = %q, want %q", got, tc.wantReason)
			}
			if got, _ := rec["kid"].(string); got != tc.wantKID {
				t.Errorf("log kid = %q, want %q", got, tc.wantKID)
			}
			if got, _ := rec["iss"].(string); got != tc.wantIss {
				t.Errorf("log iss = %q, want %q", got, tc.wantIss)
			}
			if got, _ := rec["aud"].(string); got != tc.wantAud {
				t.Errorf("log aud = %q, want %q", got, tc.wantAud)
			}
			if got, _ := rec["remote_addr"].(string); got != remoteAddr {
				t.Errorf("log remote_addr = %q, want %q (the caller-supplied value)", got, remoteAddr)
			}
			if got, _ := rec["request_id"].(string); got != requestID {
				t.Errorf("log request_id = %q, want %q (the caller-supplied value)", got, requestID)
			}
			// Neither the presented token nor any key material may ever
			// reach the log -- checked against the exact bytes actually
			// written, not just the structured fields this test already
			// asserts on (a leak in an unexpected field would still show
			// up here).
			if strings.Contains(logBuf.String(), token) {
				t.Fatal("rejection log output contains the raw token -- must never log token material")
			}
		})
	}

	// A token that verifies cleanly must trip neither the rejection log
	// nor the rejection counter -- otherwise "a rejection was logged"
	// would be meaningless noise on every successful request. Run against
	// the SAME provider/reader as the reject cases above (see the
	// function doc comment for why a fresh one here would silently prove
	// nothing).
	t.Run("valid_envelope_logs_and_counts_nothing", func(t *testing.T) {
		var logBuf bytes.Buffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
		defer slog.SetDefault(prevLogger)

		token := signEnvelope(t, priv, testKID, baseClaims())
		ctx := WithRequestMeta(context.Background(), remoteAddr, requestID)
		if _, err := v.Verify(ctx, token); err != nil {
			t.Fatalf("Verify: unexpected error for a valid envelope: %v", err)
		}
		if strings.Contains(logBuf.String(), "query_api.envelope.rejected") {
			t.Errorf("a successful Verify must not emit a rejection log, got:\n%s", logBuf.String())
		}
	})

	// Collected once, after all 8 calls above (7 rejections + 1 success)
	// -- see the function doc comment for why this cannot be done
	// per-subtest.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}
	var totalDataPoints int
	for _, tc := range cases {
		dp := findCounterDataPoint(t, rm, "dev_health_query_api_envelope_rejected_total", "reason", tc.wantReason)
		if dp.Value != 1 {
			t.Errorf("reason=%s counter value = %d, want exactly 1", tc.wantReason, dp.Value)
		}
		totalDataPoints++
	}
	if got := countDataPoints(rm, "dev_health_query_api_envelope_rejected_total"); got != totalDataPoints {
		t.Errorf("dev_health_query_api_envelope_rejected_total has %d data points, want exactly %d (one per reason, none from the successful call)",
			got, totalDataPoints)
	}
}

// findLogRecord parses logOutput as newline-delimited JSON (slog.JSONHandler's
// wire format) and returns the first record whose "msg" field matches want,
// failing the test if none is found.
func findLogRecord(t *testing.T, logOutput []byte, want string) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(logOutput, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		if got, _ := rec["msg"].(string); got == want {
			return rec
		}
	}
	t.Fatalf("no log record with msg=%q found in:\n%s", want, logOutput)
	return nil
}

// findCounterDataPoint returns the data point of metricName whose attrKey
// attribute equals attrVal, failing the test if metricName was not
// exported by the reader at all or has no matching data point -- the same
// "prove it reaches a real consumer" standard
// operatingreview_test.go's TestKnownCountGuardFiredCounter_ConsumedByARealMeterReader
// applies to its own counter.
func findCounterDataPoint(t *testing.T, rm metricdata.ResourceMetrics, metricName, attrKey, attrVal string) metricdata.DataPoint[int64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(attrKey)); ok && v.AsString() == attrVal {
					return dp
				}
			}
		}
	}
	t.Fatalf("%s not found in collected metrics with %s=%q -- the reader consumed nothing", metricName, attrKey, attrVal)
	return metricdata.DataPoint[int64]{}
}

// countDataPoints returns the total number of data points recorded for
// metricName across every scope, so a test can assert nothing extra
// (e.g. a stray label from an unrelated outcome) landed on it.
func countDataPoints(rm metricdata.ResourceMetrics, metricName string) int {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				return len(sum.DataPoints)
			}
		}
	}
	return 0
}
