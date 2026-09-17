package principal

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestEdgeVerify_RejectionsAreLoggedAndCountedByReason is the edge access
// token's own proof of the standard TestVerify_RejectionsAreLoggedAndCountedByReason
// already pins for the envelope: before this, EdgeVerifier.Verify's
// rejection path returned an error with NO log line and NO per-reason
// counter at all -- a bare 401 on a REST route gave an operator nothing
// to distinguish a wrong secret from an expired token or a malformed
// claim from outside the process. This drives one real request per
// reason class through the REAL verifier and proves both deliverables:
// a WARN log record naming reason/iss/aud/remote_addr/request_id, and
// real dev_health_query_api_edge_rejected_total{reason} data points read
// back through sharedTestMetricReader (metrics_reader_test.go) -- see
// that var's own doc comment for why every telemetry test in this
// package, including this one, reads through that ONE shared reader
// rather than installing a provider of its own.
func TestEdgeVerify_RejectionsAreLoggedAndCountedByReason(t *testing.T) {
	v := mustEdgeVerifier(t, edgeTestSecret, edgeTestIssuer, edgeTestAudience)

	const remoteAddr = "203.0.113.9:51903"
	const requestID = "req-edge-abc123"

	cases := []struct {
		name       string
		wantReason string
		wantIss    string
		wantAud    string
		token      func() string
	}{
		{
			name:       "bad_signature",
			wantReason: "bad_signature",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				return signEdgeToken(t, "a-completely-different-fixture-secret!!", validEdgeClaims("org-1"))
			},
		},
		{
			name:       "audience",
			wantReason: "audience",
			wantIss:    edgeTestIssuer,
			wantAud:    "some-other-service",
			token: func() string {
				claims := validEdgeClaims("org-1")
				claims["aud"] = "some-other-service"
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "issuer",
			wantReason: "issuer",
			wantIss:    "not-the-real-edge",
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				claims["iss"] = "not-the-real-edge"
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "expired",
			wantReason: "expired",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				claims["exp"] = time.Now().Add(-time.Hour).Unix()
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "not_yet_valid",
			wantReason: "not_yet_valid",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				claims["nbf"] = time.Now().Add(time.Hour).Unix()
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "malformed",
			wantReason: "malformed",
			wantIss:    "",
			wantAud:    "",
			token: func() string {
				return "not-a-jwt-at-all"
			},
		},
		{
			name:       "missing_sub",
			wantReason: "missing_sub",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				delete(claims, "sub")
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "missing_type",
			wantReason: "missing_type",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				delete(claims, "type")
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
		{
			name:       "type_mismatch",
			wantReason: "type_mismatch",
			wantIss:    edgeTestIssuer,
			wantAud:    edgeTestAudience,
			token: func() string {
				claims := validEdgeClaims("org-1")
				claims["type"] = "refresh"
				return signEdgeToken(t, edgeTestSecret, claims)
			},
		},
	}

	// Uses sharedTestMetricReader (metrics_reader_test.go), never a
	// private provider of its own -- see that var's own doc comment for
	// why a second real MeterProvider installed here would silently
	// break the envelope's own equivalent test's collection.
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

			rec := findLogRecord(t, logBuf.Bytes(), "query_api.edge.rejected")
			if lvl, _ := rec["level"].(string); lvl != "WARN" {
				t.Errorf("log level = %q, want WARN", lvl)
			}
			if got, _ := rec["reason"].(string); got != tc.wantReason {
				t.Errorf("log reason = %q, want %q", got, tc.wantReason)
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
			// Neither the presented token nor the secret may ever reach
			// the log -- checked against the exact bytes actually
			// written, not just the structured fields already asserted
			// on above.
			if strings.Contains(logBuf.String(), token) {
				t.Fatal("rejection log output contains the raw token -- must never log token material")
			}
			if strings.Contains(logBuf.String(), edgeTestSecret) {
				t.Fatal("rejection log output contains the signing secret -- must never log key material")
			}
		})
	}

	// A token that verifies cleanly must trip neither the rejection log
	// nor the rejection counter.
	t.Run("valid_edge_token_logs_and_counts_nothing", func(t *testing.T) {
		var logBuf bytes.Buffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
		defer slog.SetDefault(prevLogger)

		token := signEdgeToken(t, edgeTestSecret, validEdgeClaims("org-1"))
		ctx := WithRequestMeta(context.Background(), remoteAddr, requestID)
		if _, err := v.Verify(ctx, token); err != nil {
			t.Fatalf("Verify: unexpected error for a valid edge token: %v", err)
		}
		if strings.Contains(logBuf.String(), "query_api.edge.rejected") {
			t.Errorf("a successful Verify must not emit a rejection log, got:\n%s", logBuf.String())
		}
	})

	// Collected once, after all 10 calls above (9 rejections + 1
	// success) -- see TestVerify_RejectionsAreLoggedAndCountedByReason's
	// own doc comment for why this cannot be done per-subtest.
	var rm metricdata.ResourceMetrics
	if err := sharedTestMetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("sharedTestMetricReader.Collect: %v", err)
	}
	var totalDataPoints int
	for _, tc := range cases {
		dp := findCounterDataPoint(t, rm, "dev_health_query_api_edge_rejected_total", "reason", tc.wantReason)
		if dp.Value != 1 {
			t.Errorf("reason=%s counter value = %d, want exactly 1", tc.wantReason, dp.Value)
		}
		totalDataPoints++
	}
	if got := countDataPoints(rm, "dev_health_query_api_edge_rejected_total"); got != totalDataPoints {
		t.Errorf("dev_health_query_api_edge_rejected_total has %d data points, want exactly %d (one per reason, none from the successful call)",
			got, totalDataPoints)
	}

	rejectedOutcome := findCounterDataPoint(t, rm, "devhealth_query_api_edge_verify_total", "outcome", "rejected")
	if rejectedOutcome.Value != int64(len(cases)) {
		t.Errorf("outcome=rejected counter value = %d, want %d (one per rejection case)", rejectedOutcome.Value, len(cases))
	}
	verifiedOutcome := findCounterDataPoint(t, rm, "devhealth_query_api_edge_verify_total", "outcome", "verified")
	if verifiedOutcome.Value != 1 {
		t.Errorf("outcome=verified counter value = %d, want 1 (the one successful call)", verifiedOutcome.Value)
	}
}
