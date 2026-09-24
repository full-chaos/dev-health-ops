//go:build integration

package routeswitch

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const testSchemaDigest = "sha256:test-schema-digest"

func startRoutingStatePostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// Minimal shape of go_api_routing_state (alembic 0114) -- only the
	// columns PostgresSwitch actually reads/needs for its PK, not the
	// full FK to go_api_candidate_build (out of scope for this test: it
	// exercises the Switch, not the whole registry schema).
	if _, err := pool.Exec(ctx, `
		CREATE TABLE go_api_routing_state (
			schema_digest TEXT NOT NULL,
			document_digest TEXT NOT NULL,
			selected_operation TEXT NOT NULL,
			mode TEXT NOT NULL,
			PRIMARY KEY (schema_digest, document_digest, selected_operation)
		)
	`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func insertRoutingState(t *testing.T, pool *pgxpool.Pool, documentDigest, operation, mode string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, mode)
		VALUES ($1, $2, $3, $4)
	`, testSchemaDigest, documentDigest, operation, mode); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresSwitch_ModeDrivesReachability is the registry-backed
// counterpart to switch_test.go's in-memory-switch reachability tests
// (plan §6 "cited constructor is not proof of capability", applied here
// to a real Postgres-backed registry read): a handler registered in the
// Mux is reachable only when `go_api_routing_state.mode` is `canary` or
// `primary` for that exact (schema_digest, document_digest,
// selected_operation) triple -- table-driven, clause by clause across
// every mode in the plan §5 vocabulary, not a single happy-path check.
func TestPostgresSwitch_ModeDrivesReachability(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	cases := []struct {
		mode          string
		wantReachable bool
	}{
		{mode: "python", wantReachable: false},
		{mode: "shadow", wantReachable: false},
		{mode: "canary", wantReachable: true},
		{mode: "primary", wantReachable: true},
		{mode: "disabled", wantReachable: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("mode_"+tc.mode, func(t *testing.T) {
			operation := "op_" + tc.mode
			documentDigest := "doc-" + tc.mode
			insertRoutingState(t, pool, documentDigest, operation, tc.mode)

			sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{operation: documentDigest})
			mux := NewMux(sw)
			mux.Register(operation, handlerNamed(operation))

			rec := httptest.NewRecorder()
			mux.Dispatch(operation, rec, httptest.NewRequest(http.MethodGet, "/query", nil))

			gotReachable := rec.Code == http.StatusOK
			if gotReachable != tc.wantReachable {
				t.Errorf("mode=%q: reachable=%v, want %v (status %d)", tc.mode, gotReachable, tc.wantReachable, rec.Code)
			}
		})
	}
}

// TestPostgresSwitch_UnregisteredOperationIsUnreachable: an operation with
// no row in go_api_routing_state at all (never registered) must resolve
// to unreachable, the same safe default as StaticSwitch/DynamicSwitch --
// a broken or empty registry must never fail open.
func TestPostgresSwitch_UnregisteredOperationIsUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"neverRegistered": "doc-x"})
	mux := NewMux(sw)
	mux.Register("neverRegistered", handlerNamed("neverRegistered"))

	rec := httptest.NewRecorder()
	mux.Dispatch("neverRegistered", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("operation with no routing-state row responded %d, want 404", rec.Code)
	}
}

// TestPostgresSwitch_OperationWithNoDocumentDigestIsUnreachable: an
// operation name the caller never mapped to a document digest cannot be
// looked up at all -- this must not panic or default to enabled.
func TestPostgresSwitch_OperationWithNoDocumentDigestIsUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, "doc-x", "hasDigest", "primary")

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"hasDigest": "doc-x"})
	mux := NewMux(sw)
	mux.Register("noDigestMapped", handlerNamed("noDigestMapped"))

	rec := httptest.NewRecorder()
	mux.Dispatch("noDigestMapped", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("operation with no document-digest mapping responded %d, want 404", rec.Code)
	}
}

// TestPostgresSwitch_RollbackRevokesReachabilityImmediately: plan §5
// "rollback is a registry change, not an image rollback" -- flipping mode
// back from primary/canary must revoke reachability on the very next
// read, with no separate deploy or cache to invalidate.
func TestPostgresSwitch_RollbackRevokesReachabilityImmediately(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, "doc-rb", "rollbackOp", "primary")

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"rollbackOp": "doc-rb"})
	mux := NewMux(sw)
	mux.Register("rollbackOp", handlerNamed("rollbackOp"))

	rec := httptest.NewRecorder()
	mux.Dispatch("rollbackOp", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 while mode=primary, got %d", rec.Code)
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE go_api_routing_state SET mode = 'disabled'
		WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3
	`, testSchemaDigest, "doc-rb", "rollbackOp"); err != nil {
		t.Fatal(err)
	}

	rec = httptest.NewRecorder()
	mux.Dispatch("rollbackOp", rec, httptest.NewRequest(http.MethodGet, "/query", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 immediately after rollback to mode=disabled, got %d", rec.Code)
	}
}

// TestPostgresSwitch_DigestMissEmitsCounterAndWarnLog is CHAOS-5415's
// proof: before this fix, a (schema_digest, document_digest, operation)
// key with no go_api_routing_state row -- the exact live state found on
// the bigboy compose stack, every row keyed on a schema_digest one SDL
// move stale (CHAOS-4703) -- returned false with NO log line and NO
// counter, indistinguishable from "not canaried yet". Delegation had
// been silently reverting to Python for every operation, undetected.
// This drives a real digest-miss lookup through the real Enabled() path
// and proves both deliverables: a WARN log record naming
// operation/schema_digest/document_digest (via a real slog.JSONHandler),
// and a real devhealth_query_api_routeswitch_digest_miss_total{operation}
// data point read back through a real sdkmetric.ManualReader -- same
// standard as principal/rejection_telemetry_test.go's
// TestVerify_RejectionsAreLoggedAndCountedByReason.
func TestPostgresSwitch_DigestMissEmitsCounterAndWarnLog(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	// No insertRoutingState call: the row for "missingOp"/"doc-missing"
	// never exists -- this IS the digest-miss case.

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(prevProvider)

	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	defer slog.SetDefault(prevLogger)

	sw := NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"missingOp": "doc-missing"})
	if sw.Enabled("missingOp") {
		t.Fatal("operation with no routing-state row must resolve to unreachable")
	}

	rec := findDigestMissLogRecord(t, logBuf.Bytes())
	if got, _ := rec["level"].(string); got != "WARN" {
		t.Errorf("log level = %q, want WARN", got)
	}
	if got, _ := rec["operation"].(string); got != "missingOp" {
		t.Errorf("log operation = %q, want %q", got, "missingOp")
	}
	if got, _ := rec["schema_digest"].(string); got != testSchemaDigest {
		t.Errorf("log schema_digest = %q, want %q", got, testSchemaDigest)
	}
	if got, _ := rec["document_digest"].(string); got != "doc-missing" {
		t.Errorf("log document_digest = %q, want %q", got, "doc-missing")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}
	dp := findDigestMissCounterDataPoint(t, rm, "missingOp")
	if dp.Value != 1 {
		t.Errorf("digest-miss counter value = %d, want exactly 1", dp.Value)
	}
}

// findDigestMissLogRecord parses logOutput as newline-delimited JSON
// (slog.JSONHandler's wire format) and returns the first record whose
// msg names the digest-miss fallback, failing the test if none is found.
func findDigestMissLogRecord(t *testing.T, logOutput []byte) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(logOutput, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("unmarshal log line %q: %v", line, err)
		}
		if msg, _ := rec["msg"].(string); strings.Contains(msg, "routeswitch") {
			return rec
		}
	}
	t.Fatalf("no digest-miss WARN log record found in:\n%s", logOutput)
	return nil
}

// findDigestMissCounterDataPoint returns the collected data point for
// devhealth_query_api_routeswitch_digest_miss_total with operation=op,
// failing the test if the metric was not exported at all -- the same
// "prove it reaches a real consumer" standard
// rejection_telemetry_test.go's findCounterDataPoint applies.
func findDigestMissCounterDataPoint(t *testing.T, rm metricdata.ResourceMetrics, op string) metricdata.DataPoint[int64] {
	t.Helper()
	const metricName = "devhealth_query_api_routeswitch_digest_miss_total"
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
				if v, ok := dp.Attributes.Value(attribute.Key("operation")); ok && v.AsString() == op {
					return dp
				}
			}
		}
	}
	t.Fatalf("%s not found in collected metrics with operation=%q -- the reader consumed nothing", metricName, op)
	return metricdata.DataPoint[int64]{}
}
