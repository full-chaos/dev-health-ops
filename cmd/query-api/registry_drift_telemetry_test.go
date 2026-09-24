package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// driftMeterReader is the ONE real OTel SDK meter reader for this test
// binary, following the exact one-time-global-delegation discipline
// internal/queryapi/analytics/main_test.go documents: every
// package-level instrument in this package is created via the global
// otel.Meter(...) proxy at package-init time, and the process-wide
// delegateMeterOnce binds it to whichever provider FIRST calls
// otel.SetMeterProvider. A second, independent provider set up inside an
// individual test would silently lose that instrument.
var driftMeterReader *sdkmetric.ManualReader

// metricsRegistryForTests is the same Prometheus registry production's
// newPrometheusMeterProvider mounts at /metrics, built here (once, on the
// ONE provider every instrument in this test binary delegates to) so a
// /metrics-shaped test can scrape exactly what the drift/readyz/routeswitch
// recorders wrote -- a second, independently constructed provider would
// bind no instruments at all, for the same one-time-delegation reason
// driftMeterReader's comment above describes.
var metricsRegistryForTests *prometheus.Registry

func TestMain(m *testing.M) {
	driftMeterReader = sdkmetric.NewManualReader()
	meterProvider, promRegistry, err := newPrometheusMeterProvider(driftMeterReader)
	if err != nil {
		panic(err)
	}
	metricsRegistryForTests = promRegistry
	otel.SetMeterProvider(meterProvider)
	os.Exit(m.Run())
}

// collectGaugeForDigest reads the named gauge's data point tagged with
// schema_digest=wantDigest. The gauge keeps one data point PER distinct
// attribute set, and this test binary shares one meter provider across
// every test case (the one-time global delegation collectGauge's sibling
// tests all rely on) -- so filtering by digest, not "whichever point
// collected last", is what makes each test see only the value IT wrote.
func collectGaugeForDigest(t *testing.T, name, wantDigest string) (value int64, found bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := driftMeterReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s: data shape = %+v, want an int64 gauge", name, m.Data)
			}
			for _, dp := range data.DataPoints {
				got, _ := dp.Attributes.Value("schema_digest")
				if got.AsString() == wantDigest {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

// This is the exact regression this whole deliverable exists to close:
// twelve rows alive, none at the digest the running binary computes, and
// nothing before this change said so anywhere durable. Red against the
// pre-change binary, which recorded no metric and logged with no level at
// all.
func TestDefaultRecordRoutingRowsForDigest_StaleCaseIsObservable(t *testing.T) {
	var logBuf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	defaultRecordRoutingRowsForDigest(context.Background(),
		map[string]int64{"sha256:67b87d38": 12}, "sha256:29d509cd")

	live, found := collectGaugeForDigest(t, "devhealth_query_api_routing_rows_for_digest", "sha256:29d509cd")
	if !found {
		t.Fatal("devhealth_query_api_routing_rows_for_digest was never emitted for sha256:29d509cd")
	}
	if live != 0 {
		t.Errorf("routing_rows_for_digest = %d, want 0 (nothing lives at the digest this binary computed)", live)
	}

	total, found := collectGaugeForDigest(t, "devhealth_query_api_routing_rows_total", "sha256:29d509cd")
	if !found {
		t.Fatal("devhealth_query_api_routing_rows_total was never emitted for sha256:29d509cd")
	}
	if total != 12 {
		t.Errorf("routing_rows_total = %d, want 12", total)
	}

	var record map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &record); err != nil {
		t.Fatalf("log record is not JSON: %v (body=%q)", err, logBuf.String())
	}
	if record["level"] != "ERROR" {
		t.Errorf("log level = %v, want ERROR -- a real incident (rows exist, none reachable) must page, not just log", record["level"])
	}
	if record["routing_rows_for_digest"] != float64(0) {
		t.Errorf("log record routing_rows_for_digest = %v, want 0", record["routing_rows_for_digest"])
	}
	if record["routing_rows_total"] != float64(12) {
		t.Errorf("log record routing_rows_total = %v, want 12", record["routing_rows_total"])
	}
}

// The default posture (nothing enabled yet) must never log at ERROR --
// conflating it with the stale case is the exact defect this file exists
// to prevent, one layer up from classifyRoutingDrift's own text-only
// distinction.
func TestDefaultRecordRoutingRowsForDigest_EmptyTableDoesNotError(t *testing.T) {
	var logBuf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	defaultRecordRoutingRowsForDigest(context.Background(), map[string]int64{}, "sha256:empty-case")

	live, found := collectGaugeForDigest(t, "devhealth_query_api_routing_rows_for_digest", "sha256:empty-case")
	if !found {
		t.Fatal("devhealth_query_api_routing_rows_for_digest was never emitted for sha256:empty-case")
	}
	if live != 0 {
		t.Errorf("routing_rows_for_digest = %d, want 0", live)
	}

	var record map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &record); err != nil {
		t.Fatalf("log record is not JSON: %v (body=%q)", err, logBuf.String())
	}
	if record["level"] == "ERROR" {
		t.Errorf("empty table (default posture, not an incident) logged at ERROR: %v", record)
	}
}

// A healthy digest -- rows exist and this process's own digest is among
// them -- must record the live count, not zero, and must not error.
func TestDefaultRecordRoutingRowsForDigest_LiveDigestReportsLiveCount(t *testing.T) {
	var logBuf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	defaultRecordRoutingRowsForDigest(context.Background(),
		map[string]int64{"sha256:live": 15, "sha256:old": 12}, "sha256:live")

	live, found := collectGaugeForDigest(t, "devhealth_query_api_routing_rows_for_digest", "sha256:live")
	if !found {
		t.Fatal("devhealth_query_api_routing_rows_for_digest was never emitted for sha256:live")
	}
	if live != 15 {
		t.Errorf("routing_rows_for_digest = %d, want 15", live)
	}

	total, found := collectGaugeForDigest(t, "devhealth_query_api_routing_rows_total", "sha256:live")
	if !found {
		t.Fatal("devhealth_query_api_routing_rows_total was never emitted for sha256:live")
	}
	if total != 27 {
		t.Errorf("routing_rows_total = %d, want 27", total)
	}

	var record map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &record); err != nil {
		t.Fatalf("log record is not JSON: %v (body=%q)", err, logBuf.String())
	}
	if record["level"] == "ERROR" {
		t.Errorf("a live, healthy digest logged at ERROR: %v", record)
	}
}

// A nil pool (unconfigured deployment) must never reach the recorder --
// there is nothing to report, and recordRoutingRowsForDigest recording a
// false "0 rows, 0 total" for an environment that never configured
// go_api_routing_state at all would be indistinguishable from a real
// empty-table posture.
func TestLogRoutingStateDrift_NilPoolNeverCallsRecorder(t *testing.T) {
	called := false
	previousRecorder := recordRoutingRowsForDigest
	recordRoutingRowsForDigest = func(context.Context, map[string]int64, string) { called = true }
	t.Cleanup(func() { recordRoutingRowsForDigest = previousRecorder })

	logRoutingStateDrift(nil, "sha256:d")

	if called {
		t.Error("logRoutingStateDrift called the recorder for a nil pool")
	}
}
