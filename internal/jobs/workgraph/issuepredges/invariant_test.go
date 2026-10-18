package issuepredges

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

func fallbackInvariantGauge(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	return fallbackInvariantGaugeNamed(t, reader, FallbackBesideNativeGauge)
}

func fallbackInvariantGaugeNamed(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 gauge", m.Name, m.Data)
			}
			for _, point := range gauge.DataPoints {
				org, _ := point.Attributes.Value("organization_id")
				out[org.AsString()] = point.Value
			}
		}
	}
	return out
}

func logLines(t *testing.T, buffer *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
		if raw == "" {
			continue
		}
		line := map[string]any{}
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// A build that left a lower-tier link beside a native one is LOUD: a WARN
// line with the org and the counts, and the per-org gauge above 0. An org
// with none reports 0 on the gauge and logs no WARN.
func TestReportFallbackInvariantWarnsAndSetsTheGaugeOnABreach(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
	stored := []issueprlinks.Link{
		{RepoID: fallbackRepo, PRNumber: 704, WorkItemID: "linear:CHAOS-1", Provenance: "native"},
		{RepoID: fallbackRepo, PRNumber: 704, WorkItemID: "jira:API-1", Provenance: "explicit_text", Evidence: "body quotes API-1"},
		{RepoID: fallbackRepo, PRNumber: 705, WorkItemID: "linear:CHAOS-1", Provenance: "heuristic"},
	}
	reportFallbackInvariant(context.Background(), logger, InvariantOutcome{
		OrganizationID: "org-breach", LinksRead: len(stored), Beside: issueprlinks.CountFallbacksBesideNative(stored),
	})
	reportFallbackInvariant(context.Background(), logger, InvariantOutcome{
		OrganizationID: "org-clean", LinksRead: 1, Beside: issueprlinks.CountFallbacksBesideNative(stored[:1]),
	})

	if got := fallbackInvariantGauge(t, reader); got["org-breach"] != 2 || got["org-clean"] != 0 || len(got) != 2 {
		t.Fatalf("gauge = %v, want org-breach 2 and org-clean 0", got)
	}
	for name, want := range map[string]map[string]int64{
		FallbackBesideNativePullRequestGauge: {"org-breach": 1, "org-clean": 0},
		FallbackBesideNativeIssueGauge:       {"org-breach": 1, "org-clean": 0},
		FallbackPullRequestIssueSideGauge:    {"org-breach": 0, "org-clean": 0},
	} {
		if got := fallbackInvariantGaugeNamed(t, reader, name); len(got) != 2 || got["org-breach"] != want["org-breach"] || got["org-clean"] != want["org-clean"] {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	lines := logLines(t, &buffer)
	if len(lines) != 2 {
		t.Fatalf("log lines = %v, want 2", lines)
	}
	breach, clean := lines[0], lines[1]
	if breach["level"] != "WARN" || breach["msg"] != FallbackInvariantBreachedLog || breach["organization_id"] != "org-breach" ||
		breach["beside_native_total"] != float64(2) || breach["beside_native_pull_request"] != float64(1) || breach["beside_native_issue"] != float64(1) {
		t.Fatalf("breach line = %v", breach)
	}
	if clean["level"] != "INFO" || clean["msg"] != FallbackInvariantHeldLog || clean["beside_native_total"] != float64(0) {
		t.Fatalf("clean line = %v", clean)
	}
	for _, line := range lines {
		for key := range line {
			if strings.Contains(key, "work_item") || strings.Contains(key, "repo") || strings.Contains(key, "evidence") {
				t.Fatalf("log line carries row contents (%s): %v", key, line)
			}
		}
	}
	if strings.Contains(buffer.String(), "API-1") || strings.Contains(buffer.String(), "CHAOS-1") {
		t.Fatalf("log carries row contents: %s", buffer.String())
	}
}

func TestCheckFallbackInvariantRefusesWithoutAConnectionOrAnOrg(t *testing.T) {
	var nilService *Service
	if _, err := nilService.CheckFallbackInvariant(context.Background(), "org"); err != ErrUnavailable {
		t.Fatalf("nil service err = %v, want ErrUnavailable", err)
	}
	if _, err := (&Service{}).CheckFallbackInvariant(context.Background(), "org"); err != ErrUnavailable {
		t.Fatalf("no connection err = %v, want ErrUnavailable", err)
	}
}

// Each side has its own gauge, so an alert from metrics alone names the side
// (CHAOS-10070); the total keeps its own series.
func TestReportFallbackInvariantSetsOneGaugePerSide(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	reportFallbackInvariant(context.Background(), slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), InvariantOutcome{
		OrganizationID: "org-sides",
		Beside:         issueprlinks.FallbacksBesideNative{Total: 11, ByPullRequest: 3, ByIssue: 5, PullRequestIssueSide: 7},
	})
	for name, want := range map[string]int64{
		FallbackBesideNativeGauge:            11,
		FallbackBesideNativePullRequestGauge: 3,
		FallbackBesideNativeIssueGauge:       5,
		FallbackPullRequestIssueSideGauge:    7,
	} {
		if got := fallbackInvariantGaugeNamed(t, reader, name); len(got) != 1 || got["org-sides"] != want {
			t.Errorf("%s = %v, want org-sides %d", name, got, want)
		}
	}
}
