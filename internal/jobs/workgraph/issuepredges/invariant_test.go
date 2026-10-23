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

// fallbackInvariantCounter reads one counter by name: its summed value, the
// number of data points, and how many of them carry any attribute.
func fallbackInvariantCounter(t *testing.T, reader *sdkmetric.ManualReader, name string) (total int64, points, attributed int) {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			if !sum.IsMonotonic {
				t.Fatalf("%s is not monotonic", m.Name)
			}
			for _, point := range sum.DataPoints {
				points++
				total += point.Value
				if point.Attributes.Len() != 0 {
					attributed++
				}
			}
		}
	}
	return total, points, attributed
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
// line with the org and the counts, and the counters above 0. An org
// with none adds 0 to them and logs no WARN.
func TestReportFallbackInvariantWarnsAndCountsOnABreach(t *testing.T) {
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

	for name, want := range map[string]int64{
		FallbackBesideNativeCounter:            2,
		FallbackBesideNativePullRequestCounter: 1,
		FallbackBesideNativeIssueCounter:       1,
		FallbackPullRequestIssueSideCounter:    0,
		FallbackInvariantChecksCounter:         2,
	} {
		if total, _, attributed := fallbackInvariantCounter(t, reader, name); total != want || attributed != 0 {
			t.Fatalf("%s = %d (%d attributed points), want %d with no attributes", name, total, attributed, want)
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

// Each side has its own counter, so an alert from metrics alone names the side
// (CHAOS-10070); the total keeps its own series. No instrument carries any
// attribute: the organization is named by the log line only.
func TestReportFallbackInvariantCountsOnePerSideWithNoAttribute(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	reportFallbackInvariant(context.Background(), slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), InvariantOutcome{
		OrganizationID: "org-sides",
		Beside:         issueprlinks.FallbacksBesideNative{Total: 11, ByPullRequest: 3, ByIssue: 5, PullRequestIssueSide: 7},
	})
	for name, want := range map[string]int64{
		FallbackBesideNativeCounter:            11,
		FallbackBesideNativePullRequestCounter: 3,
		FallbackBesideNativeIssueCounter:       5,
		FallbackPullRequestIssueSideCounter:    7,
		FallbackInvariantChecksCounter:         1,
	} {
		total, points, attributed := fallbackInvariantCounter(t, reader, name)
		if total != want || points != 1 || attributed != 0 {
			t.Errorf("%s = %d over %d points (%d attributed), want %d over 1 point with none", name, total, points, attributed, want)
		}
	}
}

// A counter, unlike the gauge it replaces, is not overwritten by the next
// organization's build: a breach stays visible after a clean build follows.
func TestReportFallbackInvariantBreachSurvivesALaterCleanBuild(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	reportFallbackInvariant(context.Background(), logger, InvariantOutcome{OrganizationID: "org-a", Beside: issueprlinks.FallbacksBesideNative{Total: 4}})
	reportFallbackInvariant(context.Background(), logger, InvariantOutcome{OrganizationID: "org-b"})
	if total, _, _ := fallbackInvariantCounter(t, reader, FallbackBesideNativeCounter); total != 4 {
		t.Fatalf("breach rows = %d after a clean build, want 4", total)
	}
	if total, _, _ := fallbackInvariantCounter(t, reader, FallbackInvariantChecksCounter); total != 2 {
		t.Fatalf("checks = %d, want 2", total)
	}
}
