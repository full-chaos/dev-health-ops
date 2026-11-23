package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// doneWithoutCompletedAtMetric is the counter's name, spelled out here: the cell reads what is exported.
const doneWithoutCompletedAtMetric = "dev_health_work_item_done_without_completed_at_total"

// doneWithoutCompletedAtReader swaps the global meter provider for one this test reads alone.
func doneWithoutCompletedAtReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

// doneWithoutCompletedAtCounts is the counter's value per provider label.
func doneWithoutCompletedAtCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != doneWithoutCompletedAtMetric {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", metric.Name, metric.Data)
			}
			for _, point := range sum.DataPoints {
				if point.Attributes.Len() != 1 {
					t.Errorf("%s point has labels %v, want provider only", metric.Name, point.Attributes.ToSlice())
				}
				provider, _ := point.Attributes.Value("provider")
				counts[provider.AsString()] += point.Value
			}
		}
	}
	return counts
}

func doneWithoutCompletedAtRow(provider, workItemID, status string, completedAt *time.Time) workItemStoredRow {
	return workItemStoredRow{
		WorkItemID: workItemID, Provider: provider, Status: status, CompletedAt: completedAt,
		Assignees: []string{}, Labels: []string{}, CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}
}

func writeDoneWithoutCompletedAtRows(t *testing.T, conn *heldVersionConn, insert string, rows ...workItemStoredRow) error {
	t.Helper()
	for _, row := range rows {
		conn.repos[row.WorkItemID] = row.RepoID
	}
	return writeWorkItemsKeepingHeldColumns(context.Background(), conn, "org-acme", insert, rows)
}

// A stored work item that is done with no completed_at counts on no day of the daily reads. The writer counts each
// such row it stores, by provider, so the first one shows. A done row with a completion time and a canceled row with
// none are not counted.
func TestWorkItemsWriterCountsADoneRowWithNoCompletedAt(t *testing.T) {
	reader := doneWithoutCompletedAtReader(t)
	completed := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	github := &heldVersionConn{held: map[string]*time.Time{}, repos: map[string]uuid.UUID{}}
	if err := writeDoneWithoutCompletedAtRows(t, github, gitHubWorkItemsInsert,
		doneWithoutCompletedAtRow("github", "gh:acme/api#1", "done", nil),
		doneWithoutCompletedAtRow("github", "gh:acme/api#2", "done", nil),
		doneWithoutCompletedAtRow("github", "gh:acme/api#3", "done", &completed),
		doneWithoutCompletedAtRow("github", "gh:acme/api#4", "canceled", nil),
		doneWithoutCompletedAtRow("github", "gh:acme/api#5", "in_progress", nil),
	); err != nil {
		t.Fatal(err)
	}
	linear := &heldVersionConn{held: map[string]*time.Time{}, repos: map[string]uuid.UUID{}}
	if err := writeDoneWithoutCompletedAtRows(t, linear, linearWorkItemsInsert,
		doneWithoutCompletedAtRow("linear", "lin:FX-1", "done", nil),
		doneWithoutCompletedAtRow("linear", "lin:FX-2", "done", &completed),
	); err != nil {
		t.Fatal(err)
	}
	if len(github.appended) != 5 || len(linear.appended) != 2 {
		t.Fatalf("stored %d github and %d linear rows, want 5 and 2", len(github.appended), len(linear.appended))
	}
	got := doneWithoutCompletedAtCounts(t, reader)
	if len(got) != 2 || got["github"] != 2 || got["linear"] != 1 {
		t.Fatalf("%s by provider = %v, want github 2 and linear 1", doneWithoutCompletedAtMetric, got)
	}
}
