package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLinearProjectEnsureChangesRow(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	row := linearEnsureProjectsRow("org", "p1", "Alpha", now)
	same := storedEnsureProject{
		name: "Alpha", isActive: row.IsActive, state: row.State, stateSource: row.StateSource, updatedAt: linearProjectsEpoch,
	}
	for name, tc := range map[string]struct {
		mutate func(*storedEnsureProject)
		want   bool
	}{
		"identical content, same version":    {func(*storedEnsureProject) {}, false},
		"stored version is newer":            {func(s *storedEnsureProject) { s.updatedAt = now; s.name = "Other" }, false},
		"stored version is older, changed":   {func(s *storedEnsureProject) { s.updatedAt = linearProjectsEpoch.Add(-time.Second); s.name = "Other" }, true},
		"stored version is older, identical": {func(s *storedEnsureProject) { s.updatedAt = linearProjectsEpoch.Add(-time.Second) }, false},
		"stored version is newer, identical": {func(s *storedEnsureProject) { s.updatedAt = now }, false},
		"same version, name changed":         {func(s *storedEnsureProject) { s.name = "" }, true},
		"same version, state changed":        {func(s *storedEnsureProject) { s.state = "completed" }, true},
		"same version, state source":         {func(s *storedEnsureProject) { s.stateSource = "x" }, true},
		"same version, active flag":          {func(s *storedEnsureProject) { s.isActive ^= 1 }, true},
		"stored older, identical, target stated": {func(s *storedEnsureProject) {
			s.updatedAt = linearProjectsEpoch.Add(-time.Second)
			s.targetState = "stated"
		}, true},
		"stored older, identical, target held": {func(s *storedEnsureProject) {
			s.updatedAt = linearProjectsEpoch.Add(-time.Second)
			s.targetState = "held"
		}, false},
		"stored newer, identical, target stated": {func(s *storedEnsureProject) { s.updatedAt = now; s.targetState = "stated" }, false},
		"same version, project key":              {func(s *storedEnsureProject) { s.key = "KEY" }, true},
	} {
		stored := same
		tc.mutate(&stored)
		if got := projectEnsureChangesRow(row, stored); got != tc.want {
			t.Errorf("%s: changes = %v, want %v", name, got, tc.want)
		}
	}
}

type failingEnsureQuerier struct{}

func (failingEnsureQuerier) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("clickhouse down")
}

func TestLinearProjectEnsureKeepsEveryRowWhenTheStoredRowsAreUnreadable(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	rows := []projectmembership.CatalogRow{
		linearEnsureProjectsRow("org", "p1", "A", now), linearEnsureProjectsRow("org", "p2", "B", now),
	}
	keep := projectEnsureKeep(context.Background(), failingEnsureQuerier{}, "linear", "org", rows)
	if len(keep) != 2 || !keep[0] || !keep[1] {
		t.Fatalf("keep = %v, want both rows kept", keep)
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var got int64
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != projectEnsureName {
				continue
			}
			sum, _ := metric.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				if value, ok := point.Attributes.Value("outcome"); ok && value.AsString() == "read_error" {
					got += point.Value
				}
			}
		}
	}
	if got != 2 {
		t.Fatalf("read_error count = %d, want 2", got)
	}
}
