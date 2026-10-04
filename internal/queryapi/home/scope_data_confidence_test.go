package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

func TestBuildScopeDataConfidenceDistinguishesEmptyScopeAndPartialCoverage(t *testing.T) {
	ingested := time.Date(2026, 10, 4, 18, 0, 0, 0, time.UTC)

	t.Run("empty scope does not report a made-up zero coverage value", func(t *testing.T) {
		got := buildScopeDataConfidence(0, 0, nil)
		if got.Level != "low" {
			t.Fatalf("level = %q, want low", got.Level)
		}
		if got.CoveragePct != nil {
			t.Fatalf("coverage = %v, want nil when no repository denominator exists", *got.CoveragePct)
		}
		if got.LastIngestedAt != nil {
			t.Fatalf("last ingested = %v, want nil", got.LastIngestedAt)
		}
		if len(got.Caveats) != 1 || !strings.Contains(got.Caveats[0], "no repositories") {
			t.Fatalf("caveats = %#v, want empty-scope explanation", got.Caveats)
		}
	})

	t.Run("partial metrics remain a warning", func(t *testing.T) {
		got := buildScopeDataConfidence(4, 2, &ingested)
		if got.Level != "medium" {
			t.Fatalf("level = %q, want medium", got.Level)
		}
		if got.CoveragePct == nil || *got.CoveragePct != 50 {
			t.Fatalf("coverage = %v, want 50", got.CoveragePct)
		}
		if got.LastIngestedAt == nil || !time.Time(*got.LastIngestedAt).Equal(ingested) {
			t.Fatalf("last ingested = %v, want %s", got.LastIngestedAt, ingested)
		}
		if len(got.Caveats) != 1 || !strings.Contains(got.Caveats[0], "partial") {
			t.Fatalf("caveats = %#v, want partial-coverage explanation", got.Caveats)
		}
	})

	t.Run("no ingested metrics stays low", func(t *testing.T) {
		got := buildScopeDataConfidence(4, 0, nil)
		if got.Level != "low" {
			t.Fatalf("level = %q, want low", got.Level)
		}
		if got.CoveragePct == nil || *got.CoveragePct != 0 {
			t.Fatalf("coverage = %v, want measured zero", got.CoveragePct)
		}
		if len(got.Caveats) != 1 || !strings.Contains(got.Caveats[0], "No repository metrics") {
			t.Fatalf("caveats = %#v, want no-ingestion explanation", got.Caveats)
		}
	})
}

func TestFetchScopeDataConfidenceUsesRepositoryOwnershipAndWindow(t *testing.T) {
	var query string
	var bindings []dhclickhouse.Binding
	client := fakeQueryClient{t: t, handler: func(t *testing.T, gotQuery string, gotBindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		query = gotQuery
		bindings = gotBindings
		return &fixtureRowScanner{rows: [][]any{{int64(4), int64(2), time.Date(2026, 10, 4, 18, 0, 0, 0, time.UTC)}}}, nil
	}}
	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-a"}}}
	got, err := fetchScopeDataConfidence(context.Background(), client, f, day(2026, 9, 27), day(2026, 10, 4), "org-1", teamScopeAsOf)
	if err != nil {
		t.Fatalf("fetchScopeDataConfidence: %v", err)
	}
	if got.Level != "medium" || got.CoveragePct == nil || *got.CoveragePct != 50 {
		t.Fatalf("confidence = %#v, want medium with 50%% coverage", got)
	}
	for _, marker := range []string{"FROM repos FINAL AS r", "FROM repo_metrics_daily FINAL", teamscope.Marker, "day >= {start_day:Date}", "day < {end_day:Date}"} {
		if !strings.Contains(query, marker) {
			t.Fatalf("scope confidence query missing %q", marker)
		}
	}
	if !strings.Contains(query, "WHERE r.org_id = {org_id:String}") {
		t.Fatalf("scope confidence query does not scope repositories by org")
	}
	assertOrgIDSameDepthAsMarker(t, query, "FROM repo_metrics_daily FINAL")
	value, ok := bindingValue(bindings, teamscope.BindingTeamIDs)
	if !ok || fmt.Sprint(value) != "[team-a]" {
		t.Fatalf("team ownership binding = %v, want [team-a]", value)
	}
}
