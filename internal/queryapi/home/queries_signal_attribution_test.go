package home

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

func TestFetchSignalAttributionServesCurrentPrimaryDistribution(t *testing.T) {
	var captured string
	var capturedBindings []dhclickhouse.Binding
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		capturedBindings = bindings
		return &fixtureRowScanner{rows: [][]any{
			{"native_team", "high", int64(2)},
			{"linked_issue", "medium", int64(1)},
			{"unassigned", "none", int64(1)},
		}}, nil
	}}

	asOf := time.Date(2026, 10, 4, 20, 0, 0, 0, time.UTC)
	got, err := fetchSignalAttribution(
		context.Background(), client,
		Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-a"}}},
		day(2026, 9, 1), day(2026, 10, 1), "org-a", asOf,
	)
	if err != nil {
		t.Fatalf("fetchSignalAttribution: %v", err)
	}
	want := &SignalAttribution{
		Items: 4,
		Sources: []SignalAttributionSourceCount{
			{Source: "linked_issue", Items: 1, Share: 0.25},
			{Source: "native_team", Items: 2, Share: 0.5},
			{Source: "unassigned", Items: 1, Share: 0.25},
		},
		Confidence: []SignalAttributionConfidenceCount{
			{Confidence: "high", Items: 2, Share: 0.5},
			{Confidence: "medium", Items: 1, Share: 0.25},
			{Confidence: "none", Items: 1, Share: 0.25},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fetchSignalAttribution = %+v, want %+v", got, want)
	}

	for _, marker := range []string{
		"FROM work_item_cycle_times AS wct FINAL",
		"INNER JOIN work_items AS wi FINAL",
		"FROM work_item_team_attributions FINAL",
		teamscope.Marker,
		"GROUP BY provider, work_item_id",
		"uniqExact(tuple(wct.provider, wct.work_item_id))",
	} {
		if !strings.Contains(captured, marker) {
			t.Fatalf("signal attribution query missing %q:\n%s", marker, captured)
		}
	}
	if strings.Contains(captured, "wct.team_id IN") {
		t.Fatalf("signal attribution must resolve team scope through repository ownership, not wct.team_id:\n%s", captured)
	}
	if !strings.Contains(captured, "wi.provider = wct.provider") || !strings.Contains(captured, "a.provider = wct.provider") {
		t.Fatalf("signal attribution must join work items and attributions by provider as well as work-item id:\n%s", captured)
	}
	assertSignalAttributionOrgScope(t, captured, "work_item_cycle_times AS wct FINAL", "wct.org_id = {org_id:String}")
	assertSignalAttributionOrgScope(t, captured, "work_items AS wi FINAL", "wi.org_id = {org_id:String}")
	assertSignalAttributionOrgScope(t, captured, "work_item_team_attributions FINAL", "org_id = {org_id:String}")
	if value, ok := bindingValue(capturedBindings, teamscope.BindingTeamIDs); !ok || !reflect.DeepEqual(value, []string{"team-a"}) {
		t.Fatalf("team ownership binding = %v, want [team-a]; all bindings: %+v", value, capturedBindings)
	}
}

func assertSignalAttributionOrgScope(t *testing.T, query, marker, predicate string) {
	t.Helper()
	depths := sqlshape.Depths(query)
	markerIndex := strings.Index(query, marker)
	if markerIndex < 0 {
		t.Fatalf("query missing %q:\n%s", marker, query)
	}
	predicateOffset := strings.Index(query[markerIndex+len(marker):], predicate)
	if predicateOffset < 0 {
		t.Fatalf("query missing %q after %q:\n%s", predicate, marker, query)
	}
	predicateIndex := markerIndex + len(marker) + predicateOffset
	if depths[markerIndex] != depths[predicateIndex] {
		t.Fatalf("%q is at nesting depth %d but %q is at depth %d; the org predicate must scope the same read:\n%s", marker, depths[markerIndex], predicate, depths[predicateIndex], query)
	}
}

func TestFetchSignalAttributionKeepsNoAttributionDistinctFromUnassigned(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	}}
	got, err := fetchSignalAttribution(context.Background(), client, Filters{}, day(2026, 9, 1), day(2026, 10, 1), "org-a", day(2026, 10, 4))
	if err != nil {
		t.Fatalf("fetchSignalAttribution: %v", err)
	}
	if got != nil {
		t.Fatalf("fetchSignalAttribution = %+v, want nil when no current attribution exists", got)
	}
}

func TestFetchSignalAttributionReturnsReadFailure(t *testing.T) {
	boom := errors.New("clickhouse: connection lost")
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, boom
	}}
	_, err := fetchSignalAttribution(context.Background(), client, Filters{}, day(2026, 9, 1), day(2026, 10, 1), "org-a", day(2026, 10, 4))
	if !errors.Is(err, boom) {
		t.Fatalf("fetchSignalAttribution error = %v, want wrapped read failure", err)
	}
}

func TestAttachSignalAttributionTargetsOnlyWorkItemMetrics(t *testing.T) {
	attribution := &SignalAttribution{Items: 1}
	signals := []Signal{
		{ID: "metric:cycle_time", Metric: "cycle_time"},
		{ID: "metric:throughput", Metric: "throughput"},
		{ID: "metric:wip_saturation", Metric: "wip_saturation"},
		{ID: "metric:blocked_work", Metric: "blocked_work"},
		{ID: "metric:review_latency", Metric: "review_latency"},
		{ID: "recommendation:1", Metric: "recommendation"},
		{ID: "risk:repo:1", Metric: "compounding_risk"},
	}
	got := AttachSignalAttribution(signals, attribution)
	for i := range got {
		want := i < 4
		if (got[i].Attribution != nil) != want {
			t.Fatalf("signal %q attribution = %v, want attached=%t", got[i].ID, got[i].Attribution, want)
		}
	}
}

func TestBuildResponseAttachesCurrentPrimaryAttributionOnlyToWorkItemSignals(t *testing.T) {
	base := orgGoldenHandler(t)
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_team_attributions FINAL") && strings.Contains(query, "GROUP BY a.source, a.confidence") {
			return &fixtureRowScanner{rows: [][]any{{"native_team", "high", int64(3)}}}, nil
		}
		return base(t, query, bindings)
	}}
	filters, now := signalReadFilters("org")
	response, err := BuildResponse(context.Background(), client, nil, "org-1", filters, now)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	seenWorkItem := 0
	for _, signal := range response.Signals {
		if isWorkItemMetric(signal.Metric) {
			seenWorkItem++
			if signal.Attribution == nil || signal.Attribution.Items != 3 {
				t.Fatalf("work-item signal %q attribution = %+v, want current primary distribution", signal.ID, signal.Attribution)
			}
			continue
		}
		if signal.Attribution != nil {
			t.Fatalf("non-work-item signal %q attribution = %+v, want nil", signal.ID, signal.Attribution)
		}
	}
	if seenWorkItem == 0 {
		t.Fatal("fixture has no work-item Home signals; the attachment assertion measured nothing")
	}
}

func TestHasCurrentWorkItemMetricData(t *testing.T) {
	if hasCurrentWorkItemMetricData([]MetricDelta{{Metric: "review_latency", HasData: true}, {Metric: "cycle_time", HasData: false}}) {
		t.Fatal("repository data or a prior-only work-item metric must not trigger an attribution read")
	}
	if !hasCurrentWorkItemMetricData([]MetricDelta{{Metric: "blocked_work", HasData: true}}) {
		t.Fatal("a current work-item metric must trigger the attribution read")
	}
}
