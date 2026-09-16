package heatmap

import (
	"context"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

// Fixture values and expected shapes below were cross-checked against a
// live run of the real Python build_heatmap_response (monkeypatched
// ClickHouse readers, see this PR's TEST-EVIDENCE for the exact
// uncommitted one-off command) for: the fixed 00..23 hour axis
// regardless of data, the weekday axis filtered to only labels present
// in the data (falling back to the full Mon..Sun list only when none
// match), an out-of-range weekday/hour falling back to Python's plain
// str(value) form, the day/week axis's plain ISO sort, the default
// (repo/file) axis's sort-by-summed-value-descending, that a cell can
// name an axis label the axes list itself omits, and the hotspot_risk
// branch's evidence fetch running (and its error propagating) while the
// rows it returns are discarded from the response. Evidence datetime
// fields are the one deliberate divergence from that capture (a declared
// Python-plane defect: Go emits RFC 3339, Python a naive isoformat) --
// expectations below use Go's own canonical time.Time values, not the
// captured naive strings.

// TestBuildResponseReviewWaitDensity replays a review_wait_density
// request at org scope (no repo resolution needed) with a valid x/y
// evidence request, including one out-of-range weekday row.
func TestBuildResponseReviewWaitDensity(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "toDayOfWeek(created_at)") && strings.Contains(query, "GROUP BY weekday, hour"):
			return &fixtureRowScanner{rows: [][]any{
				{int32(3), int32(9), 2.5},
				{int32(3), int32(14), 1.0},
				{int32(8), int32(25), 99.0},
			}}, nil
		case strings.Contains(query, "toDayOfWeek(created_at) = {weekday:UInt8}"):
			return &fixtureRowScanner{rows: [][]any{
				{"12345678-1234-5678-1234-567812345678", uint32(42), "Add retry logic", datetime(2024, 1, 10, 9, 0, 0), datetime(2024, 1, 10, 14, 0, 0)},
				{"87654321-4321-8765-4321-876543218765", uint32(7), nil, datetime(2024, 1, 10, 9, 30, 0), datetime(2024, 1, 10, 9, 45, 0)},
			}}, nil
		default:
			t.Fatalf("unexpected query for review_wait_density fixture:\n%s", query)
			return nil, nil
		}
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "temporal_load", Metric: "review_wait_density", ScopeType: "org",
		RangeDays: 14, X: "09", Y: "Wed", Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}

	wantXAxis := hourLabels()
	if !reflect.DeepEqual(got.Axes.X, wantXAxis) {
		t.Fatalf("Axes.X = %v, want the fixed 00..23 hour list", got.Axes.X)
	}
	if want := []string{"Wed"}; !reflect.DeepEqual(got.Axes.Y, want) {
		t.Fatalf("Axes.Y = %v, want %v", got.Axes.Y, want)
	}
	wantCells := []Cell{
		{X: "09", Y: "Wed", Value: 2.5},
		{X: "14", Y: "Wed", Value: 1.0},
		{X: "25", Y: "8", Value: 99.0}, // out-of-range weekday=8 falls back to str(8)="8"
	}
	if !reflect.DeepEqual(got.Cells, wantCells) {
		t.Fatalf("Cells = %+v, want %+v", got.Cells, wantCells)
	}
	if got.Legend != (Legend{Unit: "hours", Scale: "linear"}) {
		t.Fatalf("Legend = %+v", got.Legend)
	}
	evidence, ok := got.Evidence.([]ReviewWaitEvidenceItem)
	if !ok {
		t.Fatalf("Evidence type = %T, want []ReviewWaitEvidenceItem", got.Evidence)
	}
	if len(evidence) != 2 || evidence[0].Number != 42 || evidence[1].Title != nil {
		t.Fatalf("Evidence = %+v", evidence)
	}
}

// TestBuildResponseReviewWaitDensityNoEvidenceWithoutXY pins that
// evidence stays nil when x/y are absent.
func TestBuildResponseReviewWaitDensityNoEvidenceWithoutXY(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "GROUP BY weekday, hour") {
			return &fixtureRowScanner{rows: nil}, nil
		}
		t.Fatalf("unexpected query (no evidence fetch expected):\n%s", query)
		return nil, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "temporal_load", Metric: "review_wait_density", ScopeType: "org", RangeDays: 14, Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if got.Evidence != nil {
		t.Fatalf("Evidence = %v, want nil", got.Evidence)
	}
}

// TestBuildResponseRepoTouchpoints replays the two-query (top-N then
// series) shape, org scope.
func TestBuildResponseRepoTouchpoints(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "ORDER BY total DESC"):
			return &fixtureRowScanner{rows: [][]any{
				{"repoA", uint64(10)},
				{"repoB", uint64(5)},
			}}, nil
		case strings.Contains(query, "GROUP BY day, repo"):
			return &fixtureRowScanner{rows: [][]any{
				{day(2024, 1, 1), "repoA", 3.0},
				{day(2024, 1, 2), "repoA", 7.0},
				{day(2024, 1, 1), "repoB", 5.0},
			}}, nil
		default:
			t.Fatalf("unexpected query for repo_touchpoints fixture:\n%s", query)
			return nil, nil
		}
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "context_switch", Metric: "repo_touchpoints", ScopeType: "org", RangeDays: 14, Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if want := []string{"2024-01-01", "2024-01-02"}; !reflect.DeepEqual(got.Axes.X, want) {
		t.Fatalf("Axes.X = %v, want %v", got.Axes.X, want)
	}
	// repoA total=10.0 > repoB total=5.0 -> descending by total.
	if want := []string{"repoA", "repoB"}; !reflect.DeepEqual(got.Axes.Y, want) {
		t.Fatalf("Axes.Y = %v, want %v", got.Axes.Y, want)
	}
	if got.Evidence != nil {
		t.Fatalf("Evidence = %v, want nil (repo_touchpoints never sets it)", got.Evidence)
	}
}

// TestBuildResponseRepoTouchpointsNoRepos pins the top-query's own
// "return []" short-circuit when it finds no repos.
func TestBuildResponseRepoTouchpointsNoRepos(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "ORDER BY total DESC") {
			return &fixtureRowScanner{rows: nil}, nil
		}
		t.Fatalf("unexpected query (second query must not run when top-N is empty):\n%s", query)
		return nil, nil
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "context_switch", Metric: "repo_touchpoints", ScopeType: "org", RangeDays: 14, Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if len(got.Cells) != 0 {
		t.Fatalf("Cells = %+v, want empty", got.Cells)
	}
}

// TestBuildResponseHotspotRiskEvidenceDiscarded replays hotspot_risk with
// a valid x/y: the evidence fetch must run (its error must propagate),
// but the rows it returns are never attached to the response -- the
// genuine Python quirk this package's doc comment declares.
func TestBuildResponseHotspotRiskEvidenceDiscarded(t *testing.T) {
	evidenceCalls := 0
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "ORDER BY total DESC"):
			return &fixtureRowScanner{rows: [][]any{
				{"repoA:a.go", 14.0},
				{"repoB:b.go", 20.0},
			}}, nil
		case strings.Contains(query, "GROUP BY week, file_key"):
			return &fixtureRowScanner{rows: [][]any{
				{day(2024, 1, 1), "repoA:a.go", 5.0},
				{day(2024, 1, 8), "repoA:a.go", 9.0},
				{day(2024, 1, 1), "repoB:b.go", 20.0},
			}}, nil
		case strings.Contains(query, "concat(repos.repo, ':', path) = {file_key:String}"):
			evidenceCalls++
			return &fixtureRowScanner{rows: [][]any{
				{day(2024, 1, 1), "repoA", "a.go", int64(1), int64(2), int64(3), 5.0},
			}}, nil
		default:
			t.Fatalf("unexpected query for hotspot_risk fixture:\n%s", query)
			return nil, nil
		}
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "risk", Metric: "hotspot_risk", ScopeType: "org", RangeDays: 14,
		X: "2024-01-01", Y: "repoA:a.go", Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if evidenceCalls != 1 {
		t.Fatalf("hotspot evidence query ran %d times, want 1", evidenceCalls)
	}
	if got.Evidence != nil {
		t.Fatalf("Evidence = %v, want nil (hotspot_risk's own evidence result is discarded)", got.Evidence)
	}
	// repoB:b.go total=20.0 > repoA:a.go total=14.0 -> descending by total.
	if want := []string{"repoB:b.go", "repoA:a.go"}; !reflect.DeepEqual(got.Axes.Y, want) {
		t.Fatalf("Axes.Y = %v, want %v", got.Axes.Y, want)
	}
}

// TestBuildResponseHotspotRiskEvidenceErrorPropagates pins that a
// ClickHouse error from the discarded evidence fetch still fails the
// whole request.
func TestBuildResponseHotspotRiskEvidenceErrorPropagates(t *testing.T) {
	boom := &RequestError{Status: 503, Message: "boom"}
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "ORDER BY total DESC"):
			return &fixtureRowScanner{rows: [][]any{{"repoA:a.go", 14.0}}}, nil
		case strings.Contains(query, "GROUP BY week, file_key"):
			return &fixtureRowScanner{rows: [][]any{{day(2024, 1, 1), "repoA:a.go", 5.0}}}, nil
		case strings.Contains(query, "concat(repos.repo, ':', path) = {file_key:String}"):
			return nil, boom
		default:
			t.Fatalf("unexpected query:\n%s", query)
			return nil, nil
		}
	}}

	_, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "risk", Metric: "hotspot_risk", ScopeType: "org", RangeDays: 14,
		X: "2024-01-01", Y: "repoA:a.go", Limit: 50,
	})
	if err == nil {
		t.Fatal("BuildResponse: want error from discarded evidence fetch, got nil")
	}
}

// TestBuildResponseActiveHours replays the individual/developer scope
// branch: identity resolution, then the two identity-scoped reads.
func TestBuildResponseActiveHours(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "lower(hex(MD5(identity)))"):
			return &fixtureRowScanner{rows: [][]any{{"alice@example.com"}}}, nil
		case strings.Contains(query, "GROUP BY weekday, hour") && strings.Contains(query, "author_email IN"):
			return &fixtureRowScanner{rows: [][]any{{int32(2), int32(10), 4.0}}}, nil
		case strings.Contains(query, "toDayOfWeek(author_when) = {weekday:UInt8}"):
			return &fixtureRowScanner{rows: [][]any{
				{"repoA", "abc123", "fix bug", "Alice", "alice@example.com", datetime(2024, 1, 9, 10, 15, 0)},
			}}, nil
		default:
			t.Fatalf("unexpected query for active_hours fixture:\n%s", query)
			return nil, nil
		}
	}}

	got, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "individual", Metric: "active_hours", ScopeType: "developer",
		ScopeID: personIDFor("alice@example.com"), RangeDays: 14, X: "10", Y: "Tue", Limit: 50,
	})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if want := []string{"Tue"}; !reflect.DeepEqual(got.Axes.Y, want) {
		t.Fatalf("Axes.Y = %v, want %v", got.Axes.Y, want)
	}
	evidence, ok := got.Evidence.([]IndividualActiveEvidenceItem)
	if !ok || len(evidence) != 1 || evidence[0].CommitHash != "abc123" {
		t.Fatalf("Evidence = %+v (ok=%v)", got.Evidence, ok)
	}
}

// TestBuildResponseIndividualNotFound pins the 404 when identity
// resolution finds nothing at all (neither a live ClickHouse identity nor
// an alias-config match).
func TestBuildResponseIndividualNotFound(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "lower(hex(MD5(identity)))") {
			return &fixtureRowScanner{rows: nil}, nil
		}
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}}

	_, err := BuildResponse(context.Background(), client, "org-1", Params{
		Type: "individual", Metric: "active_hours", ScopeType: "developer",
		ScopeID: "not-a-real-person", RangeDays: 14, Limit: 50,
	})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 || reqErr.Message != "Individual not found" {
		t.Fatalf("err = %v, want 404 Individual not found", err)
	}
}

// TestBuildResponseUnknownMetricIs404 pins metricFor's own 404.
func TestBuildResponseUnknownMetricIs404(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("no query expected, got:\n%s", query)
		return nil, nil
	}}
	_, err := BuildResponse(context.Background(), client, "org-1", Params{Type: "temporal_load", Metric: "nope"})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 {
		t.Fatalf("err = %v, want 404", err)
	}
}

// personIDFor computes the same md5 digest resolveIdentityVariants uses,
// so this test can build a realistic scope_id for a fixture identity.
func personIDFor(identity string) string {
	return quadrant.PersonIDForIdentity(identity)
}
