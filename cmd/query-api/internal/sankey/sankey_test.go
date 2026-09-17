package sankey

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// dispatchClient answers system.tables/system.columns probes with every
// requested name present, then dispatches everything else to handler.
func dispatchClient(t *testing.T, handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)) fakeQueryClient {
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return allTablesColumnsPresent(t, query, bindings, handler)
	}}
}

func TestBuildResponseUnknownModeIsError(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("no data query expected for an unknown mode, got:\n%s", query)
		return nil, nil
	})
	_, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "not-a-mode"})
	if err == nil {
		t.Fatal("BuildResponse: want error for unknown mode, got nil")
	}
}

func TestBuildResponseHotpotTypoNormalizesToHotspot(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	})
	resp, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "hotpot", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if resp.Mode != "hotspot" {
		t.Fatalf("Mode = %q, want hotspot", resp.Mode)
	}
}

func TestBuildResponseMissingTableIsEmptyNotError(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM system.tables") {
			return &fixtureRowScanner{rows: nil}, nil // nothing present
		}
		t.Fatalf("no data query expected once the table check fails, got:\n%s", query)
		return nil, nil
	}}
	resp, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "investment", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if len(resp.Nodes) != 0 || len(resp.Links) != 0 {
		t.Fatalf("Nodes/Links = %+v/%+v, want both empty", resp.Nodes, resp.Links)
	}
}

// TestBuildInvestmentFlow replays a small investment result: two theme->
// repo edges, one target unresolved (empty repo, falls back to "Other").
func TestBuildInvestmentFlow(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "ARRAY JOIN CAST(work_unit_investments.theme_distribution_json") {
			return &fixtureRowScanner{rows: [][]any{
				{"planned", "repoA", 12.5},
				{"unplanned", "", 3.0},
				{"planned", "repoA", 0.0}, // zero value dropped
			}}, nil
		}
		t.Fatalf("unexpected query for investment fixture:\n%s", query)
		return nil, nil
	})

	nodes, links, err := buildInvestmentFlow(context.Background(), client, day(2024, 1, 1), day(2024, 1, 31), "org", nil, nil, nil, "org-1")
	if err != nil {
		t.Fatalf("buildInvestmentFlow: %v", err)
	}
	wantNodes := []Node{
		{Name: "planned", Group: strPtr("initiative")},
		{Name: "repoA", Group: strPtr("project")},
		{Name: "unplanned", Group: strPtr("initiative")},
		{Name: "Other", Group: strPtr("project")},
	}
	if !reflect.DeepEqual(nodes, wantNodes) {
		t.Fatalf("Nodes = %+v, want %+v", nodes, wantNodes)
	}
	wantLinks := []Link{
		{Source: "planned", Target: "repoA", Value: 12.5},
		{Source: "unplanned", Target: "Other", Value: 3.0},
	}
	if !reflect.DeepEqual(links, wantLinks) {
		t.Fatalf("Links = %+v, want %+v", links, wantLinks)
	}
}

// TestBuildExpenseFlow pins the unplanned/rework/abandoned clamp math
// (services/sankey.py:368-370).
func TestBuildExpenseFlow(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM work_item_metrics_daily FINAL"):
			return &fixtureRowScanner{rows: [][]any{{10.0, 8.0, 5.0}}}, nil // new_items, new_bugs=8, bug_completed_estimate=5
		case strings.Contains(query, "FROM work_item_cycle_times FINAL"):
			return &fixtureRowScanner{rows: [][]any{{2.0}}}, nil // canceled_items
		default:
			t.Fatalf("unexpected query for expense fixture:\n%s", query)
			return nil, nil
		}
	})

	nodes, links, err := buildExpenseFlow(context.Background(), client, day(2024, 1, 1), day(2024, 1, 31), "org", nil, nil, "org-1")
	if err != nil {
		t.Fatalf("buildExpenseFlow: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("Nodes = %+v, want 4 nodes always touched", nodes)
	}
	// unplanned = max(0, 8) = 8; rework = max(0, min(8, 5)) = 5;
	// abandoned = max(0, min(5, 2)) = 2.
	wantLinks := []Link{
		{Source: "Planned work", Target: "Unplanned work", Value: 8},
		{Source: "Unplanned work", Target: "Rework", Value: 5},
		{Source: "Rework", Target: "Abandonment / rewrite", Value: 2},
	}
	if !reflect.DeepEqual(links, wantLinks) {
		t.Fatalf("Links = %+v, want %+v", links, wantLinks)
	}
}

// TestBuildExpenseFlowNoRows pins the `if not rows: return [], []`
// short-circuit (services/sankey.py:350-351).
func TestBuildExpenseFlowNoRows(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_metrics_daily FINAL") {
			return &fixtureRowScanner{rows: nil}, nil
		}
		t.Fatalf("unexpected query (abandoned fetch must not run when counts are empty):\n%s", query)
		return nil, nil
	})
	nodes, links, err := buildExpenseFlow(context.Background(), client, day(2024, 1, 1), day(2024, 1, 31), "org", nil, nil, "org-1")
	if err != nil {
		t.Fatalf("buildExpenseFlow: %v", err)
	}
	if len(nodes) != 0 || len(links) != 0 {
		t.Fatalf("Nodes/Links = %+v/%+v, want both empty", nodes, links)
	}
}

// TestBuildStateFlow pins the state-machine clamp math (services/
// sankey.py:454-461) and that node touch order follows the query's own
// items_touched-descending order, not alphabetical.
func TestBuildStateFlow(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_state_durations_daily FINAL") {
			return &fixtureRowScanner{rows: [][]any{
				{"in_progress", 20.0},
				{"done", 15.0},
				{"todo", 10.0},
				{"backlog", 5.0},
				{"blocked", 3.0},
				{"in_review", 12.0},
			}}, nil
		}
		t.Fatalf("unexpected query for state fixture:\n%s", query)
		return nil, nil
	})

	nodes, links, err := buildStateFlow(context.Background(), client, day(2024, 1, 1), day(2024, 1, 31), "org", nil, nil, "org-1")
	if err != nil {
		t.Fatalf("buildStateFlow: %v", err)
	}
	wantOrder := []string{"In Progress", "Done", "Todo", "Backlog", "Blocked", "In Review"}
	gotOrder := make([]string, len(nodes))
	for i, n := range nodes {
		gotOrder[i] = n.Name
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("node order = %v, want %v (query's own items_touched-descending order)", gotOrder, wantOrder)
	}
	// flow_backlog=min(5,10)=5; flow_todo=min(10,20)=10;
	// blocked_flow=min(20,3)=3; remaining=20-3=17;
	// review_flow=min(17,12)=12; remaining=17-12=5;
	// canceled_flow=min(5,0)=0 (dropped, canceled absent);
	// done_flow=min(12,15)=12.
	byPair := map[[2]string]float64{}
	for _, l := range links {
		byPair[[2]string{l.Source, l.Target}] = l.Value
	}
	checks := []struct {
		source, target string
		value          float64
	}{
		{"Backlog", "Todo", 5},
		{"Todo", "In Progress", 10},
		{"In Progress", "Blocked", 3},
		{"In Progress", "In Review", 12},
		{"In Review", "Done", 12},
	}
	for _, c := range checks {
		got, ok := byPair[[2]string{c.source, c.target}]
		if !ok || got != c.value {
			t.Fatalf("link %s->%s = %v (present=%v), want %v", c.source, c.target, got, ok, c.value)
		}
	}
	if _, ok := byPair[[2]string{"In Progress", "Canceled"}]; ok {
		t.Fatalf("In Progress->Canceled link present, want dropped (zero value)")
	}
	if len(links) != 5 {
		t.Fatalf("len(links) = %d, want 5 (canceled_flow dropped)", len(links))
	}
}

// TestBuildHotspotFlow pins the repo/directory/file/change_type chain and
// the "(root)" directory fallback for a top-level file.
func TestBuildHotspotFlow(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM file_metrics_daily AS metrics FINAL") {
			return &fixtureRowScanner{rows: [][]any{
				{"repoA", "(root)", "README.md", "feature", 4.0},
				{"repoA", "src", "src/main.go", "refactor", 40.0},
			}}, nil
		}
		t.Fatalf("unexpected query for hotspot fixture:\n%s", query)
		return nil, nil
	})

	nodes, links, err := buildHotspotFlow(context.Background(), client, day(2024, 1, 1), day(2024, 1, 31), "org", nil, nil, "org-1")
	if err != nil {
		t.Fatalf("buildHotspotFlow: %v", err)
	}
	if len(nodes) != 7 { // repoA, repoA/(root), repoA/README.md, feature, repoA/src, repoA/src/main.go, refactor
		t.Fatalf("Nodes = %+v, want 7", nodes)
	}
	if len(links) != 6 {
		t.Fatalf("Links = %+v, want 6", links)
	}
	byPair := map[[2]string]float64{}
	for _, l := range links {
		byPair[[2]string{l.Source, l.Target}] = l.Value
	}
	if v := byPair[[2]string{"repoA", "repoA / (root)"}]; v != 4.0 {
		t.Fatalf("repoA->repoA / (root) = %v, want 4.0", v)
	}
	if v := byPair[[2]string{"repoA / (root)", "repoA / README.md"}]; v != 4.0 {
		t.Fatalf("repoA / (root)->repoA / README.md = %v, want 4.0", v)
	}
	if v := byPair[[2]string{"repoA / README.md", "feature"}]; v != 4.0 {
		t.Fatalf("repoA / README.md->feature = %v, want 4.0", v)
	}
}
