package sankey

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// Golden fixtures under testdata/ were captured from a live run of the
// real Python build_sankey_response (ClickHouse readers monkeypatched to
// fixed rows via unittest.mock.patch.object, _tables_present/
// _columns_present forced true) -- the exact command:
//
//	.venv/bin/python -c "
//	import asyncio, json
//	from contextlib import asynccontextmanager
//	from unittest import mock
//	from dev_health_ops.api.models.filters import MetricFilter
//	from dev_health_ops.api.services import sankey as m
//
//	class _Sink:
//	    backend_type = 'clickhouse'
//
//	@asynccontextmanager
//	async def _client(_url):
//	    yield _Sink()
//
//	async def _true(*a, **k):
//	    return True
//
//	async def _items(_sink, **_k):
//	    return [
//	        {'source': 'planned', 'target': 'repoA', 'value': 12.5},
//	        {'source': 'planned', 'target': 'repoB', 'value': 4.0},
//	        {'source': 'unplanned', 'target': 'repoA', 'value': 2.0},
//	    ]
//
//	async def main():
//	    with mock.patch.object(m, '_tables_present', _true), \
//	         mock.patch.object(m, '_columns_present', _true), \
//	         mock.patch.object(m, 'fetch_investment_flow_items', _items), \
//	         mock.patch.object(m, 'clickhouse_client', _client):
//	        resp = await m.build_sankey_response(db_url='clickhouse://fake', mode='investment', filters=MetricFilter(), org_id='org-1')
//	    print(json.dumps(resp.model_dump(mode='json'), indent=2, sort_keys=True))
//
//	asyncio.run(main())
//	"
//
// -- run once per mode (expense/state/hotspot swap the mocked fetch_*
// function and its fixture rows for the ones named in each TestGolden*
// below), and the printed JSON committed verbatim as testdata/<mode>.json.
// DisallowUnknownFields makes a field-name mismatch (Python emitted a key
// this Go type does not declare) a hard test failure rather than a silent
// drop.
func loadGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// TestGoldenInvestment replays testdata/investment.json: two repos fed by
// two themes, byte-for-byte against the real Python builder's output.
func TestGoldenInvestment(t *testing.T) {
	want := loadGolden(t, "investment.json")
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "ARRAY JOIN CAST(work_unit_investments.theme_distribution_json") {
			return &fixtureRowScanner{rows: [][]any{
				{"planned", "repoA", 12.5},
				{"planned", "repoB", 4.0},
				{"unplanned", "repoA", 2.0},
			}}, nil
		}
		t.Fatalf("unexpected query for investment golden:\n%s", query)
		return nil, nil
	})
	got, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "investment", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	assertGoldenMatch(t, *got, want)
}

// TestGoldenExpense replays testdata/expense.json.
func TestGoldenExpense(t *testing.T) {
	want := loadGolden(t, "expense.json")
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM work_item_metrics_daily FINAL"):
			return &fixtureRowScanner{rows: [][]any{{10.0, 8.0, 5.0}}}, nil
		case strings.Contains(query, "FROM work_item_cycle_times FINAL"):
			return &fixtureRowScanner{rows: [][]any{{int64(2)}}}, nil
		default:
			t.Fatalf("unexpected query for expense golden:\n%s", query)
			return nil, nil
		}
	})
	got, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "expense", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	assertGoldenMatch(t, *got, want)
}

// TestGoldenState replays testdata/state.json.
func TestGoldenState(t *testing.T) {
	want := loadGolden(t, "state.json")
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
		t.Fatalf("unexpected query for state golden:\n%s", query)
		return nil, nil
	})
	got, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "state", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	assertGoldenMatch(t, *got, want)
}

// TestGoldenHotspot replays testdata/hotspot.json.
func TestGoldenHotspot(t *testing.T) {
	want := loadGolden(t, "hotspot.json")
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM file_metrics_daily FINAL AS metrics") {
			return &fixtureRowScanner{rows: [][]any{
				{"repoA", "(root)", "README.md", "feature", 4.0},
				{"repoA", "src", "src/main.go", "refactor", 40.0},
			}}, nil
		}
		t.Fatalf("unexpected query for hotspot golden:\n%s", query)
		return nil, nil
	})
	got, err := BuildResponse(context.Background(), client, "org-1", Params{Mode: "hotspot", StartDay: day(2024, 1, 1), EndDay: day(2024, 1, 31)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	assertGoldenMatch(t, *got, want)
}

func assertGoldenMatch(t *testing.T, got, want Response) {
	t.Helper()
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("response mismatch:\n--- got ---\n%s\n--- want (golden) ---\n%s", gotJSON, wantJSON)
	}
}
