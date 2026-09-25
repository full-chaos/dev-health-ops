package aggflame

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestFiltersKeepPythonInsertionOrder pins meta.filters to the order
// aggregated_flame.py fills filters_used in, per mode. The live dict-order
// oracle cannot cover cycle_breakdown: no ClickHouse migration creates
// work_item_cycle_milestones_daily, so both sides answer 503 there.
func TestFiltersKeepPythonInsertionOrder(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{}, nil
	}}
	for _, tc := range []struct {
		params Params
		want   string
	}{
		{Params{Mode: "cycle_breakdown", TeamID: "t", Provider: "p", WorkScopeID: "w"}, `"filters":{"team_id":"t","provider":"p","work_scope_id":"w"}`},
		{Params{Mode: "code_hotspots", RepoID: "r"}, `"filters":{"repo_id":"r"}`},
		{Params{Mode: "throughput", TeamID: "t", RepoID: "r"}, `"filters":{"team_id":"t","repo_id":"r"}`},
	} {
		tc.params.StartDay, tc.params.EndDay, tc.params.Limit, tc.params.MinValue = day(2024, 3, 1), day(2024, 3, 5), 500, 1
		got, err := BuildResponse(context.Background(), client, "org-1", tc.params)
		if err != nil {
			t.Fatalf("%s: BuildResponse: %v", tc.params.Mode, err)
		}
		value, err := pyjson.FromGoModel(got)
		if err != nil {
			t.Fatal(err)
		}
		body, err := pyjson.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), tc.want) {
			t.Errorf("%s: body %s does not contain %s", tc.params.Mode, body, tc.want)
		}
	}
}
