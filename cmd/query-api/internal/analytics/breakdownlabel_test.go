package analytics

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/scopelabel"
)

const labelUUID = "0f9a1c52-8b7e-4c1d-9a3f-6d2b5e7a9c10"

// The expected labels were produced by running the Python
// _build_breakdown_item on the same key and label map.
func TestBreakdownLabel_MatchesPythonCells(t *testing.T) {
	cases := []struct {
		key    string
		labels map[string]string
		want   string // "" means nil
	}{
		{"acme/web", nil, "acme/web"},
		{"acme/web", map[string]string{"acme/web": "Web"}, "Web"},
		{labelUUID, nil, "#0f9a1c52"},
		{labelUUID, map[string]string{labelUUID: "Platform"}, "Platform"},
		{strings.ToUpper(labelUUID), nil, "#0F9A1C52"},
		{labelUUID, map[string]string{labelUUID: labelUUID}, "#0f9a1c52"},
		{"", nil, ""},
		{"theme-a", nil, "theme-a"},
		{"abc", nil, "abc"},
		{" " + labelUUID, nil, "# 0f9a1c5"},
		{"ab-cd-ef-01-23", nil, "ab-cd-ef-01-23"},
		{"héllo-wörld", nil, "héllo-wörld"},
	}
	for _, c := range cases {
		got := breakdownLabel(c.key, c.labels)
		if c.want == "" {
			if got != nil {
				t.Errorf("key %q: %q want nil", c.key, *got)
			}
			continue
		}
		if got == nil || *got != c.want {
			t.Errorf("key %q labels %v: got %v want %q", c.key, c.labels, got, c.want)
		}
	}
}

type sequenceClient struct {
	responses  []*fakeRowScanner
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *sequenceClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	i := len(c.statements) - 1
	if i >= len(c.responses) {
		return &fakeRowScanner{}, nil
	}
	return c.responses[i], nil
}

func TestExecuteBreakdown_ResolvesRepoAndTeamLabels(t *testing.T) {
	for _, tc := range []struct{ dim, table string }{{"REPO", "FROM repos\n"}, {"TEAM", "FROM teams\n"}, {"repo", "FROM repos\n"}} {
		c := &sequenceClient{responses: []*fakeRowScanner{
			{rows: [][]any{{labelUUID, 3.0}, {"unresolved-id", 2.0}, {"22222222-2222-4222-8222-222222222222", 1.0}}},
			{rows: [][]any{{labelUUID, "Platform"}, {"22222222-2222-4222-8222-222222222222", "22222222-2222-4222-8222-222222222222"}}},
		}}
		got, err := ExecuteBreakdown(context.Background(), c, "org-1", compiledQuery{sql: "SELECT 1"}, tc.dim, "COUNT")
		if err != nil {
			t.Fatal(err)
		}
		if len(c.statements) != 2 || !strings.Contains(c.statements[1], tc.table) || strings.Contains(c.statements[1], "FINAL") {
			t.Fatalf("%s: label statement %v", tc.dim, c.statements)
		}
		b := map[string]any{}
		for _, x := range c.bindings[1] {
			b[x.Name] = x.Value
		}
		if b["org_id"] != "org-1" {
			t.Errorf("%s: label lookup must be org-scoped: %v", tc.dim, b)
		}
		want := []string{"Platform", "unresolved-id", "#22222222"}
		for i, it := range got.Items {
			if it.Label == nil || *it.Label != want[i] {
				t.Errorf("%s item %d label %v want %s", tc.dim, i, it.Label, want[i])
			}
		}
	}
}

// Dimensions without a repo or team id read no lookup and label themselves.
func TestExecuteBreakdown_OtherDimensionsSkipTheLookup(t *testing.T) {
	c := &sequenceClient{responses: []*fakeRowScanner{{rows: [][]any{{"feature", 3.0}}}}}
	got, err := ExecuteBreakdown(context.Background(), c, "org-1", compiledQuery{sql: "SELECT 1"}, "THEME", "COUNT")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.statements) != 1 || got.Items[0].Label == nil || *got.Items[0].Label != "feature" {
		t.Fatalf("%d statements, %#v", len(c.statements), got.Items)
	}
}

func TestScopeLabelOptions_FinalFlagShapesTheStatement(t *testing.T) {
	for _, final := range []bool{false, true} {
		c := &sequenceClient{}
		_ = scopelabel.Resolve(context.Background(), c, "o", "repo", []string{"a"}, scopelabel.Options{Final: final})
		if got := strings.Contains(c.statements[0], "FROM repos FINAL"); got != final {
			t.Errorf("final=%v statement %s", final, c.statements[0])
		}
	}
}
