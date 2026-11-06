package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

const (
	linkOrg   = "org-13085"
	linkRepoA = "11111111-1111-1111-1111-111111111111"
)

type linkKeyRows struct {
	paths []string
	ids   [][]string
	i     int
}

func (r *linkKeyRows) Next() bool { r.i++; return r.i <= len(r.paths) }
func (r *linkKeyRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.paths[r.i-1]
	*(dest[1].(*[]string)) = r.ids[r.i-1]
	return nil
}
func (r *linkKeyRows) Err() error   { return nil }
func (r *linkKeyRows) Close() error { return nil }

type linkKeyQuerier map[string][]string

func (q linkKeyQuerier) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	out := &linkKeyRows{}
	for _, b := range bindings {
		if b.Name != teamscope.BindingCeilingKeys {
			continue
		}
		for _, key := range b.Value.([]string) {
			out.paths, out.ids = append(out.paths, key), append(out.ids, q[key])
		}
	}
	return out, nil
}

func linkCeiling(t *testing.T, id string) teamscope.Ceiling {
	t.Helper()
	ceiling, err := teamscope.ResolveCeiling(context.Background(), linkKeyQuerier{"acme/a": {id}}, pepgate.Allowlist(linkOrg, []string{"acme/a"}))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling: restricted=%t err=%v", ceiling.Restricted(), err)
	}
	return ceiling
}

// C1/C2/C7/F-6: the person's issues are read through the link, and the repositories an item names are cut to the
// allowed ones: the link read of the names carries the ceiling.
func TestDrilldownIssuesOfAnAllowlistCallerAreLinkedAndTheirRepoNamesCut(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	var seen []string
	var nameBindings []dhclickhouse.Binding
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		seen = append(seen, query)
		switch {
		case strings.Contains(query, "FROM work_item_cycle_times"):
			return &issuesRowScanner{rows: [][]any{{"wi-1", "github", "done", nil, nil, nil, nil, nil}}}, nil
		case strings.Contains(query, "AS repo_name"):
			nameBindings = bindings
			return &pairRowScanner{rows: [][2]string{{"wi-1", "acme/a"}}}, nil
		}
		return &pairRowScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildDrilldownIssuesResponse(context.Background(), reader, linkOrg, DrilldownIssuesParams{
		PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0), Ceiling: linkCeiling(t, linkRepoA),
	})
	if err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items = %v, want wi-1", got.Items)
	}
	for _, query := range seen {
		if strings.Contains(query, "FROM work_item_cycle_times") &&
			(!strings.Contains(query, "FROM work_graph_issue_pr FINAL") || !strings.Contains(query, "{link_repo_ids:Array(UUID)}")) {
			t.Fatalf("the cycle-time read is not narrowed by the link:\n%s", query)
		}
		if strings.Contains(query, "AS repo_name") && !strings.Contains(query, "{ceiling_ids:Array(String)}") {
			t.Fatalf("the repository names are not cut to the ceiling:\n%s", query)
		}
	}
	var ids []string
	for _, b := range nameBindings {
		if b.Name == teamscope.BindingCeilingIDs {
			ids, _ = b.Value.([]string)
		}
	}
	if len(ids) != 1 || ids[0] != linkRepoA {
		t.Fatalf("ceiling_ids of the name read = %v, want [%s]", ids, linkRepoA)
	}
}

// An Organization or Legacy scope reads today's statement.
func TestDrilldownIssuesWithoutACeilingCarryNoLink(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := personIdentityDispatchClient{identity: "alice@example.com", t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "link_repo_ids") || strings.Contains(query, "ceiling_ids") {
			t.Fatalf("an unrestricted statement carries a link or ceiling:\n%s", query)
		}
		if strings.Contains(query, "FROM work_item_cycle_times") {
			return &issuesRowScanner{rows: [][]any{{"wi-1", "github", "done", nil, nil, nil, nil, nil}}}, nil
		}
		return &pairRowScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildDrilldownIssuesResponse(context.Background(), reader, linkOrg, DrilldownIssuesParams{PersonID: "anyone", RangeDays: 14, Limit: 50, Now: dt(2024, 6, 15, 0, 0, 0)}); err != nil {
		t.Fatalf("BuildDrilldownIssuesResponse: %v", err)
	}
}
