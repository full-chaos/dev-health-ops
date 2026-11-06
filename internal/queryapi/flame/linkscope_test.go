package flame

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
	linkRepoB = "22222222-2222-2222-2222-222222222222"
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

func linkCeiling(t *testing.T, ids ...string) teamscope.Ceiling {
	t.Helper()
	keys, rows := make([]string, 0, len(ids)), linkKeyQuerier{}
	for i, id := range ids {
		key := "acme/r" + string(rune('a'+i))
		keys, rows[key] = append(keys, key), []string{id}
	}
	ceiling, err := teamscope.ResolveCeiling(context.Background(), rows, pepgate.Allowlist(linkOrg, keys))
	if err != nil || !ceiling.Restricted() {
		t.Fatalf("ceiling: restricted=%t err=%v", ceiling.Restricted(), err)
	}
	return ceiling
}

// linkIssueClient answers the issue flame of an allowlist caller: the cycle-time snapshot, the title, and the allowed
// repositories the item is linked to. It records every statement.
func linkIssueClient(t *testing.T, linked []string, seen *[]string) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		*seen = append(*seen, query)
		switch {
		case strings.Contains(query, "FROM work_item_cycle_times"):
			return &fixtureRowScanner{rows: [][]any{{"github", "bug", "done",
				day(2024, 1, 5, 8, 0, 0), day(2024, 1, 6, 9, 0, 0), day(2024, 1, 9, 17, 0, 0)}}}, nil
		case strings.Contains(query, "FROM work_items"):
			if strings.Contains(query, "repo_id") {
				t.Fatalf("an allowlist read takes a repository from work_items:\n%s", query)
			}
			return &fixtureRowScanner{rows: [][]any{{"Fix login"}}}, nil
		case strings.Contains(query, "FROM work_graph_issue_pr"):
			rows := make([][]any, 0, len(linked))
			for _, id := range linked {
				rows = append(rows, []any{id})
			}
			return &fixtureRowScanner{rows: rows}, nil
		}
		t.Fatalf("unexpected query: %s", query)
		return nil, nil
	}}
}

// C1/C2: the issue is read through the link to the allowed repository, in the same statement as the cycle-time read.
func TestIssueFlameOfAnAllowlistCallerIsReadThroughTheLink(t *testing.T) {
	var seen []string
	client := linkIssueClient(t, []string{linkRepoA}, &seen)
	got, err := BuildResponse(context.Background(), client, linkOrg, Params{EntityType: "issue", EntityID: "wi-1", Ceiling: linkCeiling(t, linkRepoA)})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	for _, query := range seen {
		if strings.Contains(query, "FROM work_item_cycle_times") &&
			(!strings.Contains(query, "FROM work_graph_issue_pr FINAL") || !strings.Contains(query, "{link_repo_ids:Array(UUID)}")) {
			t.Fatalf("the cycle-time read is not narrowed by the link:\n%s", query)
		}
	}
	if repo, _ := got.Entity.Get("repo_id"); repo != linkRepoA {
		t.Fatalf("repo_id = %#v, want the one allowed linked repository %s", repo, linkRepoA)
	}
}

// C7/Q7: the repository id is served only when exactly one allowed repository holds the item.
func TestIssueFlameOfAnAllowlistCallerServesNoRepositoryUnlessOneAllowedRepoHoldsIt(t *testing.T) {
	for name, linked := range map[string][]string{"none": nil, "two": {linkRepoA, linkRepoB}} {
		t.Run(name, func(t *testing.T) {
			var seen []string
			client := linkIssueClient(t, linked, &seen)
			got, err := BuildResponse(context.Background(), client, linkOrg, Params{EntityType: "issue", EntityID: "wi-1", Ceiling: linkCeiling(t, linkRepoA, linkRepoB)})
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if repo, _ := got.Entity.Get("repo_id"); repo != nil {
				t.Fatalf("repo_id = %#v, want null", repo)
			}
		})
	}
}

// An item that no allowed repository links is not found: the answer of an unknown id.
func TestIssueFlameOfAnUnlinkedItemIsNotFound(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_cycle_times") && !strings.Contains(query, "FROM work_graph_issue_pr FINAL") {
			t.Fatalf("an unlinked item is readable without the link:\n%s", query)
		}
		return &fixtureRowScanner{}, nil
	}}
	_, err := BuildResponse(context.Background(), client, linkOrg, Params{EntityType: "issue", EntityID: "wi-1", Ceiling: linkCeiling(t, linkRepoA)})
	if reqErr, ok := AsRequestError(err); !ok || reqErr.Status != 404 || reqErr.Message != "Issue not found" {
		t.Fatalf("err = %v, want 404 Issue not found", err)
	}
}
