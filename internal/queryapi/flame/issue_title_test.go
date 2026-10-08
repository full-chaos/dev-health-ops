package flame

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func issueTitleClient(t *testing.T, titles map[string]string) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			var org string
			for _, b := range bindings {
				if b.Name == "org_id" {
					org, _ = b.Value.(string)
				}
			}
			if title, ok := titles[org]; ok {
				return &fixtureRowScanner{rows: [][]any{{title}}}, nil
			}
			return &fixtureRowScanner{}, nil
		}
		if !strings.Contains(query, "FROM work_item_cycle_times FINAL") {
			t.Fatalf("unexpected query: %s", query)
		}
		return &fixtureRowScanner{rows: [][]any{{"github", "bug", "done",
			day(2024, 1, 5, 8, 0, 0), day(2024, 1, 6, 9, 0, 0), day(2024, 1, 9, 17, 0, 0)}}}, nil
	}}
}

func issueEntityTitle(t *testing.T, client fakeQueryClient, org string) any {
	t.Helper()
	got, err := BuildResponse(context.Background(), client, org, Params{EntityType: "issue", EntityID: "wi-1"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	title, ok := got.Entity.Get("title")
	if !ok {
		t.Fatal("issue entity has no title key")
	}
	return title
}

func TestIssueFlameEntityServesTheStoredTitle(t *testing.T) {
	title := issueEntityTitle(t, issueTitleClient(t, map[string]string{"org-1": "Fix login"}), "org-1")
	if s, ok := title.(string); !ok || s != "Fix login" {
		t.Fatalf("title = %#v, want Fix login", title)
	}
}

func TestIssueFlameEntityTitleIsNullWhenNoTitleIsStored(t *testing.T) {
	for name, titles := range map[string]map[string]string{
		"no row":      {},
		"blank title": {"org-1": "  "},
	} {
		t.Run(name, func(t *testing.T) {
			title := issueEntityTitle(t, issueTitleClient(t, titles), "org-1")
			if title != nil {
				t.Fatalf("title = %#v, want null", title)
			}
		})
	}
}

func TestIssueFlameEntityTitleNeverCrossesTheOrgBoundary(t *testing.T) {
	client := issueTitleClient(t, map[string]string{"org-2": "Other org secret"})
	title := issueEntityTitle(t, client, "org-1")
	if title != nil {
		t.Fatalf("org-1 saw title %#v of org-2", title)
	}
}

func TestFetchIssueTitleQueryIsBoundToTheCallersOrg(t *testing.T) {
	for _, want := range []string{"FROM work_items FINAL", "work_item_id = {work_item_id:String}", "AND org_id = {org_id:String}"} {
		if !strings.Contains(fetchIssueTitleQuery, want) {
			t.Fatalf("title query missing %q:\n%s", want, fetchIssueTitleQuery)
		}
	}
}
