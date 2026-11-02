package flame

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// workItemRow is fetchIssueIdentityQuery's row: the latest title, that row's
// repository id, and how many repositories hold the work item.
type workItemRow struct {
	title        string
	repoID       string
	repositories uint64
}

func issueTitleClient(t *testing.T, titles map[string]string) fakeQueryClient {
	t.Helper()
	rows := map[string]workItemRow{}
	for org, title := range titles {
		rows[org] = workItemRow{title: title, repoID: "11111111-1111-1111-1111-111111111111", repositories: 1}
	}
	return issueIdentityClient(t, rows)
}

func issueIdentityClient(t *testing.T, rowsByOrg map[string]workItemRow) fakeQueryClient {
	t.Helper()
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			var org string
			for _, b := range bindings {
				if b.Name == "org_id" {
					org, _ = b.Value.(string)
				}
			}
			if row, ok := rowsByOrg[org]; ok {
				return &fixtureRowScanner{rows: [][]any{{row.title, row.repoID, row.repositories}}}, nil
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

func TestFetchIssueIdentityQueryIsBoundToTheCallersOrg(t *testing.T) {
	for _, want := range []string{"FROM work_items FINAL", "work_item_id = {work_item_id:String}", "AND org_id = {org_id:String}"} {
		if !strings.Contains(fetchIssueIdentityQuery, want) {
			t.Fatalf("identity query missing %q:\n%s", want, fetchIssueIdentityQuery)
		}
	}
}

func TestIssueFlameRequestFailsWhenTheTitleReadFails(t *testing.T) {
	titleErr := errors.New("title read failed")
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_items FINAL") {
			return nil, titleErr
		}
		return issueTitleClient(t, nil).handler(t, query, bindings)
	}}
	got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "issue", EntityID: "wi-1"})
	if !errors.Is(err, titleErr) {
		t.Fatalf("err = %v, want the title read error", err)
	}
	if got != nil {
		t.Fatalf("response = %#v, want none on a failed title read", got)
	}
}

func issueEntityRepoID(t *testing.T, client fakeQueryClient, org string) any {
	t.Helper()
	got, err := BuildResponse(context.Background(), client, org, Params{EntityType: "issue", EntityID: "wi-1"})
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	repoID, ok := got.Entity.Get("repo_id")
	if !ok {
		t.Fatal("issue entity has no repo_id key")
	}
	return repoID
}

// CHAOS-12563: the repository id goes out only when exactly one repository
// holds the work item, so a caller never builds ACR's work_item.v2 id from a
// guess.
func TestIssueFlameEntityServesTheOneRepositoryThatHoldsTheWorkItem(t *testing.T) {
	const repo = "22222222-2222-2222-2222-222222222222"
	client := issueIdentityClient(t, map[string]workItemRow{"org-1": {title: "Fix login", repoID: repo, repositories: 1}})
	if got := issueEntityRepoID(t, client, "org-1"); got != repo {
		t.Fatalf("repo_id = %#v, want %s", got, repo)
	}
}

func TestIssueFlameEntityRepoIDIsNullUnlessExactlyOneRepositoryHoldsIt(t *testing.T) {
	for name, rows := range map[string]map[string]workItemRow{
		"no row":            {},
		"two repositories":  {"org-1": {title: "Fix login", repoID: "22222222-2222-2222-2222-222222222222", repositories: 2}},
		"blank repository":  {"org-1": {title: "Fix login", repoID: " ", repositories: 1}},
		"another org's row": {"org-2": {title: "Other", repoID: "22222222-2222-2222-2222-222222222222", repositories: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := issueEntityRepoID(t, issueIdentityClient(t, rows), "org-1"); got != nil {
				t.Fatalf("repo_id = %#v, want null", got)
			}
		})
	}
}

func TestFetchIssueIdentityQueryCountsRepositoriesBeforeItPicksTheLatestRow(t *testing.T) {
	for _, want := range []string{"uniqExact(repo_id) OVER () AS repositories", "toString(repo_id) AS repo_id", "LIMIT 1"} {
		if !strings.Contains(fetchIssueIdentityQuery, want) {
			t.Fatalf("identity query missing %q:\n%s", want, fetchIssueIdentityQuery)
		}
	}
}
