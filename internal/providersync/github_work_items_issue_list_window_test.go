package providersync

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A GitHub work-items unit holds the half-open window [since, before). GitHub's issue list has `since` and no
// upper bound, so the unit lists in updated order, oldest first, and stops at the first issue updated at or
// after the window end: everything after it is newer. nativeTestClaim's window is
// [2026-07-01T00:00:00Z, 2026-07-31T23:59:59Z).

func issueListIssue(number int, updated string) string {
	return fmt.Sprintf(`{"number":%d,"title":"issue %d","state":"open","created_at":"2026-06-01T00:00:00Z","updated_at":"%s","user":{"login":"octocat"}}`, number, number, updated)
}

func issueListClaim() Claim {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	return claim
}

func issueListPage(issues ...string) string { return "[" + strings.Join(issues, ",") + "]" }

func issueListRequests(doer *githubWorkItemsRESTDoer) []string {
	listed := []string{}
	for _, request := range doer.requests {
		if strings.HasPrefix(request, "/repos/acme/api/issues?") {
			listed = append(listed, request)
		}
	}
	return listed
}

func collectIssueList(t *testing.T, replies map[string][]githubWorkItemsRESTReply) (GitHubWorkItemsRESTResult, *githubWorkItemsRESTDoer, error) {
	t.Helper()
	replies["/repos/acme/api"] = []githubWorkItemsRESTReply{{body: `{"id":4567,"full_name":"Acme/API"}`}}
	for _, number := range []int{42, 43, 45} {
		replies[fmt.Sprintf("/repos/acme/api/issues/%d/events", number)] = []githubWorkItemsRESTReply{{body: `[]`}}
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: replies}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), issueListClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"),
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	return result, doer, err
}

// More than the page cap of issues updated after the window end follow the one in-window issue. A bounded
// window must complete: the list is ordered by update, so the reader stops at the first newer issue.
func TestGitHubWorkItemsIssueListStopsAtTheWindowEndInsteadOfReadingEveryNewerPage(t *testing.T) {
	t.Parallel()
	const pages = 120
	// Page 1 is the in-window issue and 99 newer ones; every later page holds 100 newer ones.
	newer := func(base, count int) []string {
		items := make([]string, 0, count)
		for i := 0; i < count; i++ {
			items = append(items, issueListIssue(base+i, "2026-08-05T00:00:00Z"))
		}
		return items
	}
	replies := make([]githubWorkItemsRESTReply, 0, pages)
	for page := 1; page <= pages; page++ {
		items := newer(1000*page, 100)
		if page == 1 {
			items = append([]string{issueListIssue(42, "2026-07-15T00:00:00Z")}, newer(1000, 99)...)
		}
		link := ""
		if page < pages {
			link = fmt.Sprintf(`<https://api.github.com/repos/acme/api/issues?page=%d>; rel="next"`, page+1)
		}
		replies = append(replies, githubWorkItemsRESTReply{body: issueListPage(items...), link: link})
	}
	result, doer, err := collectIssueList(t, map[string][]githubWorkItemsRESTReply{"/repos/acme/api/issues": replies})
	if err != nil {
		t.Fatalf("a capped window with %d pages of newer issues was refused: %v (list requests %d)", pages, err, len(issueListRequests(doer)))
	}
	if len(result.Rows.WorkItems) != 1 || result.Rows.WorkItems[0].WorkItemID != "gh:Acme/API#42" {
		t.Fatalf("work items = %+v, want only #42", result.Rows.WorkItems)
	}
	if got := len(issueListRequests(doer)); got != 1 {
		t.Fatalf("%d issue-list requests, want 1 (stop at the first newer issue)", got)
	}
}

func TestGitHubWorkItemsIssueListAsksForUpdatedOrderFromTheWindowStart(t *testing.T) {
	t.Parallel()
	_, doer, err := collectIssueList(t, map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api/issues": {{body: issueListPage(issueListIssue(42, "2026-07-15T00:00:00Z"))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	requests := issueListRequests(doer)
	if len(requests) != 1 {
		t.Fatalf("list requests = %v", requests)
	}
	for _, want := range []string{"sort=updated", "direction=asc", "since=2026-07-01T00%3A00%3A00Z", "state=all"} {
		if !strings.Contains(requests[0], want) {
			t.Errorf("request %q lacks %q", requests[0], want)
		}
	}
}

// An issue updated exactly at the window end belongs to the next window, as for GitLab and Jira.
func TestGitHubWorkItemsIssueListDropsAnIssueUpdatedExactlyAtTheWindowEnd(t *testing.T) {
	t.Parallel()
	result, _, err := collectIssueList(t, map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api/issues": {{body: issueListPage(
			issueListIssue(42, "2026-07-15T00:00:00Z"),
			issueListIssue(45, "2026-07-31T23:59:59Z"),
		)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows.WorkItems) != 1 || result.Rows.WorkItems[0].WorkItemID != "gh:Acme/API#42" {
		t.Fatalf("work items = %+v, want only #42 (#45 is updated exactly at the end and belongs to the next window)", result.Rows.WorkItems)
	}
}

// A list that ends inside the window is read to its end: nothing newer exists to stop at.
func TestGitHubWorkItemsIssueListReadsEveryPageWhenNothingIsNewerThanTheWindow(t *testing.T) {
	t.Parallel()
	result, doer, err := collectIssueList(t, map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api/issues": {
			{body: issueListPage(issueListIssue(42, "2026-07-10T00:00:00Z")), link: `<https://api.github.com/repos/acme/api/issues?page=2>; rel="next"`},
			{body: issueListPage(issueListIssue(43, "2026-07-20T00:00:00Z"))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows.WorkItems) != 2 || len(issueListRequests(doer)) != 2 {
		t.Fatalf("work items = %d, list requests = %d, want 2 and 2", len(result.Rows.WorkItems), len(issueListRequests(doer)))
	}
}
