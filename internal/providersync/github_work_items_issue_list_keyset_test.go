package providersync

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// issueListSimulator answers GET /repos/acme/api/issues the way GitHub does: `since` keeps the issues updated
// at or after it, sort=updated&direction=asc orders them oldest update first (number breaks ties), `per_page` and
// `page` slice the list and a rel="next" Link names the next page. After the first list request it applies
// `afterFirstRequest`, an issue moving to a later update as a person edits it while the list is being read.
type issueListSimulator struct {
	t                 *testing.T
	updated           map[int]time.Time
	afterFirstRequest func(updated map[int]time.Time)
	listRequests      []string
}

func (sim *issueListSimulator) Do(request *http.Request) (*http.Response, error) {
	body, link := "[]", ""
	switch {
	case request.URL.Path == "/repos/acme/api":
		body = `{"id":4567,"full_name":"Acme/API"}`
	case request.URL.Path == "/repos/acme/api/issues":
		body, link = sim.list(request.URL.Query())
		sim.listRequests = append(sim.listRequests, request.URL.RawQuery)
		if len(sim.listRequests) == 1 && sim.afterFirstRequest != nil {
			sim.afterFirstRequest(sim.updated)
		}
	case strings.HasPrefix(request.URL.Path, "/repos/acme/api/issues/"):
		body = "[]"
	default:
		sim.t.Fatalf("unexpected request %s", request.URL.String())
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if link != "" {
		header.Set("Link", link)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func (sim *issueListSimulator) list(query url.Values) (string, string) {
	since, _ := time.Parse(time.RFC3339, query.Get("since"))
	numbers := []int{}
	for number, at := range sim.updated {
		if !at.Before(since) {
			numbers = append(numbers, number)
		}
	}
	sort.Slice(numbers, func(i, j int) bool {
		left, right := sim.updated[numbers[i]], sim.updated[numbers[j]]
		if !left.Equal(right) {
			return left.Before(right)
		}
		return numbers[i] < numbers[j]
	})
	perPage, _ := strconv.Atoi(query.Get("per_page"))
	page, _ := strconv.Atoi(query.Get("page"))
	if page < 1 {
		page = 1
	}
	from := (page - 1) * perPage
	to := min(from+perPage, len(numbers))
	items := []string{}
	if from < len(numbers) {
		for _, number := range numbers[from:to] {
			items = append(items, issueListIssue(number, sim.updated[number].Format(time.RFC3339)))
		}
	}
	link := ""
	if to < len(numbers) {
		next := url.Values{}
		for key, values := range query {
			next[key] = values
		}
		next.Set("page", strconv.Itoa(page+1))
		link = fmt.Sprintf(`<https://api.github.com/repos/acme/api/issues?%s>; rel="next"`, next.Encode())
	}
	return "[" + strings.Join(items, ",") + "]", link
}

func collectFromSimulator(t *testing.T, sim *issueListSimulator) (GitHubWorkItemsRESTResult, error) {
	t.Helper()
	sim.t = t
	return (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), issueListClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(sim), "https://api.github.com"),
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
}

func writtenIssueNumbers(result GitHubWorkItemsRESTResult) map[string]int {
	written := map[string]int{}
	for _, item := range result.Rows.WorkItems {
		written[item.WorkItemID]++
	}
	return written
}

// An issue edited while the list is being read moves to the end of an update-ordered list and shifts every
// later issue up by one place. Reading by page number then skips the issue that shifted across the page
// boundary, and the window never writes it. 200 in-window issues, issue 50 edited after the first request
// (its new update is past the window end): every other in-window issue must still be written exactly once.
func TestGitHubWorkItemsIssueListSkipsNothingWhenAnIssueMovesWhileItIsRead(t *testing.T) {
	t.Parallel()
	updated := map[int]time.Time{}
	base := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	for number := 1; number <= 200; number++ {
		updated[number] = base.Add(time.Duration(number) * time.Minute)
	}
	sim := &issueListSimulator{updated: updated, afterFirstRequest: func(updated map[int]time.Time) {
		updated[50] = time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	}}
	result, err := collectFromSimulator(t, sim)
	if err != nil {
		t.Fatal(err)
	}
	written := writtenIssueNumbers(result)
	for number := 1; number <= 200; number++ {
		if number == 50 {
			continue
		}
		if written[fmt.Sprintf("gh:Acme/API#%d", number)] != 1 {
			t.Fatalf("issue %d written %d times, want once (rows=%d)", number, written[fmt.Sprintf("gh:Acme/API#%d", number)], len(result.Rows.WorkItems))
		}
	}
	if written["gh:Acme/API#50"] != 0 {
		t.Fatalf("issue 50 moved past the window end and must not be written by this window")
	}
}

// More than one page of issues updated in the same second: GitHub's updated_at has a one-second resolution, so
// a bulk edit leaves a tie group no update-time cursor can split. Every issue of the group is still written.
func TestGitHubWorkItemsIssueListReadsATieGroupLargerThanOnePage(t *testing.T) {
	t.Parallel()
	updated := map[int]time.Time{}
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	for number := 1; number <= 230; number++ {
		updated[number] = at
	}
	updated[231] = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	sim := &issueListSimulator{updated: updated}
	result, err := collectFromSimulator(t, sim)
	if err != nil {
		t.Fatal(err)
	}
	written := writtenIssueNumbers(result)
	for number := 1; number <= 231; number++ {
		if id := fmt.Sprintf("gh:Acme/API#%d", number); written[id] != 1 {
			t.Fatalf("issue %d written %d times, want once", number, written[id])
		}
	}
}
