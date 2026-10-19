package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
// (its new update is past the window end): every in-window issue, issue 50 included, must be written exactly once.
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
	// Issue 50 was read inside the window before it moved; its later update belongs to the next window.
	if written["gh:Acme/API#50"] != 1 {
		t.Fatalf("issue 50 written %d times, want once", written["gh:Acme/API#50"])
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

// Running out of the request cap inside a tie group fails closed: a Warn and a counter, an error, never a
// truncated list.
func TestGitHubWorkItemsIssueListTieGroupBeyondTheRequestCapFailsClosedWithAWarn(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	updated := map[int]time.Time{}
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	for number := 1; number <= 450; number++ {
		updated[number] = at
	}
	sim := &issueListSimulator{t: t, updated: updated}
	client := gitHubPullRequestClient(t, fakehttp.Client(sim), "https://api.github.com")
	client.Metrics = providerfoundation.NewMetrics()
	result, err := (GitHubWorkItemsRESTCollector{MaxPages: 3}).Collect(
		context.Background(), issueListClaim(), client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if !errors.Is(err, ErrPaginationCapExceeded) || !result.Evidence.CapReached || len(result.Rows.WorkItems) != 0 {
		t.Fatalf("err = %v, cap reached = %v, rows = %d: want the pagination cap, no rows", err, result.Evidence.CapReached, len(result.Rows.WorkItems))
	}
	if got := len(sim.listRequests); got != 3 {
		t.Fatalf("%d list requests, want 3 (the cap)", got)
	}
	output := logged.String()
	if !strings.Contains(output, "level=WARN msg=github_work_items_issue_list_cap_exceeded") {
		t.Errorf("no Warn for the exhausted cap: %s", output)
	}
	var rendered strings.Builder
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if want := `dev_health_github_work_items_issue_list_cap_exceeded_total{reason="page_cap"} 1`; !strings.Contains(rendered.String(), want) {
		t.Errorf("metrics lack %q", want)
	}
}

// The `since` key is sent in whole seconds, so the list holds an issue updated just before a fractional window
// start, which the window before it wrote. The window writes only what is updated at or after its true start.
func TestGitHubWorkItemsIssueListSkipsAnIssueBeforeAFractionalWindowStart(t *testing.T) {
	t.Parallel()
	claim := issueListClaim()
	start := time.Date(2026, 7, 10, 12, 0, 0, 500_000_000, time.UTC)
	claim.SinceAt = &start
	sim := &issueListSimulator{t: t, updated: map[int]time.Time{
		1: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
		2: time.Date(2026, 7, 10, 12, 0, 1, 0, time.UTC),
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim, gitHubPullRequestClient(t, fakehttp.Client(sim), "https://api.github.com"),
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	written := writtenIssueNumbers(result)
	if written["gh:Acme/API#1"] != 0 || written["gh:Acme/API#2"] != 1 {
		t.Fatalf("written = %v, want issue 1 (before the start) skipped and issue 2 once", written)
	}
}

// A same-second tie group of more than one page is paged by page number: the Info line carries the count and
// the counter moves, so the operator sees how often the race window opens. A list without one counts nothing.
func TestGitHubWorkItemsIssueListCountsATieGroupOfMoreThanOnePage(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	updated := map[int]time.Time{}
	at := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	for number := 1; number <= 230; number++ {
		updated[number] = at
	}
	sim := &issueListSimulator{t: t, updated: updated}
	client := gitHubPullRequestClient(t, fakehttp.Client(sim), "https://api.github.com")
	client.Metrics = providerfoundation.NewMetrics()
	if _, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), issueListClaim(), client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if output := logged.String(); !strings.Contains(output, "level=INFO msg=github_work_items_issue_list") ||
		!strings.Contains(output, "tie_groups_over_one_page=1") || strings.Contains(output, "level=WARN") {
		t.Errorf("Info line lacks the tie group count, or a Warn was logged: %s", output)
	}
	var rendered strings.Builder
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if want := `dev_health_github_work_items_issue_list_tie_paged_total{reason="same_second"} 1`; !strings.Contains(rendered.String(), want) {
		t.Errorf("metrics lack %q", want)
	}
}
