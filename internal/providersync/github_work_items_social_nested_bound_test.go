package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// gitHubSocialEventPages builds a PR whose timeline holds `pages` pages of 100
// events: page 0 rides in the batch query, the rest are continuation pages.
func gitHubSocialEventPages(pages int) []string {
	replies := make([]string, 0, pages)
	for page := 0; page < pages; page++ {
		nodes := make([]string, 0, 100)
		for offset := 0; offset < 100; offset++ {
			nodes = append(nodes, `{"__typename":"ClosedEvent","createdAt":"2026-07-20T00:00:00Z"}`)
		}
		cursor := "e" + strconv.Itoa(page)
		connection := gitHubWorkItemPRSocialConnectionJSON("["+strings.Join(nodes, ",")+"]", page < pages-1, &cursor)
		if page == 0 {
			replies = append(replies, `{"data":{"repository":{"pr0":{"number":42,"timelineItems":`+connection+`}}}}`)
			continue
		}
		replies = append(replies, `{"data":{"repository":{"pr0":{"number":42,"timelineItems":`+connection+`}}}}`)
	}
	return replies
}

func fetchGitHubSocialEvents(t *testing.T, pages int) (GitHubWorkItemPRSocialFetchResult, error) {
	t.Helper()
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: gitHubSocialEventPages(pages)}
	return (GitHubWorkItemPRSocialFetcher{MaxRequests: 1000}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"),
		[]int{42}, 0, githubWorkItemEventsUnbounded,
	)
}

// 1200 timeline events (old cap 1000) are all read.
func TestGitHubWorkItemPRSocialFetcherReadsEveryEventPastTheOldCap(t *testing.T) {
	result, err := fetchGitHubSocialEvents(t, 12)
	if err != nil || !result.Complete() || len(result.Payloads[42].Events) != 1200 {
		t.Fatalf("events=%d complete=%v err=%v", len(result.Payloads[42].Events), result.Complete(), err)
	}
}

// Literal numbers: 100 pages in all (the embedded one included) pass, 101 fail
// closed as an incomplete social layer with cause pagination_cap.
func TestGitHubWorkItemPRSocialFetcherEventBoundIsExactlyOneHundredPages(t *testing.T) {
	result, err := fetchGitHubSocialEvents(t, 100)
	if err != nil || !result.Complete() || len(result.Payloads[42].Events) != 10_000 {
		t.Fatalf("100 pages: events=%d complete=%v err=%v", len(result.Payloads[42].Events), result.Complete(), err)
	}
	result, err = fetchGitHubSocialEvents(t, 101)
	if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != "pagination_cap" {
		t.Fatalf("101 pages: complete=%v incomplete=%+v err=%v", result.Complete(), result.Incomplete, err)
	}
}

func TestGitHubWorkItemPRSocialFetcherEventBoundErrorNamesThePullRequestAndField(t *testing.T) {
	err := githubWorkItemSocialBoundExceeded("Acme/API", 42, "events", 100)
	if !errors.Is(err, errGitHubWorkItemPRSocialPaginationCap) {
		t.Fatalf("err=%v", err)
	}
	for _, want := range []string{"events of Acme/API#42", "after 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}

// A comments_limit that cuts a list with rows left is flagged; a list that holds
// exactly the limit is not.
func TestGitHubWorkItemPRSocialFetcherFlagsACommentCutByTheUserLimit(t *testing.T) {
	cursor := "more"
	cut := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c1"},{"id":"c2"}]`, true, &cursor) + `}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(cut), "https://api.github.com"), []int{42}, 2, 0,
	)
	if err != nil || !result.Payloads[42].CommentsTruncated || len(result.Payloads[42].Comments) != 2 {
		t.Fatalf("cut: payload=%+v err=%v", result.Payloads[42], err)
	}
	exact := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c1"},{"id":"c2"}]`, false, nil) + `}}}}`,
	}}
	result, err = (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(exact), "https://api.github.com"), []int{42}, 2, 0,
	)
	if err != nil || result.Payloads[42].CommentsTruncated {
		t.Fatalf("exact: payload=%+v err=%v", result.Payloads[42], err)
	}
}

// PR comments are bounded the same way: 100 pages in all pass, 101 fail closed.
func TestGitHubWorkItemPRSocialFetcherCommentBoundIsExactlyOneHundredPages(t *testing.T) {
	build := func(pages int) []string {
		replies := make([]string, 0, pages)
		for page := 0; page < pages; page++ {
			nodes := make([]string, 0, 100)
			for offset := 0; offset < 100; offset++ {
				nodes = append(nodes, `{"id":"c`+strconv.Itoa(page*100+offset)+`"}`)
			}
			cursor := "k" + strconv.Itoa(page)
			replies = append(replies, `{"data":{"repository":{"pr0":{"number":42,"comments":`+
				gitHubWorkItemPRSocialConnectionJSON("["+strings.Join(nodes, ",")+"]", page < pages-1, &cursor)+`}}}}`)
		}
		return replies
	}
	fetch := func(pages int) (GitHubWorkItemPRSocialFetchResult, error) {
		doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: build(pages)}
		return (GitHubWorkItemPRSocialFetcher{MaxRequests: 1000}).Fetch(
			context.Background(), gitHubWorkItemPRSocialClaim(),
			gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"),
			[]int{42}, githubWorkItemEventsUnbounded, 0,
		)
	}
	result, err := fetch(100)
	if err != nil || !result.Complete() || len(result.Payloads[42].Comments) != 10_000 {
		t.Fatalf("100 pages: comments=%d complete=%v err=%v", len(result.Payloads[42].Comments), result.Complete(), err)
	}
	result, err = fetch(101)
	if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != "pagination_cap" {
		t.Fatalf("101 pages: complete=%v incomplete=%+v err=%v", result.Complete(), result.Incomplete, err)
	}
}

// The drain loops report the TOTAL page count (embedded page included) in the
// log; the cause the unit records stays pagination_cap.
func TestGitHubWorkItemPRSocialFetcherBoundLogNamesPullRequestAndTotalPages(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	if _, err := fetchGitHubSocialEvents(t, 101); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"level=ERROR", "field=events", "pull_request=42", "pages=100"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %q: %s", want, logs.String())
		}
	}
}
