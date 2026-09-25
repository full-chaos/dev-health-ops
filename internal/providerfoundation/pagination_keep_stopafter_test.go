package providerfoundation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// numberedDoer serves `perPage` items per page numbered consecutively from 1
// and always advertises a next link, so a walk stops only when an option says so.
type numberedDoer struct {
	perPage int
	calls   int
}

func (d *numberedDoer) Do(r *http.Request) (*http.Response, error) {
	d.calls++
	items := make([]string, 0, d.perPage)
	for i := 0; i < d.perPage; i++ {
		items = append(items, fmt.Sprintf(`{"n":%d}`, (d.calls-1)*d.perPage+i+1))
	}
	h := http.Header{"Link": []string{fmt.Sprintf(`<https://api.github.com/x?page=%d>; rel="next"`, d.calls+1)}}
	return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader("[" + strings.Join(items, ",") + "]")), Request: r}, nil
}

func itemNumber(t *testing.T, raw json.RawMessage) int {
	t.Helper()
	var item struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatal(err)
	}
	return item.N
}

func numbers(t *testing.T, items []json.RawMessage) []int {
	t.Helper()
	out := make([]int, 0, len(items))
	for _, raw := range items {
		out = append(out, itemNumber(t, raw))
	}
	return out
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGitHubLinkPagesKeepDropsItemsAndPaginationContinues(t *testing.T) {
	t.Parallel()
	doer := &numberedDoer{perPage: 3}
	page, err := CollectGitHubLinkPages(context.Background(), stopReasonClient(t, "github", doer),
		GitHubPageOptions{Path: "/x", MaxPages: 3, Keep: func(raw json.RawMessage) bool { return itemNumber(t, raw)%2 == 1 }})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := numbers(t, page.Items), []int{1, 3, 5, 7, 9}; !sameInts(got, want) || doer.calls != 3 || !page.PageBudgetExhausted {
		t.Fatalf("kept %v after %d requests (budget exhausted %v), want %v after 3 and the budget exhausted", got, doer.calls, page.PageBudgetExhausted, want)
	}
}

func TestGitHubLinkPagesStopAfterKeepsTheItemAndRequestsNoNextPage(t *testing.T) {
	t.Parallel()
	// The third kept item is the LAST item of page 1: the walk must stop there,
	// with one request, though the response advertises a next page.
	doer := &numberedDoer{perPage: 3}
	kept := 0
	page, err := CollectGitHubLinkPages(context.Background(), stopReasonClient(t, "github", doer),
		GitHubPageOptions{Path: "/x", MaxPages: 10, StopAfter: func(json.RawMessage) bool { kept++; return kept >= 3 }})
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(t, page.Items); !sameInts(got, []int{1, 2, 3}) || doer.calls != 1 {
		t.Fatalf("kept %v after %d requests, want [1 2 3] after exactly 1", got, doer.calls)
	}

	// Mid-page: the second kept item ends the walk; the rest of the page is not read.
	doer = &numberedDoer{perPage: 3}
	kept = 0
	page, err = CollectGitHubLinkPages(context.Background(), stopReasonClient(t, "github", doer),
		GitHubPageOptions{Path: "/x", MaxPages: 10, StopAfter: func(json.RawMessage) bool { kept++; return kept >= 2 }})
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(t, page.Items); !sameInts(got, []int{1, 2}) || doer.calls != 1 {
		t.Fatalf("kept %v after %d requests, want [1 2] after 1", got, doer.calls)
	}
}

func TestGitHubLinkPagesKeepAndStopAfterCountOnlyKeptItems(t *testing.T) {
	t.Parallel()
	doer := &numberedDoer{perPage: 3}
	kept := 0
	page, err := CollectGitHubLinkPages(context.Background(), stopReasonClient(t, "github", doer),
		GitHubPageOptions{
			Path: "/x", MaxPages: 10,
			Keep:      func(raw json.RawMessage) bool { return itemNumber(t, raw)%3 == 0 },
			StopAfter: func(json.RawMessage) bool { kept++; return kept >= 2 },
		})
	if err != nil {
		t.Fatal(err)
	}
	// Items 3 and 6 are kept; StopAfter fires on 6, the last item of page 2.
	if got := numbers(t, page.Items); !sameInts(got, []int{3, 6}) || doer.calls != 2 {
		t.Fatalf("kept %v after %d requests, want [3 6] after 2", got, doer.calls)
	}

	// MaxItems counts kept items only.
	doer = &numberedDoer{perPage: 3}
	page, err = CollectGitHubLinkPages(context.Background(), stopReasonClient(t, "github", doer),
		GitHubPageOptions{Path: "/x", MaxPages: 10, MaxItems: 2, Keep: func(raw json.RawMessage) bool { return itemNumber(t, raw)%3 == 0 }})
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(t, page.Items); !sameInts(got, []int{3, 6}) || !page.ItemCapReached {
		t.Fatalf("kept %v (cap reached %v), want [3 6] with the cap reached", got, page.ItemCapReached)
	}
}
