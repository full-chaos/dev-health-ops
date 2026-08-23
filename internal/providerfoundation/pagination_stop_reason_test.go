package providerfoundation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// stopReasonDoer serves `perPage` items on every page and always advertises a
// next link, so the walk only ever stops because one of the bounds says so.
type stopReasonDoer struct {
	perPage int
	calls   int
	gitlab  bool
}

func (d *stopReasonDoer) Do(r *http.Request) (*http.Response, error) {
	d.calls++
	h := make(http.Header)
	items := make([]string, 0, d.perPage)
	for i := 0; i < d.perPage; i++ {
		items = append(items, `{"id":1}`)
	}
	if d.gitlab {
		h.Set("X-Next-Page", fmt.Sprint(d.calls+1))
		return &http.Response{StatusCode: 200, Header: h,
			Body: io.NopCloser(strings.NewReader("[" + strings.Join(items, ",") + "]")), Request: r}, nil
	}
	h.Set("Link", `<https://api.github.com/x?page=`+fmt.Sprint(d.calls+1)+`>; rel="next"`)
	return &http.Response{StatusCode: 200, Header: h,
		Body: io.NopCloser(strings.NewReader(`{"jobs":[` + strings.Join(items, ",") + `]}`)), Request: r}, nil
}

func stopReasonClient(t *testing.T, provider string, doer HTTPDoer) *HTTPClient {
	t.Helper()
	base := "https://api.github.com"
	if provider == "gitlab" {
		base = "https://gitlab.example"
	}
	client, err := NewHTTPClient(provider, base, doer,
		func(r *http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: 1, MaxWait: 1},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// A walk stops for one of two reasons and they must never be conflated. These
// were a single CapReached boolean: the page-budget stop set it, the item-cap
// stop set NOTHING, and a per-run consumer read the one flag as "too many
// items". It committed a partial run and advanced its watermark past jobs that
// were never fetched (CHAOS-4142, codex round 1).
func TestGitHubLinkPagesReportsWhichBoundStoppedTheWalk(t *testing.T) {
	t.Parallel()
	// Item cap: 3 items/page against MaxItems=7 stops mid-page, well inside a
	// generous page budget.
	itemCapped, err := CollectGitHubLinkPages(context.Background(),
		stopReasonClient(t, "github", &stopReasonDoer{perPage: 3}),
		GitHubPageOptions{Path: "/x", DataKey: "jobs", MaxPages: 100, MaxItems: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !itemCapped.ItemCapReached || itemCapped.PageBudgetExhausted {
		t.Fatalf("item-cap stop reported ItemCapReached=%v PageBudgetExhausted=%v, want true/false",
			itemCapped.ItemCapReached, itemCapped.PageBudgetExhausted)
	}
	// Cross-check that len() agrees with the structural flag rather than
	// standing in for it: the item cap is reached exactly at MaxItems.
	if len(itemCapped.Items) != 7 {
		t.Fatalf("item-cap stop collected %d items, want exactly MaxItems=7", len(itemCapped.Items))
	}

	// Page budget: the SAME 3 items/page, but only 2 pages of allowance and a
	// far higher item cap, so the walk runs out of pages first.
	pageCapped, err := CollectGitHubLinkPages(context.Background(),
		stopReasonClient(t, "github", &stopReasonDoer{perPage: 3}),
		GitHubPageOptions{Path: "/x", DataKey: "jobs", MaxPages: 2, MaxItems: 500})
	if err != nil {
		t.Fatal(err)
	}
	if !pageCapped.PageBudgetExhausted || pageCapped.ItemCapReached {
		t.Fatalf("page-budget stop reported PageBudgetExhausted=%v ItemCapReached=%v, want true/false",
			pageCapped.PageBudgetExhausted, pageCapped.ItemCapReached)
	}
	// THE REGRESSION, stated as an assertion: a page-budget stop lands far
	// UNDER the item cap, so any consumer inferring "item cap" from this stop
	// is reading a fact that is not there.
	if len(pageCapped.Items) >= 500 {
		t.Fatalf("page-budget stop collected %d items; the fixture is not exercising the page budget",
			len(pageCapped.Items))
	}
}

// The GitLab paginator takes no MaxItems at all, so ItemCapReached is not
// merely unset there — it is unreachable. Any GitLab per-run item cap must
// therefore be len-based, and this pins that fact so nobody adds an
// ItemCapReached branch that can never fire.
func TestGitLabPageParamPagesOnlyEverReportsPageBudget(t *testing.T) {
	t.Parallel()
	got, err := CollectGitLabPageParamPages(context.Background(),
		stopReasonClient(t, "gitlab", &stopReasonDoer{perPage: 3, gitlab: true}),
		GitLabPageOptions{Path: "/x/jobs", PerPage: 100, MaxPages: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !got.PageBudgetExhausted {
		t.Fatal("gitlab walk past its page budget did not report PageBudgetExhausted")
	}
	if got.ItemCapReached {
		t.Fatal("gitlab paginator reported ItemCapReached, which it has no MaxItems to produce")
	}
}
