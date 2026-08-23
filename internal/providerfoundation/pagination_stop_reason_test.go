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

// REGRESSION (codex round 2, challenge 1). Codex read the route's item-cap
// branch winning over its page-budget branch as evidence that a page-budget stop
// could be mislabelled as an item cap. At the site that reads these two fields
// that cannot happen at all: each bound sets its own flag and RETURNS
// immediately, so the two are mutually exclusive by construction, and "both set"
// is not a state the paginator can produce.
//
// The fixture deliberately arranges for the two bounds to be crossable on the
// SAME page, which is the only configuration in which conflation could occur,
// and then sweeps the boundary in both directions so neither ordering is
// special-cased.
func TestGitHubLinkPagesNeverReportsBothStopReasons(t *testing.T) {
	t.Parallel()
	const perPage = 10
	for _, probe := range []struct {
		name           string
		maxPages       int
		maxItems       int
		wantItemCap    bool
		wantPageBudget bool
	}{
		// Both bounds land on the very same page: 2 pages of 10 is exactly 20
		// items, and MaxItems is 20. The item cap is hit while filling page 2,
		// before the loop can return to its page-budget check.
		{"simultaneous", 2, perPage * 2, true, false},
		// One item earlier: still the item cap, mid-page.
		{"item cap one below", 2, perPage*2 - 1, true, false},
		// One item later: unreachable within the budget, so pages run out.
		{"page budget one above", 2, perPage*2 + 1, false, true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			t.Parallel()
			result, err := CollectGitHubLinkPages(context.Background(),
				stopReasonClient(t, "github", &stopReasonDoer{perPage: perPage}),
				GitHubPageOptions{Path: "/x", DataKey: "jobs",
					MaxPages: probe.maxPages, MaxItems: probe.maxItems})
			if err != nil {
				t.Fatal(err)
			}
			// The invariant, stated directly: never both.
			if result.ItemCapReached && result.PageBudgetExhausted {
				t.Fatalf("both stop reasons set at once (%d items over %d pages); "+
					"a consumer choosing between them would be choosing between two 'true' facts",
					len(result.Items), result.Pages)
			}
			if result.ItemCapReached != probe.wantItemCap ||
				result.PageBudgetExhausted != probe.wantPageBudget {
				t.Fatalf("ItemCapReached=%v PageBudgetExhausted=%v, want %v/%v",
					result.ItemCapReached, result.PageBudgetExhausted,
					probe.wantItemCap, probe.wantPageBudget)
			}
			// A walk must never stop silently: one of the two bounds owns
			// every stop this fixture can produce, because the doer always
			// advertises another page.
			if !result.ItemCapReached && !result.PageBudgetExhausted {
				t.Fatal("walk stopped with neither reason set; the stop is unattributable")
			}
		})
	}
}

// REGRESSION (codex round 2, challenge 1), the other half of the refutation.
// At the len-based per-run sites a page-budget stop and an item cap CAN both be
// true at once, and the routes deliberately let the item cap win. That is only
// correct if the committed prefix does not depend on the page budget -- if a
// larger budget could recover items, withholding would buy real coverage and
// advancing would lose it.
//
// This proves the prefix is budget-independent: two genuinely different walks
// over the same data yield the same first N items.
func TestCollectedPrefixIsIndependentOfThePageBudget(t *testing.T) {
	t.Parallel()
	const perPage, prefix = 10, 25
	collect := func(maxPages int) (PageCollection, int) {
		doer := &stopReasonDoer{perPage: perPage}
		result, err := CollectGitHubLinkPages(context.Background(),
			stopReasonClient(t, "github", doer),
			// No MaxItems: this is the len-based shape the routes use.
			GitHubPageOptions{Path: "/x", DataKey: "jobs", MaxPages: maxPages})
		if err != nil {
			t.Fatal(err)
		}
		if !result.PageBudgetExhausted {
			t.Fatalf("MaxPages=%d did not exhaust its budget; the fixture always advertises another page", maxPages)
		}
		return result, doer.calls
	}

	small, smallCalls := collect(4)  // 40 items
	large, largeCalls := collect(12) // 120 items

	// Anti-vacuity: the walks must really differ, or "same prefix" is trivial.
	if smallCalls >= largeCalls || len(small.Items) >= len(large.Items) {
		t.Fatalf("walks did not differ: %d calls/%d items vs %d calls/%d items",
			smallCalls, len(small.Items), largeCalls, len(large.Items))
	}
	if len(small.Items) < prefix {
		t.Fatalf("small walk collected %d items, fewer than the %d-item prefix under test",
			len(small.Items), prefix)
	}

	for index := 0; index < prefix; index++ {
		if string(small.Items[index]) != string(large.Items[index]) {
			t.Fatalf("item %d differs between page budgets (%q vs %q); a len-based cap "+
				"would not be deterministic and the item-cap branch could not safely advance",
				index, small.Items[index], large.Items[index])
		}
	}
}
