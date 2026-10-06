package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func gitLabEffectRowCount(batch CompleteRouteBatch, destination string) int {
	total := 0
	for _, effect := range batch.Effects {
		if effect.Destination == destination {
			total += len(effect.Rows)
		}
	}
	return total
}

// gitLabPagedResponses serves total rows of one list in pages of 100, the way
// GitLab's offset pagination does, ending with a short page.
func gitLabPagedResponses(total int, row func(index int) string) []string {
	pages := make([]string, 0, total/100+2)
	for start := 0; start < total; start += 100 {
		end := min(start+100, total)
		rows := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			rows = append(rows, row(index))
		}
		pages = append(pages, "["+strings.Join(rows, ",")+"]")
	}
	if total%100 == 0 {
		pages = append(pages, "[]")
	}
	return pages
}

func collectGitLabLargeNestedLists(t *testing.T, notes, labelEvents, stateEvents, nestedMaxPages int) (CompleteRouteBatch, error) {
	t.Helper()
	return collectGitLabLargeNestedListsWith(t, notes, labelEvents, stateEvents, GitLabWorkItemsRouteHandler{
		PerPage: 100, MaxPages: 10, NestedMaxPages: nestedMaxPages,
	})
}

// collectGitLabLargeNestedListsWith drives the route with the given limits;
// StatusMapping and IncludeMRs are filled in here.
func collectGitLabLargeNestedListsWith(t *testing.T, notes, labelEvents, stateEvents int, handler GitLabWorkItemsRouteHandler) (CompleteRouteBatch, error) {
	t.Helper()
	root := "/api/v4/projects/123"
	responses := gitLabWorkItemResponses()
	delete(responses, root+"/merge_requests?page=1")
	responses[root+"/merge_requests?page=1"] = []string{`[]`}
	pagesFor := func(path string, total int, row func(int) string) {
		pages := gitLabPagedResponses(total, row)
		for number, body := range pages {
			key := fmt.Sprintf("%s?page=%d", path, number+1)
			responses[key] = []string{body}
		}
	}
	pagesFor(root+"/issues/42/notes", notes, func(i int) string {
		return fmt.Sprintf(`{"system":false,"body":"note %d","created_at":"2026-07-02T12:%02d:%02dZ","author":{"username":"alice"}}`, i, (i/60)%60, i%60)
	})
	pagesFor(root+"/issues/42/resource_label_events", labelEvents, func(i int) string {
		return fmt.Sprintf(`{"action":"add","created_at":"2026-07-02T10:%02d:%02dZ","label":{"name":"done"}}`, (i/60)%60, i%60)
	})
	pagesFor(root+"/issues/42/resource_state_events", stateEvents, func(i int) string {
		return fmt.Sprintf(`{"state":"reopened","created_at":"2026-07-03T10:%02d:%02dZ","user":{"username":"bob","name":"Bob"}}`, (i/60)%60, i%60)
	})
	claim := nativeTestClaim("gitlab", "work-items")
	handler.StatusMapping = loadRealStatusMapping(t)
	handler.IncludeMRs = boolPointer(false)
	return handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
}

// Notes (was 500), label events (was 300) and state events (was 100) are all
// kept in full: one issue holds 520 notes, 330 label events and 130 state events.
func TestGitLabWorkItemsRouteKeepsEveryNestedRowPastTheOldCaps(t *testing.T) {
	batch, err := collectGitLabLargeNestedLists(t, 520, 330, 130, 50)
	if err != nil {
		t.Fatal(err)
	}
	if got := gitLabEffectRowCount(batch, "work_item_interactions"); got != 520 {
		t.Fatalf("interactions=%d want 520 (old cap 500)", got)
	}
	if got := gitLabEffectRowCount(batch, "work_item_reopen_events"); got != 130 {
		t.Fatalf("reopen events=%d want 130 (old cap 100)", got)
	}
	if got := gitLabEffectRowCount(batch, "work_item_transitions"); got != 330 {
		t.Fatalf("transitions=%d want 330 (old cap 300)", got)
	}
}

// Past the page bound the unit fails closed and names the owner and the field.
func TestGitLabWorkItemsRouteNestedBoundNamesOwnerAndField(t *testing.T) {
	_, err := collectGitLabLargeNestedLists(t, 450, 0, 0, 3)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"notes of /api/v4/projects/123/issues/42", "after 3 pages", "max 3 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}

// Production builds the handler with NO limits set
// (internal/workerservice/provider_sync.go: StatusMapping and Derived only), so
// the bound that runs there is the default one. Literal numbers: a list of 100
// pages passes (9,999 rows: the last page is short, so no 101st empty page is
// read), 101 pages fail closed naming the owner and the field.
func TestGitLabWorkItemsRouteDefaultNestedBoundIsExactlyOneHundredPages(t *testing.T) {
	production := GitLabWorkItemsRouteHandler{}
	batch, err := collectGitLabLargeNestedListsWith(t, 9_999, 0, 0, production)
	if err != nil || gitLabEffectRowCount(batch, "work_item_interactions") != 9_999 {
		t.Fatalf("100 pages: interactions=%d err=%v", gitLabEffectRowCount(batch, "work_item_interactions"), err)
	}
	_, err = collectGitLabLargeNestedListsWith(t, 10_001, 0, 0, production)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("101 pages: err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"notes of /api/v4/projects/123/issues/42", "after 100 pages", "max 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}
