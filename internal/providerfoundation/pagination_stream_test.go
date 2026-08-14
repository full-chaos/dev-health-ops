package providerfoundation

import (
	"context"
	"net/http"
	"testing"
)

func TestVisitGitHubLinkPagesDoesNotAccumulatePriorPages(t *testing.T) {
	doer := &paginationDoer{responses: []paginationResponse{
		{body: `[1,2]`, headers: http.Header{"Link": {`<https://api.github.com/items?page=2>; rel="next"`}}},
		{body: `[3,4]`, headers: http.Header{"Link": {`<https://api.github.com/items?page=3>; rel="next"`}}},
		{body: `[5]`},
	}}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	var visits int
	maxItems := 0
	result, err := VisitGitHubLinkPages(context.Background(), client, GitHubPageOptions{Path: "/items", MaxPages: 3}, func(page PageVisit) error {
		visits++
		if len(page.Items) > maxItems {
			maxItems = len(page.Items)
		}
		if len(page.Items) > 2 {
			t.Fatalf("page retained too many items: %d", len(page.Items))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if visits != 3 || result.Pages != 3 || result.Items != nil || maxItems != 2 {
		t.Fatalf("visits=%d result=%+v max_items=%d", visits, result, maxItems)
	}
}

func TestVisitGitHubLinkPagesResumesAtInitialURL(t *testing.T) {
	doer := &paginationDoer{responses: []paginationResponse{{body: `[3]`}}}
	client := paginationClient(t, "github", "https://api.github.com", doer)
	var seen PageVisit
	result, err := VisitGitHubLinkPages(context.Background(), client, GitHubPageOptions{
		Path: "/items", InitialURL: "https://api.github.com/items?page=3", MaxPages: 1,
	}, func(page PageVisit) error {
		seen = page
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 1 || seen.CursorBefore != "https://api.github.com/items?page=3" || len(doer.requests) != 1 || doer.requests[0].URL.String() != seen.CursorBefore {
		t.Fatalf("result=%+v seen=%+v requests=%d", result, seen, len(doer.requests))
	}
}

func TestVisitGitLabPageParamPagesResumesAtInitialPage(t *testing.T) {
	doer := &paginationDoer{responses: []paginationResponse{
		{body: `[3,4]`, headers: http.Header{"X-Next-Page": {"4"}}},
		{body: `[5]`},
	}}
	client := paginationClient(t, "gitlab", "https://gitlab.example", doer)
	var seen []string
	result, err := VisitGitLabPageParamPages(context.Background(), client, GitLabPageOptions{Path: "/items", PerPage: 2, MaxPages: 2, InitialPage: 3}, func(page PageVisit) error {
		seen = append(seen, page.CursorBefore+":"+page.CursorAfter)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Pages != 2 || len(seen) != 2 || seen[0] != "3:4" || seen[1] != "4:0" {
		t.Fatalf("result=%+v seen=%v", result, seen)
	}
}
