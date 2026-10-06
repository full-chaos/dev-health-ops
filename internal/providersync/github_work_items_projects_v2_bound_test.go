package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// gitHubProjectV2FuncDoer answers each GraphQL request from a function of the
// query and its variables, so a board of any size can be served page by page.
type gitHubProjectV2FuncDoer struct {
	t        *testing.T
	answer   func(query string, variables map[string]any) string
	requests int
}

func (doer *gitHubProjectV2FuncDoer) Do(request *http.Request) (*http.Response, error) {
	var body struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	doer.requests++
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(doer.answer(body.Query, body.Variables))), Request: request,
	}, nil
}

func fetchGitHubProjectV2Board(t *testing.T, answer func(string, map[string]any) string) (GitHubProjectV2FetchResult, *gitHubProjectV2FuncDoer, error) {
	t.Helper()
	doer := &gitHubProjectV2FuncDoer{t: t, answer: answer}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, fakehttp.Client(doer)),
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
	)
	return result, doer, err
}

const gitHubProjectV2EndPage = `{"hasNextPage":false,"endCursor":null}`

func gitHubProjectV2PageInfoJSON(more bool, cursor string) string {
	return `{"hasNextPage":` + strconv.FormatBool(more) + `,"endCursor":"` + cursor + `"}`
}

func gitHubProjectV2ItemJSON(fieldValues, changes string) string {
	return `{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"title":"Ship it","state":"OPEN",` +
		`"createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z","repository":{"nameWithOwner":"acme/api"},` +
		`"labels":{"nodes":[]},"assignees":{"nodes":[]}},"fieldValues":` + fieldValues + `,"changes":` + changes + `}`
}

func gitHubProjectV2ItemsReply(item string) string {
	return `{"data":{"organization":{"projectV2":{"items":{"nodes":[` + item + `],"pageInfo":` + gitHubProjectV2EndPage + `}}}}}`
}

func gitHubProjectV2NoChanges() string {
	return `{"nodes":[],"pageInfo":` + gitHubProjectV2EndPage + `}`
}

func gitHubProjectV2NumberValue(name string, value int) string {
	return `{"__typename":"ProjectV2ItemFieldNumberValue","number":` + strconv.Itoa(value) + `,"field":{"name":"` + name + `"}}`
}

// fieldValuesBoard serves one item whose fieldValues hold `total` values read
// `pageSize` at a time; the LAST value is the Estimate. The values before it are
// Number values of other fields.
func fieldValuesBoard(total, pageSize int) func(string, map[string]any) string {
	value := func(index int) string {
		if index == total-1 {
			return gitHubProjectV2NumberValue("Estimate", 5)
		}
		return gitHubProjectV2NumberValue("Field"+strconv.Itoa(index), index)
	}
	page := func(start int) string {
		end := min(start+pageSize, total)
		nodes := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			nodes = append(nodes, value(index))
		}
		return `{"nodes":[` + strings.Join(nodes, ",") + `],"pageInfo":` + gitHubProjectV2PageInfoJSON(end < total, "v"+strconv.Itoa(end)) + `}`
	}
	return func(query string, variables map[string]any) string {
		switch {
		case strings.Contains(query, "fieldValues(first: 100, after: $after)"):
			after, _ := variables["after"].(string)
			start, _ := strconv.Atoi(strings.TrimPrefix(after, "v"))
			return `{"data":{"node":{"fieldValues":` + page(start) + `}}}`
		default:
			return gitHubProjectV2ItemsReply(gitHubProjectV2ItemJSON(page(0), gitHubProjectV2NoChanges()))
		}
	}
}

// CHAOS-8777 r1 P1-2: fieldValues(first: 20) with no pageInfo dropped the 21st
// value, so the Estimate was lost and story_points came out null. 21 values read
// 20 at a time and 101 values read 100 at a time both keep the Estimate.
func TestGitHubProjectV2FetcherPagesFieldValuesSoTheEstimateSurvives(t *testing.T) {
	for _, test := range []struct{ total, pageSize int }{{21, 20}, {101, 100}} {
		t.Run(fmt.Sprintf("%d values", test.total), func(t *testing.T) {
			result, doer, err := fetchGitHubProjectV2Board(t, fieldValuesBoard(test.total, test.pageSize))
			if err != nil || len(result.Rows.WorkItems) != 1 {
				t.Fatalf("err=%v rows=%d", err, len(result.Rows.WorkItems))
			}
			points := result.Rows.WorkItems[0].StoryPoints
			if points == nil || *points != 5 {
				t.Fatalf("story_points=%v want 5 (the last value of %d)", points, test.total)
			}
			if doer.requests != 2 {
				t.Fatalf("requests=%d want 2 (items page + one continuation)", doer.requests)
			}
		})
	}
}

// Literal numbers: a fieldValues list of 100 pages in all (the embedded one
// included) passes, 101 fail closed naming the project, the item and the field.
func TestGitHubProjectV2FetcherFieldValuesBoundIsExactlyOneHundredPages(t *testing.T) {
	result, doer, err := fetchGitHubProjectV2Board(t, fieldValuesBoard(100, 1))
	if err != nil || len(result.Rows.WorkItems) != 1 || doer.requests != 100 {
		t.Fatalf("100 pages: err=%v requests=%d", err, doer.requests)
	}
	_, doer, err = fetchGitHubProjectV2Board(t, fieldValuesBoard(101, 1))
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("101 pages: err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"fieldValues of project acme#3 item PVTI_1", "after 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	if doer.requests != 100 {
		t.Fatalf("requests=%d want 100 (no request past the bound)", doer.requests)
	}
}

func changesBoard(total int) func(string, map[string]any) string {
	change := func(index int) string {
		return `{"field":{"name":"Status"},"previousValue":{"name":"Todo"},"newValue":{"name":"Doing"},` +
			`"createdAt":"2026-08-01T09:` + fmt.Sprintf("%02d:%02d", (index/60)%60, index%60) + `Z","actor":{"login":"octocat"}}`
	}
	page := func(index int) string {
		return `{"nodes":[` + change(index) + `],"pageInfo":` + gitHubProjectV2PageInfoJSON(index < total-1, "c"+strconv.Itoa(index)) + `}`
	}
	return func(query string, variables map[string]any) string {
		if strings.Contains(query, "changes(first: 100, after: $after") {
			after, _ := variables["after"].(string)
			index, _ := strconv.Atoi(strings.TrimPrefix(after, "c"))
			return `{"data":{"node":{"changes":` + page(index+1) + `}}}`
		}
		return gitHubProjectV2ItemsReply(gitHubProjectV2ItemJSON(`{"nodes":[]}`, page(0)))
	}
}

// CHAOS-8777 r1 P1-1: the per-item changes loop had no bound (101 pages were
// accepted). Literal numbers: 100 pages in all pass, 101 fail closed.
func TestGitHubProjectV2FetcherChangesBoundIsExactlyOneHundredPages(t *testing.T) {
	result, doer, err := fetchGitHubProjectV2Board(t, changesBoard(100))
	if err != nil || len(result.Rows.StatusTransitions) != 100 || doer.requests != 100 {
		t.Fatalf("100 pages: err=%v transitions=%d requests=%d", err, len(result.Rows.StatusTransitions), doer.requests)
	}
	_, doer, err = fetchGitHubProjectV2Board(t, changesBoard(101))
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("101 pages: err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"changes of project acme#3 item PVTI_1", "after 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	if doer.requests != 100 {
		t.Fatalf("requests=%d want 100", doer.requests)
	}
}

// The board's own items list is a TOP-LEVEL list: it has a runaway guard at the
// foundation paginator's top-level maximum (10,000 pages), not the nested bound.
// A board of 101 pages is fine; 10,000 pass and 10,001 fail closed.
func TestGitHubProjectV2FetcherItemsRunawayGuardIsTheTopLevelMaximum(t *testing.T) {
	board := func(total int) func(string, map[string]any) string {
		return func(query string, variables map[string]any) string {
			after, _ := variables["after"].(string)
			index, _ := strconv.Atoi(strings.TrimPrefix(after, "i"))
			if after != "" {
				index++
			}
			return `{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":` +
				gitHubProjectV2PageInfoJSON(index < total-1, "i"+strconv.Itoa(index)) + `}}}}}`
		}
	}
	if _, doer, err := fetchGitHubProjectV2Board(t, board(101)); err != nil || doer.requests != 101 {
		t.Fatalf("101 pages must pass (top-level list): err=%v requests=%d", err, doer.requests)
	}
	if _, doer, err := fetchGitHubProjectV2Board(t, board(10_000)); err != nil || doer.requests != 10_000 {
		t.Fatalf("10,000 pages: err=%v requests=%d", err, doer.requests)
	}
	_, doer, err := fetchGitHubProjectV2Board(t, board(10_001))
	if !errors.Is(err, ErrPaginationCapExceeded) || !strings.Contains(err.Error(), "items of project acme#3 still had a next page after 10000 pages") {
		t.Fatalf("10,001 pages: err=%v", err)
	}
	if doer.requests != 10_000 {
		t.Fatalf("requests=%d want 10000", doer.requests)
	}
}
