package providersync

import (
	"context"
	"testing"
	"time"
)

type githubWorkItemsRESTSelectionOracleRow struct {
	IncludeIssues        bool     `json:"include_issues"`
	IncludePullRequests  bool     `json:"include_pull_requests"`
	FetchComments        bool     `json:"fetch_comments"`
	FetchMilestones      bool     `json:"fetch_milestones"`
	CommentsLimit        int      `json:"comments_limit"`
	SelectedWorkItemIDs  []string `json:"selected_work_item_ids"`
	IssueCalls           int      `json:"issue_calls"`
	PRCalls              int      `json:"pr_calls"`
	MilestoneCalls       int      `json:"milestone_calls"`
	CommentCalls         int      `json:"comment_calls"`
	EventLimit           int      `json:"event_limit"`
	CommentLimitObserved int      `json:"comment_limit_observed"`
}

func TestGitHubWorkItemsRESTSelectionMatchesLivePythonProducer(t *testing.T) {
	cases := []oracleCase{
		{ID: "all_optional_controls", Input: map[string]any{
			"sync_prs": false,
			"dataset_options": map[string]any{
				"fetch_comments": true, "fetch_milestones": true, "comments_limit": 37,
			},
		}},
		{ID: "optional_fetches_disabled_prs_enabled", Input: map[string]any{
			"sync_prs": true,
			"dataset_options": map[string]any{
				"fetch_comments": false, "fetch_milestones": false, "comments_limit": 0,
			},
		}},
	}
	compareRowsAgainstPythonOracle(
		t, "github/work-items/rest-selection", cases,
		buildGitHubWorkItemsRESTSelectionOracleRow, nil,
	)
}

func buildGitHubWorkItemsRESTSelectionOracleRow(
	t *testing.T,
	input map[string]any,
) githubWorkItemsRESTSelectionOracleRow {
	t.Helper()
	datasetOptions, ok := input["dataset_options"].(map[string]any)
	if !ok {
		t.Fatalf("dataset_options=%T", input["dataset_options"])
	}
	claim := githubWorkItemsRESTClaim()
	claim.ProcessorFlags["sync_prs"], _ = input["sync_prs"].(bool)
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": claim.ProcessorFlags["sync_prs"],
		"fetch_comments":   datasetOptions["fetch_comments"],
		"fetch_milestones": datasetOptions["fetch_milestones"],
		"comments_limit":   datasetOptions["comments_limit"],
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTSelectionFixtures(claim)}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, doer, "https://api.github.com"),
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	options, err := githubWorkItemsRESTOptionsForClaim(claim)
	if err != nil {
		t.Fatal(err)
	}
	row := githubWorkItemsRESTSelectionOracleRow{
		IncludeIssues: options.includeIssues, IncludePullRequests: options.includePullRequests,
		FetchComments: options.fetchComments, FetchMilestones: options.fetchMilestones,
		CommentsLimit:  options.commentsLimit,
		IssueCalls:     countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/issues?"),
		PRCalls:        countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/pulls?"),
		MilestoneCalls: countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/milestones?"),
		CommentCalls:   countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/issues/1/comments?"),
		EventLimit:     githubWorkItemEventLimit, CommentLimitObserved: -1,
	}
	if row.CommentCalls > 0 {
		row.CommentLimitObserved = options.commentsLimit
	}
	for _, workItem := range result.Rows.WorkItems {
		row.SelectedWorkItemIDs = append(row.SelectedWorkItemIDs, workItem.WorkItemID)
	}
	return row
}

func githubWorkItemsRESTSelectionFixtures(claim Claim) map[string][]githubWorkItemsRESTReply {
	replies := map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api": {{body: `{"full_name":"acme/api"}`}},
		"/repos/acme/api/issues": {{body: `[
			{"number":1,"title":"Issue 1","state":"open","body":"","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z","html_url":"https://github.com/acme/api/issues/1","labels":[],"assignees":[],"user":{"login":"reporter","type":"User"}},
			{"number":2,"title":"Issue 2","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"},
			{"number":3,"title":"PR stub","updated_at":"2026-07-20T00:00:00Z","pull_request":{"url":"stub"}}
		]`}},
		"/repos/acme/api/issues/1/events": {{body: `[]`}},
	}
	if claim.DatasetOptions["fetch_milestones"] == true {
		replies["/repos/acme/api/milestones"] = []githubWorkItemsRESTReply{{body: `[]`}}
	}
	if claim.DatasetOptions["fetch_comments"] == true {
		replies["/repos/acme/api/issues/1/comments"] = []githubWorkItemsRESTReply{{body: `[]`}}
	}
	if claim.DatasetOptions["include_pull_requests"] == true {
		replies["/repos/acme/api/pulls"] = []githubWorkItemsRESTReply{{body: `[]`}}
	}
	return replies
}
