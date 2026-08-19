package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabPullRequestResponse struct {
	status  int
	body    string
	headers http.Header
}

type gitLabPullRequestDoer struct {
	t         *testing.T
	responses []gitLabPullRequestResponse
	requests  []*http.Request
}

func (doer *gitLabPullRequestDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected GitLab request %s", request.URL.RequestURI())
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	headers := response.headers
	if headers == nil {
		headers = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func gitLabPullRequestFixtureResponses() []gitLabPullRequestResponse {
	return []gitLabPullRequestResponse{
		{body: gitLabRepositoryFixture},
		{body: `[
			{"iid":99,"title":"future","state":"opened","updated_at":"2026-08-01T10:00:00Z"},
			{"iid":7,"title":"Add API","description":"body","state":"opened","author":{"username":"author"},"created_at":"2026-07-15T10:00:00Z","updated_at":"2026-07-20T10:00:00Z","source_branch":"feature","target_branch":"main","user_notes_count":"4"}
		]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `{"approved_by":[{"user":{"id":77,"username":"reviewer"}},{"user":{"id":88,"username":"approver-only"}}]}`},
		{body: `[
			{"id":1,"system":true,"body":"approved this merge request","author":{"username":"reviewer"},"created_at":"2026-07-16T11:00:00Z"},
			{"id":2,"system":false,"type":"DiffNote","body":"please change this","author":{"username":"reviewer2"},"created_at":"2026-07-17T11:00:00Z"}
		]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `[
			{"id":3,"system":false,"type":"DiffNote","body":"author reply","author":{"username":"author"},"created_at":"2026-07-18T11:00:00Z"},
			{"id":4,"system":false,"type":"DiscussionNote","body":"ordinary chatter","author":{"username":"reviewer3"},"created_at":"2026-07-18T12:00:00Z"}
		]`, headers: http.Header{"X-Next-Page": []string{"3"}}},
		{body: `[
			{"id":5,"system":true,"body":"unapproved this merge request","author":{"username":"reviewer"},"created_at":"2026-07-19T11:00:00Z"}
		]`},
		{body: `[
			{"iid":8,"title":"Ship API","description":null,"state":"merged","author":{"username":"author2"},"created_at":"2026-07-05T10:00:00Z","updated_at":"2026-07-10T10:00:00Z","merged_at":"2026-07-22T10:00:00Z","closed_at":"2026-07-22T10:00:00Z","source_branch":"release","target_branch":"main","user_notes_count":0},
			{"iid":6,"title":"old","state":"closed","updated_at":"2026-06-30T10:00:00Z"}
		]`, headers: http.Header{"X-Next-Page": []string{"3"}}},
		{status: http.StatusNotFound, body: `{"message":"approvals unavailable"}`},
		{body: `[{"id":6,"system":false,"type":"DiffNote","body":"looks good","author":{"username":"reviewer4"},"created_at":"2026-07-11T11:00:00Z"}]`},
	}
}

func TestGitLabPullRequestRouteMirrorsPythonFamilyAndEarlyStop(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.UTC)
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		dataset := dataset
		t.Run(dataset, func(t *testing.T) {
			doer := &gitLabPullRequestDoer{t: t, responses: gitLabPullRequestFixtureResponses()}
			batch, err := (GitLabPullRequestRouteHandler{PerPage: 2}).Collect(
				context.Background(), nativeTestClaim("gitlab", dataset),
				providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Effects) != 2 || batch.Effects[0].Destination != "git_pull_requests" ||
				batch.Effects[1].Destination != "git_pull_request_reviews" ||
				batch.Effects[0].Recovery != EffectReadbackRequired || batch.Effects[1].Recovery != EffectReadbackRequired {
				t.Fatalf("effects=%+v", batch.Effects)
			}
			if len(batch.Effects[0].Rows) != 2 || len(batch.Effects[1].Rows) != 5 {
				t.Fatalf("rows prs=%d reviews=%d", len(batch.Effects[0].Rows), len(batch.Effects[1].Rows))
			}
			if batch.Watermark == nil || !batch.Watermark.Equal(*nativeTestClaim("gitlab", dataset).BeforeAt) ||
				batch.Evidence.Requests != 9 || batch.Evidence.Pages != 6 || batch.Evidence.Records != 7 {
				t.Fatalf("evidence=%+v watermark=%v", batch.Evidence, batch.Watermark)
			}
			var row pullRequestRow
			if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
				t.Fatal(err)
			}
			if row.Number != 7 || row.Title == nil || *row.Title != "Add API" || row.Body == nil || *row.Body != "body" ||
				row.ReviewsCount != 4 || row.CommentsCount != 4 || row.FirstReviewAt == nil ||
				!row.FirstReviewAt.Equal(time.Date(2026, 7, 16, 11, 0, 0, 0, time.UTC)) ||
				row.ChangesRequestedCount != 0 || row.State != "open" || row.OrgID != "org-acme" ||
				!row.LastSynced.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
				t.Fatalf("row=%+v", row)
			}
			var merged pullRequestRow
			if err := json.Unmarshal(batch.Effects[0].Rows[1], &merged); err != nil {
				t.Fatal(err)
			}
			if merged.Number != 8 || merged.State != "merged" || merged.MergedAt == nil || merged.Body != nil {
				t.Fatalf("merged=%+v", merged)
			}
			states := make(map[string]int)
			ids := make(map[string]bool)
			for _, raw := range batch.Effects[1].Rows {
				var review pullRequestReviewRow
				if err := json.Unmarshal(raw, &review); err != nil {
					t.Fatal(err)
				}
				states[review.State]++
				ids[review.ReviewID] = true
			}
			if states["APPROVED"] != 2 || states["COMMENTED"] != 2 || states["DISMISSED"] != 1 ||
				!ids["note-1"] || !ids["note-2"] || !ids["note-5"] || !ids["approval-88"] || !ids["note-6"] {
				t.Fatalf("review states=%v ids=%v", states, ids)
			}
			for _, request := range doer.requests {
				if strings.Contains(request.URL.Path, "merge_requests/6/") || strings.Contains(request.URL.Path, "merge_requests/99/") {
					t.Fatalf("fetched social data for out-of-window MR: %s", request.URL.Path)
				}
			}
			if got := doer.requests[6].URL.RequestURI(); got != "/api/v4/projects/123/merge_requests?order_by=updated_at&page=2&per_page=2&sort=desc&state=all" {
				t.Fatalf("page-2 request=%q", got)
			}
		})
	}
}

func TestGitLabPullRequestRouteFailsClosedWhenNotesAreIncomplete(t *testing.T) {
	t.Parallel()
	doer := &gitLabPullRequestDoer{t: t, responses: []gitLabPullRequestResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"iid":7,"title":"Add API","state":"opened","author":{"username":"author"},"created_at":"2026-07-15T10:00:00Z","updated_at":"2026-07-20T10:00:00Z"}` + `]`},
		{body: `{}`},
		{status: http.StatusServiceUnavailable, body: `{"message":"notes unavailable"}`},
	}}
	batch, err := (GitLabPullRequestRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "prs"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitLabPullRequestReviewsIncomplete) {
		t.Fatalf("error=%v want incomplete review error", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("batch=%+v: incomplete notes must not produce durable effects or watermark", batch)
	}
}

func TestGitLabPullRequestRouteFailsClosedOnPaginationCap(t *testing.T) {
	t.Parallel()
	doer := &gitLabPullRequestDoer{t: t, responses: []gitLabPullRequestResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"iid":7,"state":"opened","updated_at":"2026-07-20T10:00:00Z"}]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `{}`},
		{body: `[]`},
	}}
	batch, err := (GitLabPullRequestRouteHandler{MaxPages: 1}).Collect(
		context.Background(), nativeTestClaim("gitlab", "pr-comments"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) || batch.Watermark != nil {
		t.Fatalf("error=%v batch=%+v", err, batch)
	}
}

func TestGitLabPullRequestRouteRejectsInvalidConfigurationBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		claim      Claim
		clientKind string
		handler    GitLabPullRequestRouteHandler
		normalized time.Time
	}{
		{name: "wrong_claim_provider", claim: nativeTestClaim("github", "prs"), normalized: time.Now()},
		{name: "wrong_claim_dataset", claim: nativeTestClaim("gitlab", "commits"), normalized: time.Now()},
		{name: "wrong_client_provider", claim: nativeTestClaim("gitlab", "prs"), clientKind: "wrong_provider", normalized: time.Now()},
		{name: "nil_client", claim: nativeTestClaim("gitlab", "prs"), clientKind: "nil", normalized: time.Now()},
		{name: "nil_client_base_URL", claim: nativeTestClaim("gitlab", "prs"), clientKind: "nil_base_url", normalized: time.Now()},
		{name: "zero_normalized_at", claim: nativeTestClaim("gitlab", "prs"), normalized: time.Time{}},
		{name: "negative_per_page", claim: nativeTestClaim("gitlab", "prs"), handler: GitLabPullRequestRouteHandler{PerPage: -1}, normalized: time.Now()},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabPullRequestDoer{t: t, responses: []gitLabPullRequestResponse{{body: gitLabRepositoryFixture}}}
			claim := test.claim
			if test.name == "wrong_claim_provider" {
				// Keep the source identity valid for the GitLab traversal so a
				// removed provider guard reaches the provider request rather than
				// being masked by the later numeric-project validation.
				claim.SourceExternalID = "123"
			}
			var client *providerfoundation.HTTPClient
			switch test.clientKind {
			case "nil":
				client = nil
			case "nil_base_url":
				client = &providerfoundation.HTTPClient{Provider: "gitlab"}
			default:
				client = gitLabRepositoryClient(t, doer, "https://gitlab.example")
				if test.clientKind == "wrong_provider" {
					client.Provider = "github"
				}
			}
			_, err := test.handler.Collect(context.Background(), claim, providerfoundation.Credential{}, client, test.normalized)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want invalid configuration", err)
			}
			if len(doer.requests) != 0 {
				t.Fatalf("invalid configuration made %d requests", len(doer.requests))
			}
		})
	}
}

func TestGitLabPullRequestRouteRejectsProjectIDMismatchBeforeListRequests(t *testing.T) {
	t.Parallel()
	project := strings.Replace(gitLabRepositoryFixture, `"id": 123`, `"id": 124`, 1)
	doer := &gitLabPullRequestDoer{t: t, responses: []gitLabPullRequestResponse{{body: project}}}
	_, err := (GitLabPullRequestRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "prs"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now(),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v want project identity normalization failure", err)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("project mismatch made %d requests; want only project fetch", len(doer.requests))
	}
}

func TestGitLabPullRequestSocialEffectsAcceptEveryAliasAndTenantFence(t *testing.T) {
	t.Parallel()
	sink := GitLabPullRequestSocialClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	pullEffect, err := effectBatchFromValues("git_pull_requests", EffectReadbackRequired, []pullRequestRow(nil))
	if err != nil {
		t.Fatal(err)
	}
	reviewEffect, err := effectBatchFromValues("git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow(nil))
	if err != nil {
		t.Fatal(err)
	}
	row := pullRequestReadbackFixture(time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC))
	nonEmptyReviewEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{{
			OrgID: "org-acme", RepoID: row.RepoID, Number: row.Number,
			ReviewID: "review-1", Reviewer: "reviewer", State: "APPROVED",
			SubmittedAt: row.CreatedAt, LastSynced: row.LastSynced,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, dataset := range []string{"prs", "pr-reviews", "pr-comments"} {
		t.Run(dataset, func(t *testing.T) {
			claim := nativeTestClaim("gitlab", dataset)
			if err := sink.WriteEffect(context.Background(), claim, pullEffect); err != nil {
				t.Fatalf("pull effect: %v", err)
			}
			if err := sink.WriteEffect(context.Background(), claim, reviewEffect); err != nil {
				t.Fatalf("review effect: %v", err)
			}
			if err := sink.WriteEffect(context.Background(), claim, nonEmptyReviewEffect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("non-empty review effect error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
	wrongProvider := nativeTestClaim("github", "prs")
	if err := sink.WriteEffect(context.Background(), wrongProvider, pullEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign provider error=%v want ErrInvalidConfiguration", err)
	}
	if err := sink.WriteEffect(context.Background(), wrongProvider, reviewEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign provider review error=%v want ErrInvalidConfiguration", err)
	}
	wrongDataset := nativeTestClaim("gitlab", "commits")
	if err := sink.WriteEffect(context.Background(), wrongDataset, reviewEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign dataset review error=%v want ErrInvalidConfiguration", err)
	}
}
