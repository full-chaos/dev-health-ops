package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

// The item decode sites left out of CHAOS-10292: each names the route that served its item. One cell per site class.

// notAnObjectText fails every item decoder: each decodes an object.
const notAnObjectText = `[]`

func TestGitLabTestsPipelineItemFailureNamesThePipelinesRoute(t *testing.T) {
	t.Parallel()
	_, err := selectGitLabTestsReportPipelines([]json.RawMessage{json.RawMessage(notAnObjectText)}, 1, nil)
	requireItemRouteFailure(t, err, gitlabPipelinesRoute)
}

func TestGitLabTestsJobItemFailureNamesTheJobsRoute(t *testing.T) {
	t.Parallel()
	items := []json.RawMessage{json.RawMessage(notAnObjectText)}
	runItemRouteCells(t, gitlabPipelineJobsRoute, map[string]func() error{
		"rank":       func() error { _, err := newGitLabTestsRunJobs(items); return err },
		"artifacts":  func() error { _, err := selectGitLabTestsArtifactJobs(items, 1); return err },
		"qualifying": func() error { _, err := gitLabTestsQualifyingArtifactJobs(items); return err },
	})
}

func TestGitHubPRSocialItemFailureNamesTheGraphQLRoute(t *testing.T) {
	t.Parallel()
	runItemRouteCells(t, graphQLRoute, map[string]func() error{
		"comment": func() error {
			_, err := adaptGitHubWorkItemPRSocialPayload(GitHubWorkItemPRSocialPayload{Comments: []json.RawMessage{json.RawMessage(notAnObjectText)}})
			return err
		},
		"event": func() error {
			_, err := adaptGitHubWorkItemPRSocialPayload(GitHubWorkItemPRSocialPayload{Events: []json.RawMessage{json.RawMessage(notAnObjectText)}})
			return err
		},
	})
}

// runItemRouteCells runs each cell as its own subtest, so each site is seen red on its own.
func runItemRouteCells(t *testing.T, route string, cells map[string]func() error) {
	t.Helper()
	for name, cell := range cells {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			requireItemRouteFailure(t, cell(), route)
		})
	}
}

func TestGitHubWorkItemsIssueListItemFailureNamesTheIssuesRoute(t *testing.T) {
	t.Parallel()
	fixtures := githubWorkItemsRESTFixtures()
	fixtures["/repos/acme/api/issues"] = []githubWorkItemsRESTReply{{body: `[[]]`}}
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_milestones"] = false
	_, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
		time.Now(),
	)
	requireItemRouteFailure(t, err, githubIssuesRoute)
}

func TestGitHubWorkItemsIssueEventItemFailureNamesTheEventsRoute(t *testing.T) {
	t.Parallel()
	fixtures := githubWorkItemsRESTFixtures()
	fixtures["/repos/acme/api/issues/42/events"] = []githubWorkItemsRESTReply{{body: `[[]]`}}
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_milestones"] = false
	claim.DatasetOptions["fetch_comments"] = false
	_, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
		time.Now(),
	)
	requireItemRouteFailure(t, err, githubIssueEventsRoute)
}

func TestGitHubWorkItemRowItemFailuresNameTheirRoute(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	names := sameGitHubRepoNames("acme/api")
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	at := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	issue := json.RawMessage(`{"number":42,"title":"Repair","state":"open","created_at":"2026-08-01T08:00:00Z","updated_at":"2026-08-03T09:30:00Z"}`)
	pull := json.RawMessage(`{"number":17,"title":"Repair","state":"open","created_at":"2026-08-01T08:00:00Z","updated_at":"2026-08-03T09:30:00Z","head":{"ref":"main"}}`)
	bad := []json.RawMessage{json.RawMessage(notAnObjectText)}
	// The dependency scan re-reads each comment with json.Unmarshal, which refuses the trailing value the comment reader allows.
	trailing := []json.RawMessage{json.RawMessage(`{"id":1,"body":"x"} []`)}
	issueBundle := func(raw json.RawMessage, comments []json.RawMessage) func() error {
		return func() error {
			_, err := normalizeGitHubIssueBundle(claim, names, repoID, raw, nil, comments, nil, at)
			return err
		}
	}
	pullBundle := func(raw json.RawMessage, events, refs []json.RawMessage) func() error {
		return func() error {
			_, err := normalizeGitHubPullRequestBundle(claim, names, repoID, raw, events, nil, refs, nil, at)
			return err
		}
	}
	runItemRouteCells(t, githubIssuesRoute, map[string]func() error{"issue": issueBundle(json.RawMessage(notAnObjectText), nil)})
	runItemRouteCells(t, githubIssueCommentRoute, map[string]func() error{
		"issue-comment": issueBundle(issue, bad), "issue-comment-dependency": issueBundle(issue, trailing),
	})
	runItemRouteCells(t, githubPullRoute, map[string]func() error{"pull": pullBundle(json.RawMessage(notAnObjectText), nil, nil)})
	runItemRouteCells(t, graphQLRoute, map[string]func() error{
		"pull-event": pullBundle(pull, bad, nil), "pull-closing-reference": pullBundle(pull, nil, bad),
	})
	runItemRouteCells(t, githubMilestonesRoute, map[string]func() error{"milestone": func() error {
		_, err := normalizeGitHubSprint(claim, names.Identity, json.RawMessage(notAnObjectText), at)
		return err
	}})
}

func TestLaunchDarklyItemFailuresNameTheirRoute(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	items := []json.RawMessage{json.RawMessage(notAnObjectText)}
	runItemRouteCells(t, "/api/v2/flags/{project}", map[string]func() error{"flag": func() error {
		_, err := normalizeLaunchDarklyFlags(items, "org-acme", "production", at)
		return err
	}})
	runItemRouteCells(t, "/api/v2/auditlog", map[string]func() error{"audit-event": func() error {
		_, err := normalizeLaunchDarklyEvents(items, "org-acme", "production", at)
		return err
	}})
}
