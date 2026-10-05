package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraDevStatusUnavailableCause is the typed no-op reason a caller reports
// when Jira's dev-status endpoint 400s/404s for an issue -- ruled (chris via
// team-lead, 2026-09-01) a CLEAN degradation, never an error: "if it's not
// setup to attach github <-> project management that's the user's problem"
// (team-attribution.md). Never fed into optionalIncomplete.
const jiraDevStatusUnavailableCause = "dev_status_unavailable"

// jiraDevStatusMaxRequestsPerRun bounds the N+1 REST cost this route adds:
// one extra request per issue, capped per Collect call so a large fetch_all
// backfill cannot turn "sync one project" into thousands of dev-status calls
// in a single run. Overridable via the dev_status_max_requests claim option,
// mirroring comments_limit's shape.
const jiraDevStatusMaxRequestsPerRun = 500

// jiraDevStatusApplicationTypes are the SCM applications the route asks the dev-status endpoint about, one request each
// (CHAOS-8526: GitLab was missing, so a Jira issue linked to a GitLab MR got no native link). The URL shape of each
// returned pull request, not the application type, picks the work-item id parser (jiraDevStatusPullRequestSourceID).
var jiraDevStatusApplicationTypes = []string{"GitHub", "GitLab"}

// jiraDevStatusPayload is the GET /rest/dev-status/1.0/issue/detail response
// shape for applicationType=GitHub or GitLab, dataType=pullrequest -- the
// SCM-for-Jira panel's data source and Jira's own PRIMARY provider-attached
// PR mapping (team-attribution.md's PRIMARY/FALLBACK design). Only the field
// this route consumes is modeled; the endpoint returns considerably more
// (branches, repositories, reviewers, commentCount, ...).
type jiraDevStatusPayload struct {
	Detail []struct {
		PullRequests []struct {
			URL string `json:"url"`
		} `json:"pullRequests"`
	} `json:"detail"`
}

// fetchJiraDevStatusPullRequests calls the dev-status endpoint for one
// issue's GitHub pull-request links.
//
// available=false, err=nil is the clean no-op: the org has no GitHub-for-Jira
// app configured (or configured for a different issue/project), which Jira
// reports as 400 or 404 depending on deployment. client.Do itself classifies
// any non-2xx response into a *providerfoundation.ProviderError (it never
// returns a raw 4xx *http.Response the way jiraFetchObject's callers see --
// confirmed by TestFetchJiraDevStatusPullRequestsTreats400And404AsCleanNoOp
// failing until this checked ProviderError.StatusCode instead of
// response.StatusCode), so the no-op check inspects the returned error's
// StatusCode, not a response we never receive. err!=nil (any other status,
// or a non-classified/network failure) is a genuine fetch failure and
// degrades like any other optional Jira sub-fetch (comments, sprints):
// recorded in optionalIncomplete by the caller, never blocking the whole
// issue.
func fetchJiraDevStatusPullRequests(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	applicationType string,
) (payload jiraDevStatusPayload, available bool, err error) {
	query := url.Values{
		"issueId": {issueID}, "applicationType": {applicationType}, "dataType": {"pullrequest"},
	}
	response, err := client.Do(ctx, http.MethodGet, "/rest/dev-status/1.0/issue/detail?"+query.Encode(), nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr != nil &&
			(providerErr.StatusCode == http.StatusBadRequest || providerErr.StatusCode == http.StatusNotFound) {
			return jiraDevStatusPayload{}, false, nil
		}
		return jiraDevStatusPayload{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return jiraDevStatusPayload{}, false, providerfoundation.ErrNormalizationInvalid
	}
	limited, readErr := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	if readErr != nil || len(limited) > nativeMaxObjectBytes {
		return jiraDevStatusPayload{}, false, providerfoundation.ErrNormalizationInvalid
	}
	if decodeErr := decodeJiraJSON(limited, &payload); decodeErr != nil {
		return jiraDevStatusPayload{}, false, decodeErr
	}
	return payload, true, nil
}

// jiraTrustedSCMHosts mirrors linearTrustedSCMHosts's shape and default host
// set (parity ruled, team-lead 2026-09-01) under its own env var, since this
// route's trust boundary is independently configurable from Linear's.
func jiraTrustedSCMHosts() map[string]struct{} {
	hosts, _ := jiraTrustedSCMHostsAndRoots()
	return hosts
}

func jiraTrustedSCMHostsAndRoots() (map[string]struct{}, map[string][]string) {
	return parseTrustedSCMHostEntries(os.Getenv("JIRA_TRUSTED_SCM_HOSTS"), "github.com", "www.github.com", "gitlab.com")
}

// jiraDevStatusPullRequestSourceID parses a PR/MR URL from the dev-status
// panel into the same work-item id shape every other PRIMARY producer uses:
// ghpr:owner/repo#N for GitHub (extractGitHubClosingIssueReferences,
// linearAttachmentWorkItemID) and gitlab:group/project!N for GitLab
// (normalizeGitLabMergeRequestWorkItem), trusted-host gated the same way. The
// path shape decides the provider: GitHub's `/pull/N`, GitLab's
// `/-/merge_requests/N` (or the pre-`/-/` `/merge_requests/N`).
func jiraDevStatusPullRequestSourceID(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	hosts, roots := jiraTrustedSCMHostsAndRoots()
	if _, ok := hosts[strings.ToLower(parsed.Host)]; !ok {
		return ""
	}
	parts, ok := stripSCMURLRoot(roots, parsed.Host, strings.Split(strings.Trim(parsed.Path, "/"), "/"))
	if !ok {
		return ""
	}
	if len(parts) >= 4 && parts[len(parts)-2] == "pull" {
		return "ghpr:" + strings.Join(parts[:len(parts)-2], "/") + "#" + parts[len(parts)-1]
	}
	if len(parts) >= 3 && parts[len(parts)-2] == "merge_requests" {
		project := parts[:len(parts)-2]
		if project[len(project)-1] == "-" {
			project = project[:len(project)-1]
		}
		if len(project) >= 2 {
			return "gitlab:" + strings.Join(project, "/") + "!" + parts[len(parts)-1]
		}
	}
	return ""
}

// extractJiraDevStatusDependencies emits the PRIMARY provider-attached
// PR<->issue link for Jira (CHAOS-4757): source = the GitHub PR (parsed from
// the dev-status panel), target = this Jira issue -- the same source/target
// orientation as extractGitHubClosingIssueReferences and Linear's
// extract_linear_dependencies/normalizeLinearDependencies (the PR is the edge
// SOURCE so the linked-issue inheritance resolver, build_linked_issue_team_resolver,
// attributes it to this issue's team).
func extractJiraDevStatusDependencies(
	claim Claim,
	workItemID string,
	payload jiraDevStatusPayload,
	normalizedAt time.Time,
) []jiraWorkItemDependencyRow {
	rows := make([]jiraWorkItemDependencyRow, 0)
	seen := make(map[string]struct{})
	for _, detail := range payload.Detail {
		for _, pullRequest := range detail.PullRequests {
			source := jiraDevStatusPullRequestSourceID(pullRequest.URL)
			if source == "" || source == workItemID {
				continue
			}
			if _, exists := seen[source]; exists {
				continue
			}
			seen[source] = struct{}{}
			rows = append(rows, jiraDependencyRow(
				claim, source, workItemID, "relates_to", "jira_dev_status", normalizedAt,
			))
		}
	}
	return rows
}

// jiraDevStatusCountingDoer counts actual outbound HTTP attempts, including
// every internal retry providerfoundation.HTTPClient.Do makes for one logical
// call (codex round 1, P2: HTTPClient.Do retries transient failures up to
// its RetryPolicy.MaxAttempts internally, invisible to the caller -- a naive
// "increment once per logical fetch" cap on dev_status_max_requests permits
// up to MaxAttempts times as many real wire requests during an outage, with
// no signal). Mirrors gitHubWorkItemPRSocialCountingDoer's shape
// (github_work_items_social_fetch.go), the existing precedent for this exact
// problem in this codebase.
type jiraDevStatusCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer jiraDevStatusCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts++
	return doer.delegate.Do(request)
}

// fetchJiraDevStatusPullRequestsCountingAttempts wraps
// fetchJiraDevStatusPullRequests with a counting Doer so the caller's
// dev_status_max_requests cap reflects real wire cost (including retries),
// not just logical calls. Returns the attempt count alongside the normal
// result so the caller can advance its running total by the true amount.
//
// remainingBudget caps THIS call's own retry policy to at most that many
// wire attempts (codex round 2, P2: counting attempts after the fact still
// let one issue's retries alone exceed the configured cap -- a
// dev_status_max_requests=1 budget permitted 3 real requests under
// sustained 503s, because RetryPolicy.MaxAttempts was still 3 and nothing
// stopped HTTPClient.Do's internal retry loop mid-flight). A remainingBudget
// of 0 or less, or >= the client's own RetryPolicy.MaxAttempts, leaves the
// client's policy untouched (no cap to enforce, or the client is already
// stricter).
func fetchJiraDevStatusPullRequestsCountingAttempts(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	remainingBudget int,
) (payload jiraDevStatusPayload, available bool, attempts int, outcomes []jiraDevStatusTypeOutcome, err error) {
	counted := *client
	counted.Doer = jiraDevStatusCountingDoer{delegate: client.Doer, attempts: &attempts}
	for _, applicationType := range jiraDevStatusApplicationTypes {
		budget := 0
		if remainingBudget > 0 {
			if budget = remainingBudget - attempts; budget <= 0 {
				// The cap is spent: this application type is not asked, and that is counted, not silent.
				outcomes = append(outcomes, jiraDevStatusTypeOutcome{ApplicationType: applicationType, Outcome: "cap_skipped"})
				continue
			}
		}
		call := counted
		if budget > 0 && budget < call.Retry.MaxAttempts {
			call.Retry.MaxAttempts = budget
		}
		one, ok, oneErr := fetchJiraDevStatusPullRequests(ctx, &call, issueID, applicationType)
		switch {
		case oneErr != nil:
			// A failing application type must not hide the links another one returned: the error is reported
			// (the issue is recorded incomplete) and the payload of the types that answered is still returned.
			if err == nil {
				err = oneErr
			}
			outcomes = append(outcomes, jiraDevStatusTypeOutcome{ApplicationType: applicationType, Outcome: "failed", Err: oneErr})
		case !ok:
			outcomes = append(outcomes, jiraDevStatusTypeOutcome{ApplicationType: applicationType, Outcome: jiraDevStatusUnavailableCause})
		default:
			available = true
			payload.Detail = append(payload.Detail, one.Detail...)
			pullRequests := 0
			for _, detail := range one.Detail {
				pullRequests += len(detail.PullRequests)
			}
			outcome := "synced"
			if pullRequests == 0 {
				outcome = "empty"
			}
			outcomes = append(outcomes, jiraDevStatusTypeOutcome{ApplicationType: applicationType, Outcome: outcome, PullRequests: pullRequests})
		}
	}
	return payload, available, attempts, outcomes, err
}

// jiraDevStatusTypeOutcome is the result of one dev-status request for one application type.
type jiraDevStatusTypeOutcome struct {
	ApplicationType string
	// Outcome is synced, empty (200, no pull requests), dev_status_unavailable (400/404), failed or cap_skipped.
	Outcome      string
	PullRequests int
	Err          error
}
