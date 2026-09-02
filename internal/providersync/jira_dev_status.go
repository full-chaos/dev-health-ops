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

// jiraDevStatusPayload is the GET /rest/dev-status/1.0/issue/detail response
// shape for applicationType=GitHub, dataType=pullrequest -- the
// GitHub-for-Jira panel's data source and Jira's own PRIMARY provider-attached
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
) (payload jiraDevStatusPayload, available bool, err error) {
	query := url.Values{
		"issueId": {issueID}, "applicationType": {"GitHub"}, "dataType": {"pullrequest"},
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
	hosts := map[string]struct{}{
		"github.com": {}, "www.github.com": {},
	}
	for _, value := range strings.Split(os.Getenv("JIRA_TRUSTED_SCM_HOSTS"), ",") {
		if host := strings.ToLower(strings.TrimSpace(value)); host != "" {
			hosts[host] = struct{}{}
		}
	}
	return hosts
}

// jiraDevStatusPullRequestSourceID parses a GitHub PR URL from the dev-status
// panel into the same ghpr:owner/repo#N work-item id shape every other
// PRIMARY producer uses (extractGitHubClosingIssueReferences,
// linearAttachmentWorkItemID), trusted-host gated the same way. Only GitHub
// URLs: this route only ever requests applicationType=GitHub.
func jiraDevStatusPullRequestSourceID(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	if _, ok := jiraTrustedSCMHosts()[strings.ToLower(parsed.Host)]; !ok {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) >= 4 && parts[len(parts)-2] == "pull" {
		return "ghpr:" + strings.Join(parts[:len(parts)-2], "/") + "#" + parts[len(parts)-1]
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
