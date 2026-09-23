package syncbudget

import (
	"math/big"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Dataset keys (sync/datasets.py DatasetKey) the estimators branch on.
const (
	datasetRepoMetadata       = "repo-metadata"
	datasetCommits            = "commits"
	datasetCommitStats        = "commit-stats"
	datasetFiles              = "files"
	datasetBlame              = "blame"
	datasetPRs                = "prs"
	datasetPRReviews          = "pr-reviews"
	datasetPRComments         = "pr-comments"
	datasetCICD               = "cicd"
	datasetTests              = "tests"
	datasetDeployments        = "deployments"
	datasetIncidents          = "incidents"
	datasetSecurity           = "security"
	datasetWorkItems          = "work-items"
	datasetWorkItemLabels     = "work-item-labels"
	datasetWorkItemProjects   = "work-item-projects"
	datasetWorkItemHistory    = "work-item-history"
	datasetWorkItemComments   = "work-item-comments"
	datasetFeatureFlags       = "feature-flags"
	maxJSMIncidentCandidates  = 100_000
	pagerDutyDefaultCap       = 100
	pagerDutyIncidentPages    = 2
	pagerDutyPageSize         = 100
	launchDarklyDefaultURL    = "https://app.launchdarkly.com"
	launchDarklyDefaultHost   = "app.launchdarkly.com"
	gitHubDefaultURL          = "https://api.github.com"
	gitHubDefaultHost         = "api.github.com"
	gitLabDefaultURL          = "https://gitlab.com"
	gitLabDefaultHost         = "gitlab.com"
	jiraDefaultURL            = "https://atlassian.net"
	jiraDefaultHost           = "atlassian.net"
	linearDefaultURL          = "https://api.linear.app/graphql"
	linearDefaultHost         = "api.linear.app"
	pagerDutyHostUS           = "api.pagerduty.com"
	pagerDutyHostEU           = "api.eu.pagerduty.com"
	pagerDutyIncidentsFamily  = "pagerduty_incidents"
	pagerDutyEnrichmentNote   = "PagerDuty enrichment fan-out capped per sync unit"
	pagerDutyPaginationNote   = "PagerDuty incident pagination"
	pagerDutyOffsetPagination = "PagerDuty offset pagination"
)

func in(value string, set ...string) bool {
	for _, candidate := range set {
		if value == candidate {
			return true
		}
	}
	return false
}

// --- github (providers/github/budget.py) ---

func estimateGitHub(context Context) ([]Estimate, error) {
	scope := fallbackScope(context)
	if mapping, ok := credentialMapping(context); ok {
		candidate := newObject()
		copyPresent(candidate, mapping, "app_id", "installation_id")
		if baseURL := baseURLOf(mapping); baseURL != nil {
			candidate.set("base_url", baseURL)
		}
		if token := get(mapping, "token"); truthy(token) {
			digest, err := sha256Hex(pyStr(token))
			if err != nil {
				return nil, err
			}
			candidate.set("token_sha256", digest)
		}
		if len(candidate.keys) > 0 {
			scope = candidate
		}
	}
	host, err := hostFrom(context, gitHubDefaultURL, gitHubDefaultHost, "base_url", "baseUrl")
	if err != nil {
		return nil, err
	}
	bucket := bucketsFor("github", context, host, fingerprintOf(scope))
	span := windowSpanDays(context)
	key := context.DatasetKey
	switch {
	case key == datasetRepoMetadata:
		return []Estimate{newEstimate(bucket(DimensionRESTCore), 1, confidenceHigh, "repo")}, nil
	case key == datasetCommits:
		return []Estimate{newEstimate(bucket(DimensionRESTCore), scaledUnits(2, span), confidenceMedium, "git")}, nil
	case key == datasetCommitStats:
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), scaledUnits(4, span), confidenceLow, "commit_stats"),
			newEstimate(bucket(DimensionContentsBlob), scaledUnits(2, span), confidenceLow, "commit_stats",
				"commit-file expansion varies by commit volume"),
		}, nil
	case key == datasetFiles:
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), scaledUnits(3, span), confidenceLow, "files"),
			newEstimate(bucket(DimensionContentsBlob), scaledUnits(5, span), confidenceLow, "files",
				"repository tree/blob expansion is high variance"),
		}, nil
	case key == datasetBlame:
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), 3, confidenceLow, "blame"),
			newEstimate(bucket(DimensionContentsBlob), scaledUnits(8, span), confidenceLow, "blame",
				"blame expansion is file-count dependent"),
		}, nil
	case in(key, datasetPRs, datasetPRReviews, datasetPRComments):
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), scaledUnits(2, span), confidenceMedium, "prs"),
			newEstimate(bucket(DimensionGraphQLCost), scaledUnits(4, span), confidenceMedium, "pr_social"),
			newEstimate(bucket(DimensionSecondaryAbuseRisk), 1, confidenceLow, "pr_social",
				"timeline/social expansion may trigger secondary limits"),
		}, nil
	case in(key, datasetCICD, datasetTests, datasetDeployments):
		routeFamily := key
		if key == datasetCICD {
			routeFamily = datasetTests
		}
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), scaledUnits(4, span), confidenceLow, routeFamily),
			newEstimate(bucket(DimensionContentsBlob), scaledUnits(2, span), confidenceLow, routeFamily,
				"workflow artifact expansion varies by repository activity"),
		}, nil
	case key == datasetSecurity:
		return []Estimate{newEstimate(bucket(DimensionRESTCore), scaledUnits(2, span), confidenceLow, "security")}, nil
	case in(key, datasetWorkItems, datasetWorkItemLabels, datasetWorkItemProjects, datasetWorkItemHistory, datasetWorkItemComments):
		floor := 1
		if key == datasetWorkItems {
			floor = 2
		}
		estimates := []Estimate{newEstimate(bucket(DimensionRESTCore), scaledUnits(floor, span), confidenceMedium, "work_items")}
		if flagEnabled(context, "sync_prs", false) {
			estimates = append(estimates,
				newEstimate(bucket(DimensionGraphQLCost), scaledUnits(3, span), confidenceMedium, "work_item_prs"),
				newEstimate(bucket(DimensionSecondaryAbuseRisk), 1, confidenceLow, "work_item_prs",
					"PR work-item expansion shares social/timeline pressure"))
		}
		return estimates, nil
	}
	return []Estimate{}, nil
}

// --- gitlab (providers/gitlab/budget.py) ---

func estimateGitLab(context Context) ([]Estimate, error) {
	scope := fallbackScope(context)
	if mapping, ok := credentialMapping(context); ok {
		candidate := newObject()
		copyPresent(candidate, mapping, "user_id", "username", "group_id", "project_id")
		if baseURL := baseURLOf(mapping); baseURL != nil {
			candidate.set("base_url", baseURL)
		}
		token := firstTruthy(get(mapping, "token"), get(mapping, "private_token"), get(mapping, "access_token"))
		if truthy(token) {
			digest, err := sha256Hex(pyStr(token))
			if err != nil {
				return nil, err
			}
			candidate.set("token_sha256", digest)
		}
		if len(candidate.keys) > 0 {
			scope = candidate
		}
	}
	host, err := hostFrom(context, gitLabDefaultURL, gitLabDefaultHost, "gitlab_url", "url", "base_url", "baseUrl")
	if err != nil {
		return nil, err
	}
	bucket := bucketsFor("gitlab", context, host, fingerprintOf(scope))
	span := windowSpanDays(context)
	key := context.DatasetKey
	rest := func(units int, confidence, family string, notes ...string) Estimate {
		return newEstimate(bucket(DimensionRESTCore), units, confidence, family, notes...)
	}
	switch {
	case key == datasetRepoMetadata:
		return []Estimate{rest(1, confidenceHigh, "project")}, nil
	case key == datasetCommits:
		return []Estimate{rest(scaledUnits(2, span), confidenceMedium, "project")}, nil
	case key == datasetCommitStats:
		return []Estimate{rest(scaledUnits(4, span), confidenceLow, "project", "commit detail expansion varies by commit volume")}, nil
	case in(key, datasetFiles, datasetBlame):
		floor := 3
		if key == datasetBlame {
			floor = 5
		}
		return []Estimate{rest(scaledUnits(floor, span), confidenceLow, "project", "repository file expansion is high variance")}, nil
	case in(key, datasetPRs, datasetPRReviews):
		return []Estimate{rest(scaledUnits(4, span), confidenceMedium, "merge_requests", "merge request iterators are pagination-heavy")}, nil
	case key == datasetPRComments:
		return []Estimate{
			rest(scaledUnits(4, span), confidenceMedium, "merge_requests", "merge request iterators are pagination-heavy"),
			rest(scaledUnits(3, span), confidenceLow, "notes", "MR note expansion varies by discussion volume"),
		}, nil
	case in(key, datasetCICD, datasetTests, datasetDeployments):
		return []Estimate{rest(scaledUnits(6, span), confidenceLow, "pipelines", "pipeline job expansion varies by pipeline volume")}, nil
	case key == datasetIncidents:
		return []Estimate{
			rest(1, confidenceHigh, "project"),
			rest(scaledUnits(1, span), confidenceMedium, "issues", "native incident issue pages vary by incident volume"),
		}, nil
	case key == datasetSecurity:
		return []Estimate{rest(2, confidenceLow, "project")}, nil
	case key == datasetWorkItemLabels:
		return []Estimate{rest(1, confidenceMedium, "issues")}, nil
	case key == datasetWorkItemProjects:
		return []Estimate{rest(2, confidenceMedium, "milestones", "project and group milestone iterators may both run")}, nil
	case in(key, datasetWorkItems, datasetWorkItemHistory, datasetWorkItemComments):
		estimates := []Estimate{
			rest(1, confidenceHigh, "project"),
			rest(2, confidenceMedium, "milestones", "project milestone iterator runs before work-item fetch"),
			rest(scaledUnits(4, span), confidenceLow, "epics", "group epic expansion requires premium APIs when available"),
			rest(scaledUnits(4, span), confidenceMedium, "issues", "issue iterator and per-issue events are pagination-heavy"),
			// The Python branch appends notes for all three dataset keys of
			// this case.
			rest(scaledUnits(3, span), confidenceLow, "notes", "issue/MR notes and state events vary by activity"),
		}
		if flagEnabled(context, "sync_prs", true) {
			estimates = append(estimates,
				rest(scaledUnits(4, span), confidenceMedium, "merge_requests", "MR iterator runs alongside issue ingestion by default"))
		}
		return estimates, nil
	case key == datasetFeatureFlags:
		return []Estimate{rest(scaledUnits(2, span), confidenceLow, "project", "GitLab feature-flag APIs share the REST core budget")}, nil
	}
	return []Estimate{}, nil
}

// --- jira (providers/jira/budget.py) ---

var jiraEnvFlags = map[string]string{
	"jira_fetch_worklogs":   "JIRA_FETCH_WORKLOGS",
	"fetch_worklogs":        "JIRA_FETCH_WORKLOGS",
	"atlassian_gql_enabled": "ATLASSIAN_GQL_ENABLED",
	"gql_enabled":           "ATLASSIAN_GQL_ENABLED",
}

func getenv(context Context, name string) string {
	if context.Getenv == nil {
		return ""
	}
	return context.Getenv(name)
}

// jiraFlagEnabled is _flag_enabled: any named processor flag, else any of
// their environment variables set to 1/true/yes/on.
func jiraFlagEnabled(context Context, names ...string) bool {
	for _, name := range names {
		if flagEnabled(context, name, false) {
			return true
		}
	}
	for _, name := range names {
		env, ok := jiraEnvFlags[name]
		if !ok {
			continue
		}
		value := pythonparity.Lower(pythonparity.Strip(getenv(context, env)))
		if in(value, "1", "true", "yes", "on") {
			return true
		}
	}
	return false
}

// normalizeJiraBaseURL is _normalize_base_url.
func normalizeJiraBaseURL(value string) string {
	url := strings.TrimRight(pythonparity.Strip(value), "/")
	switch {
	case url == "":
		return url
	case strings.HasPrefix(url, "http://"):
		return "https://" + url[len("http://"):]
	case strings.HasPrefix(url, "https://"):
		return url
	}
	return "https://" + strings.TrimLeft(url, "/")
}

func estimateJira(context Context) ([]Estimate, error) {
	scope := fallbackScope(context)
	mapping, isMapping := credentialMapping(context)
	if isMapping {
		candidate := newObject()
		copyPresent(candidate, mapping, "email", "cloud_id", "cloudId", "client_id", "clientId")
		baseURL := firstTruthy(get(mapping, "base_url"), get(mapping, "baseUrl"), get(mapping, "jira_base_url"), get(mapping, "jiraBaseUrl"))
		if baseURL != nil {
			candidate.set("base_url", normalizeJiraBaseURL(pyStr(baseURL)))
		}
		for _, secretKey := range []string{"api_token", "apiToken", "access_token", "accessToken", "refresh_token", "refreshToken"} {
			if value := get(mapping, secretKey); truthy(value) {
				digest, err := sha256Hex(pyStr(value))
				if err != nil {
					return nil, err
				}
				candidate.set(secretKey+"_sha256", digest)
			}
		}
		if len(candidate.keys) > 0 {
			scope = candidate
		}
	}
	// base_url = os.getenv("ATLASSIAN_JIRA_BASE_URL") or os.getenv("JIRA_BASE_URL"),
	// then a truthy credential value overrides it.
	baseURL := getenv(context, "ATLASSIAN_JIRA_BASE_URL")
	if baseURL == "" {
		baseURL = getenv(context, "JIRA_BASE_URL")
	}
	if isMapping {
		raw := firstTruthy(get(mapping, "base_url"), get(mapping, "baseUrl"), get(mapping, "jira_base_url"), get(mapping, "jiraBaseUrl"))
		if truthy(raw) {
			baseURL = pyStr(raw)
		}
	}
	if baseURL == "" {
		baseURL = jiraDefaultURL
	}
	host, ok, err := urlHostname(normalizeJiraBaseURL(baseURL))
	if err != nil {
		return nil, err
	}
	if !ok {
		host = jiraDefaultHost
	}
	bucket := bucketsFor("jira", context, host, fingerprintOf(scope))
	span := windowSpanDays(context)
	key := context.DatasetKey
	switch {
	case in(key, datasetWorkItemLabels, datasetWorkItemProjects):
		return []Estimate{newEstimate(bucket(DimensionRESTCore), 1, confidenceHigh, "jira_metadata")}, nil
	case key == datasetIncidents:
		return []Estimate{
			newEstimate(bucket(DimensionRESTCore), scaledUnits(1, span), confidenceMedium, "jira_metadata",
				"JSM incident collection enumerates service desks before enhanced JQL search"),
			newEstimate(bucket(DimensionSearch), scaledUnits(1, span), confidenceMedium, "jira_jql",
				"JSM incident collection fetches incident issues through enhanced JQL search"),
			newEstimate(bucket(DimensionRESTCore), maxJSMIncidentCandidates, confidenceLow, "jira_jsm_incident_admission",
				"JSM native incident admission performs one bounded GET per JQL candidate; "+
					"the client planner caps candidates at 100000 per sync unit"),
		}, nil
	case !in(key, datasetWorkItems, datasetWorkItemHistory, datasetWorkItemComments):
		return []Estimate{}, nil
	}
	estimates := []Estimate{
		newEstimate(bucket(DimensionSearch), scaledUnits(2, span), confidenceMedium, "jira_jql",
			"Jira work-item listing uses REST /search/jql pagination"),
		newEstimate(bucket(DimensionRESTCore), scaledUnits(2, span), confidenceMedium, "jira_issue_enrichment",
			"per-issue changelog/comment/sprint enrichment varies by issue count"),
	}
	if key == datasetWorkItemComments {
		estimates = append(estimates, newEstimate(bucket(DimensionRESTCore), scaledUnits(2, span), confidenceLow, "jira_comments",
			"comment pagination is issue-activity dependent"))
	}
	if jiraFlagEnabled(context, "jira_fetch_worklogs", "fetch_worklogs") {
		estimates = append(estimates, newEstimate(bucket(DimensionRESTCore), scaledUnits(3, span), confidenceLow, "jira_worklogs",
			"JIRA_FETCH_WORKLOGS adds per-issue worklog expansion"))
	}
	if jiraFlagEnabled(context, "atlassian_gql_enabled", "gql_enabled") {
		estimates = append(estimates, newEstimate(bucket(DimensionGraphQLCost), scaledUnits(3, span), confidenceMedium, "jira_gql_enrichment",
			"ATLASSIAN_GQL_ENABLED routes Jira enrichment through AGG"))
	}
	return estimates, nil
}

// --- linear (providers/linear/budget.py) ---

// linearScaledUnits is Linear's own _scaled_units.
func linearScaledUnits(fixedFloor, spanDays, perDayWeight int) int {
	if spanDays <= 1 {
		return fixedFloor
	}
	return max(fixedFloor, fixedFloor*spanDays*max(1, perDayWeight))
}

func estimateLinear(context Context) ([]Estimate, error) {
	scope := fallbackScope(context)
	if mapping, ok := credentialMapping(context); ok {
		copyPresent(scope, mapping, "organization_id", "workspace_id", "team_id")
		if baseURL := baseURLOf(mapping); baseURL != nil {
			scope.set("base_url", baseURL)
		}
	}
	host, err := hostFrom(context, linearDefaultURL, linearDefaultHost, "base_url", "baseUrl")
	if err != nil {
		return nil, err
	}
	bucket := bucketsFor("linear", context, host, fingerprintOf(scope))
	span := windowSpanDays(context)
	gql := func(units int, confidence, family string, notes ...string) Estimate {
		return newEstimate(bucket(DimensionGraphQLCost), units, confidence, family, notes...)
	}
	switch context.DatasetKey {
	case datasetWorkItems:
		return []Estimate{
			gql(1, confidenceMedium, "teams"),
			gql(linearScaledUnits(5, span, 2), confidenceLow, "issues",
				"Linear issue pages include nested labels, project, comments, attachments, relations, and history edges"),
			gql(2, confidenceLow, "cycles"),
			gql(linearScaledUnits(2, span, 1), confidenceLow, "comments"),
			gql(linearScaledUnits(1, span, 1), confidenceLow, "attachments"),
			gql(linearScaledUnits(2, span, 1), confidenceLow, "relations"),
			gql(linearScaledUnits(2, span, 1), confidenceLow, "history"),
		}, nil
	case datasetWorkItemLabels:
		return []Estimate{
			gql(1, confidenceMedium, "teams"),
			gql(1, confidenceMedium, "team_members",
				"Linear team pages include a small member edge; large teams require member pagination"),
		}, nil
	case datasetWorkItemProjects:
		return []Estimate{gql(2, confidenceMedium, "projects")}, nil
	case datasetWorkItemHistory:
		return []Estimate{gql(linearScaledUnits(3, span, 1), confidenceLow, "history")}, nil
	case datasetWorkItemComments:
		return []Estimate{gql(linearScaledUnits(3, span, 1), confidenceLow, "comments")}, nil
	}
	return []Estimate{}, nil
}

// --- pagerduty (providers/pagerduty/budget.py) ---

var pagerDutyDatasetFamilies = map[string]string{
	"incidents":           "pagerduty_incidents",
	"services":            "pagerduty_services",
	"business-services":   "pagerduty_business_services",
	"escalation-policies": "pagerduty_escalation_policies",
	"schedules":           "pagerduty_schedules",
	"on-calls":            "pagerduty_oncalls",
	"users":               "pagerduty_users",
	"teams":               "pagerduty_teams",
}

var pagerDutyEnrichmentFamilies = map[string]string{
	"incident-alerts":      "pagerduty_alerts",
	"incident-log-entries": "pagerduty_log_entries",
	"incident-notes":       "pagerduty_notes",
}

func estimatePagerDuty(context Context) ([]Estimate, error) {
	mapping, ok := credentialMapping(context)
	if !ok {
		mapping = newObject()
	}
	region := "us"
	if value, present := mapping.get("region"); present {
		region = pyStr(value)
	}
	host := pagerDutyHostUS
	if region == "eu" {
		host = pagerDutyHostEU
	}
	subdomain := "env"
	if value, present := mapping.get("subdomain"); present {
		subdomain = pyStr(value)
	}
	fingerprint, err := sha256Hex(subdomain)
	if err != nil {
		return nil, err
	}
	bucket := Bucket{
		Provider: "pagerduty", OrgID: context.OrgID, Host: host,
		CredentialFingerprint: fingerprint, Dimension: DimensionRESTCore,
	}
	dataset := context.DatasetKey
	if family, enrichment := pagerDutyEnrichmentFamilies[dataset]; enrichment {
		// enrichment_cap: an int that is not a bool and not negative, else
		// the default. Python ints are unbounded, so the arithmetic runs on
		// big.Int; an estimate past int64 is refused (the old bridge client
		// could not decode it either).
		enrichmentCap := big.NewInt(pagerDutyDefaultCap)
		if value, present := context.DatasetOptions.get("enrichment_cap"); present {
			switch number := value.(type) {
			case int64:
				if number >= 0 {
					enrichmentCap = big.NewInt(number)
				}
			case *big.Int:
				if number.Sign() >= 0 {
					enrichmentCap = number
				}
			}
		}
		estimates := []Estimate{newEstimate(bucket, pagerDutyIncidentPages, confidenceMedium, pagerDutyIncidentsFamily, pagerDutyPaginationNote)}
		if enrichmentCap.Sign() > 0 {
			pageSize := big.NewInt(pagerDutyPageSize)
			childPages := new(big.Int).Div(new(big.Int).Add(enrichmentCap, big.NewInt(pagerDutyPageSize-1)), pageSize)
			units := new(big.Int).Mul(childPages, big.NewInt(pagerDutyIncidentPages*pagerDutyPageSize))
			if !units.IsInt64() {
				return nil, errEstimateOutOfRange
			}
			estimates = append(estimates, newEstimate(bucket, int(units.Int64()), confidenceLow, family, pagerDutyEnrichmentNote))
		}
		return estimates, nil
	}
	family, ok := pagerDutyDatasetFamilies[dataset]
	if !ok {
		family = "pagerduty_" + dataset
	}
	return []Estimate{newEstimate(bucket, pagerDutyIncidentPages, confidenceMedium, family, pagerDutyOffsetPagination)}, nil
}

// --- launchdarkly (providers/launchdarkly/budget.py) ---

func estimateLaunchDarkly(context Context) ([]Estimate, error) {
	if context.DatasetKey != datasetFeatureFlags {
		return []Estimate{}, nil
	}
	scope := fallbackScope(context)
	if mapping, ok := credentialMapping(context); ok {
		copyPresent(scope, mapping, "project_key", "environment")
		if baseURL := baseURLOf(mapping); baseURL != nil {
			scope.set("base_url", baseURL)
		}
	}
	host, err := hostFrom(context, launchDarklyDefaultURL, launchDarklyDefaultHost, "base_url", "baseUrl")
	if err != nil {
		return nil, err
	}
	bucket := bucketsFor("launchdarkly", context, host, fingerprintOf(scope))
	return []Estimate{
		newEstimate(bucket(DimensionRESTCore), 2, confidenceMedium, "flags"),
		newEstimate(bucket(DimensionRESTCore), 52, confidenceLow, "audit_log"),
		newEstimate(bucket(DimensionRESTCore), 1, confidenceMedium, "code_refs"),
		newEstimate(bucket(DimensionSecondaryAbuseRisk), 1, confidenceLow, "code_refs"),
	}, nil
}
