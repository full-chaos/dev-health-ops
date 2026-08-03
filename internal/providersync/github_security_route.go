package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const defaultGitHubSecurityMaxAlerts = 1_000

// securityAlertRow mirrors the SecurityAlert construction in
// _fetch_github_security_alerts_async and its ClickHouse persistence boundary.
type securityAlertRow struct {
	OrgID       string     `json:"org_id"`
	RepoID      string     `json:"repo_id"`
	AlertID     string     `json:"alert_id"`
	Source      string     `json:"source"`
	Severity    *string    `json:"severity"`
	State       *string    `json:"state"`
	PackageName *string    `json:"package_name"`
	CVEID       *string    `json:"cve_id"`
	URL         *string    `json:"url"`
	Title       *string    `json:"title"`
	Description *string    `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	FixedAt     *time.Time `json:"fixed_at"`
	DismissedAt *time.Time `json:"dismissed_at"`
	LastSynced  time.Time  `json:"last_synced"`
}

type gitHubSecurityPayload struct {
	Number             json.Number    `json:"number"`
	GHSAID             any            `json:"ghsa_id"`
	State              any            `json:"state"`
	HTMLURL            any            `json:"html_url"`
	CreatedAt          *string        `json:"created_at"`
	FixedAt            *string        `json:"fixed_at"`
	DismissedAt        *string        `json:"dismissed_at"`
	SecurityAdvisory   map[string]any `json:"security_advisory"`
	Dependency         map[string]any `json:"dependency"`
	Rule               map[string]any `json:"rule"`
	MostRecentInstance map[string]any `json:"most_recent_instance"`
	Severity           any            `json:"severity"`
	CVEID              any            `json:"cve_id"`
	Summary            any            `json:"summary"`
	Description        any            `json:"description"`
}

// GitHubSecurityRouteHandler owns exactly security_alerts. It mirrors the
// three best-effort Python sources: Dependabot, code scanning, and advisories.
type GitHubSecurityRouteHandler struct{ MaxAlerts int }

func (handler GitHubSecurityRouteHandler) Collect(ctx context.Context, claim Claim, _ providerfoundation.Credential, client *providerfoundation.HTTPClient, normalizedAt time.Time) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "security" || client == nil || client.Provider != "github" || client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	root := providerRelativePath(client, "repos", owner, repository)
	var repo gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repo); err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := repositoryIdentity(repo.FullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	maxAlerts := handler.MaxAlerts
	if maxAlerts == 0 {
		maxAlerts = defaultGitHubSecurityMaxAlerts
	}
	if maxAlerts < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	pages := (maxAlerts + nativePerPage - 1) / nativePerPage
	rows := make([]securityAlertRow, 0)
	requests := 1
	for _, source := range []struct {
		name, path string
		query      url.Values
	}{
		{"dependabot", root + "/dependabot/alerts", url.Values{"state": {"open"}, "per_page": {"100"}}},
		{"code_scanning", root + "/code-scanning/alerts", url.Values{"state": {"open"}, "per_page": {"100"}}},
		{"advisory", root + "/security-advisories", url.Values{"per_page": {"100"}}},
	} {
		items, fetched, ok := fetchGitHubSecurityPage(ctx, client, source.path, source.query, pages)
		requests += fetched
		if !ok {
			continue
		}
		if len(items) > maxAlerts {
			items = items[:maxAlerts]
		}
		for _, item := range items {
			row, include := normalizeGitHubSecurityAlert(claim, repoID, source.name, item, normalizedAt)
			if !include || securityAlertOutsideWindow(row.CreatedAt, claim) {
				continue
			}
			rows = append(rows, row)
		}
	}
	effect, err := effectBatchFromValues("security_alerts", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{Effects: []EffectBatch{effect}, Result: map[string]any{"security_alerts_synced": len(rows), "repo": repo.FullName}, Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests, Pages: requests - 1, Records: len(rows)}}, nil
}

func fetchGitHubSecurityPage(ctx context.Context, client *providerfoundation.HTTPClient, path string, query url.Values, maxPages int) ([]gitHubSecurityPayload, int, bool) {
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: path, Query: query, MaxPages: maxPages})
	if err != nil {
		return nil, 0, false
	}
	items := make([]gitHubSecurityPayload, 0, len(page.Items))
	for _, raw := range page.Items {
		var item gitHubSecurityPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&item) != nil {
			return nil, page.Pages, false
		}
		items = append(items, item)
	}
	return items, page.Pages, true
}

func normalizeGitHubSecurityAlert(claim Claim, repoID, source string, item gitHubSecurityPayload, normalizedAt time.Time) (securityAlertRow, bool) {
	createdAt := parseGitHubPullTime(item.CreatedAt)
	if createdAt == nil {
		return securityAlertRow{}, false
	}
	row := securityAlertRow{OrgID: claim.OrgID, RepoID: repoID, Source: source, CreatedAt: *createdAt, LastSynced: normalizedAt, URL: securityOptionalString(item.HTMLURL), FixedAt: parseGitHubPullTime(item.FixedAt), DismissedAt: parseGitHubPullTime(item.DismissedAt)}
	switch source {
	case "dependabot":
		advisory, dependency := item.SecurityAdvisory, item.Dependency
		packageData := mapValue(dependency, "package")
		row.AlertID = "dependabot:" + stringValue(item.Number)
		row.Severity, row.State = securityOptionalString(mapGet(advisory, "severity")), securityOptionalString(item.State)
		row.PackageName, row.CVEID = securityOptionalString(mapGet(packageData, "name")), securityOptionalString(mapGet(advisory, "cve_id"))
		row.Title, row.Description = securityOptionalString(mapGet(advisory, "summary")), securityOptionalString(mapGet(advisory, "description"))
	case "code_scanning":
		rule, instance := item.Rule, item.MostRecentInstance
		message := mapValue(instance, "message")
		row.AlertID = "code_scanning:" + stringValue(item.Number)
		row.Severity, row.State = securityOptionalString(mapGet(rule, "severity")), securityOptionalString(item.State)
		row.Title, row.Description = securityOptionalString(mapGet(rule, "description")), securityOptionalString(mapGet(message, "text"))
	case "advisory":
		row.AlertID = "advisory:" + stringValue(item.GHSAID)
		row.Severity, row.State, row.CVEID = securityOptionalString(item.Severity), securityOptionalString(item.State), securityOptionalString(item.CVEID)
		row.Title, row.Description = securityOptionalString(item.Summary), securityOptionalString(item.Description)
	default:
		return securityAlertRow{}, false
	}
	if row.AlertID == "dependabot:" || row.AlertID == "code_scanning:" || row.AlertID == "advisory:" {
		return securityAlertRow{}, false
	}
	return row, true
}

func mapValue(value map[string]any, key string) map[string]any {
	if nested, ok := value[key].(map[string]any); ok {
		return nested
	}
	return map[string]any{}
}

func mapGet(value map[string]any, key string) any { return value[key] }

func securityOptionalString(value any) *string {
	text := strings.TrimSpace(stringValue(value))
	if text == "" {
		return nil
	}
	return &text
}

func securityAlertOutsideWindow(createdAt time.Time, claim Claim) bool {
	return (claim.SinceAt != nil && createdAt.Before(claim.SinceAt.UTC())) || (claim.BeforeAt != nil && createdAt.After(claim.BeforeAt.UTC()))
}

func (row securityAlertRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.RepoID == "" || len(row.RepoID) != 36 || row.AlertID == "" || row.Source == "" || row.CreatedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubSecurityRouteHandler{}
