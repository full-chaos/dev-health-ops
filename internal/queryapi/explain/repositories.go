package explain

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// Repository is one repository behind a metric that is stored per
// repository (CHAOS-8103): its id, its display name, its value in the
// request window and the provider URL of the repository.
//
// The list is the per-repository aggregation BuildExplainResponse already
// reads for the contributors (fetchMetricContributors: the metric's own
// aggregator, grouped by repo_id, ordered by value, limit 6). It is no new
// rollup, and its value is the contributor's value, after the metric's own
// transform.
type Repository struct {
	ID string `json:"id"`
	// Name is the repository's stored name (resolveScopeDisplayNames); null
	// when no name is stored, never the id.
	Name  *string `json:"name"`
	Value float64 `json:"value"`
	// SourceURL is the repository URL the provider sync stored
	// (repos.settings.url); null when none is stored.
	SourceURL *string `json:"source_url"`
}

// fetchRepoSourceURLs reads the stored provider URL of each repository id
// of one organization: the `url` key of repos.settings, which the provider
// syncs write from the provider's own repository URL (github html_url,
// gitlab web_url). repos is a ReplacingMergeTree, so the read takes FINAL.
// An id with no row, no settings or an empty url is absent from the map;
// a URL is never built from a name and a host. A failed read is an error.
func (reader *Reader) fetchRepoSourceURLs(ctx context.Context, orgID string, repoIDs []string) (map[string]string, error) {
	ids := uniqueSortedNonEmpty(repoIDs)
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	query := fmt.Sprintf(`
SELECT toString(id) AS id, ifNull(JSONExtractString(settings, 'url'), '') AS source_url
FROM repos FINAL
WHERE org_id = {org_id:String}
  AND toString(id) IN {repo_ids:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := reader.client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: ids},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch repository source urls: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, sourceURL string
		if err := rows.Scan(&id, &sourceURL); err != nil {
			return nil, fmt.Errorf("scan repository source url row: %w", err)
		}
		if served, ok := servableSourceURL(sourceURL); ok {
			out[id] = served
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repository source url rows: %w", err)
	}
	return out, nil
}

// fetchRepoProviders reads the stored provider of each repository id of one
// organization (repos.provider: github, gitlab, ...). An id with no row, an
// empty provider or the migration's placeholder "unknown" is absent from the
// map: no provider is never served as a name. A failed read is an error.
func (reader *Reader) fetchRepoProviders(ctx context.Context, orgID string, repoIDs []string) (map[string]string, error) {
	ids := uniqueSortedNonEmpty(repoIDs)
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	query := fmt.Sprintf(`
SELECT toString(id) AS id, ifNull(provider, '') AS provider
FROM repos FINAL
WHERE org_id = {org_id:String}
  AND toString(id) IN {repo_ids:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := reader.client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: ids},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch repository providers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, provider string
		if err := rows.Scan(&id, &provider); err != nil {
			return nil, fmt.Errorf("scan repository provider row: %w", err)
		}
		provider = strings.TrimSpace(provider)
		if provider != "" && !strings.EqualFold(provider, "unknown") {
			out[id] = provider
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repository provider rows: %w", err)
	}
	return out, nil
}

// fetchWorkItemProviders reads the distinct providers stored in the work-item
// table behind a metric stored per team, for the request window, scope and
// organization (the same filter as the metric's own value read). Both
// work-item metric tables carry a provider column. The empty string and the
// placeholder "unknown" are no provider. A failed read is an error.
func (reader *Reader) fetchWorkItemProviders(ctx context.Context, table string, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]string, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	query := fmt.Sprintf(`
SELECT DISTINCT provider
FROM %s
WHERE day >= {start_day:Date} AND day < {end_day:Date}
%s
  AND org_id = {org_id:String}
%s
`, table, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)
	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch work item providers: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var provider string
		if err := rows.Scan(&provider); err != nil {
			return nil, fmt.Errorf("scan work item provider row: %w", err)
		}
		provider = strings.TrimSpace(provider)
		if provider != "" && !strings.EqualFold(provider, "unknown") {
			out = append(out, provider)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item provider rows: %w", err)
	}
	return out, nil
}

// joinProviderNames is the sorted, de-duplicated, ", "-joined provider list; nil when empty.
func joinProviderNames(providers []string) *string {
	seen := map[string]struct{}{}
	var names []string
	for _, provider := range providers {
		if _, dup := seen[provider]; !dup {
			seen[provider] = struct{}{}
			names = append(names, provider)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	joined := strings.Join(names, ", ")
	return &joined
}

// joinProviders is the served source of an item: the distinct providers of
// the given repositories, sorted and joined by ", "; nil when none is stored.
func joinProviders(repoIDs []string, providers map[string]string) *string {
	seen := map[string]struct{}{}
	var names []string
	for _, id := range repoIDs {
		if provider, ok := providers[id]; ok {
			if _, dup := seen[provider]; !dup {
				seen[provider] = struct{}{}
				names = append(names, provider)
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	joined := strings.Join(names, ", ")
	return &joined
}

// servableSourceURL returns a stored URL only when it is an absolute http or
// https URL with a host and no user info: the value is served as a link to the
// browser, so any other stored text (empty, relative, another scheme) is
// served as no URL, and so is a URL that carries a user name or password
// (https://user:token@host/...): it is never stripped and served, because no
// URL is built here.
func servableSourceURL(stored string) (string, bool) {
	stored = strings.TrimSpace(stored)
	if stored == "" {
		return "", false
	}
	parsed, err := url.Parse(stored)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", false
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return stored, true
	default:
		return "", false
	}
}

// scopeRepositoryRef returns the one repository reference of a request whose
// scope is one repository (scope level "repo" with exactly one non-empty id).
func scopeRepositoryRef(params Params) (string, bool) {
	if params.ScopeLevel != "repo" {
		return "", false
	}
	refs := uniqueSortedNonEmpty(params.ScopeIDs)
	if len(refs) != 1 {
		return "", false
	}
	return refs[0], true
}

// buildRepositories turns the contributor rows of a metric stored per
// repository into the served list, in the rows' own order. Name comes from
// the display names already resolved for the contributors (a name that
// looks like a UUID is no name); SourceURL from sourceURLs.
func buildRepositories(rows []metricRow, transform func(float64) float64, displayNames, sourceURLs map[string]string) []Repository {
	out := make([]Repository, 0, len(rows))
	for _, row := range rows {
		repository := Repository{ID: row.ID, Value: safeTransform(transform, safeFloat(row.Value))}
		if name, ok := displayNames[row.ID]; ok && name != "" && !looksLikeUUID(name) {
			name := name
			repository.Name = &name
		}
		if sourceURL, ok := sourceURLs[row.ID]; ok {
			sourceURL := sourceURL
			repository.SourceURL = &sourceURL
		}
		out = append(out, repository)
	}
	return out
}
