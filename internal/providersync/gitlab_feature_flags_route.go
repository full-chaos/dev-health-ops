package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// GitLabFeatureFlagsClient uses a 100-item page and a 1,000-page defensive
	// bound. Unlike the Python client, the complete Go route refuses to advance
	// the watermark when that bound is reached: a truncated inventory is not a
	// complete unit.
	gitLabFeatureFlagsDefaultPerPage = 100
	gitLabFeatureFlagsMaxPages       = 1_000
)

// GitLabFeatureFlagsRouteHandler is the complete provider-local route for
// gitlab/feature-flags. The request order deliberately follows
// GitLabFeatureFlagsClient: feature-flag pages first, then the project GET
// used to resolve the canonical path. No LaunchDarkly code-reference or link
// behavior belongs on this provider route.
type GitLabFeatureFlagsRouteHandler struct {
	MaxPages int
	PerPage  int
}

type gitLabFeatureFlagsCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabFeatureFlagsCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts++
	response, err := doer.delegate.Do(request)
	if response != nil && gitLabFeatureFlagsQualified403(response) {
		// providerfoundation's shared classifier intentionally gives GitLab's
		// 403 the ordinary authentication meaning. GitLabFeatureFlagsClient has
		// one provider-specific exception: a 403 carrying Retry-After or
		// RateLimit-Remaining: 0 is a retryable throttle. Convert only that
		// qualified shape to the shared 429 class at this provider boundary so
		// the HTTP client's retry accounting remains physical and bounded.
		response.StatusCode = http.StatusTooManyRequests
	}
	return response, err
}

func gitLabFeatureFlagsQualified403(response *http.Response) bool {
	if response == nil || response.StatusCode != http.StatusForbidden {
		return false
	}
	return strings.TrimSpace(gitLabFeatureFlagsHeaderValue(response.Header, "Retry-After")) != "" ||
		strings.TrimSpace(gitLabFeatureFlagsHeaderValue(response.Header, "RateLimit-Remaining")) == "0"
}

func gitLabFeatureFlagsHeaderValue(headers http.Header, wanted string) string {
	for key, values := range headers {
		if strings.EqualFold(key, wanted) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func (handler GitLabFeatureFlagsRouteHandler) limits() (int, int, error) {
	maxPages, perPage := handler.MaxPages, handler.PerPage
	if maxPages == 0 {
		maxPages = gitLabFeatureFlagsMaxPages
	}
	if perPage == 0 {
		perPage = gitLabFeatureFlagsDefaultPerPage
	}
	if maxPages < 1 || maxPages > gitLabFeatureFlagsMaxPages || perPage < 1 || perPage > gitLabFeatureFlagsDefaultPerPage {
		return 0, 0, ErrInvalidConfiguration
	}
	return maxPages, perPage, nil
}

func (handler GitLabFeatureFlagsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "feature-flags" || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil || client.Doer == nil ||
		normalizedAt.IsZero() || strings.TrimSpace(claim.SourceExternalID) == "" {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxPages, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	projectIDOrPath := strings.TrimSpace(claim.SourceExternalID)
	requests := 0
	counted := *client
	counted.Doer = gitLabFeatureFlagsCountingDoer{
		delegate: client.Doer, attempts: &requests,
	}
	flagsPath := providerRelativePath(
		&counted, "api", "v4", "projects", projectIDOrPath, "feature_flags",
	)
	flagsPage, err := providerfoundation.CollectGitLabPageParamPages(
		ctx, &counted, providerfoundation.GitLabPageOptions{
			Path: flagsPath, PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if flagsPage.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}

	// The Python producer calls get_project_name only after the complete flag
	// inventory. Keep this second physical request after all flag pages,
	// including the empty terminating page.
	projectPath := providerRelativePath(
		&counted, "api", "v4", "projects", projectIDOrPath,
	)
	projectKey, err := fetchGitLabFeatureFlagProjectName(
		ctx, &counted, projectPath, projectIDOrPath,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	flags, err := normalizeGitLabFeatureFlags(
		flagsPage.Items, claim.OrgID, projectKey, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	events, err := normalizeGitLabFeatureFlagEvents(
		flagsPage.Items, claim.OrgID, projectKey, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	edges := gitLabFeatureFlagEdges(flags, events, projectKey, normalizedAt)
	flagEffect, err := effectBatchFromValues(
		"feature_flag", EffectReplaySafe, flags,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	eventEffect, err := effectBatchFromValues(
		"feature_flag_event", EffectReadbackRequired, events,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	edgeEffect, err := effectBatchFromValues(
		"work_graph_edges", EffectReplaySafe, edges,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{flagEffect, eventEffect, edgeEffect},
		Result: map[string]any{
			"flags_synced": len(flags), "events_synced": len(events),
			"project_key": projectKey, "project_id_or_path": projectIDOrPath,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: flagsPage.Pages, Records: len(flags) + len(events) + len(edges),
		},
	}, nil
}

func fetchGitLabFeatureFlagProjectName(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	fallback string,
) (string, error) {
	response, err := client.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	if err != nil || len(body) > nativeMaxObjectBytes {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		return fallback, nil
	}
	for _, key := range []string{"path_with_namespace", "path"} {
		candidate, exists := object[key]
		if !exists || !pythonTruthy(candidate) {
			continue
		}
		if text := stringValue(candidate); text != "" {
			return text, nil
		}
	}
	return fallback, nil
}

func normalizeGitLabFeatureFlags(
	items []json.RawMessage,
	orgID string,
	projectKey string,
	normalizedAt time.Time,
) ([]launchDarklyFlagRow, error) {
	rows := make([]launchDarklyFlagRow, 0, len(items))
	for _, raw := range items {
		flag, err := decodeGitLabFeatureFlagObject(raw)
		if err != nil {
			return nil, err
		}
		scopes, err := gitLabFeatureFlagScopes(flag)
		if err != nil {
			return nil, err
		}
		createdAt := parseGitLabFeatureFlagTime(flag["created_at"])
		if createdAt == nil {
			fallback := normalizedAt.UTC()
			createdAt = &fallback
		}
		flagKey := gitLabFeatureFlagFirstString(flag, "name", "key")
		flagType := gitLabFeatureFlagFirstString(flag, "version")
		if flagType == "" {
			flagType = "new_version_flag"
		}
		for _, environment := range scopes {
			rows = append(rows, launchDarklyFlagRow{
				OrgID: orgID, Provider: "gitlab", FlagKey: flagKey,
				ProjectKey: projectKey, Environment: environment,
				FlagType: flagType, CreatedAt: createdAt,
				LastSynced: normalizedAt.UTC(),
			})
		}
	}
	return rows, nil
}

func normalizeGitLabFeatureFlagEvents(
	items []json.RawMessage,
	orgID string,
	projectKey string,
	normalizedAt time.Time,
) ([]launchDarklyEventRow, error) {
	rows := make([]launchDarklyEventRow, 0, len(items))
	for _, raw := range items {
		flag, err := decodeGitLabFeatureFlagObject(raw)
		if err != nil {
			return nil, err
		}
		scopes, err := gitLabFeatureFlagScopes(flag)
		if err != nil {
			return nil, err
		}
		flagKey := gitLabFeatureFlagFirstString(flag, "name", "key")
		state := "off"
		if pythonTruthy(flag["active"]) {
			state = "on"
		}
		eventAt := parseGitLabFeatureFlagTime(flag["updated_at"])
		if eventAt == nil {
			fallback := normalizedAt.UTC()
			eventAt = &fallback
		}
		for _, environment := range scopes {
			rows = append(rows, launchDarklyEventRow{
				OrgID: orgID, EventType: "toggle", FlagKey: flagKey,
				Environment: environment, ActorType: "snapshot",
				NextState: state, EventAt: eventAt.UTC(),
				IngestedAt: normalizedAt.UTC(), SourceEventID: flagKey,
				DedupeKey: "gitlab:" + projectKey + ":" + flagKey + ":" + environment + ":" + state,
			})
		}
	}
	return rows, nil
}

func decodeGitLabFeatureFlagObject(raw json.RawMessage) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	object, ok := value.(map[string]any)
	if !ok || object == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return object, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return providerfoundation.ErrNormalizationInvalid
		}
		return err
	}
	return nil
}

func gitLabFeatureFlagScopes(flag map[string]any) ([]string, error) {
	rawStrategies, exists := flag["strategies"]
	if !exists || !pythonTruthy(rawStrategies) {
		return []string{""}, nil
	}
	strategies, ok := rawStrategies.([]any)
	if !ok {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	scopes := make([]string, 0)
	for _, rawStrategy := range strategies {
		strategy, ok := rawStrategy.(map[string]any)
		if !ok || strategy == nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		rawScopes, exists := strategy["scopes"]
		if !exists || !pythonTruthy(rawScopes) {
			continue
		}
		scopeValues, ok := rawScopes.([]any)
		if !ok {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		for _, rawScope := range scopeValues {
			scope, ok := rawScope.(map[string]any)
			if !ok || scope == nil {
				return nil, providerfoundation.ErrNormalizationInvalid
			}
			environment := strings.TrimSpace(stringValue(scope["environment_scope"]))
			if environment == "" {
				continue
			}
			seen := false
			for _, existing := range scopes {
				if existing == environment {
					seen = true
					break
				}
			}
			if !seen {
				scopes = append(scopes, environment)
			}
		}
	}
	if len(scopes) == 0 {
		return []string{""}, nil
	}
	return scopes, nil
}

func gitLabFeatureFlagFirstString(flag map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := flag[key]
		if !ok || !pythonTruthy(value) {
			continue
		}
		if text := stringValue(value); text != "" {
			return text
		}
	}
	return ""
}

func parseGitLabFeatureFlagTime(value any) *time.Time {
	if !pythonTruthy(value) {
		return nil
	}
	text := stringValue(value)
	if text == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		for _, layout := range []string{
			"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999",
		} {
			parsed, err = time.ParseInLocation(layout, text, time.UTC)
			if err == nil {
				break
			}
		}
	}
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func pythonTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case json.Number:
		if typed == "" {
			return false
		}
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		return err != nil || parsed != 0
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func gitLabFeatureFlagEdges(
	flags []launchDarklyFlagRow,
	events []launchDarklyEventRow,
	fallbackProjectKey string,
	normalizedAt time.Time,
) []launchDarklyEdgeRow {
	latest := map[string]launchDarklyEventRow{}
	for _, event := range events {
		current, ok := latest[event.FlagKey]
		if !ok || event.EventAt.After(current.EventAt) {
			latest[event.FlagKey] = event
		}
	}
	edges := make([]launchDarklyEdgeRow, 0, len(flags)*2)
	for _, flag := range flags {
		projectKey := flag.ProjectKey
		if projectKey == "" {
			projectKey = fallbackProjectKey
		}
		flagID := launchDarklyFeatureFlagID(
			flag.OrgID, flag.Provider, projectKey, flag.FlagKey,
		)
		eventAt := normalizedAt.UTC()
		if flag.CreatedAt != nil {
			eventAt = flag.CreatedAt.UTC()
		}
		edges = append(edges, newLaunchDarklyEdge(
			flag.OrgID, flagID, "feature_flag", flagID, "feature_flag",
			"relates", "", flag.Provider, 1.0,
			"flag:"+flag.Provider+"/"+projectKey+"/"+flag.FlagKey,
			eventAt, normalizedAt,
		))
		if event, ok := latest[flag.FlagKey]; ok {
			evidence := gitLabFeatureFlagPythonISOTime(event.EventAt) + "|" +
				event.EventType + "|" + event.NextState
			edges = append(edges, newLaunchDarklyEdge(
				flag.OrgID, flagID, "feature_flag", flagID, "feature_flag",
				"config_changed_by", "", flag.Provider, 1.0,
				evidence, event.EventAt, normalizedAt,
			))
		}
	}
	return edges
}

func gitLabFeatureFlagPythonISOTime(value time.Time) string {
	text := value.UTC().Format(time.RFC3339Nano)
	if strings.HasSuffix(text, "Z") {
		text = strings.TrimSuffix(text, "Z") + "+00:00"
	}
	dot := strings.IndexByte(text, '.')
	if dot < 0 {
		return text
	}
	zone := strings.IndexAny(text[dot:], "+-")
	if zone < 0 {
		return text
	}
	zone += dot
	frac := text[dot+1 : zone]
	if len(frac) > 6 {
		frac = frac[:6]
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		return text[:dot] + text[zone:]
	}
	return text[:dot+1] + frac + text[zone:]
}

var _ CompleteRouteHandler = GitLabFeatureFlagsRouteHandler{}
