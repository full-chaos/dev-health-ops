package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	jiraWorkItemsMaxPages = 1_000
	jiraWorkItemsMaxRows  = 100_000
	jiraWorkItemsPerPage  = 100
)

// JiraWorkItemsRouteHandler is intentionally unregistered.  It owns the
// provider-local canonical work-items family while registry/activation remains
// a separate migration slice.
type JiraWorkItemsRouteHandler struct {
	StatusMapping *StatusMapping
	Identity      jiraIdentityResolver
	MaxPages      int
	MaxRows       int
	PerPage       int
}

func (handler JiraWorkItemsRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = jiraWorkItemsMaxPages
	}
	if rows == 0 {
		rows = jiraWorkItemsMaxRows
	}
	if perPage == 0 {
		perPage = jiraWorkItemsPerPage
	}
	if pages < 1 || pages > jiraWorkItemsMaxPages || rows < 1 || rows > jiraWorkItemsMaxRows || perPage < 1 || perPage > jiraWorkItemsPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler JiraWorkItemsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) || client == nil || client.Provider != "jira" ||
		client.BaseURL == nil || normalizedAt.IsZero() || claim.SinceAt == nil ||
		claim.BeforeAt == nil || !claim.SinceAt.Before(*claim.BeforeAt) || handler.StatusMapping == nil {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	_ = credential // Auth is already sealed into providerfoundation.HTTPClient.
	maxPages, maxRows, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectKey := strings.TrimSpace(claim.SourceExternalID)
	if projectKey == "" || strings.ContainsAny(projectKey, "'(),\"\r\n") {
		return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
	}

	jql := jiraWorkItemsJQL(claim, projectKey)
	fetchAll := jiraOptionBool(claim, "fetch_all", false)
	if !fetchAll && (claim.SinceAt == nil || claim.BeforeAt == nil) {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	issues, searchPages, err := collectJiraWorkItemIssues(
		ctx, client, jql, maxPages, maxRows, perPage,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	rows := jiraWorkItemRows{
		WorkItems:    make([]jiraWorkItemRow, 0, len(issues)),
		Transitions:  make([]jiraWorkItemTransitionRow, 0),
		Dependencies: make([]jiraWorkItemDependencyRow, 0),
		ReopenEvents: make([]jiraWorkItemReopenRow, 0),
		Interactions: make([]jiraWorkItemInteractionRow, 0),
		Sprints:      make([]jiraSprintRow, 0),
	}
	optionalIncomplete := make([]string, 0)
	requests := searchPages
	fetchComments := jiraOptionBool(claim, "fetch_comments", true)
	commentsLimit := jiraOptionInt(claim, "comments_limit", 0)
	sprintIDs := make(map[string]struct{})
	for _, issue := range issues {
		item, transitions, normalizeErr := normalizeJiraWorkItem(
			claim, jiraWorkItemFixtureInput{Raw: issue}, handler.StatusMapping,
			handler.Identity, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.Transitions = append(rows.Transitions, transitions...)
		dependencies := normalizeJiraDependencies(claim, item.WorkItemID, issue, normalizedAt)
		rows.Dependencies = append(rows.Dependencies, dependencies...)
		rows.ReopenEvents = append(rows.ReopenEvents, jiraReopenEvents(claim, transitions, normalizedAt)...)

		if fetchComments {
			comments, commentPages, commentErr := collectJiraIssueComments(
				ctx, client, item.WorkItemID, maxPages, perPage, commentsLimit,
			)
			requests += commentPages
			if commentErr != nil {
				optionalIncomplete = append(optionalIncomplete, "comments:"+item.WorkItemID)
			} else {
				rows.Interactions = append(rows.Interactions,
					normalizeJiraInteractions(claim, item.WorkItemID, comments, handler.Identity, normalizedAt)...,
				)
			}
		}
		if item.SprintID != nil && *item.SprintID != "" {
			sprintIDs[*item.SprintID] = struct{}{}
		}
	}

	for sprintID := range sprintIDs {
		payload, sprintErr := fetchJiraSprint(ctx, client, sprintID)
		requests++
		if sprintErr != nil {
			optionalIncomplete = append(optionalIncomplete, "sprint:"+sprintID)
			continue
		}
		sprint, sprintErr := normalizeJiraSprint(claim, payload, normalizedAt)
		if sprintErr != nil {
			return CompleteRouteBatch{}, sprintErr
		}
		if err := validateJiraSprint(sprint, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
		rows.Sprints = append(rows.Sprints, sprint)
	}
	for _, row := range rows.Transitions {
		if err := validateJiraTransition(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	for _, row := range rows.Dependencies {
		if err := validateJiraDependency(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	for _, row := range rows.ReopenEvents {
		if err := validateJiraReopen(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	for _, row := range rows.Interactions {
		if err := validateJiraInteraction(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}

	effects, err := BuildJiraWorkItemEffects(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	result := map[string]any{
		"work_items_synced":    len(rows.WorkItems),
		"transitions_synced":   len(rows.Transitions),
		"dependencies_synced":  len(rows.Dependencies),
		"reopen_events_synced": len(rows.ReopenEvents),
		"interactions_synced":  len(rows.Interactions),
		"sprints_synced":       len(rows.Sprints),
		"project_key":          projectKey,
	}
	if len(optionalIncomplete) > 0 {
		result["incomplete"] = optionalIncomplete
	}
	var watermark *time.Time
	if len(optionalIncomplete) == 0 {
		value := claim.BeforeAt.UTC()
		watermark = &value
	}
	return CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: watermark,
		Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: searchPages, Records: len(rows.WorkItems)},
	}, nil
}

func jiraWorkItemsJQL(claim Claim, projectKey string) string {
	if override := jiraOptionString(claim, "jql_override", ""); override != "" {
		return override
	}
	if jiraOptionBool(claim, "fetch_all", false) {
		return fmt.Sprintf("project = '%s' ORDER BY updated DESC", projectKey)
	}
	updatedSince := claim.SinceAt.UTC().Format("2006-01-02")
	activeUntil := claim.BeforeAt.UTC().Format("2006-01-02")
	return fmt.Sprintf("project = '%s' AND (updated >= '%s' OR (statusCategory != Done AND created <= '%s')) ORDER BY updated DESC", projectKey, updatedSince, activeUntil)
}

func collectJiraWorkItemIssues(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	jql string,
	maxPages, maxRows, perPage int,
) ([]map[string]any, int, error) {
	issues := make([]map[string]any, 0)
	token := ""
	seenTokens := make(map[string]struct{})
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, pages, ErrPaginationCapExceeded
		}
		query := url.Values{
			"jql": {jql}, "maxResults": {strconv.Itoa(perPage)},
			"fields": {"*all"}, "expand": {"changelog"},
		}
		if token != "" {
			query.Set("nextPageToken", token)
		} else {
			query.Set("startAt", "0")
		}
		var page struct {
			Issues        []json.RawMessage `json:"issues"`
			IsLast        *bool             `json:"isLast"`
			NextPageToken string            `json:"nextPageToken"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/search/jql?"+query.Encode(), nil, &page); err != nil {
			return nil, pages + 1, err
		}
		if page.IsLast == nil || page.Issues == nil {
			return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
		}
		if len(issues)+len(page.Issues) > maxRows {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		for _, raw := range page.Issues {
			var issue map[string]any
			if err := decodeJiraJSON(raw, &issue); err != nil || issue == nil {
				return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
			}
			issues = append(issues, issue)
		}
		if *page.IsLast {
			return issues, pages + 1, nil
		}
		if page.NextPageToken == "" {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		if _, duplicate := seenTokens[page.NextPageToken]; duplicate {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		seenTokens[page.NextPageToken] = struct{}{}
		token = page.NextPageToken
	}
}

func collectJiraIssueComments(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	workItemID string,
	maxPages, perPage, limit int,
) ([]map[string]any, int, error) {
	issueKey := strings.TrimPrefix(workItemID, "jira:")
	comments := make([]map[string]any, 0)
	start := 0
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, pages, ErrPaginationCapExceeded
		}
		query := url.Values{"startAt": {strconv.Itoa(start)}, "maxResults": {strconv.Itoa(perPage)}}
		var page struct {
			Comments []json.RawMessage `json:"comments"`
			IsLast   *bool             `json:"isLast"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/issue/"+url.PathEscape(issueKey)+"/comment?"+query.Encode(), nil, &page); err != nil {
			return nil, pages + 1, err
		}
		if page.Comments == nil {
			return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
		}
		for _, raw := range page.Comments {
			if limit > 0 && len(comments) >= limit {
				return comments, pages + 1, nil
			}
			var comment map[string]any
			if err := decodeJiraJSON(raw, &comment); err != nil || comment == nil {
				return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
			}
			comments = append(comments, comment)
		}
		if len(page.Comments) == 0 || (page.IsLast != nil && *page.IsLast) || (limit > 0 && len(comments) >= limit) {
			return comments, pages + 1, nil
		}
		next := start + len(page.Comments)
		if next <= start {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		start = next
	}
}

func fetchJiraSprint(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	sprintID string,
) (map[string]any, error) {
	var payload map[string]any
	err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/agile/1.0/sprint/"+url.PathEscape(sprintID), nil, &payload)
	return payload, err
}

func jiraFetchObject(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	method, path string,
	body io.Reader,
	target any,
) error {
	response, err := client.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return providerfoundation.ErrNormalizationInvalid
	}
	limited, err := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	if err != nil || len(limited) > nativeMaxObjectBytes {
		return providerfoundation.ErrNormalizationInvalid
	}
	return decodeJiraJSON(limited, target)
}

func decodeJiraJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func jiraOptionBool(claim Claim, key string, fallback bool) bool {
	if claim.DatasetOptions == nil {
		return fallback
	}
	value, ok := claim.DatasetOptions[key]
	if !ok {
		return fallback
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

func jiraOptionInt(claim Claim, key string, fallback int) int {
	if claim.DatasetOptions == nil {
		return fallback
	}
	value, ok := claim.DatasetOptions[key]
	if !ok {
		return fallback
	}
	if number, ok := value.(int); ok {
		return number
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(stringFrom(value)))
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}
