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

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
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
	StatusMapping    *StatusMapping
	Identity         jiraIdentityResolver
	MaxPages         int
	MaxRows          int
	PerPage          int
	ReferenceSprints []jiraSprintRow
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
		WorkItems:          make([]jiraWorkItemRow, 0, len(issues)),
		Transitions:        make([]jiraWorkItemTransitionRow, 0),
		Dependencies:       make([]jiraWorkItemDependencyRow, 0),
		ReopenEvents:       make([]jiraWorkItemReopenRow, 0),
		Interactions:       make([]jiraWorkItemInteractionRow, 0),
		Sprints:            make([]jiraSprintRow, 0),
		ProjectMemberships: make([]projectmembership.Row, 0),
		Projects:           make([]projectmembership.CatalogRow, 0),
	}
	optionalIncomplete := make([]string, 0)
	requests := searchPages
	fetchComments := jiraOptionBool(claim, "fetch_comments", true)
	commentsLimit := jiraOptionInt(claim, "comments_limit", 0)
	sprintIDs := make(map[string]struct{})
	// jiraProjectCache resolves a project id to (key, name) at most once per
	// Collect call, mirroring sprintCache's own per-run cache below.
	// unresolvedProjectMemberships counts CHAOS-4193's ruled "unresolvable ->
	// drop + counter": in practice this stays at 0 for jira (the changelog's
	// own toString/fromString already supply a usable name, so a failed live
	// lookup falls back rather than dropping the row), but the counter exists
	// per the shared contract every producer of this table honors.
	jiraProjectCache := make(map[string]jiraProjectCatalogEntry)
	unresolvedProjectMemberships := 0
	sprintCache := make(map[string]jiraSprintRow, len(handler.ReferenceSprints))
	for _, sprint := range handler.ReferenceSprints {
		if sprint.Provider != "jira" || sprint.OrgID != claim.OrgID || sprint.SprintID == "" {
			return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
		}
		if err := validateJiraSprint(sprint, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
		sprintCache[sprint.SprintID] = sprint
	}
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

		for _, move := range jiraIssueProjectMoves(jiraWorkItemFixtureInput{Raw: issue}, handler.Identity) {
			if move.OccurredAt.IsZero() {
				unresolvedProjectMemberships++
				continue
			}
			toEntry, ok := resolveJiraProjectCatalog(
				ctx, client, jiraProjectCache, &requests, move.ToProjectID, move.ToProjectName,
				derefString(item.ProjectID), derefString(item.ProjectKey),
			)
			if !ok {
				unresolvedProjectMemberships++
				continue
			}
			rows.Projects = append(rows.Projects, projectmembership.EnsureProjectsRow(
				claim.OrgID, "jira", move.ToProjectID, toEntry.Key, toEntry.Name, normalizedAt,
			))
			fromProjectID, fromProjectKey := "", ""
			if move.FromProjectID != "" {
				// An unresolvable FROM side does not drop the row:
				// to_project_id is what presence actually resolves on, and
				// degrading to "" here is the same honest "we don't know
				// what it moved from" shape the first-assignment sentinel
				// already carries, not a fabricated value.
				if fromEntry, ok := resolveJiraProjectCatalog(
					ctx, client, jiraProjectCache, &requests, move.FromProjectID, move.FromProjectName,
					derefString(item.ProjectID), derefString(item.ProjectKey),
				); ok {
					rows.Projects = append(rows.Projects, projectmembership.EnsureProjectsRow(
						claim.OrgID, "jira", move.FromProjectID, fromEntry.Key, fromEntry.Name, normalizedAt,
					))
					fromProjectID, fromProjectKey = move.FromProjectID, fromEntry.Key
				}
			}
			membership := projectmembership.Row{
				OrgID: claim.OrgID, RepoID: uuid.Nil, SubjectKind: projectmembership.SubjectWorkItem,
				SubjectID: item.WorkItemID, Provider: "jira",
				FromProjectID: fromProjectID, ToProjectID: move.ToProjectID,
				FromProjectKey: fromProjectKey, ToProjectKey: toEntry.Key,
				Actor: move.Actor, OccurredAt: move.OccurredAt.UTC(), LastSynced: normalizedAt.UTC(),
			}
			if move.EventID != "" {
				membership.EventID = move.EventID
			} else {
				membership.EventID = projectmembership.EventID(membership)
			}
			rows.ProjectMemberships = append(rows.ProjectMemberships, membership)
		}

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
		if sprint, ok := sprintCache[sprintID]; ok {
			rows.Sprints = append(rows.Sprints, sprint)
			continue
		}
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
	for _, row := range rows.ProjectMemberships {
		if err := validateJiraProjectMembership(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}

	effects, err := BuildJiraWorkItemEffects(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	result := map[string]any{
		"work_items_synced":              len(rows.WorkItems),
		"transitions_synced":             len(rows.Transitions),
		"dependencies_synced":            len(rows.Dependencies),
		"reopen_events_synced":           len(rows.ReopenEvents),
		"interactions_synced":            len(rows.Interactions),
		"sprints_synced":                 len(rows.Sprints),
		"project_memberships_synced":     len(rows.ProjectMemberships),
		"unresolved_project_memberships": unresolvedProjectMemberships,
		"project_key":                    projectKey,
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

// fetchJiraProject resolves a Jira internal numeric project id to its current
// key/name, for CHAOS-4193's project-membership catalog rows. A changelog
// "project" item's own fromString/toString are the project NAME already
// (confirmed live, CHAOS-4193 probe) but never necessarily the KEY -- Collect
// tries this live lookup for the real key and falls back to the changelog's
// own name, rather than dropping the membership row, when it fails.
func fetchJiraProject(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	projectID string,
) (key, name string, err error) {
	var payload map[string]any
	if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/project/"+url.PathEscape(projectID), nil, &payload); err != nil {
		return "", "", err
	}
	return stringFrom(payload["key"]), stringFrom(payload["name"]), nil
}

type jiraProjectCatalogEntry struct {
	Key  string
	Name string
}

// resolveJiraProjectCatalog resolves one Jira project id to (key, name) at
// most once per Collect call (results cache across every issue/move in the
// run), trying fetchJiraProject first and falling back to the changelog's own
// display name when the live lookup fails. Only a blank id is unresolvable --
// once an id is known, the id itself is what project_membership_presence
// resolves on, so a lookup failure degrades the row to a blank key and/or
// blank name rather than dropping it (codex review finding, CHAOS-4193): an
// empty display name is exactly the shape Linear's own from-side catalog rows
// already carry (no live lookup, ruled), not a new kind of incompleteness.
//
// currentProjectID/currentProjectKey are the ISSUE's own, already-resolved
// current project (from fields.project, never the changelog). When id
// matches it and the live lookup fails to return a key, that known key is
// used instead of leaving the key blank: an empty key here suppresses
// project_membership_presence's work_items fallback arm, and Jira ownership
// records resolve teams by key (team_autoimport_jira.py), so losing a key we
// already had on hand for free would silently break attribution (codex
// review finding, CHAOS-4193).
//
// Every resolution is cached, complete or not -- a persistently unavailable
// project (deleted, out of scope) would otherwise retry its live lookup on
// every single move that ever references it, turning one outage into
// thousands of redundant requests within a run (codex review finding,
// CHAOS-4193). A cache HIT still checks the known-key fallback and upgrades
// the stored entry in place: an earlier issue that only touched this id
// historically must not permanently freeze it blank for a LATER issue whose
// own current project this id actually is (codex review finding, CHAOS-4193).
func resolveJiraProjectCatalog(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	cache map[string]jiraProjectCatalogEntry,
	requests *int,
	id, changelogName string,
	currentProjectID, currentProjectKey string,
) (jiraProjectCatalogEntry, bool) {
	if id == "" {
		return jiraProjectCatalogEntry{}, false
	}
	if cached, ok := cache[id]; ok {
		if cached.Key == "" && id == currentProjectID && currentProjectID != "" {
			cached.Key = currentProjectKey
			cache[id] = cached
		}
		return cached, true
	}
	key, name, err := fetchJiraProject(ctx, client, id)
	*requests++
	if err != nil || name == "" {
		name = changelogName
	}
	if key == "" && id == currentProjectID && currentProjectID != "" {
		key = currentProjectKey
	}
	entry := jiraProjectCatalogEntry{Key: key, Name: name}
	cache[id] = entry
	return entry, true
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
