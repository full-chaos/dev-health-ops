package providersync

// Jira's atlassian-client path is deliberately kept beside the legacy JQL
// route.  The two clients do not have the same fetch contract: the Atlassian
// path pages the REST search endpoint by startAt, fetches changelogs as a
// second resource, optionally enriches worklogs through GraphQL with a REST
// fallback, and can enumerate board sprints.  Keeping that contract explicit
// prevents a compatibility alias from silently dropping the worklog and
// reference surfaces.

import (
	"bytes"
	"context"
	"encoding/json"
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
	jiraAtlassianMaxPages       = 1_000
	jiraAtlassianMaxRows        = 100_000
	jiraAtlassianPerPage        = 50
	jiraAtlassianWorklogPerPage = 100
)

// jiraWorklogRow mirrors models.work_items.Worklog and the worklogs sink
// projection.  Unlike the six direct work-item tables, worklogs are
// DateTime64(6); the adapter below therefore preserves microsecond precision.
type jiraWorklogRow struct {
	WorkItemID       string    `json:"work_item_id"`
	Provider         string    `json:"provider"`
	WorklogID        string    `json:"worklog_id"`
	Author           *string   `json:"author"`
	StartedAt        time.Time `json:"started_at"`
	TimeSpentSeconds int64     `json:"time_spent_seconds"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	LastSynced       time.Time `json:"last_synced"`
	OrgID            string    `json:"org_id"`
}

type jiraAtlassianRows struct {
	jiraWorkItemRows
	Worklogs []jiraWorklogRow
}

// JiraWorklogFetchObservation records both sides of the optional GraphQL
// worklog route. It is returned as typed fetch evidence so a GraphQL failure
// followed by a successful REST read cannot look like an uninstrumented REST
// fetch.
type JiraWorklogFetchObservation struct {
	IssueKey         string
	GraphQLAttempted bool
	GraphQLRequests  int
	GraphQLSucceeded bool
	RESTFallbackUsed bool
	RESTRequests     int
}

var jiraAtlassianRawDestinations = JiraAtlassianEffectDestinations()

// JiraAtlassianWorkItemsResult keeps route breadth and readiness explicit even
// though CompleteRouteBatch predates provider-specific result types. Worklogs
// are a Jira-only extra and do not replace any of the canonical sixteen.
type JiraAtlassianWorkItemsResult struct {
	WorkItemsSynced                  int      `json:"work_items_synced"`
	TransitionsSynced                int      `json:"transitions_synced"`
	DependenciesSynced               int      `json:"dependencies_synced"`
	ReopenEventsSynced               int      `json:"reopen_events_synced"`
	InteractionsSynced               int      `json:"interactions_synced"`
	SprintsSynced                    int      `json:"sprints_synced"`
	WorklogsSynced                   int      `json:"worklogs_synced"`
	RawDestinations                  []string `json:"raw_destinations"`
	DerivedDestinationsImplemented   []string `json:"derived_destinations_implemented"`
	DerivedDestinationsUnimplemented []string `json:"derived_destinations_unimplemented"`
	WatermarkHeldForIncomplete       bool     `json:"watermark_held_for_incomplete"`
}

// JiraSprintReferenceSink is the narrow reference-cache boundary used by the
// provider path.  It is intentionally not a registry or activation hook.
type JiraSprintReferenceSink func([]jiraSprintRow) error

// JiraAtlassianRouteHandler is intentionally unregistered.  It is a complete
// provider-local route for parity and recovery tests; registry/activation is a
// separate migration slice.
type JiraAtlassianRouteHandler struct {
	StatusMapping *StatusMapping
	Identity      jiraIdentityResolver
	CloudID       string
	// GraphQLClient may target api.atlassian.com while client targets the Jira
	// Cloud REST origin. When nil, the REST client is used for tests and for
	// same-origin deployments that expose /graphql.
	GraphQLClient    *providerfoundation.HTTPClient
	MaxPages         int
	MaxRows          int
	PerPage          int
	ReferenceSprints []jiraSprintRow
	ReferenceSink    JiraSprintReferenceSink
	Derived          jiraWorkItemsDeriver
}

func (handler JiraAtlassianRouteHandler) limits() (int, int, int, error) {
	pages, rows, perPage := handler.MaxPages, handler.MaxRows, handler.PerPage
	if pages == 0 {
		pages = jiraAtlassianMaxPages
	}
	if rows == 0 {
		rows = jiraAtlassianMaxRows
	}
	if perPage == 0 {
		perPage = jiraAtlassianPerPage
	}
	if pages < 1 || pages > jiraAtlassianMaxPages || rows < 1 || rows > jiraAtlassianMaxRows || perPage < 1 || perPage > jiraAtlassianPerPage {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return pages, rows, perPage, nil
}

func (handler JiraAtlassianRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) || client == nil || client.Provider != "jira" ||
		client.BaseURL == nil || normalizedAt.IsZero() || claim.SinceAt == nil ||
		claim.BeforeAt == nil || !claim.SinceAt.Before(*claim.BeforeAt) || handler.StatusMapping == nil ||
		(handler.GraphQLClient != nil && (handler.GraphQLClient.Provider != "jira" || handler.GraphQLClient.BaseURL == nil)) {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	_ = credential // Authentication is sealed into providerfoundation.HTTPClient.
	maxPages, maxRows, perPage, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectKey := strings.TrimSpace(claim.SourceExternalID)
	if projectKey == "" || strings.ContainsAny(projectKey, "'(),\"\r\n") {
		return CompleteRouteBatch{}, providerfoundation.ErrInvalidScope
	}

	jql := jiraWorkItemsJQL(claim, projectKey)
	issues, searchPages, err := collectJiraAtlassianIssues(ctx, client, jql, maxPages, maxRows, perPage)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	rows := jiraAtlassianRows{jiraWorkItemRows: jiraWorkItemRows{
		WorkItems:          make([]jiraWorkItemRow, 0, len(issues)),
		Transitions:        make([]jiraWorkItemTransitionRow, 0),
		Dependencies:       make([]jiraWorkItemDependencyRow, 0),
		ReopenEvents:       make([]jiraWorkItemReopenRow, 0),
		Interactions:       make([]jiraWorkItemInteractionRow, 0),
		Sprints:            cloneJiraSprintRows(handler.ReferenceSprints),
		ProjectMemberships: make([]projectmembership.Row, 0),
		Projects:           make([]projectmembership.CatalogRow, 0),
	}, Worklogs: make([]jiraWorklogRow, 0)}
	optionalIncomplete := make([]string, 0)
	worklogObservations := make([]JiraWorklogFetchObservation, 0)
	requests := searchPages
	fetchComments := jiraOptionBool(claim, "fetch_comments", false)
	commentsLimit := jiraOptionInt(claim, "comments_limit", 0)
	fetchWorklogs := jiraOptionBool(claim, "fetch_worklogs", false)
	useGraphQL := jiraOptionBool(claim, "atlassian_gql_enabled", false)
	fetchBoardSprints := jiraOptionBool(claim, "fetch_board_sprints", false)
	// CHAOS-4193: same project-membership resolution cache/counter as
	// jira_work_items_route.go's Collect -- see resolveJiraProjectCatalog's
	// own doc comment.
	jiraProjectCache := make(map[string]jiraProjectCatalogEntry)
	unresolvedProjectMemberships := 0

	for _, issue := range issues {
		key := stringFrom(issue["key"])
		if key == "" {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		changelog, changelogPages, changelogErr := collectJiraAtlassianChangelog(ctx, client, key, maxPages)
		requests += changelogPages
		if changelogErr != nil {
			// Changelog is part of the required canonical transition boundary.
			return CompleteRouteBatch{}, changelogErr
		}
		issue["changelog"] = map[string]any{"histories": changelog}
		item, transitions, normalizeErr := normalizeJiraWorkItem(
			claim, jiraWorkItemFixtureInput{Raw: issue, AtlassianShape: true}, handler.StatusMapping,
			handler.Identity, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.Transitions = append(rows.Transitions, transitions...)
		rows.Dependencies = append(rows.Dependencies,
			normalizeJiraDependencies(claim, item.WorkItemID, issue, normalizedAt)...)
		rows.ReopenEvents = append(rows.ReopenEvents,
			jiraReopenEvents(claim, transitions, normalizedAt)...)

		for _, move := range jiraIssueProjectMoves(jiraWorkItemFixtureInput{Raw: issue, AtlassianShape: true}, handler.Identity) {
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
		if fetchWorklogs {
			worklogClient := client
			if handler.GraphQLClient != nil {
				worklogClient = handler.GraphQLClient
			}
			worklogs, worklogRequests, worklogObservation, worklogErr := collectJiraAtlassianWorklogs(
				ctx, client, worklogClient, key, useGraphQL, maxPages, handler.CloudID,
			)
			requests += worklogRequests
			worklogObservations = append(worklogObservations, worklogObservation)
			if worklogErr != nil {
				optionalIncomplete = append(optionalIncomplete, "worklogs:"+item.WorkItemID)
			} else {
				for _, raw := range worklogs {
					row, rowErr := normalizeJiraWorklog(claim, item.WorkItemID, raw, handler.Identity, normalizedAt)
					if rowErr != nil {
						return CompleteRouteBatch{}, rowErr
					}
					rows.Worklogs = append(rows.Worklogs, row)
				}
			}
		}
	}

	// The Atlassian path deliberately does not issue per-issue sprint reads:
	// reference rows are authoritative for known IDs, and board enumeration is
	// the only source for new sprint rows. This mirrors the Python producer's
	// `reference_sprints` guard and keeps cache behavior tenant-scoped.
	if fetchBoardSprints && len(rows.Sprints) == 0 {
		boards, boardPages, boardErr := collectJiraBoards(ctx, client, maxPages, perPage)
		requests += boardPages
		if boardErr != nil {
			optionalIncomplete = append(optionalIncomplete, "boards")
		} else {
			fetched := make([]jiraSprintRow, 0)
			for _, board := range boards {
				sprints, sprintPages, sprintErr := collectJiraBoardSprints(ctx, client, board.ID, maxPages, perPage)
				requests += sprintPages
				if sprintErr != nil {
					optionalIncomplete = append(optionalIncomplete, "board_sprints:"+strconv.FormatInt(board.ID, 10))
					continue
				}
				for _, payload := range sprints {
					sprint, sprintErr := normalizeJiraSprint(claim, payload, normalizedAt)
					if sprintErr != nil {
						return CompleteRouteBatch{}, sprintErr
					}
					if err := validateJiraSprint(sprint, claim); err != nil {
						return CompleteRouteBatch{}, err
					}
					fetched = append(fetched, sprint)
				}
			}
			rows.Sprints = append(rows.Sprints, fetched...)
			if len(fetched) > 0 && handler.ReferenceSink != nil {
				if err := handler.ReferenceSink(fetched); err != nil {
					optionalIncomplete = append(optionalIncomplete, "reference_sink")
				}
			}
		}
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
	for _, row := range rows.Sprints {
		if err := validateJiraSprint(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	for _, row := range rows.Worklogs {
		if err := validateJiraWorklog(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	for _, row := range rows.ProjectMemberships {
		if err := validateJiraProjectMembership(row, claim); err != nil {
			return CompleteRouteBatch{}, err
		}
	}

	effects, err := BuildJiraAtlassianEffects(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	derivedImplemented := []string{}
	derivedUnimplemented := append([]string(nil), jiraWorkItemDerivedDestinations...)
	derivedRecords := 0
	var derivedWatermark *time.Time
	if handler.Derived != nil {
		derived, deriveErr := handler.Derived.Derive(
			ctx, claim, rows.jiraWorkItemRows, normalizedAt,
		)
		if deriveErr != nil {
			return CompleteRouteBatch{}, deriveErr
		}
		if derived.Watermark == nil || !derived.Watermark.Equal(*claim.BeforeAt) {
			return CompleteRouteBatch{}, ErrInvalidConfiguration
		}
		derivedEffects, effectErr := BuildJiraWorkItemDerivedEffects(derived.EffectRows())
		if effectErr != nil {
			return CompleteRouteBatch{}, effectErr
		}
		effects = append(effects, derivedEffects...)
		derivedImplemented = derived.producedDestinations()
		derivedUnimplemented = []string{}
		derivedWatermark = derived.Watermark
		derivedRecords = len(derived.EstimateCoverageMetricsDaily) +
			len(derived.InvestmentClassificationsDaily) + len(derived.InvestmentMetricsDaily) +
			len(derived.IssueTypeMetricsDaily) + len(derived.WorkItemCycleTimes) +
			len(derived.WorkItemMetricsDaily) + len(derived.WorkItemStateDurationsDaily) +
			len(derived.WorkItemTeamAttributions) + len(derived.WorkItemUserMetricsDaily)
	}
	summary := JiraAtlassianWorkItemsResult{
		WorkItemsSynced: len(rows.WorkItems), TransitionsSynced: len(rows.Transitions),
		DependenciesSynced: len(rows.Dependencies), ReopenEventsSynced: len(rows.ReopenEvents),
		InteractionsSynced: len(rows.Interactions), SprintsSynced: len(rows.Sprints),
		WorklogsSynced:                   len(rows.Worklogs),
		RawDestinations:                  append([]string(nil), jiraAtlassianRawDestinations...),
		DerivedDestinationsImplemented:   append([]string(nil), derivedImplemented...),
		DerivedDestinationsUnimplemented: append([]string(nil), derivedUnimplemented...),
		WatermarkHeldForIncomplete:       len(optionalIncomplete) > 0 || derivedWatermark == nil,
	}
	result := map[string]any{
		"work_items_synced": len(rows.WorkItems), "transitions_synced": len(rows.Transitions),
		"dependencies_synced": len(rows.Dependencies), "reopen_events_synced": len(rows.ReopenEvents),
		"interactions_synced": len(rows.Interactions), "sprints_synced": len(rows.Sprints),
		"worklogs_synced": len(rows.Worklogs), "project_key": projectKey,
		"project_memberships_synced":         len(rows.ProjectMemberships),
		"unresolved_project_memberships":     unresolvedProjectMemberships,
		"raw_destinations":                   append([]string(nil), jiraAtlassianRawDestinations...),
		"derived_destinations_implemented":   append([]string(nil), derivedImplemented...),
		"derived_destinations_unimplemented": append([]string(nil), derivedUnimplemented...),
		"watermark_held_for_incomplete":      summary.WatermarkHeldForIncomplete,
		"jira_work_items":                    summary,
	}
	if len(optionalIncomplete) > 0 {
		result["incomplete"] = optionalIncomplete
	}
	result = attachWorkItemTeamInheritanceObservation(result, handler.Derived)
	var watermark *time.Time
	if len(optionalIncomplete) == 0 && derivedWatermark != nil {
		value := derivedWatermark.UTC()
		watermark = &value
	}
	return CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: watermark,
		Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: searchPages, Records: len(rows.WorkItems) + derivedRecords},
		WorklogObservations: worklogObservations,
	}, nil
}

func cloneJiraSprintRows(rows []jiraSprintRow) []jiraSprintRow {
	result := make([]jiraSprintRow, len(rows))
	copy(result, rows)
	return result
}

// collectJiraAtlassianIssues used to page GET /rest/api/3/search by
// startAt/total. Atlassian retired that endpoint outright (410 Gone --
// CHAOS-4585); the replacement, /rest/api/3/search/jql, pages by cursor
// (nextPageToken/isLast) instead, so the request builder and the response
// parser had to change together, not incrementally (a startAt-shaped request
// against the new path 404s, and a total-based stop condition against a
// response that no longer carries `total` never terminates). Rather than
// keep two Jira issue-search implementations that could drift again the same
// way this one drifted from Python's, this now delegates to
// collectJiraWorkItemIssues (jira_work_items_route.go), which already
// targets /search/jql with cursor paging and is the one this repo's live
// proof confirmed against real Jira (org 70d529e0, project SUP) -- see
// CHAOS-4585. expandChangelog=false: this route's own Collect loop fetches
// each issue's changelog separately (collectJiraAtlassianChangelog) and
// overwrites issue["changelog"] with it, so an inlined changelog from the
// search response would be wasted bytes the caller never reads -- and could
// trip jiraFetchObject's 2MiB per-object cap on a history-heavy page,
// failing an otherwise-healthy sync (caught in codex review).
func collectJiraAtlassianIssues(ctx context.Context, client *providerfoundation.HTTPClient, jql string, maxPages, maxRows, perPage int) ([]map[string]any, int, error) {
	return collectJiraWorkItemIssues(ctx, client, jql, maxPages, maxRows, perPage, false)
}

func collectJiraAtlassianChangelog(ctx context.Context, client *providerfoundation.HTTPClient, issueKey string, maxPages int) ([]any, int, error) {
	values := make([]any, 0)
	start := 0
	seen := make(map[int]struct{})
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, pages, ErrPaginationCapExceeded
		}
		if _, ok := seen[start]; ok {
			return nil, pages, ErrPaginationCapExceeded
		}
		seen[start] = struct{}{}
		query := url.Values{"startAt": {strconv.Itoa(start)}, "maxResults": {strconv.Itoa(jiraAtlassianWorklogPerPage)}}
		var page struct {
			Values []json.RawMessage `json:"values"`
			Total  *int              `json:"total"`
			IsLast *bool             `json:"isLast"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/issue/"+url.PathEscape(issueKey)+"/changelog?"+query.Encode(), nil, &page); err != nil {
			return nil, pages + 1, err
		}
		if page.Values == nil {
			return nil, pages + 1, providerfoundation.ErrNormalizationInvalid
		}
		for _, raw := range page.Values {
			var value any
			if err := decodeJiraJSON(raw, &value); err != nil {
				return nil, pages + 1, err
			}
			values = append(values, value)
		}
		if (page.IsLast != nil && *page.IsLast) || (page.Total != nil && start+len(page.Values) >= *page.Total) || len(page.Values) < jiraAtlassianWorklogPerPage {
			return values, pages + 1, nil
		}
		if len(page.Values) == 0 {
			return nil, pages + 1, ErrPaginationCapExceeded
		}
		start += len(page.Values)
	}
}

func normalizeJiraWorklog(claim Claim, workItemID string, raw map[string]any, resolveIdentity jiraIdentityResolver, normalizedAt time.Time) (jiraWorklogRow, error) {
	if claim.Validate() != nil || claim.Provider != "jira" || workItemID == "" || normalizedAt.IsZero() {
		return jiraWorklogRow{}, ErrInvalidConfiguration
	}
	id := stringFrom(firstJiraValue(raw, "id", "worklogId"))
	started, created, updated := jiraTime(firstJiraValue(raw, "started", "startDate")), jiraTime(raw["created"]), jiraTime(raw["updated"])
	if id == "" || started == nil {
		return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
	}
	if created == nil {
		created = started
	}
	if updated == nil {
		updated = created
	}
	seconds := int64(0)
	value := firstJiraValue(raw, "timeSpentSeconds", "timeInSeconds")
	if nested, ok := jiraMapValue(raw["timeSpent"]); ok {
		value = firstJiraValue(nested, "timeInSeconds", "seconds")
	}
	switch number := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseInt(string(number), 10, 64)
		if err != nil || parsed < 0 {
			return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
		}
		seconds = parsed
	case float64:
		if number < 0 || number != float64(int64(number)) {
			return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
		}
		seconds = int64(number)
	case int64:
		if number < 0 {
			return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
		}
		seconds = number
	case int:
		if number < 0 {
			return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
		}
		seconds = int64(number)
	default:
		return jiraWorklogRow{}, providerfoundation.ErrNormalizationInvalid
	}
	author := (*string)(nil)
	if value := raw["author"]; value != nil {
		if resolved := jiraResolveMapUser(value, resolveIdentity); resolved != "" && resolved != "unknown" {
			author = &resolved
		}
	}
	return jiraWorklogRow{WorkItemID: workItemID, Provider: "jira", WorklogID: id, Author: author, StartedAt: started.UTC(), TimeSpentSeconds: seconds, CreatedAt: created.UTC(), UpdatedAt: updated.UTC(), LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID}, nil
}

func firstJiraValue(values map[string]any, names ...string) any {
	for _, name := range names {
		if value, ok := values[name]; ok {
			return value
		}
	}
	return nil
}

type jiraAtlassianBoard struct{ ID int64 }

func collectJiraBoards(ctx context.Context, client *providerfoundation.HTTPClient, maxPages, perPage int) ([]jiraAtlassianBoard, int, error) {
	boards := make([]jiraAtlassianBoard, 0)
	start, requests := 0, 0
	seen := make(map[int]struct{})
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, requests, ErrPaginationCapExceeded
		}
		if _, ok := seen[start]; ok {
			return nil, requests, ErrPaginationCapExceeded
		}
		seen[start] = struct{}{}
		query := url.Values{"startAt": {strconv.Itoa(start)}, "maxResults": {strconv.Itoa(perPage)}}
		var page struct {
			Values []struct {
				ID json.Number `json:"id"`
			} `json:"values"`
			IsLast *bool `json:"isLast"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/agile/1.0/board?"+query.Encode(), nil, &page); err != nil {
			return nil, requests + 1, err
		}
		requests++
		for _, value := range page.Values {
			id, err := strconv.ParseInt(string(value.ID), 10, 64)
			if err != nil || id <= 0 {
				return nil, requests, providerfoundation.ErrNormalizationInvalid
			}
			boards = append(boards, jiraAtlassianBoard{ID: id})
		}
		if (page.IsLast != nil && *page.IsLast) || len(page.Values) < perPage {
			return boards, requests, nil
		}
		if len(page.Values) == 0 {
			return nil, requests, ErrPaginationCapExceeded
		}
		start += len(page.Values)
	}
}

func collectJiraBoardSprints(ctx context.Context, client *providerfoundation.HTTPClient, boardID int64, maxPages, perPage int) ([]map[string]any, int, error) {
	if boardID <= 0 {
		return nil, 0, providerfoundation.ErrNormalizationInvalid
	}
	sprints := make([]map[string]any, 0)
	start, requests := 0, 0
	seen := make(map[int]struct{})
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, requests, ErrPaginationCapExceeded
		}
		if _, ok := seen[start]; ok {
			return nil, requests, ErrPaginationCapExceeded
		}
		seen[start] = struct{}{}
		query := url.Values{"startAt": {strconv.Itoa(start)}, "maxResults": {strconv.Itoa(perPage)}}
		var page struct {
			Values []json.RawMessage `json:"values"`
			IsLast *bool             `json:"isLast"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/agile/1.0/board/"+strconv.FormatInt(boardID, 10)+"/sprint?"+query.Encode(), nil, &page); err != nil {
			return nil, requests + 1, err
		}
		requests++
		if page.Values == nil {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		for _, raw := range page.Values {
			var value map[string]any
			if err := decodeJiraJSON(raw, &value); err != nil {
				return nil, requests, err
			}
			sprints = append(sprints, value)
		}
		if (page.IsLast != nil && *page.IsLast) || len(page.Values) < perPage {
			return sprints, requests, nil
		}
		if len(page.Values) == 0 {
			return nil, requests, ErrPaginationCapExceeded
		}
		start += len(page.Values)
	}
}

func collectJiraAtlassianWorklogs(ctx context.Context, client, graphqlClient *providerfoundation.HTTPClient, issueKey string, graphql bool, maxPages int, cloudID string) ([]map[string]any, int, JiraWorklogFetchObservation, error) {
	observation := JiraWorklogFetchObservation{IssueKey: issueKey}
	graphqlRequests := 0
	if graphql && strings.TrimSpace(cloudID) != "" {
		observation.GraphQLAttempted = true
		if values, requests, err := collectJiraGraphQLWorklogs(ctx, graphqlClient, issueKey, maxPages, cloudID); err == nil {
			observation.GraphQLRequests = requests
			observation.GraphQLSucceeded = true
			return values, requests, observation, nil
		} else {
			graphqlRequests = requests
			observation.GraphQLRequests = requests
			observation.RESTFallbackUsed = true
		}
	}
	values := make([]map[string]any, 0)
	start, requests := 0, 0
	seen := make(map[int]struct{})
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, graphqlRequests + requests, observation, ErrPaginationCapExceeded
		}
		if _, ok := seen[start]; ok {
			return nil, graphqlRequests + requests, observation, ErrPaginationCapExceeded
		}
		seen[start] = struct{}{}
		query := url.Values{"startAt": {strconv.Itoa(start)}, "maxResults": {strconv.Itoa(jiraAtlassianWorklogPerPage)}}
		var page struct {
			Worklogs []json.RawMessage `json:"worklogs"`
			Total    *int              `json:"total"`
		}
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/issue/"+url.PathEscape(issueKey)+"/worklog?"+query.Encode(), nil, &page); err != nil {
			observation.RESTRequests = requests + 1
			return nil, graphqlRequests + requests + 1, observation, err
		}
		requests++
		observation.RESTRequests = requests
		if page.Worklogs == nil {
			return nil, graphqlRequests + requests, observation, providerfoundation.ErrNormalizationInvalid
		}
		for _, raw := range page.Worklogs {
			var value map[string]any
			if err := decodeJiraJSON(raw, &value); err != nil {
				return nil, graphqlRequests + requests, observation, err
			}
			values = append(values, value)
		}
		if page.Total != nil && start+len(page.Worklogs) >= *page.Total {
			return values, graphqlRequests + requests, observation, nil
		}
		if page.Total == nil && len(page.Worklogs) < jiraAtlassianWorklogPerPage {
			return values, graphqlRequests + requests, observation, nil
		}
		if len(page.Worklogs) == 0 {
			return nil, graphqlRequests + requests, observation, ErrPaginationCapExceeded
		}
		start += len(page.Worklogs)
	}
}

const jiraAtlassianWorklogQuery = `query JiraIssueWorklogsPage($cloudId: ID!, $key: String!, $first: Int!, $after: String) { issue: issueByKey(key: $key, cloudId: $cloudId) { worklogs(first: $first, after: $after) { pageInfo { hasNextPage endCursor } edges { cursor node { worklogId author { accountId name } timeSpent { timeInSeconds } created updated startDate } } } } }`

func collectJiraGraphQLWorklogs(ctx context.Context, client *providerfoundation.HTTPClient, issueKey string, maxPages int, cloudID string) ([]map[string]any, int, error) {
	values := make([]map[string]any, 0)
	after := ""
	seen := make(map[string]struct{})
	requests := 0
	for pages := 0; ; pages++ {
		if pages >= maxPages {
			return nil, requests, ErrPaginationCapExceeded
		}
		if _, ok := seen[after]; ok {
			return nil, requests, ErrPaginationCapExceeded
		}
		seen[after] = struct{}{}
		body, err := json.Marshal(map[string]any{"query": jiraAtlassianWorklogQuery, "variables": map[string]any{"cloudId": cloudID, "key": issueKey, "first": jiraAtlassianWorklogPerPage, "after": nilIfEmpty(after)}})
		if err != nil {
			return nil, requests, err
		}
		response, err := client.Do(ctx, http.MethodPost, "/graphql", bytes.NewReader(body))
		requests++
		if err != nil {
			return nil, requests, err
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
		_ = response.Body.Close()
		if err != nil || len(raw) > nativeMaxObjectBytes {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		var payload struct {
			Data struct {
				Issue struct {
					Worklogs struct {
						Edges []struct {
							Node map[string]any `json:"node"`
						} `json:"edges"`
						PageInfo struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
					} `json:"worklogs"`
				} `json:"issue"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := decodeJiraJSON(raw, &payload); err != nil || len(payload.Errors) > 0 || payload.Data.Issue.Worklogs.Edges == nil {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		for _, edge := range payload.Data.Issue.Worklogs.Edges {
			if edge.Node == nil {
				return nil, requests, providerfoundation.ErrNormalizationInvalid
			}
			values = append(values, edge.Node)
		}
		info := payload.Data.Issue.Worklogs.PageInfo
		if !info.HasNextPage {
			return values, requests, nil
		}
		if info.EndCursor == "" {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		after = info.EndCursor
	}
}

func nilIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
