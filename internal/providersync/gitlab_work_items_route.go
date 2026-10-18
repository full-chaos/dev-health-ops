package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	gitLabWorkItemsDefaultPerPage  = 100
	gitLabWorkItemsMaximumPerPage  = 100
	gitLabWorkItemsDefaultMaxPages = 10_000
	// gitLabWorkItemsNestedHardMaxPages is the per-item, per-list runaway guard
	// for notes, label events, state events, links, closing merge requests and
	// milestones (CHAOS-8777). 100 pages is 10,000 rows at 100 per page: far
	// above any real item, low enough to stop a list that never ends. The
	// production wiring (internal/workerservice/provider_sync.go, the GitLab
	// work-items case) sets no NestedMaxPages, so this is the bound production
	// runs with; a configured value is clamped to it (limits()).
	gitLabWorkItemsNestedHardMaxPages = 100
)

// gitLabWorkItemRawDestinations are the six raw facts emitted by the Python
// GitLabProvider batch. They are deliberately kept separate from the canonical
// derived destinations; the unconfigured/raw-only route does not manufacture
// empty effects for a destination whose producer is not active.
var gitLabWorkItemRawDestinations = []string{
	"work_items",
	"work_item_transitions",
	"work_item_dependencies",
	"work_item_reopen_events",
	"work_item_interactions",
	"sprints",
}

// GitLabWorkItemsRequestUsage retains the route's physical request count for
// later budget evidence without coupling this provider-only slice to registry
// or activation wiring.
type GitLabWorkItemsRequestUsage struct {
	Transport    string `json:"transport"`
	RouteFamily  string `json:"route_family"`
	Dimension    string `json:"dimension"`
	RequestCount int    `json:"request_count"`
}

// GitLabWorkItemsResult is the concrete provider result carried inside the
// framework's legacy result map. It keeps the raw row counts typed even
// though CompleteRouteBatch predates provider-specific result structs.
type GitLabWorkItemsResult struct {
	WorkItemsSynced    int      `json:"work_items_synced"`
	TransitionsSynced  int      `json:"transitions_synced"`
	DependenciesSynced int      `json:"dependencies_synced"`
	ReopenEventsSynced int      `json:"reopen_events_synced"`
	InteractionsSynced int      `json:"interactions_synced"`
	SprintsSynced      int      `json:"sprints_synced"`
	EpicsSynced        int      `json:"epics_synced"`
	RawDestinations    []string `json:"raw_destinations"`
}

// GitLabWorkItemsRouteHandler is the canonical provider-only route. It mirrors
// GitLabProvider._ingest_with_client's issues, merge requests, label/state
// events, notes, links, and milestones while retaining the live
// fetch_gitlab_work_items boundary for issue status/type normalization.
//
// WIRING: WIRED. internal/workerservice/provider_sync.go's
// `provider == "gitlab" && dataset == "work-items"` case constructs this
// handler and assigns it to routeHandler, with
// NewGitLabWorkItemFamilyClickHouseEffects as sink and readback. The unit
// writes raw rows only; the daily job writes every table computed from them.
//
// WIRED IS NOT EXECUTING, and this comment claims only the former. Planning
// admits a canonical work-items unit only when the descriptor satisfies
// RouteReady AND Plannable AND ExecutedProofSatisfied
// (internal/scheduler/sync/planner.go:401-423); an attempted-but-unproven
// executed-proof state returns `PlannedUnit{}, false` before BuildExecutor or
// Execute is ever reached, which is precisely the condition that silently
// vetoed github/work-items for two months under CHAOS-4060/CHAOS-4731. Whether
// this route runs for a given org is a question about that gate and the org's
// dataset selection, not about the wiring documented above.
//
// SUPERSEDED: "The handler is intentionally not registered here. Activation,
// SUPERSEDED: alias watermarks, configuration loading, and deployment wiring
// SUPERSEDED: are separate work."
//
// Activation happened; kept visible rather than deleted so the wrong model is
// refuted rather than merely absent (CHAOS-4848).
type GitLabWorkItemsRouteHandler struct {
	// RepoIdentity reads the project's stored repository identity (CHAOS-3383).
	RepoIdentity    GitLabRepoIdentityReader
	PerPage         int
	MaxPages        int
	NestedMaxPages  int
	StatusMapping   *StatusMapping
	ResolveIdentity gitlabIdentityResolver
	FetchComments   *bool
	FetchHistory    *bool
	FetchLabels     *bool
	FetchLinks      *bool
	FetchMilestones *bool
	IncludeMRs      *bool
	// FetchEpics is Python's GITLAB_FETCH_EPICS: off, no issue is linked to
	// its epic. Default on, as in Python.
	FetchEpics *bool
}

func gitLabWorkItemsFlag(value *bool) bool { return value == nil || *value }

// gitLabFetchEpicsEnv is the worker environment flag Python's GitLab provider
// reads to link issues to epics (providers/gitlab/provider.py:114).
const gitLabFetchEpicsEnv = "GITLAB_FETCH_EPICS"

// gitLabFetchEpicsFromEnv reads GITLAB_FETCH_EPICS from the worker
// environment with Python's env_flag rule.
func gitLabFetchEpicsFromEnv() *bool {
	raw, ok := os.LookupEnv(gitLabFetchEpicsEnv)
	return pythonEnvFlag(gitLabFetchEpicsEnv, raw, ok)
}

// pythonEnvFlag is Python's env_flag (providers/utils.py) on a value the
// caller read: 1/true/yes/on is true, 0/false/no/off is false, trimmed and
// case-insensitive; unset or any other value is nil, so the route keeps its
// default.
func pythonEnvFlag(name, raw string, ok bool) *bool {
	if !ok {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		value := true
		return &value
	case "0", "false", "no", "off":
		value := false
		return &value
	}
	slog.Warn("providersync.env_flag_ignored", "name", name)
	return nil
}

func (handler GitLabWorkItemsRouteHandler) limits() (int, int, int, error) {
	perPage, maxPages, nestedMaxPages := handler.PerPage, handler.MaxPages, handler.NestedMaxPages
	if perPage == 0 {
		perPage = gitLabWorkItemsDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = gitLabWorkItemsDefaultMaxPages
	}
	if nestedMaxPages == 0 {
		nestedMaxPages = maxPages
	}
	if perPage < 1 || perPage > gitLabWorkItemsMaximumPerPage || maxPages < 1 ||
		maxPages > gitLabWorkItemsDefaultMaxPages || nestedMaxPages < 1 ||
		nestedMaxPages > gitLabWorkItemsDefaultMaxPages {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	// The nested ceiling is a hard one: a configured value (or the top-level
	// MaxPages it defaults to) may only lower it.
	return perPage, maxPages, min(nestedMaxPages, gitLabWorkItemsNestedHardMaxPages), nil
}

type gitLabWorkItemsCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	requests *int
}

func (doer gitLabWorkItemsCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests++
	return doer.delegate.Do(request)
}

func (handler GitLabWorkItemsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "work-items" || credential.Provider != "gitlab" ||
		credential.ID == "" || credential.ID != claim.CredentialID || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil || client.Doer == nil ||
		client.Lease == nil || normalizedAt.IsZero() || claim.BeforeAt == nil ||
		handler.StatusMapping == nil {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if _, err := workItemsUnitWindowDays(claim, normalizedAt); err != nil {
		return CompleteRouteBatch{}, err
	}
	perPage, maxPages, nestedMaxPages, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	query := url.Values{"state": {"all"}}
	// CHAOS-8805: the fetch honours the window end. The frozen Python producer
	// passes updated_after only (its until value is not part of the API call);
	// chris's ruling (CHAOS-8805, 2026-10-06 D2/D4980: a unit holds the window it
	// was given, as GitHub and Linear do) adds updated_before. The API treats the
	// bound as inclusive, so an item updated exactly at the end is in two adjacent
	// windows; the writes are idempotent.
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	// Logged before the project lookup, so a unit that fails binding its project still names its window.
	slog.Default().InfoContext(ctx, "gitlab_work_items_window",
		"org_id", claim.OrgID, "unit_id", claim.ID, "updated_after", query.Get("updated_after"),
		"updated_before", query.Get("updated_before"))
	requests := 0
	counted := *client
	counted.Doer = gitLabWorkItemsCountingDoer{delegate: client.Doer, requests: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObjectRoute(budgetAccount(ctx, "project"), &counted, gitlabProjectRoute, root, &project); err != nil {
		return CompleteRouteBatch{}, err
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	fullName := gitLabProjectFullName(project)
	if strings.TrimSpace(fullName) == "" {
		fullName = strings.TrimSpace(claim.SourceName)
	}
	repoIdentity, err := resolveGitLabRepoIdentity(ctx, handler.RepoIdentity, claim, parsedProjectID, fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := uuid.Parse(repoIdentity.RepoID)
	if err != nil || repoID == uuid.Nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	catalogID, ok := GitLabCatalogProjectID(claim.OrgID, projectID)
	if !ok {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	catalogProjectID := catalogID.String()
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	rows := gitlabWorkItemRows{
		WorkItems:         make([]gitlabWorkItemRow, 0),
		StatusTransitions: make([]gitlabWorkItemTransitionRow, 0),
		Dependencies:      make([]gitlabWorkItemDependencyRow, 0),
		ReopenEvents:      make([]gitlabWorkItemReopenRow, 0),
		Interactions:      make([]gitlabWorkItemInteractionRow, 0),
		Sprints:           make([]gitlabSprintRow, 0),
	}
	fetchComments := gitLabWorkItemsFlag(handler.FetchComments)
	fetchHistory := gitLabWorkItemsFlag(handler.FetchHistory)
	fetchLabels := gitLabWorkItemsFlag(handler.FetchLabels)
	fetchLinks := gitLabWorkItemsFlag(handler.FetchLinks)
	fetchMilestones := gitLabWorkItemsFlag(handler.FetchMilestones)
	includeMRs := gitLabWorkItemsFlag(handler.IncludeMRs)
	fetchEpics := gitLabWorkItemsFlag(handler.FetchEpics)
	pages := 1 // the project binding request
	closingSynced := 0
	// dependencyOwners[i] is the synced item rows.Dependencies[i] was read from.
	dependencyOwners := make([]string, 0)
	issueLinksUnsupported := 0
	closingIncomplete := make([]string, 0)
	closingTransient, closingTerminal := 0, 0

	if fetchMilestones {
		milestones, milestonePages, milestoneErr := collectGitLabMilestones(
			budgetAccount(ctx, "milestones"), &counted, gitlabMilestonesRoute, root+"/milestones", perPage, nestedMaxPages,
		)
		if milestoneErr != nil {
			return CompleteRouteBatch{}, milestoneErr
		}
		pages += milestonePages
		seenMilestone := make(map[string]bool, len(milestones))
		for _, milestone := range milestones {
			// A milestone's id is global: one listed twice is one sprint row.
			if id := milestone.ID.String(); seenMilestone[id] {
				continue
			} else {
				seenMilestone[id] = true
			}
			// A group milestone is keyed by its group (the key the project target
			// names), so every project of the group writes the same row.
			owner := repoIdentity.Path
			if milestone.isGroupMilestone() {
				owner = gitlabGroupMilestoneOwner(milestone)
			}
			sprint, sprintErr := normalizeGitLabSprint(claim, owner, milestone, normalizedAt)
			if sprintErr != nil {
				return CompleteRouteBatch{}, sprintErr
			}
			rows.Sprints = append(rows.Sprints, sprint)
		}
		slog.Default().DebugContext(ctx, "gitlab_work_items_milestones",
			"org_id", claim.OrgID, "unit_id", claim.ID, "milestones_listed", len(milestones), "sprints_written", len(rows.Sprints),
			"group_milestones", countGroupMilestones(milestones))
	}

	epicsSynced, epicsSkipped, epicsHeld := 0, 0, false
	var epicsIncomplete []string
	if gitLabWorkItemsFlag(handler.FetchEpics) {
		outcome, epicErr := handler.collectGitLabGroupEpics(ctx, claim, &counted, fullName, query, perPage, maxPages, nestedMaxPages,
			fetchHistory, fetchComments, normalizedAt, &rows, &pages)
		if epicErr != nil {
			return CompleteRouteBatch{}, epicErr
		}
		epicsSynced, epicsSkipped, epicsHeld, epicsIncomplete = outcome.synced, outcome.skipped, outcome.held, outcome.incomplete
	}
	issuePayloads, issuePages, issueErr := collectGitLabPayloads(
		budgetAccount(ctx, "issues"), &counted, gitlabIssuesRoute, root+"/issues", query, perPage, maxPages,
	)
	if issueErr != nil {
		return CompleteRouteBatch{}, issueErr
	}
	pages += issuePages
	issuePayloads = dropOutsideWorkItemWindow(ctx, counted.Metrics, claim, issuePayloads, gitLabWorkItemWindowView)
	for _, raw := range issuePayloads {
		var payload gitlabIssueWorkItemPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		labelEvents := []gitlabLabelEventPayload(nil)
		if fetchLabels {
			var labelPages int
			labelEvents, labelPages, err = collectGitLabLabelEvents(
				budgetAccount(ctx, "issues"), &counted, gitlabIssuesRoute+"/{iid}/resource_label_events", root+"/issues/"+strconv.Itoa(payload.IID)+"/resource_label_events",
				perPage, nestedMaxPages,
			)
			if err != nil {
				return CompleteRouteBatch{}, err
			}
			pages += labelPages
		}
		item, transitions, normalizeErr := normalizeGitLabIssueWorkItem(
			claim, fullName, repoIdentity.Path, catalogProjectID, repoID, payload.withFetchEpics(fetchEpics), labelEvents, handler.StatusMapping,
			handler.ResolveIdentity, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
		if fetchHistory {
			events, eventPages, eventErr := collectGitLabStateEvents(
				budgetAccount(ctx, "notes"), &counted, gitlabIssuesRoute+"/{iid}/resource_state_events", root+"/issues/"+strconv.Itoa(payload.IID)+"/resource_state_events",
				perPage, nestedMaxPages,
			)
			if eventErr != nil {
				return CompleteRouteBatch{}, eventErr
			}
			pages += eventPages
			rows.ReopenEvents = append(rows.ReopenEvents,
				normalizeGitLabIssueReopens(claim, item.WorkItemID, events, handler.ResolveIdentity, normalizedAt)...)
		}
		if fetchLinks {
			links, linkPages, linkErr := collectGitLabIssueLinks(
				budgetAccount(ctx, "issues"), &counted, gitlabIssuesRoute+"/{iid}/links", root+"/issues/"+strconv.Itoa(payload.IID)+"/links",
				perPage, nestedMaxPages,
			)
			if linkErr != nil {
				return CompleteRouteBatch{}, linkErr
			}
			pages += linkPages
			description := ""
			if payload.Description != nil {
				description = *payload.Description
			}
			dependencies := normalizeGitLabDependencies(claim, item.WorkItemID, fullName, description, links, normalizedAt)
			rows.Dependencies = append(rows.Dependencies, dependencies...)
			dependencyOwners = appendGitLabDependencyOwner(dependencyOwners, item.WorkItemID, len(dependencies))
			unsupported := 0
			for _, link := range links {
				if _, ok := link.targetWorkItemID(); !ok {
					unsupported++
				}
			}
			if unsupported > 0 {
				issueLinksUnsupported += unsupported
				slog.Warn("providersync.gitlab.issue_link_unsupported_shape",
					"org_id", claim.OrgID, "unit_id", claim.ID, "issue", item.WorkItemID, "count", unsupported)
			}
			closing, closingPages, closingErr := collectGitLabClosingMergeRequests(
				budgetAccount(ctx, "issues"), &counted, gitlabIssuesRoute+"/{iid}/closed_by", root+"/issues/"+strconv.Itoa(payload.IID)+"/closed_by",
				perPage, nestedMaxPages,
			)
			pages += closingPages
			if closingErr != nil {
				// An optional sub-fetch: a cancelled run still stops. A terminal answer (404/403: not readable with this
				// credential) is logged, counted under its own outcome and reported incomplete, but the watermark advances,
				// since retrying cannot help. Any other failure (5xx, timeout, 429, network) is transient: logged, counted,
				// reported incomplete, and the watermark is held so the next run asks again (a closing MR does not move the
				// issue's updated_at, so a lost answer would otherwise never be re-fetched). The batch goes on either way.
				if ctx.Err() != nil {
					return CompleteRouteBatch{}, closingErr
				}
				closingIncomplete = append(closingIncomplete, item.WorkItemID)
				outcome := gitLabClosingFetchOutcome(closingErr)
				if outcome == "transient_failed" {
					closingTransient++
				} else {
					closingTerminal++
				}
				counted.Metrics.RecordGitLabClosingMRFetch(outcome)
				errorClass, errorType := logging.ErrorClass(closingErr), logging.ErrorType(closingErr) // never the error text (CHAOS-7933)
				slog.Warn("providersync.gitlab.closing_mr_fetch_failed",
					"org_id", claim.OrgID, "unit_id", claim.ID, "issue", item.WorkItemID, "outcome", outcome,
					"error_class", errorClass, "error_type", errorType)
			} else {
				counted.Metrics.RecordGitLabClosingMRFetch("synced")
				closingRows := normalizeGitLabClosingMergeRequests(claim, item.WorkItemID, closing, normalizedAt)
				closingSynced += len(closingRows)
				rows.Dependencies = append(rows.Dependencies, closingRows...)
				dependencyOwners = appendGitLabDependencyOwner(dependencyOwners, item.WorkItemID, len(closingRows))
			}
		}
		if fetchComments {
			notes, notePages, noteErr := collectGitLabNotes(
				budgetAccount(ctx, "notes"), &counted, gitlabIssuesRoute+"/{iid}/notes", root+"/issues/"+strconv.Itoa(payload.IID)+"/notes",
				perPage, nestedMaxPages,
			)
			if noteErr != nil {
				return CompleteRouteBatch{}, noteErr
			}
			pages += notePages
			rows.Interactions = append(rows.Interactions,
				normalizeGitLabNotes(claim, item.WorkItemID, notes, handler.ResolveIdentity, normalizedAt)...)
		}
	}

	if includeMRs {
		mergeRequests, mergeRequestPages, mergeRequestErr := collectGitLabPayloads(
			budgetAccount(ctx, "merge_requests"), &counted, gitlabMergeRequestsRoute, root+"/merge_requests", query, perPage, maxPages,
		)
		if mergeRequestErr != nil {
			return CompleteRouteBatch{}, mergeRequestErr
		}
		pages += mergeRequestPages
		mergeRequests = dropOutsideWorkItemWindow(ctx, counted.Metrics, claim, mergeRequests, gitLabWorkItemWindowView)
		for _, raw := range mergeRequests {
			var payload gitlabMergeRequestWorkItemPayload
			if err := json.Unmarshal(raw, &payload); err != nil {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			stateEvents := []gitlabStateEventPayload(nil)
			if fetchHistory {
				var statePages int
				stateEvents, statePages, err = collectGitLabStateEvents(
					budgetAccount(ctx, "notes"), &counted, gitlabMergeRequestsRoute+"/{iid}/resource_state_events", root+"/merge_requests/"+strconv.Itoa(payload.IID)+"/resource_state_events",
					perPage, nestedMaxPages,
				)
				if err != nil {
					return CompleteRouteBatch{}, err
				}
				pages += statePages
			}
			item, transitions, reopens, normalizeErr := normalizeGitLabMergeRequestWorkItem(
				claim, fullName, repoIdentity.Path, catalogProjectID, repoID, payload, stateEvents, handler.ResolveIdentity, normalizedAt,
			)
			if normalizeErr != nil {
				return CompleteRouteBatch{}, normalizeErr
			}
			rows.WorkItems = append(rows.WorkItems, item)
			rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
			rows.ReopenEvents = append(rows.ReopenEvents, reopens...)
			// Merge-request AI attribution is written by the prs unit alone (one
			// writer per ai_attribution key); this route emits none.
			if fetchComments {
				notes, notePages, noteErr := collectGitLabNotes(
					budgetAccount(ctx, "notes"), &counted, gitlabMergeRequestsRoute+"/{iid}/notes", root+"/merge_requests/"+strconv.Itoa(payload.IID)+"/notes",
					perPage, nestedMaxPages,
				)
				if noteErr != nil {
					return CompleteRouteBatch{}, noteErr
				}
				pages += notePages
				rows.Interactions = append(rows.Interactions,
					normalizeGitLabNotes(claim, item.WorkItemID, notes, handler.ResolveIdentity, normalizedAt)...)
			}
		}
	}
	identityPaths, err := resolveGitLabIdentityPaths(ctx, handler.RepoIdentity, claim, fullName, repoIdentity,
		gitLabDependencyPaths(rows.Dependencies, dependencyOwners))
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	rows.Dependencies = canonicalGitLabDependencies(rows.Dependencies, dependencyOwners, identityPaths)

	effects, err := buildGitLabWorkItemEffectsFromRows(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	// ai_attribution is a raw fact: it is read from the provider payload at
	// normalization, not computed from stored rows, so the unit still writes
	// it. Every table computed from stored work-item rows is left to the
	// daily job.
	aiAttribution, err := buildGitLabTypedDerivedEffect("ai_attribution", rows.AIAttributions)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	effects = append(effects, aiAttribution)
	observeWorkItemDerivedTablesLeftToDailyJob(client.Metrics, claim, len(rows.WorkItems))
	// The watermark is the end of the window whose raw rows this unit stored.
	// It is held only while a closing-reference fetch failed transiently.
	var watermark *time.Time
	if claim.BeforeAt != nil && closingTransient == 0 && !epicsHeld {
		value := claim.BeforeAt.UTC()
		watermark = &value
	}
	summary := GitLabWorkItemsResult{
		WorkItemsSynced: len(rows.WorkItems), TransitionsSynced: len(rows.StatusTransitions),
		DependenciesSynced: len(rows.Dependencies), ReopenEventsSynced: len(rows.ReopenEvents),
		InteractionsSynced: len(rows.Interactions), SprintsSynced: len(rows.Sprints), EpicsSynced: epicsSynced,
		RawDestinations: append([]string(nil), gitLabWorkItemRawDestinations...),
	}
	result := map[string]any{
		"work_items_synced":    len(rows.WorkItems),
		"transitions_synced":   len(rows.StatusTransitions),
		"dependencies_synced":  len(rows.Dependencies),
		"reopen_events_synced": len(rows.ReopenEvents),
		"interactions_synced":  len(rows.Interactions),
		"sprints_synced":       len(rows.Sprints),
		"epics_synced":         epicsSynced,
		"epics_skipped":        epicsSkipped,
		"raw_destinations":     append([]string(nil), gitLabWorkItemRawDestinations...),
		"gitlab_work_items":    summary,
		// CHAOS-8526: gitlab_closing_reference rows synced, and the issues whose closed_by fetch failed, split into
		// transient (the watermark is held while any did) and terminal 404/403 (the watermark advances).
		"closing_reference_dependencies_synced": closingSynced,
		"issue_links_unsupported_shape":         issueLinksUnsupported,
		"closing_reference_fetch_failed":        closingTransient + closingTerminal,
		"closing_reference_fetch_transient":     closingTransient,
		"closing_reference_fetch_terminal":      closingTerminal,
	}
	if incomplete := append(append([]string(nil), closingIncomplete...), epicsIncomplete...); len(incomplete) > 0 {
		result["incomplete"] = incomplete
	}
	return CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows.WorkItems) + len(rows.StatusTransitions) +
				len(rows.Dependencies) + len(rows.ReopenEvents) + len(rows.Interactions) +
				len(rows.Sprints) + len(rows.AIAttributions),
		},
	}, nil
}

func collectGitLabPayloads(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	route, path string,
	query url.Values,
	perPage, maxPages int,
) ([]json.RawMessage, int, error) {
	page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client,
		providerfoundation.GitLabPageOptions{Path: path, Route: route, Query: query, PerPage: perPage, MaxPages: maxPages})
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	return page.Items, page.Pages, nil
}

// collectGitLabNestedPayloads pages one nested list (notes, label events, state
// events, links, closing merge requests, milestones) to its end. Past the page
// bound the unit fails closed and the error names the owner item and the field
// (the request path without its query string: a project id and an iid, never a
// credential or a payload). Nothing is truncated: CHAOS-8770.
func collectGitLabNestedPayloads(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	route, path string,
	query url.Values,
	perPage, maxPages int,
) ([]json.RawMessage, int, error) {
	// maxPages+1 requests are allowed on purpose: a list of exactly maxPages
	// FULL pages cannot be told from a longer one without reading the next
	// page (with or without an X-Next-Page header the paginator infers one
	// more page from a full last page). That extra page may only be EMPTY; any
	// row on it is a row past the bound and fails closed.
	var items []json.RawMessage
	collected, err := providerfoundation.VisitGitLabPageParamPages(ctx, client,
		providerfoundation.GitLabPageOptions{Path: path, Route: route, Query: query, PerPage: perPage, MaxPages: maxPages + 1},
		func(visit providerfoundation.PageVisit) error {
			if visit.Pages > maxPages && len(visit.Items) > 0 {
				return errGitLabNestedPastBound
			}
			// An EMPTY page that still advertises a next page is an ambiguous
			// answer: the paginator stops on it, so rows after it would be left
			// out of a run that looks complete. Fail closed (CHAOS-8777 r2).
			if len(visit.Items) == 0 && visit.CursorAfter != "0" {
				return errGitLabNestedEmptyWithNext
			}
			items = append(items, visit.Items...)
			return nil
		})
	if errors.Is(err, errGitLabNestedEmptyWithNext) {
		owner, field := gitLabNestedOwnerAndField(path)
		slog.Error("providersync.gitlab.nested_list_empty_page_with_next",
			"owner", owner, "field", field, "page", collected.Pages+1, "max_pages", maxPages)
		return nil, collected.Pages, fmt.Errorf(
			"%w: gitlab %s of %s ended on an empty page that still advertised a next page",
			ErrPaginationCapExceeded, field, owner,
		)
	}
	if err != nil && !errors.Is(err, errGitLabNestedPastBound) {
		return nil, 0, err
	}
	if err == nil && !collected.PageBudgetExhausted {
		return items, min(collected.Pages, maxPages), nil
	}
	owner, field := gitLabNestedOwnerAndField(path)
	slog.Error("providersync.gitlab.nested_list_bound_exceeded",
		"owner", owner, "field", field, "pages", maxPages, "max_pages", maxPages)
	return nil, maxPages, fmt.Errorf(
		"%w: gitlab %s of %s still had a next page after %d pages (max %d pages)",
		ErrPaginationCapExceeded, field, owner, maxPages, maxPages,
	)
}

var errGitLabNestedPastBound = errors.New("gitlab nested list has rows past the page bound")
var errGitLabNestedEmptyWithNext = errors.New("gitlab nested list page is empty but advertises a next page")

// gitLabNestedOwnerAndField splits a nested list path into its owner item and
// the field: the request path without its query string (a project id and an iid,
// never a credential or payload text).
func gitLabNestedOwnerAndField(path string) (owner, field string) {
	owner, field = path, path
	if cut := strings.LastIndex(path, "/"); cut >= 0 {
		owner, field = path[:cut], path[cut+1:]
	}
	return owner, field
}

func collectGitLabMilestones(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabIssueMilestonePayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, url.Values{"state": {"all"}, "include_ancestors": {"true"}, "include_parent_milestones": {"true"}}, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabIssueMilestonePayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabIssueMilestonePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func collectGitLabLabelEvents(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabLabelEventPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabLabelEventPayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabLabelEventPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func collectGitLabStateEvents(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabStateEventPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabStateEventPayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabStateEventPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func collectGitLabIssueLinks(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabIssueLinkPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabIssueLinkPayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabIssueLinkPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func collectGitLabClosingMergeRequests(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabClosingMergeRequestPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabClosingMergeRequestPayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabClosingMergeRequestPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		if _, ok := payload.reference(); !ok {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func collectGitLabNotes(
	ctx context.Context, client *providerfoundation.HTTPClient, route, path string,
	perPage, maxPages int,
) ([]gitlabNotePayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, route, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabNotePayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabNotePayload
		// UseNumber: a note id above 2^53 must keep its exact text (CHAOS-8790).
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			return nil, pages, providerfoundation.ErrNormalizationInvalid
		}
		result = append(result, payload)
	}
	return result, pages, nil
}

func buildGitLabWorkItemEffectsFromRows(rows gitlabWorkItemRows) ([]EffectBatch, error) {
	return BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{
		WorkItems: rows.WorkItems, StatusTransitions: rows.StatusTransitions,
		Dependencies: rows.Dependencies, ReopenEvents: rows.ReopenEvents,
		Interactions: rows.Interactions, Sprints: rows.Sprints,
	})
}

var _ CompleteRouteHandler = GitLabWorkItemsRouteHandler{}

// gitLabClosingFetchOutcome classifies a failed closed_by fetch for the watermark (D4771). Terminal outcomes repeat on
// every run for the same issue, so retrying cannot help: terminal_unavailable (404, or a 403 that is not rate
// limiting: not readable with this credential), terminal_page_cap (the answer exceeds the page cap) and terminal_undecodable (the answer does not decode).
// Anything else (5xx, timeout, 429, network, or an error that is not a ProviderError) is transient_failed, the safe side
// for a watermark.
func gitLabClosingFetchOutcome(err error) string {
	switch {
	case errors.Is(err, ErrPaginationCapExceeded):
		return "terminal_page_cap"
	case errors.Is(err, providerfoundation.ErrNormalizationInvalid):
		return "terminal_undecodable"
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr != nil && providerErr.Class != providerfoundation.ErrorRateLimited &&
		(providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusForbidden) {
		return "terminal_unavailable"
	}
	return "transient_failed"
}

type gitLabEpicOutcome struct {
	synced     int
	skipped    int
	held       bool
	incomplete []string
}

// collectGitLabGroupEpics mirrors GitLabProvider._ingest_with_client's epic
// block (providers/gitlab/provider.py:150-223): the epics of the project's
// group (its path minus the last segment), each with its resource state
// events and notes, as work items of type "epic". Epics are a Premium/Ultimate
// feature: a 403 or 404 on any epic read is the free-tier answer, logged at
// Debug, and the unit goes on. Any other failure is a lost answer, not a zero:
// the part is reported incomplete and the watermark is held so the next run asks
// again (CHAOS-9857, the Jira worklog rule). The window is the issues' window,
// two-ended (CHAOS-8805); Python sent updated_after only.
func (handler GitLabWorkItemsRouteHandler) collectGitLabGroupEpics(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	fullName string,
	issueQuery url.Values,
	perPage, maxPages, nestedMaxPages int,
	fetchHistory, fetchComments bool,
	normalizedAt time.Time,
	rows *gitlabWorkItemRows,
	pages *int,
) (gitLabEpicOutcome, error) {
	var outcome gitLabEpicOutcome
	ctx = budgetAccount(ctx, "epics")
	parts := strings.Split(fullName, "/")
	if len(parts) < 2 {
		slog.Default().InfoContext(ctx, "gitlab_work_items_epics",
			"org_id", claim.OrgID, "unit_id", claim.ID, "outcome", "no_group", "epics_written", 0)
		return outcome, nil
	}
	groupPath := strings.Join(parts[:len(parts)-1], "/")
	result, listed := "error", 0
	defer func() {
		slog.Default().InfoContext(ctx, "gitlab_work_items_epics",
			"org_id", claim.OrgID, "unit_id", claim.ID, "group", groupPath, "outcome", result,
			"epics_listed", listed, "epics_written", outcome.synced, "epics_skipped", outcome.skipped,
			"incomplete", len(outcome.incomplete))
	}()
	epicQuery := url.Values{"state": {"all"}}
	for _, key := range []string{"updated_after", "updated_before"} {
		if value := issueQuery.Get(key); value != "" {
			epicQuery.Set(key, value)
		}
	}
	groupRoot := providerRelativePath(client, "api", "v4", "groups", groupPath)
	// hold records a lost answer: the part is incomplete and the watermark is held.
	hold := func(what string, err error) error {
		if ctx.Err() != nil {
			return err
		}
		outcome.held = true
		outcome.incomplete = append(outcome.incomplete, what)
		errorClass, errorType := logging.ErrorClass(err), logging.ErrorType(err) // never the error text (CHAOS-7933)
		status := 0
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr != nil {
			status = providerErr.StatusCode
		}
		slog.Default().WarnContext(ctx, "gitlab_work_items_epics_fetch_failed",
			"org_id", claim.OrgID, "unit_id", claim.ID, "group", groupPath, "part", what, "status", status,
			"error_class", errorClass, "error_type", errorType)
		return nil
	}
	unavailable := func(err error) bool {
		var providerErr *providerfoundation.ProviderError
		return errors.As(err, &providerErr) && providerErr != nil && providerErr.Class != providerfoundation.ErrorRateLimited &&
			(providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusForbidden)
	}
	payloads, listPages, err := collectGitLabPayloads(ctx, client, gitlabGroupEpicsRoute, groupRoot+"/epics", epicQuery, perPage, maxPages)
	*pages += listPages
	if err != nil {
		if unavailable(err) {
			result = "unavailable"
			return outcome, nil
		}
		if holdErr := hold("epics", err); holdErr != nil {
			return outcome, holdErr
		}
		result = "incomplete"
		return outcome, nil
	}
	listed = len(payloads)
	for _, raw := range payloads {
		var payload gitlabEpicPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return outcome, providerfoundation.ErrNormalizationInvalid
		}
		iidText, _, _ := pythonJSONScalar(payload.IID)
		epicRoot := groupRoot + "/epics/" + url.PathEscape(iidText)
		stateEvents := []gitlabStateEventPayload(nil)
		if fetchHistory {
			var statePages int
			stateEvents, statePages, err = collectGitLabStateEvents(
				ctx, client, gitlabGroupEpicsRoute+"/{iid}/resource_state_events", epicRoot+"/resource_state_events", perPage, nestedMaxPages)
			*pages += statePages
			if err != nil {
				stateEvents = nil
				if !unavailable(err) {
					if holdErr := hold("epic_state_events:"+iidText, err); holdErr != nil {
						return outcome, holdErr
					}
				}
			}
		}
		item, transitions, reopens, normalizeErr := normalizeGitLabEpicWorkItem(
			claim, groupPath, payload, stateEvents, handler.StatusMapping, handler.ResolveIdentity, normalizedAt)
		if normalizeErr != nil {
			if errors.Is(normalizeErr, ErrInvalidConfiguration) {
				// A declared divergence from Python (which would write epic:0 or an
				// empty title): the epic is skipped, never silently. One Warn per
				// skip names the group, the iid and the reason; the count is in the
				// unit result (epics_skipped) and the Info outcome line.
				outcome.skipped++
				reason := "no_usable_iid"
				if iidText != "" && strings.TrimSpace(payload.Title) == "" {
					reason = "empty_title"
				}
				slog.Default().WarnContext(ctx, "gitlab_work_items_epic_skipped",
					"org_id", claim.OrgID, "unit_id", claim.ID, "group", groupPath, "iid", iidText, "reason", reason)
				continue
			}
			return outcome, normalizeErr
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
		rows.ReopenEvents = append(rows.ReopenEvents, reopens...)
		outcome.synced++
		if fetchComments {
			notes, notePages, noteErr := collectGitLabNotes(
				ctx, client, gitlabGroupEpicsRoute+"/{iid}/notes", epicRoot+"/notes", perPage, nestedMaxPages)
			*pages += notePages
			if noteErr != nil {
				if !unavailable(noteErr) {
					if holdErr := hold("epic_notes:"+iidText, noteErr); holdErr != nil {
						return outcome, holdErr
					}
				}
				continue
			}
			rows.Interactions = append(rows.Interactions,
				normalizeGitLabNotes(claim, item.WorkItemID, notes, handler.ResolveIdentity, normalizedAt)...)
		}
	}
	result = "ok"
	if outcome.held {
		result = "incomplete"
	}
	return outcome, nil
}
