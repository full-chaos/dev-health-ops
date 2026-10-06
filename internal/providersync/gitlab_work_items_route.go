package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"log/slog"
	"net/http"
	"net/url"
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
	// runs with; a configured value may only lower it.
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

// gitLabWorkItemDerivedGap is the raw-only route's honest remainder of the
// 16-destination canonical work-item family. Once the typed deriver is injected
// at the processor boundary, all ten concrete derived destinations are emitted.
var gitLabWorkItemDerivedGap = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_user_metrics_daily",
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
// framework's legacy result map. It keeps raw/derived completion and watermark
// withholding typed even though CompleteRouteBatch predates provider-specific
// result structs.
type GitLabWorkItemsResult struct {
	WorkItemsSynced                  int      `json:"work_items_synced"`
	TransitionsSynced                int      `json:"transitions_synced"`
	DependenciesSynced               int      `json:"dependencies_synced"`
	ReopenEventsSynced               int      `json:"reopen_events_synced"`
	InteractionsSynced               int      `json:"interactions_synced"`
	SprintsSynced                    int      `json:"sprints_synced"`
	RawDestinations                  []string `json:"raw_destinations"`
	DerivedDestinationsImplemented   []string `json:"derived_destinations_implemented"`
	DerivedDestinationsUnimplemented []string `json:"derived_destinations_unimplemented"`
	WatermarkHeldForDerivedGap       bool     `json:"watermark_held_for_derived_gap"`
}

// gitlabWorkItemsDeriver is injected at the processor boundary. It is not a
// registry/configuration seam: activation remains outside this provider slice.
type gitlabWorkItemsDeriver interface {
	Derive(context.Context, Claim, gitlabWorkItemRows, time.Time) (GitLabWorkItemDerivedRows, error)
}

// GitLabWorkItemsRouteHandler is the canonical provider-only route. It mirrors
// GitLabProvider._ingest_with_client's issues, merge requests, label/state
// events, notes, links, and milestones while retaining the live
// fetch_gitlab_work_items boundary for issue status/type normalization.
//
// WIRING: WIRED. internal/workerservice/provider_sync.go's
// `provider == "gitlab" && dataset == "work-items"` case constructs this
// handler and assigns it to routeHandler, with
// NewGitLabWorkItemFamilyClickHouseEffects as sink and readback and
// NewGitLabWorkItemDeriver as Derived.
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
	Derived         gitlabWorkItemsDeriver
}

func gitLabWorkItemsFlag(value *bool) bool { return value == nil || *value }

func (handler GitLabWorkItemsRouteHandler) limits() (int, int, int, error) {
	perPage, maxPages, nestedMaxPages := handler.PerPage, handler.MaxPages, handler.NestedMaxPages
	if perPage == 0 {
		perPage = gitLabWorkItemsDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = gitLabWorkItemsDefaultMaxPages
	}
	if nestedMaxPages == 0 {
		nestedMaxPages = min(maxPages, gitLabWorkItemsNestedHardMaxPages)
	}
	if perPage < 1 || perPage > gitLabWorkItemsMaximumPerPage || maxPages < 1 ||
		maxPages > gitLabWorkItemsDefaultMaxPages || nestedMaxPages < 1 ||
		nestedMaxPages > gitLabWorkItemsDefaultMaxPages {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, nestedMaxPages, nil
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
	perPage, maxPages, nestedMaxPages, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	requests := 0
	counted := *client
	counted.Doer = gitLabWorkItemsCountingDoer{delegate: client.Doer, requests: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
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
	repoIDText, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := uuid.Parse(repoIDText)
	if err != nil || repoID == uuid.Nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
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
	pages := 1 // the project binding request
	closingSynced := 0
	issueLinksUnsupported := 0
	closingIncomplete := make([]string, 0)
	closingTransient, closingTerminal := 0, 0

	if fetchMilestones {
		milestones, milestonePages, milestoneErr := collectGitLabMilestones(
			ctx, &counted, root+"/milestones", perPage, nestedMaxPages,
		)
		if milestoneErr != nil {
			return CompleteRouteBatch{}, milestoneErr
		}
		pages += milestonePages
		for _, milestone := range milestones {
			sprint, sprintErr := normalizeGitLabSprint(claim, fullName, milestone, normalizedAt)
			if sprintErr != nil {
				return CompleteRouteBatch{}, sprintErr
			}
			rows.Sprints = append(rows.Sprints, sprint)
		}
	}

	query := url.Values{"state": {"all"}}
	// This intentionally has no updated_before parameter. The actual
	// fetch_gitlab_work_items producer passes updated_after only to
	// python-gitlab; its until value is not part of that API call.
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	issuePayloads, issuePages, issueErr := collectGitLabPayloads(
		ctx, &counted, root+"/issues", query, perPage, maxPages,
	)
	if issueErr != nil {
		return CompleteRouteBatch{}, issueErr
	}
	pages += issuePages
	for _, raw := range issuePayloads {
		var payload gitlabIssueWorkItemPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		labelEvents := []gitlabLabelEventPayload(nil)
		if fetchLabels {
			var labelPages int
			labelEvents, labelPages, err = collectGitLabLabelEvents(
				ctx, &counted, root+"/issues/"+strconv.Itoa(payload.IID)+"/resource_label_events",
				perPage, nestedMaxPages,
			)
			if err != nil {
				return CompleteRouteBatch{}, err
			}
			pages += labelPages
		}
		item, transitions, normalizeErr := normalizeGitLabIssueWorkItem(
			claim, fullName, repoID, payload, labelEvents, handler.StatusMapping,
			handler.ResolveIdentity, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		// fetch_gitlab_work_items is the live issue-normalization oracle and
		// intentionally leaves priority unset. The canonical provider batch
		// enriches its issue rows before persistence, so keep that provider-only
		// augmentation at the route boundary rather than changing oracle truth.
		item.PriorityRaw, item.ServiceClass = gitlabPriorityFromLabels(item.Labels)
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
		if fetchHistory {
			events, eventPages, eventErr := collectGitLabStateEvents(
				ctx, &counted, root+"/issues/"+strconv.Itoa(payload.IID)+"/resource_state_events",
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
				ctx, &counted, root+"/issues/"+strconv.Itoa(payload.IID)+"/links",
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
			rows.Dependencies = append(rows.Dependencies,
				normalizeGitLabDependencies(claim, item.WorkItemID, fullName, description, links, normalizedAt)...)
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
				ctx, &counted, root+"/issues/"+strconv.Itoa(payload.IID)+"/closed_by",
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
			}
		}
		if fetchComments {
			notes, notePages, noteErr := collectGitLabNotes(
				ctx, &counted, root+"/issues/"+strconv.Itoa(payload.IID)+"/notes",
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
			ctx, &counted, root+"/merge_requests", query, perPage, maxPages,
		)
		if mergeRequestErr != nil {
			return CompleteRouteBatch{}, mergeRequestErr
		}
		pages += mergeRequestPages
		for _, raw := range mergeRequests {
			var payload gitlabMergeRequestWorkItemPayload
			if err := json.Unmarshal(raw, &payload); err != nil {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			stateEvents := []gitlabStateEventPayload(nil)
			if fetchHistory {
				var statePages int
				stateEvents, statePages, err = collectGitLabStateEvents(
					ctx, &counted, root+"/merge_requests/"+strconv.Itoa(payload.IID)+"/resource_state_events",
					perPage, nestedMaxPages,
				)
				if err != nil {
					return CompleteRouteBatch{}, err
				}
				pages += statePages
			}
			item, transitions, reopens, normalizeErr := normalizeGitLabMergeRequestWorkItem(
				claim, fullName, repoID, payload, stateEvents, handler.ResolveIdentity, normalizedAt,
			)
			if normalizeErr != nil {
				return CompleteRouteBatch{}, normalizeErr
			}
			rows.WorkItems = append(rows.WorkItems, item)
			rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
			rows.ReopenEvents = append(rows.ReopenEvents, reopens...)
			attributions, attributionErr := normalizeGitLabMRAIAttributions(
				claim, repoID, payload, normalizedAt,
			)
			if attributionErr != nil {
				return CompleteRouteBatch{}, attributionErr
			}
			rows.AIAttributions = append(rows.AIAttributions, attributions...)
			if fetchComments {
				notes, notePages, noteErr := collectGitLabNotes(
					ctx, &counted, root+"/merge_requests/"+strconv.Itoa(payload.IID)+"/notes",
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

	effects, err := buildGitLabWorkItemEffectsFromRows(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	derivedUnimplemented := append([]string(nil), gitLabWorkItemDerivedGap...)
	derivedImplemented := []string{}
	derivedRecords := 0
	var watermark *time.Time
	if handler.Derived != nil {
		derived, deriveErr := handler.Derived.Derive(ctx, claim, rows, normalizedAt)
		if deriveErr != nil {
			return CompleteRouteBatch{}, deriveErr
		}
		if len(derived.Gaps) > 0 {
			gaps := make([]string, 0, len(derived.Gaps))
			for _, gap := range derived.Gaps {
				if !gitlabWorkItemDerivedDestination(gap.Destination) ||
					strings.TrimSpace(gap.AuthoritativeProducer) == "" ||
					strings.TrimSpace(gap.Reason) == "" {
					return CompleteRouteBatch{}, ErrInvalidConfiguration
				}
				gaps = append(gaps, gap.Destination)
			}
			return CompleteRouteBatch{}, fmt.Errorf(
				"%w: %s", ErrGitLabWorkItemDerivedProducerUnavailable, strings.Join(gaps, ", "),
			)
		}
		derivedEffects, effectErr := BuildGitLabWorkItemDerivedEffects(derived.EffectRows())
		if effectErr != nil {
			return CompleteRouteBatch{}, effectErr
		}
		effects = append(effects, derivedEffects...)
		derivedImplemented = derived.producedDestinations()
		derivedUnimplemented = []string{}
		if len(derivedUnimplemented) == 0 && claim.BeforeAt != nil &&
			(derived.Watermark == nil || !derived.Watermark.Equal(claim.BeforeAt.UTC())) {
			return CompleteRouteBatch{}, ErrInvalidConfiguration
		}
		watermark = derived.Watermark
		if closingTransient > 0 {
			watermark = nil
		}
		derivedRecords = len(derived.AIAttributions) + len(derived.EstimateCoverageMetricsDaily) +
			len(derived.InvestmentClassificationsDaily) + len(derived.InvestmentMetricsDaily) +
			len(derived.IssueTypeMetricsDaily) + len(derived.WorkItemCycleTimes) +
			len(derived.WorkItemMetricsDaily) + len(derived.WorkItemStateDurationsDaily) +
			len(derived.WorkItemTeamAttributions) + len(derived.WorkItemUserMetricsDaily)
	}
	summary := GitLabWorkItemsResult{
		WorkItemsSynced: len(rows.WorkItems), TransitionsSynced: len(rows.StatusTransitions),
		DependenciesSynced: len(rows.Dependencies), ReopenEventsSynced: len(rows.ReopenEvents),
		InteractionsSynced: len(rows.Interactions), SprintsSynced: len(rows.Sprints),
		RawDestinations:                  append([]string(nil), gitLabWorkItemRawDestinations...),
		DerivedDestinationsImplemented:   append([]string(nil), derivedImplemented...),
		DerivedDestinationsUnimplemented: derivedUnimplemented,
		WatermarkHeldForDerivedGap:       len(derivedUnimplemented) > 0,
	}
	result := map[string]any{
		"work_items_synced":                  len(rows.WorkItems),
		"transitions_synced":                 len(rows.StatusTransitions),
		"dependencies_synced":                len(rows.Dependencies),
		"reopen_events_synced":               len(rows.ReopenEvents),
		"interactions_synced":                len(rows.Interactions),
		"sprints_synced":                     len(rows.Sprints),
		"raw_destinations":                   append([]string(nil), gitLabWorkItemRawDestinations...),
		"derived_destinations_implemented":   derivedImplemented,
		"derived_destinations_unimplemented": derivedUnimplemented,
		"watermark_held_for_derived_gap":     len(derivedUnimplemented) > 0,
		"gitlab_work_items":                  summary,
		// CHAOS-8526: gitlab_closing_reference rows synced, and the issues whose closed_by fetch failed, split into
		// transient (the watermark is held while any did) and terminal 404/403 (the watermark advances).
		"closing_reference_dependencies_synced": closingSynced,
		"issue_links_unsupported_shape":         issueLinksUnsupported,
		"closing_reference_fetch_failed":        closingTransient + closingTerminal,
		"closing_reference_fetch_transient":     closingTransient,
		"closing_reference_fetch_terminal":      closingTerminal,
	}
	if len(closingIncomplete) > 0 {
		result["incomplete"] = closingIncomplete
	}
	result = attachWorkItemTeamInheritanceObservation(result, handler.Derived)
	return CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows.WorkItems) + len(rows.StatusTransitions) +
				len(rows.Dependencies) + len(rows.ReopenEvents) + len(rows.Interactions) +
				len(rows.Sprints) + derivedRecords,
		},
	}, nil
}

func collectGitLabPayloads(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	query url.Values,
	perPage, maxPages int,
) ([]json.RawMessage, int, error) {
	page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client,
		providerfoundation.GitLabPageOptions{Path: path, Query: query, PerPage: perPage, MaxPages: maxPages})
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
	path string,
	query url.Values,
	perPage, maxPages int,
) ([]json.RawMessage, int, error) {
	items, pages, err := collectGitLabPayloads(ctx, client, path, query, perPage, maxPages)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		return items, pages, err
	}
	owner, field := path, path
	if cut := strings.LastIndex(path, "/"); cut >= 0 {
		owner, field = path[:cut], path[cut+1:]
	}
	slog.Error("providersync.gitlab.nested_list_bound_exceeded",
		"owner", owner, "field", field, "pages", pages, "max_pages", maxPages)
	return nil, pages, fmt.Errorf(
		"%w: gitlab %s of %s still had a next page after %d pages (max %d pages)",
		ErrPaginationCapExceeded, field, owner, pages, maxPages,
	)
}

func collectGitLabMilestones(
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabIssueMilestonePayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, url.Values{"state": {"all"}}, perPage, maxPages)
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
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabLabelEventPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, nil, perPage, maxPages)
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
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabStateEventPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, nil, perPage, maxPages)
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
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabIssueLinkPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, nil, perPage, maxPages)
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
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabClosingMergeRequestPayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, nil, perPage, maxPages)
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
	ctx context.Context, client *providerfoundation.HTTPClient, path string,
	perPage, maxPages int,
) ([]gitlabNotePayload, int, error) {
	items, pages, err := collectGitLabNestedPayloads(ctx, client, path, nil, perPage, maxPages)
	if err != nil {
		return nil, pages, err
	}
	result := make([]gitlabNotePayload, 0, len(items))
	for _, raw := range items {
		var payload gitlabNotePayload
		if err := json.Unmarshal(raw, &payload); err != nil {
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
