package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// jiraTeamCatalogProjectSearchMaxResults mirrors team_discovery.
	// discover_jira's single, unpaginated GET exactly -- see
	// jiraTeamCatalogProjectSearchPayload's doc comment.
	jiraTeamCatalogProjectSearchMaxResults = 100
	// jiraTeamCatalogPerPage matches JiraClient's default per_page (100) for
	// the Agile board/sprint listing calls Python's iter_boards/
	// iter_board_sprints make.
	jiraTeamCatalogPerPage = 100
	// jiraTeamCatalogBoardsMaxPages/jiraTeamCatalogSprintsMaxPages are safety
	// caps Python's own while-True generators do not have (matching the
	// defensive bound GitLab's port already adds over its own unbounded
	// Python loops) -- 5,000 boards per project / 5,000 sprints per board.
	jiraTeamCatalogBoardsMaxPages  = 50
	jiraTeamCatalogSprintsMaxPages = 50
)

// _JIRA_BOARD_CAPABLE_PROJECT_TYPES / _JIRA_NO_BOARD_PROJECT_TYPES port
// team_autoimport_jira.py's identical allowlists verbatim (CHAOS-4575): only
// "software" projects have Agile boards; service_desk/business/
// product_discovery are confirmed to have none; any other/unknown type is a
// hard failure, never a silent skip (an unrecognized type could be a real
// board-capable project whose data would otherwise go silently missing).
var jiraTeamCatalogBoardCapableProjectTypes = map[string]bool{"software": true}

var jiraTeamCatalogNoBoardProjectTypes = map[string]bool{
	"service_desk":      true,
	"business":          true,
	"product_discovery": true,
}

// JiraTeamCatalogRouteHandler owns the provider-only team/project-ownership/
// membership/sprint catalog walk that ports
// src/dev_health_ops/workers/team_autoimport_jira.py. Claim-free (CHAOS-4431
// ruling, team-lead 2026-08-28, option (c)): team/member/project reference
// discovery runs once per sync run per provider, not as a claimed
// provider-unit. It stays DB-free -- JiraTeamCatalogCollector (below) merges
// in the jira_project_ops_team_links legacy carry-forward, which needs the
// ClickHouse connection this Handler deliberately never sees.
type JiraTeamCatalogRouteHandler struct{}

type JiraTeamCatalogResult struct {
	TeamsImported                int `json:"teams_imported"`
	ProjectsImported             int `json:"projects_imported"`
	MembersImported              int `json:"members_imported"`
	TeamMembershipsImported      int `json:"team_memberships_imported"`
	TeamProjectOwnershipImported int `json:"team_project_ownership_imported"`
	SprintsImported              int `json:"sprints_imported"`
	// WalkSkipped (Python parity, mirrors GitLabTeamCatalogResult.WalkSkipped)
	// is true when a non-strict walk failure -- project search, or (with
	// Members selected) a project's lead lookup -- skipped the ENTIRE walk,
	// matching team_autoimport_jira.py's populate() returning
	// status=skipped/_zero_summary rather than a partial write. Strict mode
	// returns the error unchanged instead (WalkSkipped stays false).
	WalkSkipped    bool   `json:"walk_skipped,omitempty"`
	WalkSkipReason string `json:"walk_skip_reason,omitempty"`
}

type JiraTeamCatalogEvidence struct {
	Provider string `json:"provider"`
	Requests int    `json:"requests"`
}

// JiraTeamCatalogBatch deliberately carries no pre-built Effects the way
// GitLab's batch does: Ownership/Projects still need the
// jira_project_ops_team_links legacy merge (which needs the ClickHouse
// connection this Handler never sees) and Teams/Memberships still need
// guard filtering -- both happen in JiraTeamCatalogCollector, which builds
// every EffectBatch itself from Rows.
type JiraTeamCatalogBatch struct {
	Rows     JiraTeamCatalogRows     `json:"rows"`
	Result   JiraTeamCatalogResult   `json:"result"`
	Evidence JiraTeamCatalogEvidence `json:"evidence"`
}

func jiraTeamCatalogWalkSkipBatch(reason string) JiraTeamCatalogBatch {
	return JiraTeamCatalogBatch{Result: JiraTeamCatalogResult{WalkSkipped: true, WalkSkipReason: reason}}
}

// jiraTeamCatalogWalkFailure is the whole-walk abort branch point (project
// search or, when Members is selected, a project's lead lookup): under
// strict, re-raise unchanged; under non-strict, log and return a clean,
// successful skip -- mirrors team_autoimport_jira.py's populate() catching
// discover_jira's failure (and run_team_autoimport's own outer catch-all for
// every OTHER exception the function raises, including a member lookup
// failure) and returning a zero summary instead of a partial write.
func jiraTeamCatalogWalkFailure(
	ctx context.Context, ref TeamCatalogReference, reason string, err error,
) (JiraTeamCatalogBatch, error) {
	if ref.Strict {
		return JiraTeamCatalogBatch{}, err
	}
	slog.Default().WarnContext(ctx, "jira_team_catalog_walk_skipped",
		"org_id", ref.OrgID, "reason", reason, "error", err)
	return jiraTeamCatalogWalkSkipBatch(reason), nil
}

func (handler JiraTeamCatalogRouteHandler) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (JiraTeamCatalogBatch, error) {
	if ctx == nil || ref.validate() != nil || credential.Provider != jiraTeamCatalogProvider ||
		client == nil || client.Provider != jiraTeamCatalogProvider || client.BaseURL == nil ||
		client.Doer == nil || client.Lease == nil || normalizedAt.IsZero() {
		return JiraTeamCatalogBatch{}, ErrInvalidConfiguration
	}
	// CHAOS-4437 parity: sprint/cycle reference discovery is unconditional
	// reference data, so the early exit below (matching Python's
	// `if not strict and not (want_teams or want_projects or want_members)`)
	// is the ONLY gate on it -- a strict call always proceeds even with
	// every selection off.
	if !ref.Strict && !selections.Any() {
		return JiraTeamCatalogBatch{}, nil
	}
	claim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: jiraTeamCatalogProvider}}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	evidence := JiraTeamCatalogEvidence{Provider: jiraTeamCatalogProvider}

	searchPath := "/rest/api/3/project/search?" + url.Values{
		"maxResults": {strconv.Itoa(jiraTeamCatalogProjectSearchMaxResults)},
	}.Encode()
	var search jiraTeamCatalogProjectSearchPayload
	if err := jiraFetchObject(ctx, client, http.MethodGet, searchPath, nil, &search); err != nil {
		return jiraTeamCatalogWalkFailure(ctx, ref, "project_discovery_failed", err)
	}
	evidence.Requests++

	rows := JiraTeamCatalogRows{}
	projectKeys := make([]string, 0, len(search.Values))
	for _, entry := range search.Values {
		team, ok := normalizeJiraTeamRow(ref.OrgID, entry, normalizedAt)
		if !ok {
			continue
		}
		rows.Teams = append(rows.Teams, team)
		rows.Ownership = append(rows.Ownership, normalizeJiraOwnershipRow(ref.OrgID, team.ID, team.ID, normalizedAt))
		rows.Projects = append(rows.Projects, normalizeJiraProjectRow(ref.OrgID, team.ID, team.Name, normalizedAt))
		projectKeys = append(projectKeys, team.ID)
	}
	rows.Projects = dedupeJiraProjectCatalogRows(rows.Projects)
	rows.Ownership = dedupeJiraOwnershipRows(rows.Ownership)

	if selections.Members {
		for teamIndex, team := range rows.Teams {
			var detail jiraTeamCatalogProjectDetailPayload
			detailPath := "/rest/api/3/project/" + url.PathEscape(team.ID)
			if err := jiraFetchObject(ctx, client, http.MethodGet, detailPath, nil, &detail); err != nil {
				return jiraTeamCatalogWalkFailure(ctx, ref, "project_lead_lookup_failed", err)
			}
			evidence.Requests++
			if detail.Lead == nil {
				continue
			}
			membership, ok := normalizeJiraMembershipRow(ref.OrgID, team.ID, *detail.Lead, normalizedAt)
			if !ok {
				continue
			}
			rows.Memberships = append(rows.Memberships, membership)
			rows.Teams[teamIndex].Members = append([]string(nil), membership.IdentityFacets...)
		}
		rows.Teams = jiraStampMembersAuthoritative(rows.Teams)
	}

	sprints, sprintRequests, sprintErr := handler.collectSprints(ctx, client, claim, projectKeys, normalizedAt)
	evidence.Requests += sprintRequests
	if sprintErr != nil {
		if ref.Strict {
			return JiraTeamCatalogBatch{}, sprintErr
		}
		slog.Default().WarnContext(ctx, "jira_team_catalog_sprint_walk_skipped",
			"org_id", ref.OrgID, "error", sprintErr)
		sprints = nil
	}
	rows.Sprints = sprints

	for _, row := range rows.Teams {
		if err := validateJiraTeamRow(claim, row); err != nil {
			return JiraTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Ownership {
		if err := validateJiraOwnershipRow(claim, row); err != nil {
			return JiraTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Memberships {
		if err := validateJiraMembershipRow(claim, row); err != nil {
			return JiraTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Projects {
		if err := row.validate(claim); err != nil {
			return JiraTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Sprints {
		if err := validateJiraSprint(row, claim); err != nil {
			return JiraTeamCatalogBatch{}, err
		}
	}

	result := JiraTeamCatalogResult{
		TeamsImported: len(rows.Teams), TeamProjectOwnershipImported: len(rows.Ownership),
		TeamMembershipsImported: len(rows.Memberships), ProjectsImported: len(rows.Projects),
		MembersImported: len(distinctJiraMembershipMembers(rows.Memberships)),
		SprintsImported: len(rows.Sprints),
	}
	return JiraTeamCatalogBatch{Rows: rows, Result: result, Evidence: evidence}, nil
}

// jiraStampMembersAuthoritative marks every team row's roster authoritative
// once the Members walk above finished without aborting -- an empty
// Members slice at that point is a genuine "this project's lead lookup
// returned no lead", not an unconfirmed read (unlike GitLab's per-group
// soft-fail carry-forward, Jira's lead lookup is all-or-nothing: any
// failure aborts the whole walk via jiraTeamCatalogWalkFailure above, so
// reaching this point means every team's lookup succeeded).
func jiraStampMembersAuthoritative(teams []jiraTeamCatalogTeamRow) []jiraTeamCatalogTeamRow {
	for index := range teams {
		teams[index].MembersAuthoritative = true
	}
	return teams
}

// collectSprints ports team_autoimport_jira.py's board/sprint discovery
// block. A non-skippable failure ANYWHERE in this walk (an unrecognized
// project type, a board-listing failure, or a non-skippable sprint-listing
// failure) aborts the WHOLE walk and returns every sprint gathered so far as
// lost -- mirroring Python's single outer try/except around this entire
// block, which resets `sprint_rows = []` on any exception that is not the
// one documented per-board 400 shape. The caller decides strict-vs-non-strict
// handling of that returned error; this function itself never distinguishes.
func (handler JiraTeamCatalogRouteHandler) collectSprints(
	ctx context.Context, client *providerfoundation.HTTPClient, claim Claim, projectKeys []string, normalizedAt time.Time,
) ([]jiraSprintRow, int, error) {
	sprints := make([]jiraSprintRow, 0)
	requests := 0
	for _, key := range projectKeys {
		var detail jiraTeamCatalogProjectDetailPayload
		detailPath := "/rest/api/3/project/" + url.PathEscape(key)
		if err := jiraFetchObject(ctx, client, http.MethodGet, detailPath, nil, &detail); err != nil {
			return nil, requests, err
		}
		requests++
		projectType := strings.ToLower(strings.TrimSpace(detail.ProjectTypeKey))
		if jiraTeamCatalogNoBoardProjectTypes[projectType] {
			continue
		}
		if !jiraTeamCatalogBoardCapableProjectTypes[projectType] {
			return nil, requests, fmt.Errorf("%w: unrecognized jira project type %q for project_key=%q",
				providerfoundation.ErrNormalizationInvalid, detail.ProjectTypeKey, key)
		}
		boards, boardRequests, err := handler.iterBoards(ctx, client, key)
		requests += boardRequests
		if err != nil {
			return nil, requests, err
		}
		for _, board := range boards {
			boardID := strings.TrimSpace(board.ID.String())
			if boardID == "" {
				continue
			}
			boardSprints, sprintRequests, skipDetail, err := handler.iterBoardSprints(ctx, client, boardID)
			requests += sprintRequests
			if err != nil {
				return nil, requests, err
			}
			for _, raw := range boardSprints {
				var payload map[string]any
				if err := decodeJiraJSON(raw, &payload); err != nil {
					return nil, requests, providerfoundation.ErrNormalizationInvalid
				}
				sprint, sprintErr := normalizeJiraSprint(claim, payload, normalizedAt)
				if sprintErr != nil {
					continue
				}
				sprints = append(sprints, sprint)
			}
			if skipDetail != "" {
				slog.Default().WarnContext(ctx, "jira_team_catalog_board_sprints_skipped",
					"org_id", claim.OrgID, "board_id", boardID, "detail", skipDetail)
			}
		}
	}
	return sprints, requests, nil
}

func (handler JiraTeamCatalogRouteHandler) iterBoards(
	ctx context.Context, client *providerfoundation.HTTPClient, projectKey string,
) ([]jiraTeamCatalogBoardPayload, int, error) {
	boards := make([]jiraTeamCatalogBoardPayload, 0)
	startAt := 0
	requests := 0
	for pages := 0; ; pages++ {
		if pages >= jiraTeamCatalogBoardsMaxPages {
			return nil, requests, ErrPaginationCapExceeded
		}
		query := url.Values{
			"startAt": {strconv.Itoa(startAt)}, "maxResults": {strconv.Itoa(jiraTeamCatalogPerPage)},
			"projectKeyOrId": {projectKey},
		}
		var page jiraTeamCatalogBoardsPage
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/agile/1.0/board?"+query.Encode(), nil, &page); err != nil {
			return nil, requests, err
		}
		requests++
		if len(page.Values) == 0 {
			break
		}
		boards = append(boards, page.Values...)
		startAt += len(page.Values)
		if page.IsLast != nil && *page.IsLast {
			break
		}
	}
	return boards, requests, nil
}

// iterBoardSprints returns the sprints gathered before an optional
// documented-skippable 400 (skipDetail non-empty, err nil -- the caller logs
// and moves on to the next board, matching Python's per-board `continue`),
// or a hard error for anything else. Mirrors JiraClient.iter_board_sprints +
// team_autoimport_jira._skippable_jira_400_detail exactly.
//
// A non-2xx response never reaches this function as a plain *http.Response*
// to inspect: providerfoundation.HTTPClient.Do classifies it first and
// returns (nil, *providerfoundation.ProviderError) instead, with the
// classification's own StatusCode/Body fields carrying exactly what a raw
// response would have (ClassifyHTTPWithMessage, http.go) -- so the 400 check
// below type-asserts the returned error rather than reading a response.
func (handler JiraTeamCatalogRouteHandler) iterBoardSprints(
	ctx context.Context, client *providerfoundation.HTTPClient, boardID string,
) (sprints []json.RawMessage, requests int, skipDetail string, err error) {
	startAt := 0
	for pages := 0; ; pages++ {
		if pages >= jiraTeamCatalogSprintsMaxPages {
			return sprints, requests, "", ErrPaginationCapExceeded
		}
		query := url.Values{"startAt": {strconv.Itoa(startAt)}, "maxResults": {strconv.Itoa(jiraTeamCatalogPerPage)}}
		path := "/rest/agile/1.0/board/" + url.PathEscape(boardID) + "/sprint?" + query.Encode()
		var page jiraTeamCatalogSprintsPage
		fetchErr := jiraFetchObject(ctx, client, http.MethodGet, path, nil, &page)
		requests++
		if fetchErr != nil {
			var providerErr *providerfoundation.ProviderError
			if errors.As(fetchErr, &providerErr) && providerErr.StatusCode == http.StatusBadRequest {
				if detail := jiraTeamCatalogSkippable400Detail([]byte(providerErr.Body)); detail != "" {
					return sprints, requests, detail, nil
				}
			}
			return sprints, requests, "", fetchErr
		}
		if len(page.Values) == 0 {
			break
		}
		sprints = append(sprints, page.Values...)
		startAt += len(page.Values)
		if page.IsLast != nil && *page.IsLast {
			break
		}
	}
	return sprints, requests, "", nil
}

// jiraTeamCatalogSkippable400Detail mirrors
// team_autoimport_jira._skippable_jira_400_detail: only a 400 body carrying
// a non-empty `errorMessages` array is treated as the documented "this
// board's type does not support sprints" shape; anything else (no body, no
// such key, an empty array) is NOT skippable.
func jiraTeamCatalogSkippable400Detail(body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	raw, ok := payload["errorMessages"]
	if !ok {
		return ""
	}
	list, ok := raw.([]any)
	if !ok || len(list) == 0 {
		return ""
	}
	parts := make([]string, 0, len(list))
	for _, item := range list {
		parts = append(parts, fmt.Sprint(item))
	}
	return strings.Join(parts, "; ")
}

// JiraTeamCatalogCollector adapts JiraTeamCatalogRouteHandler (the collection
// walk) and JiraTeamCatalogClickHouseEffects (the write) to the shared,
// claim-free TeamCatalogCollector seam (CHAOS-4431, team-lead ruling
// 2026-08-28, option (c)) -- the same shape Linear/GitHub/GitLab's
// collectors use.
type JiraTeamCatalogCollector struct {
	Handler JiraTeamCatalogRouteHandler
	Sink    JiraTeamCatalogClickHouseEffects
}

func (collector JiraTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if ctx == nil || (!ref.Strict && !selections.Any()) {
		return TeamCatalogResult{}, nil
	}
	if collector.Sink.Conn == nil || collector.Sink.Lease == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	batch, err := collector.Handler.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		return TeamCatalogResult{}, err
	}
	if batch.Result.WalkSkipped {
		return TeamCatalogResult{Skipped: true, SkipReason: batch.Result.WalkSkipReason}, nil
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: jiraTeamCatalogProvider}}
	result := TeamCatalogResult{}

	var keptMemberships []jiraTeamCatalogMembershipRow
	var membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded int
	if selections.Members {
		observedTeamIDs := make([]string, 0, len(batch.Rows.Teams))
		for _, team := range batch.Rows.Teams {
			observedTeamIDs = append(observedTeamIDs, team.ID)
		}
		if len(batch.Rows.Memberships) > 0 || len(observedTeamIDs) > 0 {
			var guardErr error
			keptMemberships, membershipsSkippedManualConflict, membershipsStagedForReview, driftChangesSuperseded, guardErr = applyJiraTeamMembershipConflictGuard(
				ctx, collector.Sink.Conn, ref.OrgID, batch.Rows.Memberships, observedTeamIDs, normalizedAt,
			)
			if guardErr != nil {
				return result, guardErr
			}
		}
	}

	if selections.Teams {
		teamRows := append([]jiraTeamCatalogTeamRow(nil), batch.Rows.Teams...)
		if selections.Members {
			roster := jiraRosterFromMemberships(keptMemberships)
			for index := range teamRows {
				teamRows[index].Members = roster[teamRows[index].ID]
			}
		} else if len(teamRows) > 0 {
			// A teams-only run (members deselected) must not overwrite
			// `teams.members` with an empty placeholder -- preserve whatever
			// roster is already persisted, exactly like Python's
			// _existing_team_members path.
			teamIDs := make([]string, 0, len(teamRows))
			for _, team := range teamRows {
				teamIDs = append(teamIDs, team.ID)
			}
			existingRoster, rosterErr := PreserveExistingTeamMembersRoster(ctx, collector.Sink.Conn, ref.OrgID, teamIDs)
			if rosterErr != nil {
				return result, rosterErr
			}
			for index := range teamRows {
				teamRows[index].Members = existingRoster[teamRows[index].ID]
			}
		}
		keptTeams, skippedTeamIDs, teamsStagedForReview, teamsDriftSuperseded, guardErr := applyJiraTeamSyncPolicyGuard(ctx, collector.Sink.Conn, ref.OrgID, teamRows, normalizedAt)
		if guardErr != nil {
			return result, guardErr
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
		result.TeamsStagedForReview = teamsStagedForReview
		driftChangesSuperseded += teamsDriftSuperseded
		teamsEffect, effectErr := effectBatchFromValues(jiraTeamCatalogTeamsDestination, EffectReadbackRequired, keptTeams)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, teamsEffect); err != nil {
			return result, err
		}
		result.TeamsWritten = len(keptTeams)
		result.TeamKeys = make([]string, 0, len(keptTeams))
		for _, team := range keptTeams {
			if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
				result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
			}
		}
	}
	if selections.Members {
		result.MembershipsSkippedManualConflict = membershipsSkippedManualConflict
		result.MembershipsStagedForReview = membershipsStagedForReview
	}
	result.DriftChangesSuperseded = driftChangesSuperseded
	if selections.Members {
		membershipsEffect, effectErr := effectBatchFromValues(jiraTeamCatalogMembershipsDestination, EffectReadbackRequired, keptMemberships)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, membershipsEffect); err != nil {
			return result, err
		}
		result.MembershipsWritten = len(keptMemberships)
		result.MembersWritten = len(distinctJiraMembershipMembers(keptMemberships))
	}
	if selections.Projects {
		projects := append([]jiraTeamCatalogProjectRow(nil), batch.Rows.Projects...)
		ownership := append([]jiraTeamCatalogOwnershipRow(nil), batch.Rows.Ownership...)
		legacyProjects, legacyOwnership, legacyErr := jiraLegacyProjectOwnershipLinks(ctx, collector.Sink.Conn, ref.OrgID, normalizedAt)
		if legacyErr != nil {
			return result, legacyErr
		}
		existing := make(map[string]bool, len(projects))
		for _, row := range projects {
			existing[row.ID] = true
		}
		for _, row := range legacyProjects {
			if existing[row.ID] {
				continue
			}
			existing[row.ID] = true
			projects = append(projects, row)
		}
		ownership = append(ownership, legacyOwnership...)
		ownership = dedupeJiraOwnershipRows(ownership)

		ownershipEffect, effectErr := effectBatchFromValues(jiraTeamCatalogOwnershipDestination, EffectReadbackRequired, ownership)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, ownershipEffect); err != nil {
			return result, err
		}
		result.OwnershipWritten = len(ownership)
		projectsEffect, effectErr := effectBatchFromValues(jiraTeamCatalogProjectsDestination, EffectReadbackRequired, projects)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, projectsEffect); err != nil {
			return result, err
		}
		result.ProjectsWritten = len(projects)
	}
	// Sprints are unconditional reference data (CHAOS-4437 parity), written
	// whenever the walk reached this point at all -- see the function doc
	// comment on collectSprints.
	sprintsEffect, effectErr := effectBatchFromValues(jiraTeamCatalogSprintsDestination, EffectReadbackRequired, batch.Rows.Sprints)
	if effectErr != nil {
		return result, effectErr
	}
	if err := collector.Sink.WriteEffect(ctx, writeClaim, sprintsEffect); err != nil {
		return result, err
	}
	result.SprintsWritten = len(batch.Rows.Sprints)
	result.SprintIDs = make([]string, 0, len(batch.Rows.Sprints))
	for _, sprint := range batch.Rows.Sprints {
		result.SprintIDs = append(result.SprintIDs, sprint.SprintID)
	}
	return result, nil
}

func jiraRosterFromMemberships(rows []jiraTeamCatalogMembershipRow) map[string][]string {
	roster := make(map[string][]string, len(rows))
	for _, row := range rows {
		values := roster[row.TeamID]
		for _, facet := range row.IdentityFacets {
			if !containsString(values, facet) {
				values = append(values, facet)
			}
		}
		roster[row.TeamID] = values
	}
	return roster
}

var _ TeamCatalogCollector = JiraTeamCatalogCollector{}
