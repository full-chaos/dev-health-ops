package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// jiraTeamCatalogProjectSearchMaxResults mirrors team_discovery.
	// discover_jira's single, unpaginated GET exactly -- see
	// jiraTeamCatalogProjectSearchPayload's doc comment.
	jiraTeamCatalogProjectSearchMaxResults = 100
	// jiraTeamCatalogProjectSearchMaxPages bounds the project search walk
	// (5,000 projects). A walk that stops at the bound is not a complete
	// snapshot: it writes what it read and closes nothing.
	jiraTeamCatalogProjectSearchMaxPages = 50
	// jiraTeamCatalogProjectStatusArchived is the `status` filter value of
	// the project search that returns archived projects. Without the
	// parameter the provider returns live projects only.
	jiraTeamCatalogProjectStatusArchived = "archived"
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
// team_autoimport_jira.py's identical allowlists verbatim: only
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

// jiraTeamCatalogCountingDoer observes actual wire attempts, including
// transport failures and retries the wrapped HTTPClient makes internally --
// unlike a decoded-page or hardcoded-per-call tally, it increments once per
// Doer.Do call regardless of whether that call ever produced a usable
// response. It is the single source of truth for one CollectTeamCatalog
// call's JiraTeamCatalogEvidence.Requests.
type jiraTeamCatalogCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer jiraTeamCatalogCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts++
	return doer.delegate.Do(request)
}

// JiraTeamCatalogRouteHandler owns the provider-only team/project-ownership/
// membership/sprint catalog walk that ports
// src/dev_health_ops/workers/team_autoimport_jira.py. Claim-free by design:
// team/member/project reference discovery runs once per sync run per
// provider, not as a claimed provider-unit. It stays DB-free --
// JiraTeamCatalogCollector (below) merges
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
	// ProjectsSkippedNoNativeID counts projects the search returned with no
	// native id. Such a project gets its team row but no `projects` row and
	// no ownership row: an id built from the key would be a second identity
	// of a project the work-items route identifies by its native id.
	ProjectsSkippedNoNativeID int `json:"projects_skipped_no_native_id,omitempty"`
	// ProjectSearchComplete says the project search was read to the
	// provider's end-of-data signal. When it is false (a later page failed,
	// the page bound was hit, a page came back empty before the end) the
	// rows are a part of the provider's projects: they are written, and no
	// ownership row is closed on their evidence.
	ProjectSearchComplete bool `json:"project_search_complete"`
	// ProjectSearchPages is the number of search pages read.
	ProjectSearchPages int `json:"project_search_pages,omitempty"`
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
	// ArchivedProjects is the identity of each ARCHIVED project the provider
	// returned. Nothing is written for them: the write leaves every open
	// ownership row of such a project as it is (jiraHoldArchivedOwnership).
	ArchivedProjects []JiraArchivedProject `json:"archived_projects,omitempty"`
	// ProjectSearchWalks is every project search walk of this batch, with the
	// number of responses each took: the live search, the archived search and
	// the live search again. The union of their answers is what the ownership
	// write takes as the projects that still exist (the live ones are written,
	// the archived ones keep their open rows), so an absence is proven by a
	// walk only when ALL of them were one response.
	ProjectSearchWalks []ListWalk `json:"-"`
}

// The project search walks of the Jira catalog (ListWalk.Name).
const (
	jiraLiveProjectSearchWalk      = "jira live project search"
	jiraArchivedProjectSearchWalk  = "jira archived project search"
	jiraLiveProjectSearchAgainWalk = "jira live project search, again"
	// jiraLegacyLinksReadWalk is the read of the legacy links table: one read
	// of the store. It is the held set of a row whose project the search holds.
	jiraLegacyLinksReadWalk = "jira legacy links (one read of the store)"
)

// JiraArchivedProject is one archived project as the project search names
// it: its native id and its key, from the same search entry.
type JiraArchivedProject struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

func jiraTeamCatalogWalkSkipBatch(reason string, requests int) JiraTeamCatalogBatch {
	return JiraTeamCatalogBatch{
		Result:   JiraTeamCatalogResult{WalkSkipped: true, WalkSkipReason: reason},
		Evidence: JiraTeamCatalogEvidence{Provider: jiraTeamCatalogProvider, Requests: requests},
	}
}

// jiraTeamCatalogWalkFailure is the whole-walk abort branch point (project
// search or, when Members is selected, a project's lead lookup): under
// strict, re-raise unchanged; under non-strict, log and return a clean,
// successful skip -- mirrors team_autoimport_jira.py's populate() catching
// discover_jira's failure (and run_team_autoimport's own outer catch-all for
// every OTHER exception the function raises, including a member lookup
// failure) and returning a zero summary instead of a partial write. The
// physical requests already made against the provider before the abort are
// real wire cost regardless of the abort, so requests (the counting Doer's
// running total) is stamped onto the skip batch rather than discarded with
// the rest of the walk's state.
func jiraTeamCatalogWalkFailure(
	ctx context.Context, ref TeamCatalogReference, reason string, requests int, err error,
) (JiraTeamCatalogBatch, error) {
	if ref.Strict {
		return JiraTeamCatalogBatch{}, err
	}
	slog.Default().WarnContext(ctx, "jira_team_catalog_walk_skipped",
		"org_id", ref.OrgID, "reason", reason, "error", err)
	return jiraTeamCatalogWalkSkipBatch(reason, requests), nil
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
	// Sprint/cycle reference discovery is unconditional reference data, so
	// the early exit below (matching Python's
	// `if not strict and not (want_teams or want_projects or want_members)`)
	// is the ONLY gate on it -- a strict call always proceeds even with
	// every selection off.
	if !ref.Strict && !selections.Any() {
		return JiraTeamCatalogBatch{}, nil
	}
	claim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: jiraTeamCatalogProvider}}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	evidence := JiraTeamCatalogEvidence{Provider: jiraTeamCatalogProvider}
	requests := 0
	counted := *client
	counted.Doer = jiraTeamCatalogCountingDoer{delegate: client.Doer, attempts: &requests}
	client = &counted
	// The search is read page by page to the provider's end-of-data signal.
	// The first page failing is the walk failing, as before. A later page
	// failing, the page bound, or an empty page before the end leaves a PART
	// of the projects: the walk goes on with it and reports the search as
	// not complete, so the ownership write closes nothing.
	search, searchComplete, searchPages, searchStop, searchErr := jiraTeamCatalogSearchProjects(ctx, client, "")
	if searchErr != nil {
		return jiraTeamCatalogWalkFailure(ctx, ref, "project_discovery_failed", requests, searchErr)
	}
	if !searchComplete {
		slog.Default().WarnContext(ctx, "jira_team_catalog_project_search_incomplete",
			"org_id", ref.OrgID, "reason", searchStop, "pages", searchPages, "projects", len(search.Values))
	}
	// The search above returns live projects only (the provider's default
	// for `status`). An archived project still exists and still owns its
	// work items, so it is read too, and only to keep its open ownership
	// rows open: it gets no team, project, member or sprint row here. This
	// read failing at any page, the first one included, does not fail the
	// walk; it leaves the snapshot not complete, so nothing is closed.
	archived, archivedComplete, archivedPages, archivedStop, _ := jiraTeamCatalogSearchProjects(
		ctx, client, jiraTeamCatalogProjectStatusArchived)
	if !archivedComplete {
		slog.Default().WarnContext(ctx, "jira_team_catalog_archived_project_search_incomplete",
			"org_id", ref.OrgID, "reason", archivedStop, "pages", archivedPages, "projects", len(archived.Values))
	}
	// The two reads above are not one atomic read: a project restored (or
	// archived) between them is in neither answer. A third read of the live
	// projects, after the archived one, closes that window: a project that
	// moved archived -> live shows here, and one that moved live -> archived
	// showed in the first read. The union of the live reads is the live set.
	liveAgain, liveAgainComplete, liveAgainPages, liveAgainStop, _ := jiraTeamCatalogSearchProjects(ctx, client, "")
	if !liveAgainComplete {
		slog.Default().WarnContext(ctx, "jira_team_catalog_project_search_recheck_incomplete",
			"org_id", ref.OrgID, "reason", liveAgainStop, "pages", liveAgainPages, "projects", len(liveAgain.Values))
	}
	search.Values = jiraUnionProjectSearchEntries(search.Values, liveAgain.Values)
	searchComplete = searchComplete && archivedComplete && liveAgainComplete
	projectSearchWalks := []ListWalk{
		{Name: jiraLiveProjectSearchWalk, Responses: searchPages},
		{Name: jiraArchivedProjectSearchWalk, Responses: archivedPages},
		{Name: jiraLiveProjectSearchAgainWalk, Responses: liveAgainPages},
	}

	rows := JiraTeamCatalogRows{}
	projectsSkippedNoNativeID := 0
	projectKeys := make([]string, 0, len(search.Values))
	// A project is a project entity and nothing else: no team row, no
	// ownership row and no membership row is built from it. A project's team
	// comes from the Atlassian team connected to it (internal/atlassianteams);
	// a project with no such team has none.
	for _, entry := range search.Values {
		key := jiraTeamID(entry.Key)
		name := strings.TrimSpace(entry.Name)
		if key == "" || name == "" {
			continue
		}
		projectKeys = append(projectKeys, key)
		nativeID := strings.TrimSpace(entry.ID)
		nativeProjectID, nativeOK := JiraProjectID(nativeID)
		if !nativeOK || jiraProjectIDIsKeyBuilt(ref.OrgID, nativeID) {
			projectsSkippedNoNativeID++
			continue
		}
		rows.Projects = append(rows.Projects, normalizeJiraProjectRow(ref.OrgID, nativeProjectID, key, name, normalizedAt))
	}
	if projectsSkippedNoNativeID > 0 {
		slog.Default().WarnContext(ctx, "jira_team_catalog_project_without_native_id",
			"org_id", ref.OrgID, "projects", projectsSkippedNoNativeID)
	}
	rows.Projects = dedupeJiraProjectCatalogRows(rows.Projects)
	rows.Ownership = dedupeJiraOwnershipRows(rows.Ownership)
	var archivedProjects []JiraArchivedProject
	archivedSeen := map[JiraArchivedProject]bool{}
	// Rule: the reads are ordered in time. Only a live read taken AFTER the
	// archived read can prove a project live again, so the skip set is the
	// second live read alone (the union feeds the project rows only).
	liveAfterArchived := make(map[string]bool, len(liveAgain.Values))
	for _, entry := range liveAgain.Values {
		liveAfterArchived[strings.TrimSpace(entry.ID)] = true
	}
	for _, entry := range archived.Values {
		project := JiraArchivedProject{ID: strings.TrimSpace(entry.ID), Key: jiraTeamID(entry.Key)}
		// A project read as archived and live in the read after it is live.
		if liveAfterArchived[project.ID] {
			continue
		}
		if project.Key == "" || project.ID == "" || jiraProjectIDIsKeyBuilt(ref.OrgID, project.ID) || archivedSeen[project] {
			continue
		}
		archivedSeen[project] = true
		archivedProjects = append(archivedProjects, project)
	}

	sprints, sprintErr := handler.collectSprints(ctx, client, claim, projectKeys, normalizedAt)
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
		MembersImported:           len(distinctJiraMembershipMembers(rows.Memberships)),
		SprintsImported:           len(rows.Sprints),
		ProjectsSkippedNoNativeID: projectsSkippedNoNativeID,
		ProjectSearchComplete:     searchComplete,
		ProjectSearchPages:        searchPages,
	}
	evidence.Requests = requests
	return JiraTeamCatalogBatch{
		Rows: rows, Result: result, Evidence: evidence, ArchivedProjects: archivedProjects, ProjectSearchWalks: projectSearchWalks,
	}, nil
}

// jiraHoldArchivedOwnership splits the open rows of this writer: held is
// every row whose project is an archived project, rest is the others.
// Archiving a project in Jira does not end its ownership, so a held row is
// left as it is: it is not given to the snapshot rule, and nothing is written
// for it.
//
// A stored row names its project by the native id or, when it was written
// before the one-id rule, by the id built from the project key. Both forms of
// an archived project hold a row: a store that still has key-built rows keeps
// them for its archived projects, where no sync writes the native row that
// replaces them.
func jiraHoldArchivedOwnership(orgID string, archived []JiraArchivedProject, open []jiraTeamCatalogOwnershipRow) (held, rest []jiraTeamCatalogOwnershipRow) {
	isArchived := make(map[string]bool, 2*len(archived))
	for _, project := range archived {
		isArchived[project.ID] = true
		isArchived[jiraKeyBuiltProjectIDPrefix(orgID)+project.Key] = true
	}
	for _, row := range open {
		if isArchived[row.ProjectID.String()] {
			held = append(held, row)
			continue
		}
		rest = append(rest, row)
	}
	return held, rest
}

// jiraUnionProjectSearchEntries returns first, then every entry of second
// whose id (or, with no id, key) first does not hold.
func jiraUnionProjectSearchEntries(first, second []jiraTeamCatalogProjectSearchEntry) []jiraTeamCatalogProjectSearchEntry {
	seen := make(map[string]bool, len(first))
	identity := func(entry jiraTeamCatalogProjectSearchEntry) string {
		if id := strings.TrimSpace(entry.ID); id != "" {
			return "id:" + id
		}
		return "key:" + jiraTeamID(entry.Key)
	}
	for _, entry := range first {
		seen[identity(entry)] = true
	}
	for _, entry := range second {
		if key := identity(entry); !seen[key] {
			seen[key] = true
			first = append(first, entry)
		}
	}
	return first
}

// jiraTeamCatalogSearchProjects reads /rest/api/3/project/search page by
// page to the provider's end-of-data signal. status is the provider's
// `status` filter; empty asks for the provider's default, live projects.
// err is the first page failing; a later page failing ends the read with
// complete false and what was read so far.
func jiraTeamCatalogSearchProjects(
	ctx context.Context, client *providerfoundation.HTTPClient, status string,
) (search jiraTeamCatalogProjectSearchPayload, complete bool, pages int, stop string, err error) {
	stop = "page_bound"
	for pages < jiraTeamCatalogProjectSearchMaxPages {
		query := url.Values{"maxResults": {strconv.Itoa(jiraTeamCatalogProjectSearchMaxResults)}}
		if len(search.Values) > 0 {
			query.Set("startAt", strconv.Itoa(len(search.Values)))
		}
		if status != "" {
			query.Set("status", status)
		}
		var page jiraTeamCatalogProjectSearchPayload
		if fetchErr := jiraFetchObject(ctx, client, http.MethodGet, "/rest/api/3/project/search?"+query.Encode(), nil, &page); fetchErr != nil {
			if pages == 0 {
				return search, false, 0, "page_error", fetchErr
			}
			return search, false, pages, "page_error", nil
		}
		pages++
		if len(page.ErrorMessages) > 0 {
			// An error body under HTTP 200 is not an answer, even when it also
			// says total 0: the read stops here and is not complete.
			return search, false, pages, "error_body", nil
		}
		search.Values = append(search.Values, page.Values...)
		if page.endOfData(len(search.Values), jiraTeamCatalogProjectSearchMaxResults) {
			return search, true, pages, "", nil
		}
		if len(page.Values) == 0 {
			return search, false, pages, "empty_page", nil
		}
	}
	return search, false, pages, stop, nil
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
) ([]jiraSprintRow, error) {
	sprints := make([]jiraSprintRow, 0)
	for _, key := range projectKeys {
		var detail jiraTeamCatalogProjectDetailPayload
		detailPath := "/rest/api/3/project/" + url.PathEscape(key)
		if err := jiraFetchObject(ctx, client, http.MethodGet, detailPath, nil, &detail); err != nil {
			return nil, err
		}
		projectType := strings.ToLower(strings.TrimSpace(detail.ProjectTypeKey))
		if jiraTeamCatalogNoBoardProjectTypes[projectType] {
			continue
		}
		if !jiraTeamCatalogBoardCapableProjectTypes[projectType] {
			return nil, fmt.Errorf("%w: unrecognized jira project type %q for project_key=%q",
				providerfoundation.ErrNormalizationInvalid, detail.ProjectTypeKey, key)
		}
		boards, err := handler.iterBoards(ctx, client, key)
		if err != nil {
			return nil, err
		}
		for _, board := range boards {
			boardID := strings.TrimSpace(board.ID.String())
			if boardID == "" {
				continue
			}
			boardSprints, skipDetail, err := handler.iterBoardSprints(ctx, client, boardID)
			if err != nil {
				return nil, err
			}
			for _, raw := range boardSprints {
				var payload map[string]any
				if err := decodeJiraJSON(raw, &payload); err != nil {
					return nil, providerfoundation.ErrNormalizationInvalid
				}
				sprint, sprintErr := normalizeJiraSprint(claim, payload, normalizedAt)
				if sprintErr != nil {
					continue
				}
				sprints = append(sprints, sprint)
			}
			if skipDetail != "" {
				slog.Default().WarnContext(ctx, "jira_team_catalog_board_sprints_skipped",
					"org_id", claim.OrgID, logging.ProviderIDAttr("board_id", boardID), "skip_reason", "board_rejects_sprints")
			}
		}
	}
	return sprints, nil
}

func (handler JiraTeamCatalogRouteHandler) iterBoards(
	ctx context.Context, client *providerfoundation.HTTPClient, projectKey string,
) ([]jiraTeamCatalogBoardPayload, error) {
	boards := make([]jiraTeamCatalogBoardPayload, 0)
	startAt := 0
	for pages := 0; ; pages++ {
		if pages >= jiraTeamCatalogBoardsMaxPages {
			return nil, ErrPaginationCapExceeded
		}
		query := url.Values{
			"startAt": {strconv.Itoa(startAt)}, "maxResults": {strconv.Itoa(jiraTeamCatalogPerPage)},
			"projectKeyOrId": {projectKey},
		}
		var page jiraTeamCatalogBoardsPage
		if err := jiraFetchObject(ctx, client, http.MethodGet, "/rest/agile/1.0/board?"+query.Encode(), nil, &page); err != nil {
			return nil, err
		}
		if len(page.Values) == 0 {
			break
		}
		boards = append(boards, page.Values...)
		startAt += len(page.Values)
		if page.IsLast != nil && *page.IsLast {
			break
		}
	}
	return boards, nil
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
) (sprints []json.RawMessage, skipDetail string, err error) {
	startAt := 0
	for pages := 0; ; pages++ {
		if pages >= jiraTeamCatalogSprintsMaxPages {
			return sprints, "", ErrPaginationCapExceeded
		}
		query := url.Values{"startAt": {strconv.Itoa(startAt)}, "maxResults": {strconv.Itoa(jiraTeamCatalogPerPage)}}
		path := "/rest/agile/1.0/board/" + url.PathEscape(boardID) + "/sprint?" + query.Encode()
		var page jiraTeamCatalogSprintsPage
		fetchErr := jiraFetchObject(ctx, client, http.MethodGet, path, nil, &page)
		if fetchErr != nil {
			var providerErr *providerfoundation.ProviderError
			if errors.As(fetchErr, &providerErr) && providerErr.StatusCode == http.StatusBadRequest {
				if detail := jiraTeamCatalogSkippable400Detail([]byte(providerErr.Body)); detail != "" {
					return sprints, detail, nil
				}
			}
			return sprints, "", fetchErr
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
	return sprints, "", nil
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
// claim-free TeamCatalogCollector seam -- the same shape Linear/GitHub/
// GitLab's collectors use.
type JiraTeamCatalogCollector struct {
	Handler JiraTeamCatalogRouteHandler
	Sink    JiraTeamCatalogClickHouseEffects
	// ScopeCensus counts the org's other active Jira integrations. Without
	// it no ownership row is closed (ProveSoleScope).
	ScopeCensus OwnershipScopeCensus
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
	// The project-as-team rows an earlier catalog wrote are retired here, at
	// every run and before the walk: the step reads no provider answer, so a
	// failed or skipped walk does not hold it back. With nothing left to
	// retire it is one count read.
	retired, retireErr := RetireJiraProjectAsTeamRows(ctx, collector.Sink.Conn, ref.OrgID, normalizedAt, false)
	if retireErr != nil {
		return TeamCatalogResult{}, retireErr
	}
	if retired.Retired() > 0 {
		slog.Default().InfoContext(ctx, "jira_project_as_team_retired",
			"teams", retired.TeamsRetired, "ownership", retired.OwnershipClosed, "memberships", retired.MembershipClosed,
			"repo_ownership", retired.RepoOwnershipClosed,
			"teams_with_manual_members", retired.TeamsWithManualMembers, "teams_with_sync_policy", retired.TeamsWithSyncPolicy)
	}
	batch, err := collector.Handler.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		return TeamCatalogResult{ProjectAsTeamRetired: int(retired.Retired())}, err
	}
	if batch.Result.WalkSkipped {
		return TeamCatalogResult{Skipped: true, SkipReason: batch.Result.WalkSkipReason, ProjectAsTeamRetired: int(retired.Retired())}, nil
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: jiraTeamCatalogProvider}}
	result := TeamCatalogResult{ProjectAsTeamRetired: int(retired.Retired())}

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
		// The legacy links table holds project keys only. The native id of
		// a key comes from this walk's own project search, never from a
		// read of `projects`: a key the provider did not return this run has
		// no identity to write.
		nativeIDByKey := make(map[string]ProjectID, len(projects))
		for _, row := range projects {
			if row.ProjectKey != nil {
				nativeIDByKey[*row.ProjectKey] = row.ID
			}
		}
		legacyOwnership, legacySkipped, legacyComplete, legacyErr := jiraLegacyProjectOwnershipLinks(
			ctx, collector.Sink.Conn, ref.OrgID, nativeIDByKey, normalizedAt.UTC().Truncate(time.Millisecond))
		if legacyErr != nil {
			return result, legacyErr
		}
		if legacySkipped > 0 {
			slog.Default().WarnContext(ctx, "jira_team_catalog_legacy_link_without_native_id",
				"org_id", ref.OrgID, "links", legacySkipped)
		}
		ownership = append(ownership, legacyOwnership...)
		ownership = dedupeJiraOwnershipRows(ownership)
		// Snapshot rule, the same one atlassianteams.Write applies: an open
		// row of this writer that the fresh snapshot no longer holds is
		// closed in this write, and a row it still holds keeps the valid_from
		// it was first seen with. valid_from is a key column, so a new stamp
		// at each sync would add one more open row for the same fact.
		open, openErr := jiraOpenCatalogOwnership(ctx, collector.Sink.Conn, ref.OrgID)
		if openErr != nil {
			return result, openErr
		}
		// The open rows of an archived project are left as they are: they
		// are not part of what the snapshot rule may close.
		held, open := jiraHoldArchivedOwnership(ref.OrgID, batch.ArchivedProjects, open)
		if len(held) > 0 {
			slog.Default().InfoContext(ctx, "jira_team_catalog_archived_ownership_held",
				"org_id", ref.OrgID, "rows", len(held))
		}
		// The snapshot closes only through its kind: every read behind it
		// reached its end (all pages of the project search and the legacy
		// links), and the answer holds at least one live ownership row (no
		// live project is far more often an access change than an
		// organization that removed every project), whatever the archived
		// read holds.
		//
		// What the write takes as "this project still exists" is the UNION of
		// three walks: the live search, the archived search (its projects
		// keep their open rows, see jiraHoldArchivedOwnership above) and the
		// live search again. Each reads by offset, and a project removed
		// between two requests of ANY of them moves the later ones: a
		// project that still exists, live or archived, is then in no answer.
		// So an open row the answer does not hold is gone by the walks only
		// when EVERY one of the three was one response. Otherwise the row is
		// a candidate, closed on the provider's own answer for that project,
		// asked for live AND archived projects.
		//
		// A row whose project IS in the live answer is another case: the
		// project is there, so what went is its legacy link, and the links
		// are one read of the store.
		searched := make(map[string]bool, len(projects))
		for _, row := range projects {
			searched[row.ID.String()] = true
		}
		lookups := NewOwnershipAbsenceLookups(ctx, jiraProjectAbsence{client: client})
		snapshot := JiraLegacyOwnershipKind().Snapshot(
			ProveSoleScope(ctx, collector.ScopeCensus, ref.OrgID, jiraTeamCatalogProvider, ref.IntegrationID), ProveSnapshot(
				SnapshotTerm{Holds: batch.Result.ProjectSearchComplete, Reason: jiraSnapshotProjectSearch},
				SnapshotTerm{Holds: legacyComplete, Reason: jiraSnapshotLegacyLinks},
			), AbsenceByListing(func(row OwnershipSnapshotRow) []ListWalk {
				if searched[row.ProjectID.String()] {
					return []ListWalk{{Name: jiraLegacyLinksReadWalk, Responses: 1}}
				}
				return batch.ProjectSearchWalks
			}, lookups.Answer))
		var retracted []jiraTeamCatalogOwnershipRow
		var plan SnapshotPlan
		ownership, retracted, plan = jiraOwnershipSnapshot(ownership, open, normalizedAt.UTC().Truncate(time.Millisecond), snapshot)
		result.OwnershipSnapshotIncomplete = judgeJiraOwnershipSnapshot(ctx, ref.OrgID, plan, len(open)+len(held))
		result.DegradedLegs = append(result.DegradedLegs, SnapshotAbsenceLegs(plan)...)
		if len(retracted) > 0 {
			slog.Default().InfoContext(ctx, "jira_team_catalog_ownership_retracted",
				"org_id", ref.OrgID, "rows", len(retracted), "project_ids", jiraRetractedProjectIDs(retracted))
		}
		result.OwnershipRetracted = len(retracted)
		freshOwnership := len(ownership)
		ownership = append(ownership, retracted...)

		ownershipEffect, effectErr := effectBatchFromValues(jiraTeamCatalogOwnershipDestination, EffectReadbackRequired, ownership)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, ownershipEffect); err != nil {
			return result, err
		}
		result.OwnershipWritten = freshOwnership
		projectsEffect, effectErr := effectBatchFromValues(jiraTeamCatalogProjectsDestination, EffectReadbackRequired, projects)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, projectsEffect); err != nil {
			return result, err
		}
		result.ProjectsWritten = len(projects)
	}
	// Sprints are unconditional reference data, written whenever the walk
	// reached this point at all -- see the function doc comment on
	// collectSprints.
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

// jiraRetractedProjectIDs names the projects whose open ownership rows a
// sync closes, sorted and without repeats, so a wrong closure is traceable.
func jiraRetractedProjectIDs(rows []jiraTeamCatalogOwnershipRow) []string {
	seen := map[string]bool{}
	ids := []string{}
	for _, row := range rows {
		id := row.ProjectID.String()
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
