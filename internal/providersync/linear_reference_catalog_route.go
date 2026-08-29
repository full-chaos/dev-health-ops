package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	linearReferenceCatalogDefaultPerPage = 50
	linearReferenceCatalogMaxPerPage     = 100
	linearReferenceCatalogDefaultPages   = 100
	linearReferenceCatalogMaxPages       = 10_000
	linearReferenceCatalogMemberPageSize = 100
)

const linearReferenceCatalogTeamsQuery = `
query LinearReferenceCatalogTeams($first: Int!, $after: String) {
  teams(first: $first, after: $after) {
    nodes {
      id key name description
      members(first: 10) {
        nodes { id name email active }
        pageInfo { hasNextPage endCursor }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

const linearReferenceCatalogMembersQuery = `
query LinearReferenceCatalogMembers($teamId: String!, $first: Int!, $after: String) {
  team(id: $teamId) {
    members(first: $first, after: $after) {
      nodes { id name email active }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const linearReferenceCatalogProjectsQuery = `
query LinearReferenceCatalogProjects($first: Int!, $after: String, $includeArchived: Boolean) {
  projects(first: $first, after: $after, includeArchived: $includeArchived) {
    nodes {
      id name description
      status { id name type }
      trashed progress startDate targetDate createdAt updatedAt archivedAt url
      lead { id name email }
      teams { nodes { id key } }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

type LinearReferenceCatalogFailureCode string

const (
	LinearReferenceCatalogInvalidResponse LinearReferenceCatalogFailureCode = "invalid_response"
	LinearReferenceCatalogGraphQLError    LinearReferenceCatalogFailureCode = "graphql_error"
	LinearReferenceCatalogPaginationCap   LinearReferenceCatalogFailureCode = "pagination_cap"
	LinearReferenceCatalogNormalization   LinearReferenceCatalogFailureCode = "normalization"
)

type LinearReferenceCatalogFailure struct {
	Code    LinearReferenceCatalogFailureCode `json:"code"`
	Surface string                            `json:"surface"`
	Pages   int                               `json:"pages"`
	Records int                               `json:"records"`
	Message string                            `json:"message"`
}

type LinearReferenceCatalogEvidence struct {
	Provider         string `json:"provider"`
	Dataset          string `json:"dataset"`
	Requests         int    `json:"requests"`
	Pages            int    `json:"pages"`
	Records          int    `json:"records"`
	TeamsComplete    bool   `json:"teams_complete"`
	MembersComplete  bool   `json:"members_complete"`
	ProjectsComplete bool   `json:"projects_complete"`
}

type LinearReferenceCatalogResult struct {
	Teams       int  `json:"teams"`
	Members     int  `json:"members"`
	Memberships int  `json:"memberships"`
	Projects    int  `json:"projects"`
	Ownership   int  `json:"ownership"`
	Sprints     int  `json:"sprints"`
	Complete    bool `json:"complete"`
	// ProjectsWithoutKey is CHAOS-4530 telemetry: how many of Projects were
	// written with a nil ProjectKey. See TeamCatalogResult.ProjectsWithoutKey.
	ProjectsWithoutKey int `json:"projects_without_key"`
}

type LinearReferenceCatalogBatch struct {
	Rows     LinearReferenceCatalogRows     `json:"rows"`
	Effects  LinearReferenceCatalogEffects  `json:"effects"`
	Result   LinearReferenceCatalogResult   `json:"result"`
	Evidence LinearReferenceCatalogEvidence `json:"evidence"`
	Failure  *LinearReferenceCatalogFailure `json:"failure"`
}

// LinearReferenceCatalogRouteHandler owns the provider-only team/member/
// project catalog walk. It is deliberately not a CompleteRouteHandler: the
// catalog is reference discovery for the work-items unit, not a new matrix
// pair. The caller may hand Effects.Batches() to the shared effect ledger.
type LinearReferenceCatalogRouteHandler struct {
	PerPage  int
	MaxPages int
	// Now overrides the clock used to stamp NATIVE PROJECT rows only (CHAOS-
	// 4431 codex review P2). Python timestamps each native project at its own
	// observation boundary (team_autoimport_linear.py:590-602), deliberately
	// NOT at walk start, so that two overlapping discovery runs for the same
	// org order a ReplacingMergeTree row by when a project was actually SEEN
	// rather than by which run started first. Team/member/team-derived-
	// project rows are not observed independently of the walk (no per-node
	// network fetch happens for them), so they keep using the caller's single
	// normalizedAt, matching Python. Nil uses time.Now.
	Now func() time.Time
}

func (handler LinearReferenceCatalogRouteHandler) observedAt() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC().Truncate(time.Millisecond)
	}
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (handler LinearReferenceCatalogRouteHandler) limits() (int, int, error) {
	perPage, maxPages := handler.PerPage, handler.MaxPages
	if perPage == 0 {
		perPage = linearReferenceCatalogDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = linearReferenceCatalogDefaultPages
	}
	if perPage < 1 || perPage > linearReferenceCatalogMaxPerPage || maxPages < 1 || maxPages > linearReferenceCatalogMaxPages {
		return 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, nil
}

// CollectReferenceCatalog walks Linear's teams/members/projects once per
// sync run (CHAOS-4431 ruling, team-lead 2026-08-28, option (c)): claim-free,
// by a TeamCatalogReference rather than a claimed provider-unit Claim. It was
// originally gated on claim.Dataset=="work-items" because the catalog was
// designed to be invoked from inside the work-items unit's Collect(); that
// precondition belonged to a route this code never actually ran on and is
// dropped here along with the Claim parameter itself. Internally it still
// builds a minimal Claim{OrgID, Provider} (never claim.Validate()'d) purely
// because the row normalizers below read claim.OrgID/claim.Provider -- no
// lease or generation semantics attach to it.
func (handler LinearReferenceCatalogRouteHandler) CollectReferenceCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (LinearReferenceCatalogBatch, error) {
	if ctx == nil || ref.validate() != nil || credential.Provider != "linear" ||
		credential.ID == "" || client == nil ||
		client.Provider != "linear" || client.BaseURL == nil || client.Doer == nil ||
		client.Lease == nil || normalizedAt.IsZero() {
		return LinearReferenceCatalogBatch{}, ErrInvalidConfiguration
	}
	claim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: "linear"}}
	perPage, maxPages, err := handler.limits()
	if err != nil {
		return LinearReferenceCatalogBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	evidence := LinearReferenceCatalogEvidence{Provider: "linear", Dataset: "reference-catalog"}
	rows := LinearReferenceCatalogRows{
		Teams: make([]linearReferenceTeamRow, 0), Members: make([]linearReferenceMemberRow, 0),
		Memberships: make([]linearReferenceMembershipRow, 0), Projects: make([]linearReferenceProjectRow, 0),
		Ownership: make([]linearReferenceOwnershipRow, 0), Sprints: make([]linearSprintRow, 0),
	}
	// Teams are always fetched regardless of selections.Teams: cycles/sprints
	// below are unconditional reference data keyed per team (CHAOS-4431 codex
	// review P1, matching team_autoimport_linear.py:451's unconditional
	// discover_linear call), and the selection only gates whether the TEAM
	// ROW itself gets written later (LinearTeamCatalogCollector).
	teamRaw, pages, capReached, collectErr := collectLinearReferenceConnection(
		ctx, client, linearReferenceCatalogTeamsQuery, linearReferenceConnectionTeams,
		linearReferenceConnectionVariables{First: perPage}, perPage, maxPages,
	)
	evidence.Requests += pages
	evidence.Pages += pages
	if collectErr != nil || capReached {
		return linearReferenceCatalogFailureBatch(evidence, "teams", pages, 0, collectErr, capReached)
	}
	evidence.TeamsComplete = true
	evidence.Records += len(teamRaw)
	// See the cycles-fetch block below (CHAOS-4431 codex review round 2, P1)
	// for why this is tracked across loop iterations.
	cyclesAbandoned := false
	for _, raw := range teamRaw {
		var payload linearReferenceCatalogTeamPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return linearReferenceCatalogFailureBatch(evidence, "teams", evidence.Pages, evidence.Records, err, false)
		}
		team, normalizeErr := normalizeLinearReferenceTeam(claim, payload, normalizedAt)
		if normalizeErr != nil {
			return linearReferenceCatalogFailureBatch(evidence, "teams", evidence.Pages, evidence.Records, normalizeErr, false)
		}
		// Members.Nodes (page 1, free with the teams query above) and any
		// extra pages below are only fetched/normalized when Members is
		// selected (CHAOS-4431 codex review P1): a deselected member fetch
		// must cost nothing and its absence must never abort an enabled
		// teams/projects import. When deselected, the team row keeps
		// normalizeLinearReferenceTeam's page-1 roster as a harmless
		// placeholder -- LinearTeamCatalogCollector overwrites it with the
		// team's PRESERVED existing roster before writing, exactly like
		// Python's _existing_team_members path, so this placeholder is never
		// actually persisted.
		if selections.Members {
			memberNodes := append([]linearReferenceCatalogMemberPayload(nil), payload.Members.Nodes...)
			membersComplete := !payload.Members.PageInfo.HasNextPage
			if payload.Members.PageInfo.HasNextPage {
				extra, memberPages, memberCap, memberErr := collectLinearReferenceConnection(
					ctx, client, linearReferenceCatalogMembersQuery, linearReferenceConnectionTeamMembers,
					linearReferenceConnectionVariables{TeamID: payload.ID}, linearReferenceCatalogMemberPageSize, maxPages,
				)
				evidence.Requests += memberPages
				evidence.Pages += memberPages
				if memberErr != nil || memberCap {
					return linearReferenceCatalogFailureBatch(evidence, "members", evidence.Pages, evidence.Records, memberErr, memberCap)
				}
				membersComplete = true
				for _, memberRaw := range extra {
					var memberPayload linearReferenceCatalogMemberPayload
					if err := json.Unmarshal(memberRaw, &memberPayload); err != nil {
						return linearReferenceCatalogFailureBatch(evidence, "members", evidence.Pages, evidence.Records, err, false)
					}
					memberNodes = append(memberNodes, memberPayload)
				}
			}
			if !membersComplete {
				return linearReferenceCatalogFailureBatch(evidence, "members", evidence.Pages, evidence.Records, ErrPaginationCapExceeded, true)
			}
			// CHAOS-4431 codex review P1: rebuild the roster from the
			// COMPLETE, post-pagination memberNodes set, not the page-1-only
			// value normalizeLinearReferenceTeam provisionally set above --
			// otherwise any team with more than 10 members gets a `teams.
			// members` silently truncated to its first page forever.
			team.Members = linearReferenceTeamRosterFacets(memberNodes)
			for _, memberPayload := range memberNodes {
				if memberPayload.Active != nil && !*memberPayload.Active {
					continue
				}
				member, membership, _, memberErr := normalizeLinearReferenceMember(claim, team.ID, memberPayload, normalizedAt)
				if memberErr != nil {
					// Python skips members without an email and provider id. A node
					// with either field absent is not an authoritative identity row.
					if errors.Is(memberErr, ErrInvalidConfiguration) &&
						strings.TrimSpace(memberPayload.ID) == "" && strings.TrimSpace(memberPayload.Email) == "" {
						continue
					}
					return linearReferenceCatalogFailureBatch(evidence, "members", evidence.Pages, evidence.Records, memberErr, false)
				}
				rows.Members = append(rows.Members, member)
				rows.Memberships = append(rows.Memberships, membership)
			}
			evidence.MembersComplete = evidence.MembersComplete && membersComplete
			if len(rows.Teams) == 0 {
				evidence.MembersComplete = membersComplete
			}
		}
		rows.Teams = append(rows.Teams, team)

		// Cycles/sprints are unconditional reference data (CHAOS-4431 codex
		// review P1, team_autoimport_linear.py:575-576,624-635: "sprints/
		// cycles below stay unconditional -- reference data, not a
		// category"): fetched per team regardless of every CHAOS-4323
		// selection, including when all three are off, because dispatch-
		// blocking sprint keys must never go stale just because an org
		// disabled every writable category.
		//
		// CHAOS-4431 codex review round 2, P1: a cycles failure must NOT
		// discard the teams/members/projects already fetched in non-strict
		// (post-sync) mode -- team_autoimport_linear.py:636-639's outer
		// except only zeroes sprint_rows on a non-strict failure ("if strict:
		// raise; sprint_rows = []"), it never aborts the whole populate()
		// call. Strict (reference-discovery) mode keeps propagating the
		// error, matching Python's "if strict: raise" exactly. Once a cycles
		// fetch has failed in non-strict mode, stop attempting further ones
		// (cyclesAbandoned) and discard whatever partial rows.Sprints had
		// already accumulated -- Python's except clause resets the WHOLE
		// list, not just the failed team's, since one shared try wraps the
		// entire per-team cycles loop there.
		if !cyclesAbandoned {
			cyclePayloads, cyclePages, cycleErr := collectLinearCycles(ctx, client, payload.ID)
			evidence.Requests += cyclePages
			evidence.Pages += cyclePages
			if cycleErr != nil {
				if ref.Strict {
					return linearReferenceCatalogFailureBatch(evidence, "sprints", evidence.Pages, evidence.Records, cycleErr, false)
				}
				cyclesAbandoned = true
				rows.Sprints = nil
			} else {
				teamClaim := claim
				teamClaim.SourceExternalID = team.ID
				for _, cyclePayload := range cyclePayloads {
					sprint, normalizeErr := normalizeLinearSprint(teamClaim, cyclePayload, normalizedAt)
					if normalizeErr != nil {
						if ref.Strict {
							return linearReferenceCatalogFailureBatch(evidence, "sprints", evidence.Pages, evidence.Records, normalizeErr, false)
						}
						cyclesAbandoned = true
						rows.Sprints = nil
						break
					}
					rows.Sprints = append(rows.Sprints, sprint)
				}
			}
		}
	}
	// No teams is still a complete, authoritative workspace snapshot. Set the
	// member bit after the loop so an empty workspace is not reported as an
	// unmeasured path. Vacuously complete when Members was never selected --
	// nothing was attempted, so nothing can be incomplete.
	if len(teamRaw) == 0 || !selections.Members {
		evidence.MembersComplete = true
	}

	if selections.Projects {
		projectRaw, projectPages, projectCap, projectErr := collectLinearReferenceConnection(
			ctx, client, linearReferenceCatalogProjectsQuery, linearReferenceConnectionProjects,
			linearReferenceConnectionVariables{First: perPage, IncludeArchived: true}, perPage, maxPages,
		)
		evidence.Requests += projectPages
		evidence.Pages += projectPages
		// CHAOS-4431 codex review round 3, P2: team_autoimport_linear.py:
		// 605-623's except clause keeps whatever native_project_rows already
		// accumulated and marks native_projects_complete=False, then STILL
		// extends project_rows with that partial prefix and continues the
		// function -- it never discards the whole call (if strict: raise is
		// the only abort path). An earlier revision here aborted on ANY
		// projects error regardless of strict, discarding teams/members/
		// cycles already fetched along with it -- exactly the same bug class
		// already fixed for cycles failures above, just missed here.
		if (projectErr != nil || projectCap) && ref.Strict {
			return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, projectErr, projectCap)
		}
		if projectErr == nil && !projectCap {
			evidence.ProjectsComplete = true
		}
		evidence.Records += len(projectRaw)
	projectNodes:
		for _, raw := range projectRaw {
			var payload linearReferenceProjectPayload
			if err := json.Unmarshal(raw, &payload); err != nil {
				if ref.Strict {
					return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, err, false)
				}
				break projectNodes
			}
			// CHAOS-4431 codex review P2: each native project is versioned at
			// the moment THIS node was observed, not at walk start, so two
			// overlapping discovery runs order by what they saw rather than
			// by which run started first (team_autoimport_linear.py:590-602).
			observedAt := handler.observedAt()
			project, normalizeErr := normalizeLinearReferenceProject(claim, payload, observedAt)
			if normalizeErr != nil {
				if ref.Strict {
					return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, normalizeErr, false)
				}
				break projectNodes
			}
			rows.Projects = append(rows.Projects, project)
			for _, team := range payload.Teams.Nodes {
				teamID := strings.TrimSpace(team.Key)
				if teamID == "" {
					teamID = strings.TrimSpace(team.ID)
				}
				if teamID == "" {
					continue
				}
				// CHAOS-4530: ProjectKey stays nil for a REAL project's
				// ownership row. team.Key here is the OWNING TEAM's key,
				// never a per-project key -- Linear has no such concept the
				// collector can populate yet, and stamping the team key here
				// was the defect ("a team key is not a project key"): it
				// made this row's project_key collide with every other
				// project this team owns, and acr's projectOwnershipJoinSQL
				// join is on project_key, so it never distinguished one real
				// project from another anyway. ProjectID (the real UUID)
				// remains the row's genuine, already-correct identity.
				rows.Ownership = append(rows.Ownership, linearReferenceOwnershipRow{
					OrgID: claim.OrgID, Provider: "linear", TeamID: teamID, ProjectID: project.ID,
					ProjectKey: nil, Source: "native", IsPrimary: 1, Specificity: 100,
					Priority: 10, ValidFrom: observedAt, UpdatedAt: observedAt,
				})
			}
		}
	} else {
		evidence.ProjectsComplete = true
	}

	// CHAOS-4530 ("a team key is not a project key"): the Python producer
	// used to emit a team-derived PROJECT catalog row for each discovered
	// team (id={org}:linear:{teamKey}, project_key=teamKey, name=team.Name)
	// -- an un-typed, team-shaped row written into `projects` so CHAOS could
	// "own" repos through project_keys=[team key]. acr's
	// projectOwnershipJoinSQL only matches project facts through
	// projects.project_key, and this synthetic row was the ONLY non-empty
	// project_key ever written for Linear -- so every project fact resolved
	// to "team CHAOS" and no real Linear project was ever reachable. CHAOS
	// is a TEAM, not a project: that `projects` row is gone, full stop.
	//
	// The MATCHING team_project_ownership row below is intentionally KEPT.
	// team_repo_ownership_derivation.go's linearTeamKeyProjectID (CHAOS-4458
	// part (b), live on prod 5.6) reconstructs this exact
	// "{org_id}:linear:{team_key}" identity and joins team_project_ownership
	// on it DIRECTLY -- confirmed by reading loadTeamRepoOwnershipProjectLinks
	// (team_repo_ownership_derivation_clickhouse.go): it selects only from
	// team_project_ownership, never from `projects`. This is a documented,
	// already-shipped contract (docs/contribute/architecture/team-attribution.md
	// "Two Linear id spaces, one resolver" / "unchanged shape/writer
	// intent"), and it is the reason prod's team_repo_ownership inferred
	// rows went 0 -> 10 in the 5.6 readback -- the first non-zero measurement
	// ever recorded there. Deleting this row too would silently zero that
	// arm back out. ProjectKey stays teamKey here ON PURPOSE: this row does
	// not claim to describe a project (there is no matching `projects` row
	// for it any more), it is a team-ownership signal keyed by the
	// reconstructed identity its one reader (linearTeamKeyProjectID) expects.
	for _, team := range rows.Teams {
		projectKey := team.ID
		projectID := claim.OrgID + ":linear:" + projectKey
		projectKeyPtr := optionalLinearString(projectKey)
		teamID := team.ID
		rows.Ownership = append(rows.Ownership, linearReferenceOwnershipRow{
			OrgID: claim.OrgID, Provider: "linear", TeamID: teamID, ProjectID: projectID,
			ProjectKey: projectKeyPtr, Source: "native", IsPrimary: 1, Specificity: 100,
			Priority: 10, ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
		})
	}
	rows = dedupeLinearReferenceCatalogRows(rows)
	effects, err := BuildLinearReferenceCatalogEffects(rows)
	if err != nil {
		return LinearReferenceCatalogBatch{}, err
	}
	projectsWithoutKey := 0
	for _, project := range rows.Projects {
		if project.ProjectKey == nil {
			projectsWithoutKey++
		}
	}
	result := LinearReferenceCatalogResult{
		Teams: len(rows.Teams), Members: len(rows.Members), Memberships: len(rows.Memberships),
		Projects: len(rows.Projects), Ownership: len(rows.Ownership), Sprints: len(rows.Sprints), Complete: true,
		ProjectsWithoutKey: projectsWithoutKey,
	}
	evidence.Records = result.Teams + result.Members + result.Memberships + result.Projects + result.Ownership + result.Sprints
	// No claim, no lease window: this walk has no watermark concept (it was
	// never load-bearing here -- CollectReferenceCatalog filters nothing by
	// SinceAt/BeforeAt, it always walks the whole catalog).
	return LinearReferenceCatalogBatch{Rows: rows, Effects: effects, Result: result, Evidence: evidence}, nil
}

func linearReferenceCatalogFailureBatch(
	evidence LinearReferenceCatalogEvidence,
	surface string,
	pages, records int,
	collectErr error,
	capReached bool,
) (LinearReferenceCatalogBatch, error) {
	code := LinearReferenceCatalogInvalidResponse
	if capReached || errors.Is(collectErr, ErrPaginationCapExceeded) {
		code = LinearReferenceCatalogPaginationCap
	} else if errors.Is(collectErr, providerfoundation.ErrGraphQLResponse) || errors.Is(collectErr, providerfoundation.ErrGraphQLComplexity) {
		code = LinearReferenceCatalogGraphQLError
	}
	message := "Linear reference catalog collection failed"
	if collectErr != nil {
		message = collectErr.Error()
	}
	failure := &LinearReferenceCatalogFailure{Code: code, Surface: surface, Pages: pages, Records: records, Message: message}
	return LinearReferenceCatalogBatch{Evidence: evidence, Failure: failure}, linearReferenceCatalogError(code, collectErr)
}

func linearReferenceCatalogError(code LinearReferenceCatalogFailureCode, cause error) error {
	if cause != nil {
		return cause
	}
	switch code {
	case LinearReferenceCatalogPaginationCap:
		return ErrPaginationCapExceeded
	default:
		return providerfoundation.ErrNormalizationInvalid
	}
}

type linearReferenceConnectionPath string

const (
	linearReferenceConnectionTeams       linearReferenceConnectionPath = "teams"
	linearReferenceConnectionTeamMembers linearReferenceConnectionPath = "team.members"
	linearReferenceConnectionProjects    linearReferenceConnectionPath = "projects"
)

type linearReferencePageInfo struct {
	HasNextPage *bool   `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

func collectLinearReferenceConnection(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	query string,
	path linearReferenceConnectionPath,
	variables linearReferenceConnectionVariables,
	perPage, maxPages int,
) ([]json.RawMessage, int, bool, error) {
	if ctx == nil || client == nil || strings.TrimSpace(query) == "" || perPage < 1 || maxPages < 1 {
		return nil, 0, false, ErrInvalidConfiguration
	}
	items := make([]json.RawMessage, 0)
	cursor := ""
	seen := make([]string, 0)
	for pages := 0; ; {
		if pages >= maxPages {
			return items, pages, true, ErrPaginationCapExceeded
		}
		requestVariables := variables
		requestVariables.First = perPage
		if cursor == "" {
			requestVariables.After = nil
		} else {
			requestVariables.After = &cursor
		}
		body, err := json.Marshal(linearReferenceGraphQLRequest{Query: query, Variables: requestVariables})
		if err != nil {
			return items, pages, false, err
		}
		response, err := client.Do(ctx, http.MethodPost, "/graphql", bytes.NewReader(body))
		if err != nil {
			return items, pages, false, err
		}
		var envelope linearReferenceGraphQLEnvelope
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			response.Body.Close()
			return items, pages, false, err
		}
		response.Body.Close()
		if len(envelope.Errors) > 0 {
			if strings.Contains(strings.ToLower(envelope.Errors[0].Message), "complexity") {
				return items, pages, false, providerfoundation.ErrGraphQLComplexity
			}
			return items, pages, false, providerfoundation.ErrGraphQLResponse
		}
		connection, ok := linearReferenceConnection(envelope.Data, path)
		if !ok {
			return items, pages, false, providerfoundation.ErrPaginationInvalid
		}
		page := connection
		hasNextPagePresent := page.PageInfo.HasNextPage != nil
		hasNextPage := false
		if hasNextPagePresent {
			hasNextPage = *page.PageInfo.HasNextPage
		}
		if page.Nodes == nil || !hasNextPagePresent {
			return items, pages, false, providerfoundation.ErrPaginationInvalid
		}
		items = append(items, page.Nodes...)
		pages++
		if !hasNextPage {
			return items, pages, false, nil
		}
		if page.PageInfo.EndCursor == nil || strings.TrimSpace(*page.PageInfo.EndCursor) == "" || containsString(seen, *page.PageInfo.EndCursor) || *page.PageInfo.EndCursor == cursor {
			return items, pages, false, providerfoundation.ErrPaginationInvalid
		}
		seen = append(seen, *page.PageInfo.EndCursor)
		cursor = *page.PageInfo.EndCursor
	}
}

type linearReferenceGraphQLRequest struct {
	Query     string                             `json:"query"`
	Variables linearReferenceConnectionVariables `json:"variables"`
}

// linearReferenceConnectionVariables is the closed set of GraphQL inputs
// used by this catalog. A typed request prevents a new provider field from
// leaking through an unreviewed map and makes the request shape visible at
// the provider boundary.
type linearReferenceConnectionVariables struct {
	First           int     `json:"first"`
	After           *string `json:"after"`
	TeamID          string  `json:"teamId,omitempty"`
	IncludeArchived bool    `json:"includeArchived,omitempty"`
}

type linearReferenceGraphQLError struct {
	Message string `json:"message"`
}

type linearReferenceGraphQLEnvelope struct {
	Data   json.RawMessage               `json:"data"`
	Errors []linearReferenceGraphQLError `json:"errors"`
}

type linearReferenceConnectionPage struct {
	Nodes    []json.RawMessage       `json:"nodes"`
	PageInfo linearReferencePageInfo `json:"pageInfo"`
}

type linearReferenceCatalogTeamMemberConnection struct {
	Members *linearReferenceConnectionPage `json:"members"`
}

type linearReferenceCatalogGraphQLData struct {
	Teams    *linearReferenceConnectionPage              `json:"teams"`
	Team     *linearReferenceCatalogTeamMemberConnection `json:"team"`
	Projects *linearReferenceConnectionPage              `json:"projects"`
}

func linearReferenceConnection(
	data json.RawMessage,
	path linearReferenceConnectionPath,
) (*linearReferenceConnectionPage, bool) {
	if len(data) == 0 || string(data) == "null" {
		return nil, false
	}
	var payload linearReferenceCatalogGraphQLData
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, false
	}
	switch path {
	case linearReferenceConnectionTeams:
		return payload.Teams, payload.Teams != nil
	case linearReferenceConnectionTeamMembers:
		if payload.Team == nil {
			return nil, false
		}
		return payload.Team.Members, payload.Team.Members != nil
	case linearReferenceConnectionProjects:
		return payload.Projects, payload.Projects != nil
	default:
		return nil, false
	}
}

func dedupeLinearReferenceCatalogRows(rows LinearReferenceCatalogRows) LinearReferenceCatalogRows {
	rows.Teams = dedupeLinearReferenceTeams(rows.Teams)
	rows.Members = dedupeLinearReferenceMembers(rows.Members)
	rows.Memberships = dedupeLinearReferenceMemberships(rows.Memberships)
	rows.Projects = dedupeLinearReferenceProjects(rows.Projects)
	rows.Ownership = dedupeLinearReferenceOwnership(rows.Ownership)
	rows.Sprints = dedupeLinearReferenceSprints(rows.Sprints)
	return rows
}

// dedupeLinearReferenceSprints collapses by SprintID -- a team's cycle can
// legitimately recur across teams sharing a cycle is not possible in Linear,
// but defensive dedup matches every other dimension's contract here and
// guards a future shared-cycle workspace shape without re-reviewing this
// file. Last write wins, same as every other dedupe* helper in this file.
func dedupeLinearReferenceSprints(rows []linearSprintRow) []linearSprintRow {
	result := make([]linearSprintRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].Provider == row.Provider && result[index].SprintID == row.SprintID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeLinearReferenceTeams(rows []linearReferenceTeamRow) []linearReferenceTeamRow {
	result := make([]linearReferenceTeamRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].Provider == row.Provider && result[index].ID == row.ID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeLinearReferenceMembers(rows []linearReferenceMemberRow) []linearReferenceMemberRow {
	result := make([]linearReferenceMemberRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].MemberID == row.MemberID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeLinearReferenceMemberships(rows []linearReferenceMembershipRow) []linearReferenceMembershipRow {
	result := make([]linearReferenceMembershipRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].Provider == row.Provider && result[index].TeamID == row.TeamID && result[index].MemberID == row.MemberID && result[index].Source == row.Source {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeLinearReferenceProjects(rows []linearReferenceProjectRow) []linearReferenceProjectRow {
	result := make([]linearReferenceProjectRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].Provider == row.Provider && result[index].ID == row.ID {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}

func dedupeLinearReferenceOwnership(rows []linearReferenceOwnershipRow) []linearReferenceOwnershipRow {
	result := make([]linearReferenceOwnershipRow, 0, len(rows))
	for _, row := range rows {
		found := false
		for index := range result {
			if result[index].OrgID == row.OrgID && result[index].Provider == row.Provider && result[index].ProjectID == row.ProjectID && result[index].TeamID == row.TeamID && result[index].Source == row.Source {
				result[index] = row
				found = true
				break
			}
		}
		if !found {
			result = append(result, row)
		}
	}
	return result
}
