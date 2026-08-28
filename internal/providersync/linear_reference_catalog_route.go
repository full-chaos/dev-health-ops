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
	Complete    bool `json:"complete"`
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
		Ownership: make([]linearReferenceOwnershipRow, 0),
	}
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
	for _, raw := range teamRaw {
		var payload linearReferenceCatalogTeamPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return linearReferenceCatalogFailureBatch(evidence, "teams", evidence.Pages, evidence.Records, err, false)
		}
		team, normalizeErr := normalizeLinearReferenceTeam(claim, payload, normalizedAt)
		if normalizeErr != nil {
			return linearReferenceCatalogFailureBatch(evidence, "teams", evidence.Pages, evidence.Records, normalizeErr, false)
		}
		rows.Teams = append(rows.Teams, team)
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
		if len(rows.Teams) == 1 {
			evidence.MembersComplete = membersComplete
		}
	}
	// No teams is still a complete, authoritative workspace snapshot. Set the
	// member bit after the loop so an empty workspace is not reported as an
	// unmeasured path.
	if len(teamRaw) == 0 {
		evidence.MembersComplete = true
	}

	projectRaw, projectPages, projectCap, projectErr := collectLinearReferenceConnection(
		ctx, client, linearReferenceCatalogProjectsQuery, linearReferenceConnectionProjects,
		linearReferenceConnectionVariables{First: perPage, IncludeArchived: true}, perPage, maxPages,
	)
	evidence.Requests += projectPages
	evidence.Pages += projectPages
	if projectErr != nil || projectCap {
		return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, projectErr, projectCap)
	}
	evidence.ProjectsComplete = true
	evidence.Records += len(projectRaw)
	for _, raw := range projectRaw {
		var payload linearReferenceProjectPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, err, false)
		}
		project, normalizeErr := normalizeLinearReferenceProject(claim, payload, normalizedAt)
		if normalizeErr != nil {
			return linearReferenceCatalogFailureBatch(evidence, "projects", evidence.Pages, evidence.Records, normalizeErr, false)
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
			projectKey := optionalLinearString(team.Key)
			rows.Ownership = append(rows.Ownership, linearReferenceOwnershipRow{
				OrgID: claim.OrgID, Provider: "linear", TeamID: teamID, ProjectID: project.ID,
				ProjectKey: projectKey, Source: "native", IsPrimary: 1, Specificity: 100,
				Priority: 10, ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
			})
		}
	}

	// The Python producer emits a team-derived project catalog row for each
	// discovered team, in addition to native Linear projects. This is the row
	// space used by existing team/project attribution and must remain present.
	for _, team := range rows.Teams {
		projectKey := team.ID
		projectID := claim.OrgID + ":linear:" + projectKey
		projectKeyPtr := optionalLinearString(projectKey)
		teamID := team.ID
		rows.Projects = append(rows.Projects, linearReferenceProjectRow{
			ID: projectID, OrgID: claim.OrgID, Provider: "linear", ProjectKey: projectKeyPtr,
			Name: team.Name, IsActive: 1,
			UpdatedAt: normalizedAt, LastSynced: normalizedAt,
		})
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
	result := LinearReferenceCatalogResult{
		Teams: len(rows.Teams), Members: len(rows.Members), Memberships: len(rows.Memberships),
		Projects: len(rows.Projects), Ownership: len(rows.Ownership), Complete: true,
	}
	evidence.Records = result.Teams + result.Members + result.Memberships + result.Projects + result.Ownership
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
	return rows
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
