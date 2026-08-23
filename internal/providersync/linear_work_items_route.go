package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	linearWorkItemsDefaultPerPage = 50
	linearWorkItemsMaxPerPage     = 100
	linearWorkItemsDefaultPages   = 100
	// LinearClient's bulk issue query asks for 50 comments.  The Python
	// provider's explicit comment helper has a bounded 100-comment contract;
	// keep the native route on that same boundary instead of silently dropping
	// a second page or issuing an unbounded nested crawl.
	linearWorkItemsCommentsPerPage  = 50
	linearWorkItemsCommentsMaxPages = 2
	linearWorkItemsHistoryPerPage   = 50
)

// linearWorkItemsQuery deliberately follows the fields selected by
// LinearClient. The provider boundary owns the raw GraphQL response; the
// normalizer below owns the semantic row and transition contract.
const linearWorkItemsQuery = `
query LinearWorkItems($first: Int!, $after: String, $filter: IssueFilter) {
  issues(first: $first, after: $after, filter: $filter, orderBy: updatedAt) {
    nodes {
      id identifier title description priority estimate createdAt updatedAt startedAt completedAt canceledAt dueDate url archivedAt
      state { name type }
      assignee { name email }
      creator { name email }
      labels { nodes { name } }
      parent { identifier }
      project { id name }
      cycle { id number name }
      team { id key name }
      history(first: 50) {
        nodes {
          createdAt
          fromState { name type }
          toState { name type }
          actor { name email }
        }
        pageInfo { hasNextPage endCursor }
      }
      comments(first: 50) {
        nodes {
          body
          createdAt
          user { name email }
        }
        pageInfo { hasNextPage endCursor }
      }
      attachments(first: 50) {
        nodes { url sourceType }
        pageInfo { hasNextPage endCursor }
      }
      relations(first: 50) {
        nodes {
          type
          issue { identifier }
          relatedIssue { identifier }
        }
        pageInfo { hasNextPage endCursor }
      }
      inverseRelations(first: 50) {
        nodes {
          type
          issue { identifier }
          relatedIssue { identifier }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

const linearWorkItemsTeamQuery = `
query LinearWorkItemsTeam($first: Int!, $after: String, $filter: TeamFilter) {
  teams(first: $first, after: $after, filter: $filter) {
    nodes { id key name }
    pageInfo { hasNextPage endCursor }
  }
}`

const linearWorkItemsCyclesQuery = `
query LinearWorkItemsCycles($first: Int!, $after: String, $filter: CycleFilter) {
  cycles(first: $first, after: $after, filter: $filter) {
    nodes {
      id number name startsAt endsAt completedAt progress
      team { id key name }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

const linearWorkItemsAttachmentsQuery = `
query LinearWorkItemsAttachments($first: Int!, $after: String, $issueId: String!) {
  issue(id: $issueId) {
    attachments(first: $first, after: $after) {
      nodes { url sourceType }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const linearWorkItemsRelationsQuery = `
query LinearWorkItemsRelations($first: Int!, $after: String, $issueId: String!) {
  issue(id: $issueId) {
    relations(first: $first, after: $after) {
      nodes { type issue { identifier } relatedIssue { identifier } }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const linearWorkItemsCommentsQuery = `
query LinearWorkItemsComments($first: Int!, $after: String, $issueId: String!) {
  issue(id: $issueId) {
    comments(first: $first, after: $after) {
      nodes {
        body
        createdAt
        user { name email }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const linearWorkItemsHistoryQuery = `
query LinearWorkItemsHistory($first: Int!, $after: String, $issueId: String!) {
  issue(id: $issueId) {
    history(first: $first, after: $after) {
      nodes {
        createdAt
        fromState { name type }
        toState { name type }
        actor { name email }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

const linearWorkItemsInverseRelationsQuery = `
query LinearWorkItemsInverseRelations($first: Int!, $after: String, $issueId: String!) {
  issue(id: $issueId) {
    inverseRelations(first: $first, after: $after) {
      nodes { type issue { identifier } relatedIssue { identifier } }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

type linearWorkItemPayload struct {
	ID               string                   `json:"id"`
	Identifier       string                   `json:"identifier"`
	Title            string                   `json:"title"`
	Description      *string                  `json:"description"`
	Priority         *int                     `json:"priority"`
	Estimate         *float64                 `json:"estimate"`
	CreatedAt        string                   `json:"createdAt"`
	UpdatedAt        string                   `json:"updatedAt"`
	StartedAt        *string                  `json:"startedAt"`
	CompletedAt      *string                  `json:"completedAt"`
	CanceledAt       *string                  `json:"canceledAt"`
	DueDate          *string                  `json:"dueDate"`
	URL              *string                  `json:"url"`
	ArchivedAt       *string                  `json:"archivedAt"`
	State            *linearStatePayload      `json:"state"`
	Assignee         *linearIdentityPayload   `json:"assignee"`
	Creator          *linearIdentityPayload   `json:"creator"`
	Labels           linearLabelsPayload      `json:"labels"`
	Parent           *linearParentPayload     `json:"parent"`
	Project          *linearProjectPayload    `json:"project"`
	Cycle            *linearCyclePayload      `json:"cycle"`
	Team             *linearTeamPayload       `json:"team"`
	History          linearHistoryPayload     `json:"history"`
	Comments         linearCommentsPayload    `json:"comments"`
	Attachments      linearAttachmentsPayload `json:"attachments"`
	Relations        linearRelationsPayload   `json:"relations"`
	InverseRelations linearRelationsPayload   `json:"inverseRelations"`
}

type linearStatePayload struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type linearIdentityPayload struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type linearLabelsPayload struct {
	Nodes []struct {
		Name string `json:"name"`
	} `json:"nodes"`
}

type linearParentPayload struct {
	Identifier string `json:"identifier"`
}

type linearProjectPayload struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type linearCyclePayload struct {
	ID          string             `json:"id"`
	Number      *int               `json:"number"`
	Name        string             `json:"name"`
	StartsAt    *string            `json:"startsAt"`
	EndsAt      *string            `json:"endsAt"`
	CompletedAt *string            `json:"completedAt"`
	Progress    *float64           `json:"progress"`
	Team        *linearTeamPayload `json:"team"`
}

type linearTeamPayload struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

type linearTeamsPayload struct {
	Nodes    []linearTeamPayload   `json:"nodes"`
	PageInfo linearPageInfoPayload `json:"pageInfo"`
}

type linearCyclesPayload struct {
	Nodes    []linearCyclePayload  `json:"nodes"`
	PageInfo linearPageInfoPayload `json:"pageInfo"`
}

type linearHistoryPayload struct {
	Nodes    []linearHistoryEntry  `json:"nodes"`
	PageInfo linearPageInfoPayload `json:"pageInfo"`
}

type linearPageInfoPayload struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type linearCommentPayload struct {
	Body      string                 `json:"body"`
	CreatedAt string                 `json:"createdAt"`
	User      *linearIdentityPayload `json:"user"`
}

type linearCommentsPayload struct {
	Nodes    []linearCommentPayload `json:"nodes"`
	PageInfo linearPageInfoPayload  `json:"pageInfo"`
}

type linearAttachmentPayload struct {
	URL        string `json:"url"`
	SourceType string `json:"sourceType"`
}

type linearAttachmentsPayload struct {
	Nodes    []linearAttachmentPayload `json:"nodes"`
	PageInfo linearPageInfoPayload     `json:"pageInfo"`
}

type linearRelationIssuePayload struct {
	Identifier string `json:"identifier"`
}

type linearRelationPayload struct {
	Type         string                      `json:"type"`
	Issue        *linearRelationIssuePayload `json:"issue"`
	RelatedIssue *linearRelationIssuePayload `json:"relatedIssue"`
}

type linearRelationsPayload struct {
	Nodes    []linearRelationPayload `json:"nodes"`
	PageInfo linearPageInfoPayload   `json:"pageInfo"`
}

type linearHistoryEntry struct {
	CreatedAt string                 `json:"createdAt"`
	FromState *linearStatePayload    `json:"fromState"`
	ToState   *linearStatePayload    `json:"toState"`
	Actor     *linearIdentityPayload `json:"actor"`
}

// linearWorkItemRow is the complete normalized WorkItem field set plus the
// unit-owned persistence stamp. The latter two fields are intentionally
// excluded by the generic Python oracle because Python's normalizer does not
// own tenant stamping or retry-stable last_synced values.
type linearWorkItemRow struct {
	WorkItemID    string     `json:"work_item_id"`
	Provider      string     `json:"provider"`
	Title         string     `json:"title"`
	Type          string     `json:"type"`
	Status        string     `json:"status"`
	StatusRaw     *string    `json:"status_raw"`
	Description   *string    `json:"description"`
	RepoID        *string    `json:"repo_id"`
	NativeTeamKey *string    `json:"native_team_key"`
	ProjectKey    *string    `json:"project_key"`
	ProjectID     *string    `json:"project_id"`
	ProjectName   *string    `json:"project_name"`
	Assignees     []string   `json:"assignees"`
	Reporter      *string    `json:"reporter"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	StartedAt     *time.Time `json:"started_at"`
	CompletedAt   *time.Time `json:"completed_at"`
	ClosedAt      *time.Time `json:"closed_at"`
	Labels        []string   `json:"labels"`
	StoryPoints   *float64   `json:"story_points"`
	SprintID      *string    `json:"sprint_id"`
	SprintName    *string    `json:"sprint_name"`
	ParentID      *string    `json:"parent_id"`
	EpicID        *string    `json:"epic_id"`
	URL           *string    `json:"url"`
	PriorityRaw   *string    `json:"priority_raw"`
	ServiceClass  *string    `json:"service_class"`
	DueAt         *time.Time `json:"due_at"`
	OrgID         string     `json:"org_id"`
	LastSynced    time.Time  `json:"last_synced"`
}

type linearWorkItemTransitionRow struct {
	WorkItemID    string    `json:"work_item_id"`
	Provider      string    `json:"provider"`
	OccurredAt    time.Time `json:"occurred_at"`
	FromStatusRaw *string   `json:"from_status_raw"`
	ToStatusRaw   *string   `json:"to_status_raw"`
	FromStatus    string    `json:"from_status"`
	ToStatus      string    `json:"to_status"`
	Actor         *string   `json:"actor"`
	OrgID         string    `json:"org_id"`
	LastSynced    time.Time `json:"last_synced"`
}

// These rows reuse the provider-neutral direct sink contracts already proven
// against the ClickHouse schema. Linear owns collection/normalization; the
// direct adapters own the exact Python sink projection.
type linearWorkItemDependencyRow = githubWorkItemDependencyRow
type linearWorkItemReopenRow = githubWorkItemReopenRow
type linearWorkItemInteractionRow = githubWorkItemInteractionRow
type linearSprintRow = githubSprintRow

type linearWorkItemRows struct {
	WorkItems         []linearWorkItemRow
	StatusTransitions []linearWorkItemTransitionRow
	Dependencies      []linearWorkItemDependencyRow
	ReopenEvents      []linearWorkItemReopenRow
	Interactions      []linearWorkItemInteractionRow
	Sprints           []linearSprintRow
}

// LinearWorkItemsRouteHandler is the provider-only canonical work-items
// vertical slice. It is intentionally not registered or activated here; the
// family planner and route gate remain separate work.
type LinearWorkItemsRouteHandler struct {
	PerPage       int
	MaxPages      int
	FetchComments *bool
	FetchHistory  *bool
	FetchCycles   *bool
	// GlobalDiscovery mirrors Python's repo=None mode. The claim still carries
	// a non-empty source identity for lease/watermark fencing, but the route
	// discovers every Linear team and applies the same bounded issue crawl to
	// each team. It is provider-local and intentionally not a registry switch.
	GlobalDiscovery bool
	// ReferenceTeams and ReferenceSprints model the reference dimensions that
	// Python receives through IngestionContext.  They are deliberately inputs
	// to this provider-owned route only; no registry, planner, or sink wiring is
	// implied by their presence here.
	ReferenceTeams   []LinearReferenceTeam
	ReferenceSprints []linearSprintRow
}

// LinearReferenceTeam is the small reference projection used to resolve a
// team-scoped Linear unit before the issue crawl.  The Python resolver accepts
// id, name, native_team_key, and project_keys as candidates, and treats a
// blank provider as provider-agnostic; the native resolver below preserves
// those semantics exactly.
type LinearReferenceTeam struct {
	// OrgID is populated by the reference catalog.  Existing callers that pass
	// a provider-only cache may leave it blank; the legacy resolver below keeps
	// that behavior, while tenant-aware catalog resolution requires an exact
	// match through linearReferenceTeamPayloadForOrg.
	OrgID         string
	Provider      string
	ID            string
	Name          string
	NativeTeamKey string
	ProjectKeys   []string
}

func linearWorkItemsFlag(value *bool) bool {
	return value == nil || *value
}

func linearConnectionCursor(pageInfo linearPageInfoPayload) (string, error) {
	if !pageInfo.HasNextPage {
		return "", nil
	}
	cursor := strings.TrimSpace(pageInfo.EndCursor)
	if cursor == "" {
		return "", providerfoundation.ErrPaginationInvalid
	}
	return cursor, nil
}

func appendLinearUnique[T any](existing []T, extra ...[]T) []T {
	seen := make(map[string]struct{}, len(existing))
	key := func(value T) string {
		encoded, err := json.Marshal(value)
		if err != nil {
			return ""
		}
		return string(encoded)
	}
	for _, value := range existing {
		seen[key(value)] = struct{}{}
	}
	for _, values := range extra {
		for _, value := range values {
			valueKey := key(value)
			if _, duplicate := seen[valueKey]; duplicate {
				continue
			}
			seen[valueKey] = struct{}{}
			existing = append(existing, value)
		}
	}
	return existing
}

func linearNestedPageLimit() int { return 5 } // 5 * 100 == Python's 500-row bound

func linearReferenceTeamPayload(
	rows []LinearReferenceTeam,
	teamKey string,
) (linearTeamPayload, bool) {
	teamKey = strings.TrimSpace(teamKey)
	if teamKey == "" {
		return linearTeamPayload{}, false
	}
	for _, row := range rows {
		provider := strings.TrimSpace(row.Provider)
		if provider != "" && provider != "linear" {
			continue
		}
		id := strings.TrimSpace(row.ID)
		name := strings.TrimSpace(row.Name)
		nativeKey := strings.TrimSpace(row.NativeTeamKey)
		candidates := map[string]struct{}{}
		for _, candidate := range append([]string{id, name, nativeKey}, row.ProjectKeys...) {
			if candidate = strings.TrimSpace(candidate); candidate != "" {
				candidates[candidate] = struct{}{}
			}
		}
		if _, ok := candidates[teamKey]; !ok {
			continue
		}
		if id == "" {
			id = teamKey
		}
		if nativeKey == "" {
			nativeKey = teamKey
		}
		if name == "" {
			name = teamKey
		}
		return linearTeamPayload{ID: id, Key: nativeKey, Name: name}, true
	}
	return linearTeamPayload{}, false
}

func (handler LinearWorkItemsRouteHandler) resolveLinearTeam(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	teamKey string,
) (linearTeamPayload, int, error) {
	tenantScoped := false
	for _, reference := range handler.ReferenceTeams {
		if strings.TrimSpace(reference.OrgID) != "" {
			tenantScoped = true
			break
		}
	}
	if tenantScoped {
		if team, ok := linearReferenceTeamPayloadForOrg(handler.ReferenceTeams, claim.OrgID, teamKey); ok {
			return team, 0, nil
		}
	} else if team, ok := linearReferenceTeamPayload(handler.ReferenceTeams, teamKey); ok {
		return team, 0, nil
	}
	return collectLinearTeam(ctx, client, teamKey)
}

func linearReferenceTeamPayloads(rows []LinearReferenceTeam) []linearTeamPayload {
	teams := make([]linearTeamPayload, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		provider := strings.TrimSpace(row.Provider)
		if provider != "" && provider != "linear" {
			continue
		}
		id := strings.TrimSpace(row.ID)
		key := strings.TrimSpace(row.NativeTeamKey)
		name := strings.TrimSpace(row.Name)
		if key == "" {
			for _, candidate := range row.ProjectKeys {
				if candidate = strings.TrimSpace(candidate); candidate != "" {
					key = candidate
					break
				}
			}
		}
		if key == "" {
			continue
		}
		if id == "" {
			id = key
		}
		if name == "" {
			name = key
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		teams = append(teams, linearTeamPayload{ID: id, Key: key, Name: name})
	}
	return teams
}

func collectLinearTeams(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	maxPages int,
) ([]linearTeamPayload, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          linearWorkItemsTeamQuery,
			Variables:      map[string]any{},
			ConnectionPath: []string{"teams"},
			PerPage:        linearWorkItemsMaxPerPage,
			MaxPages:       maxPages,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	teams := make([]linearTeamPayload, 0, len(page.Items))
	seen := make(map[string]struct{}, len(page.Items))
	for _, raw := range page.Items {
		var team linearTeamPayload
		if err := json.Unmarshal(raw, &team); err != nil ||
			strings.TrimSpace(team.ID) == "" || strings.TrimSpace(team.Key) == "" {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		if _, duplicate := seen[team.Key]; duplicate {
			continue
		}
		seen[team.Key] = struct{}{}
		teams = append(teams, team)
	}
	return teams, page.Pages, nil
}

func linearReferenceSprints(
	claim Claim,
	rows []linearSprintRow,
) ([]linearSprintRow, error) {
	result := make([]linearSprintRow, 0, len(rows))
	for _, row := range rows {
		if row.Provider != "linear" || row.OrgID != claim.OrgID || row.NativeTeamKey == nil ||
			*row.NativeTeamKey != claim.SourceExternalID {
			continue
		}
		if err := validateLinearSprint(row, claim); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, nil
}

func collectLinearTeam(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	teamKey string,
) (linearTeamPayload, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query: linearWorkItemsTeamQuery,
			Variables: map[string]any{
				"filter": map[string]any{"key": map[string]any{"eq": teamKey}},
			},
			ConnectionPath: []string{"teams"}, PerPage: 2, MaxPages: 1,
		},
	)
	if err != nil {
		return linearTeamPayload{}, 0, err
	}
	if page.PageBudgetExhausted {
		return linearTeamPayload{}, page.Pages, ErrPaginationCapExceeded
	}
	for _, raw := range page.Items {
		var team linearTeamPayload
		if err := json.Unmarshal(raw, &team); err != nil {
			return linearTeamPayload{}, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		if team.Key == teamKey || team.Name == teamKey {
			return team, page.Pages, nil
		}
	}
	return linearTeamPayload{}, page.Pages, providerfoundation.ErrNormalizationInvalid
}

func collectLinearCycles(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	teamID string,
) ([]linearCyclePayload, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query: linearWorkItemsCyclesQuery,
			Variables: map[string]any{
				"filter": map[string]any{"team": map[string]any{
					"id": map[string]any{"eq": teamID},
				}},
			},
			ConnectionPath: []string{"cycles"}, PerPage: 100, MaxPages: linearNestedPageLimit(),
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	cycles := make([]linearCyclePayload, 0, len(page.Items))
	for _, raw := range page.Items {
		var cycle linearCyclePayload
		if err := json.Unmarshal(raw, &cycle); err != nil {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		cycles = append(cycles, cycle)
	}
	return cycles, page.Pages, nil
}

func collectLinearIssueAttachments(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	after string,
) ([]linearAttachmentPayload, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          linearWorkItemsAttachmentsQuery,
			Variables:      map[string]any{"issueId": issueID},
			ConnectionPath: []string{"issue", "attachments"},
			PerPage:        100, MaxPages: linearNestedPageLimit(),
			InitialCursor: after,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	items := make([]linearAttachmentPayload, 0, len(page.Items))
	for _, raw := range page.Items {
		var item linearAttachmentPayload
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		items = append(items, item)
	}
	return items, page.Pages, nil
}

func collectLinearIssueHistory(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	after string,
) ([]linearHistoryEntry, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          linearWorkItemsHistoryQuery,
			Variables:      map[string]any{"issueId": issueID},
			ConnectionPath: []string{"issue", "history"},
			PerPage:        linearWorkItemsHistoryPerPage,
			MaxPages:       linearNestedPageLimit(),
			InitialCursor:  after,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	items := make([]linearHistoryEntry, 0, len(page.Items))
	for _, raw := range page.Items {
		var item linearHistoryEntry
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		items = append(items, item)
	}
	return items, page.Pages, nil
}

func collectLinearIssueComments(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	after string,
) ([]linearCommentPayload, int, error) {
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          linearWorkItemsCommentsQuery,
			Variables:      map[string]any{"issueId": issueID},
			ConnectionPath: []string{"issue", "comments"},
			PerPage:        linearWorkItemsCommentsPerPage,
			MaxPages:       linearWorkItemsCommentsMaxPages,
			InitialCursor:  after,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	comments := make([]linearCommentPayload, 0, len(page.Items))
	for _, raw := range page.Items {
		var comment linearCommentPayload
		if err := json.Unmarshal(raw, &comment); err != nil {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		comments = append(comments, comment)
	}
	return comments, page.Pages, nil
}

func collectLinearIssueRelations(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	issueID string,
	inverse bool,
	after string,
) ([]linearRelationPayload, int, error) {
	query := linearWorkItemsRelationsQuery
	connection := "relations"
	if inverse {
		query = linearWorkItemsInverseRelationsQuery
		connection = "inverseRelations"
	}
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          query,
			Variables:      map[string]any{"issueId": issueID},
			ConnectionPath: []string{"issue", connection},
			PerPage:        100, MaxPages: linearNestedPageLimit(),
			InitialCursor: after,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if page.PageBudgetExhausted {
		return nil, page.Pages, ErrPaginationCapExceeded
	}
	items := make([]linearRelationPayload, 0, len(page.Items))
	for _, raw := range page.Items {
		var item linearRelationPayload
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, page.Pages, providerfoundation.ErrNormalizationInvalid
		}
		items = append(items, item)
	}
	return items, page.Pages, nil
}

func normalizeLinearSprint(
	claim Claim,
	cycle linearCyclePayload,
	normalizedAt time.Time,
) (linearSprintRow, error) {
	if strings.TrimSpace(cycle.ID) == "" {
		return linearSprintRow{}, providerfoundation.ErrNormalizationInvalid
	}
	name := strings.TrimSpace(cycle.Name)
	if name == "" && cycle.Number != nil {
		name = "Cycle " + strconv.Itoa(*cycle.Number)
	}
	if name == "" {
		return linearSprintRow{}, providerfoundation.ErrNormalizationInvalid
	}
	state := "future"
	if cycle.CompletedAt != nil && parseLinearTimePtr(cycle.CompletedAt) != nil {
		state = "closed"
	} else if cycle.Progress != nil && *cycle.Progress > 0 {
		state = "active"
	}
	nativeTeamKey := claim.SourceExternalID
	row := linearSprintRow{
		Provider: "linear", SprintID: "linear:cycle:" + cycle.ID,
		Name: nullableLinearString(name), State: nullableLinearString(state),
		StartedAt: parseLinearTimePtr(cycle.StartsAt), EndedAt: parseLinearTimePtr(cycle.EndsAt),
		CompletedAt: parseLinearTimePtr(cycle.CompletedAt), NativeTeamKey: nullableLinearString(nativeTeamKey),
		LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
	}
	if err := validateLinearSprint(row, claim); err != nil {
		return linearSprintRow{}, err
	}
	return row, nil
}

func validateLinearSprint(row linearSprintRow, claim Claim) error {
	if row.Provider != "linear" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.SprintID == "" || row.Name == nil || row.State == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func linearTrustedSCMHosts() map[string]struct{} {
	hosts := map[string]struct{}{
		"github.com": {}, "www.github.com": {},
		"gitlab.com": {}, "www.gitlab.com": {},
	}
	for _, value := range strings.Split(os.Getenv("LINEAR_TRUSTED_SCM_HOSTS"), ",") {
		if host := strings.ToLower(strings.TrimSpace(value)); host != "" {
			hosts[host] = struct{}{}
		}
	}
	return hosts
}

func linearAttachmentWorkItemID(attachment linearAttachmentPayload) string {
	sourceType := strings.ToLower(attachment.SourceType)
	if !strings.Contains(sourceType, "github") && !strings.Contains(sourceType, "gitlab") {
		return ""
	}
	parsed, err := url.Parse(attachment.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	// Python's `_trusted_scm_hosts` compares urlsplit(...).netloc exactly.  Use
	// URL.Host (including an explicit port) and reject userinfo so a URL that
	// merely has a trusted hostname does not widen the Python allowlist.
	if _, ok := linearTrustedSCMHosts()[strings.ToLower(parsed.Host)]; !ok {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if strings.Contains(sourceType, "github") && len(parts) >= 4 &&
		parts[len(parts)-2] == "pull" {
		return "ghpr:" + strings.Join(parts[:len(parts)-2], "/") + "#" + parts[len(parts)-1]
	}
	if strings.Contains(sourceType, "gitlab") && len(parts) >= 4 &&
		parts[len(parts)-2] == "merge_requests" && parts[len(parts)-3] == "-" {
		return "gitlab:" + strings.Join(parts[:len(parts)-3], "/") + "!" + parts[len(parts)-1]
	}
	return ""
}

func normalizeLinearDependencies(
	claim Claim,
	payload linearWorkItemPayload,
	workItemID string,
	normalizedAt time.Time,
) []linearWorkItemDependencyRow {
	rows := make([]linearWorkItemDependencyRow, 0)
	seen := make(map[string]struct{})
	appendRow := func(source, target, relationType, relationRaw string) {
		if source == "" || target == "" || relationType == "" {
			return
		}
		key := source + "\x00" + relationType + "\x00" + target
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		rows = append(rows, linearWorkItemDependencyRow{
			SourceWorkItemID: source, TargetWorkItemID: target,
			RelationshipType: relationType, RelationshipTypeRaw: relationRaw,
			RelationshipSemanticsVersion: "canonical-blocks.v2",
			LastSynced:                   normalizedAt.UTC(), OrgID: claim.OrgID,
		})
	}
	for _, attachment := range payload.Attachments.Nodes {
		if source := linearAttachmentWorkItemID(attachment); source != "" {
			appendRow(source, workItemID, "relates_to", "linear_attachment")
		}
	}
	for _, relations := range []linearRelationsPayload{payload.Relations, payload.InverseRelations} {
		for _, relation := range relations.Nodes {
			if relation.Issue == nil || relation.RelatedIssue == nil {
				continue
			}
			source := "linear:" + relation.Issue.Identifier
			target := "linear:" + relation.RelatedIssue.Identifier
			relationType := strings.ToLower(strings.TrimSpace(relation.Type))
			switch relationType {
			case "blocked_by", "is_blocked_by", "blocked":
				source, target, relationType = target, source, "blocks"
			case "blocks", "blocking":
				relationType = "blocks"
			case "duplicate", "duplicates":
				relationType = "duplicates"
			case "related", "relates", "relates_to":
				relationType = "relates_to"
			default:
				continue
			}
			appendRow(source, target, relationType, "linear_relation:"+strings.ToLower(strings.TrimSpace(relation.Type)))
		}
	}
	return rows
}

func normalizeLinearReopens(
	claim Claim,
	workItemID string,
	history []linearHistoryEntry,
	normalizedAt time.Time,
) []linearWorkItemReopenRow {
	rows := make([]linearWorkItemReopenRow, 0)
	for _, entry := range history {
		if entry.FromState == nil || entry.ToState == nil {
			continue
		}
		fromType := strings.ToLower(entry.FromState.Type)
		toType := strings.ToLower(entry.ToState.Type)
		if (fromType != "completed" && fromType != "canceled" && fromType != "cancelled") ||
			(toType != "backlog" && toType != "unstarted" && toType != "started") {
			continue
		}
		occurred := parseLinearTime(entry.CreatedAt)
		if occurred == nil {
			fallback := normalizedAt.UTC()
			occurred = &fallback
		}
		var actor *string
		if entry.Actor != nil {
			identity := linearIdentity(*entry.Actor)
			if identity != "unknown" {
				actor = &identity
			}
		}
		rows = append(rows, linearWorkItemReopenRow{
			WorkItemID: workItemID, OccurredAt: occurred.UTC(),
			FromStatus: linearStatus(fromType), ToStatus: linearStatus(toType),
			FromStatusRaw: nullableLinearString(entry.FromState.Name),
			ToStatusRaw:   nullableLinearString(entry.ToState.Name), Actor: actor,
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		})
	}
	return rows
}

func normalizeLinearInteractions(
	claim Claim,
	workItemID string,
	comments []linearCommentPayload,
	normalizedAt time.Time,
) []linearWorkItemInteractionRow {
	rows := make([]linearWorkItemInteractionRow, 0, len(comments))
	for _, comment := range comments {
		if comment.Body == "" {
			continue
		}
		occurred := parseLinearTime(comment.CreatedAt)
		if occurred == nil {
			fallback := normalizedAt.UTC()
			occurred = &fallback
		}
		var actor *string
		if comment.User != nil {
			identity := linearIdentity(*comment.User)
			if identity != "unknown" {
				actor = &identity
			}
		}
		rows = append(rows, linearWorkItemInteractionRow{
			WorkItemID: workItemID, Provider: "linear", InteractionType: "comment",
			OccurredAt: occurred.UTC(), Actor: actor,
			BodyLength: len([]rune(comment.Body)), LastSynced: normalizedAt.UTC(),
			OrgID: claim.OrgID,
		})
	}
	return rows
}

func (handler LinearWorkItemsRouteHandler) limits() (int, int, error) {
	perPage, maxPages := handler.PerPage, handler.MaxPages
	if perPage == 0 {
		perPage = linearWorkItemsDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = linearWorkItemsDefaultPages
	}
	if perPage < 1 || perPage > linearWorkItemsMaxPerPage || maxPages < 1 ||
		maxPages > 10_000 {
		return 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, nil
}

func (handler LinearWorkItemsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "linear" ||
		claim.Dataset != "work-items" || credential.Provider != "linear" ||
		credential.ID == "" || credential.ID != claim.CredentialID || client == nil ||
		client.Provider != "linear" || client.BaseURL == nil || client.Doer == nil ||
		client.Lease == nil || normalizedAt.IsZero() || claim.BeforeAt == nil {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	perPage, maxPages, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	fetchComments := linearWorkItemsFlag(handler.FetchComments)
	fetchHistory := linearWorkItemsFlag(handler.FetchHistory)
	fetchCycles := linearWorkItemsFlag(handler.FetchCycles)
	pagesSeen := 0
	rows := linearWorkItemRows{
		WorkItems:         make([]linearWorkItemRow, 0),
		StatusTransitions: make([]linearWorkItemTransitionRow, 0),
		Dependencies:      make([]linearWorkItemDependencyRow, 0),
		ReopenEvents:      make([]linearWorkItemReopenRow, 0),
		Interactions:      make([]linearWorkItemInteractionRow, 0),
		Sprints:           make([]linearSprintRow, 0),
	}
	var teams []linearTeamPayload
	if handler.GlobalDiscovery {
		teams = linearReferenceTeamPayloads(handler.ReferenceTeams)
		if len(teams) == 0 {
			var teamErr error
			var teamPages int
			teams, teamPages, teamErr = collectLinearTeams(ctx, client, maxPages)
			pagesSeen += teamPages
			if teamErr != nil {
				return CompleteRouteBatch{}, teamErr
			}
		}
	} else {
		// Python resolves the scoped team before it starts the issue crawl even
		// when cycles are disabled. That check prevents a malformed or stale
		// source external id from producing a clean-looking empty batch.
		team, teamPages, teamErr := handler.resolveLinearTeam(ctx, claim, client, claim.SourceExternalID)
		pagesSeen += teamPages
		if teamErr != nil {
			return CompleteRouteBatch{}, teamErr
		}
		teams = []linearTeamPayload{team}
	}
	if len(teams) == 0 && !handler.GlobalDiscovery {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	sprintSeen := make(map[string]struct{})
	for _, team := range teams {
		teamKey := strings.TrimSpace(team.Key)
		if strings.TrimSpace(team.ID) == "" || teamKey == "" {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		teamClaim := claim
		teamClaim.SourceExternalID = teamKey
		filter := map[string]any{
			"team": map[string]any{"key": map[string]any{
				"in": []string{teamKey},
			}},
			"archivedAt": map[string]any{"null": true},
		}
		updatedAt := map[string]any{}
		if claim.SinceAt != nil {
			updatedAt["gte"] = claim.SinceAt.UTC().Format(time.RFC3339Nano)
		}
		if claim.BeforeAt != nil {
			updatedAt["lte"] = claim.BeforeAt.UTC().Format(time.RFC3339Nano)
		}
		if len(updatedAt) > 0 {
			filter["updatedAt"] = updatedAt
		}
		if fetchCycles {
			var referenceSprints []linearSprintRow
			var referenceErr error
			if !handler.GlobalDiscovery {
				referenceSprints, referenceErr = linearReferenceSprints(teamClaim, handler.ReferenceSprints)
			}
			if referenceErr != nil {
				return CompleteRouteBatch{}, referenceErr
			}
			if len(referenceSprints) > 0 {
				for _, sprint := range referenceSprints {
					if _, duplicate := sprintSeen[sprint.SprintID]; !duplicate {
						rows.Sprints = append(rows.Sprints, sprint)
						sprintSeen[sprint.SprintID] = struct{}{}
					}
				}
			} else {
				cycles, cyclePages, cycleErr := collectLinearCycles(ctx, client, team.ID)
				pagesSeen += cyclePages
				if cycleErr != nil {
					return CompleteRouteBatch{}, cycleErr
				}
				for _, cycle := range cycles {
					sprint, sprintErr := normalizeLinearSprint(teamClaim, cycle, normalizedAt)
					if sprintErr != nil {
						return CompleteRouteBatch{}, sprintErr
					}
					if _, duplicate := sprintSeen[sprint.SprintID]; !duplicate {
						rows.Sprints = append(rows.Sprints, sprint)
						sprintSeen[sprint.SprintID] = struct{}{}
					}
				}
			}
		}
		page, pageErr := providerfoundation.CollectLinearGraphQLPages(
			ctx, client, providerfoundation.LinearPageOptions{
				Query:          linearWorkItemsQuery,
				Variables:      map[string]any{"filter": filter},
				ConnectionPath: []string{"issues"}, PerPage: perPage, MaxPages: maxPages,
			},
		)
		if pageErr != nil {
			return CompleteRouteBatch{}, pageErr
		}
		if page.PageBudgetExhausted {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		pagesSeen += page.Pages
		for _, raw := range page.Items {
			var payload linearWorkItemPayload
			if decoderErr := json.Unmarshal(raw, &payload); decoderErr != nil {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			if payload.ArchivedAt != nil {
				continue
			}
			if payload.ID == "" {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			if payload.Attachments.PageInfo.HasNextPage {
				cursor, cursorErr := linearConnectionCursor(payload.Attachments.PageInfo)
				if cursorErr != nil {
					return CompleteRouteBatch{}, cursorErr
				}
				attachments, attachmentPages, attachmentErr := collectLinearIssueAttachments(
					ctx, client, payload.ID, cursor,
				)
				pagesSeen += attachmentPages
				if attachmentErr != nil {
					return CompleteRouteBatch{}, attachmentErr
				}
				payload.Attachments.Nodes = appendLinearUnique(payload.Attachments.Nodes, attachments)
				payload.Attachments.PageInfo = linearPageInfoPayload{}
			}
			if fetchHistory && payload.History.PageInfo.HasNextPage {
				cursor, cursorErr := linearConnectionCursor(payload.History.PageInfo)
				if cursorErr != nil {
					return CompleteRouteBatch{}, cursorErr
				}
				history, historyPages, historyErr := collectLinearIssueHistory(
					ctx, client, payload.ID, cursor,
				)
				pagesSeen += historyPages
				if historyErr != nil {
					return CompleteRouteBatch{}, historyErr
				}
				payload.History.Nodes = appendLinearUnique(payload.History.Nodes, history)
				payload.History.PageInfo = linearPageInfoPayload{}
			}
			if fetchComments && payload.Comments.PageInfo.HasNextPage {
				cursor, cursorErr := linearConnectionCursor(payload.Comments.PageInfo)
				if cursorErr != nil {
					return CompleteRouteBatch{}, cursorErr
				}
				comments, commentPages, commentErr := collectLinearIssueComments(
					ctx, client, payload.ID, cursor,
				)
				pagesSeen += commentPages
				if commentErr != nil {
					return CompleteRouteBatch{}, commentErr
				}
				payload.Comments.Nodes = appendLinearUnique(payload.Comments.Nodes, comments)
				payload.Comments.PageInfo = linearPageInfoPayload{}
			}
			if payload.Relations.PageInfo.HasNextPage {
				cursor, cursorErr := linearConnectionCursor(payload.Relations.PageInfo)
				if cursorErr != nil {
					return CompleteRouteBatch{}, cursorErr
				}
				relations, relationPages, relationErr := collectLinearIssueRelations(
					ctx, client, payload.ID, false, cursor,
				)
				pagesSeen += relationPages
				if relationErr != nil {
					return CompleteRouteBatch{}, relationErr
				}
				payload.Relations.Nodes = appendLinearUnique(payload.Relations.Nodes, relations)
				payload.Relations.PageInfo = linearPageInfoPayload{}
			}
			if payload.InverseRelations.PageInfo.HasNextPage {
				cursor, cursorErr := linearConnectionCursor(payload.InverseRelations.PageInfo)
				if cursorErr != nil {
					return CompleteRouteBatch{}, cursorErr
				}
				relations, relationPages, relationErr := collectLinearIssueRelations(
					ctx, client, payload.ID, true, cursor,
				)
				pagesSeen += relationPages
				if relationErr != nil {
					return CompleteRouteBatch{}, relationErr
				}
				payload.InverseRelations.Nodes = appendLinearUnique(payload.InverseRelations.Nodes, relations)
				payload.InverseRelations.PageInfo = linearPageInfoPayload{}
			}
			if !fetchHistory {
				payload.History.Nodes = nil
			}
			if !fetchComments {
				payload.Comments.Nodes = nil
			}
			item, transitions, normalizeErr := normalizeLinearWorkItem(
				teamClaim, payload, normalizedAt,
			)
			if normalizeErr != nil {
				return CompleteRouteBatch{}, normalizeErr
			}
			rows.WorkItems = append(rows.WorkItems, item)
			rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
			rows.Dependencies = append(rows.Dependencies, normalizeLinearDependencies(
				teamClaim, payload, item.WorkItemID, normalizedAt,
			)...)
			if fetchHistory {
				rows.ReopenEvents = append(rows.ReopenEvents, normalizeLinearReopens(
					teamClaim, item.WorkItemID, payload.History.Nodes, normalizedAt,
				)...)
			}
			if fetchComments {
				rows.Interactions = append(rows.Interactions, normalizeLinearInteractions(
					teamClaim, item.WorkItemID, payload.Comments.Nodes, normalizedAt,
				)...)
			}
		}
	}
	effects, err := buildLinearWorkItemEffectsFromRows(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt.UTC()
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"work_items_synced":    len(rows.WorkItems),
			"transitions_synced":   len(rows.StatusTransitions),
			"dependencies_synced":  len(rows.Dependencies),
			"reopen_events_synced": len(rows.ReopenEvents),
			"interactions_synced":  len(rows.Interactions),
			"sprints_synced":       len(rows.Sprints),
		},
		Watermark: &watermark,
		Evidence: FetchEvidence{Provider: "linear", Dataset: "work-items",
			Requests: pagesSeen, Pages: pagesSeen,
			Records: len(rows.WorkItems) + len(rows.StatusTransitions) + len(rows.Dependencies) +
				len(rows.ReopenEvents) + len(rows.Interactions) + len(rows.Sprints)},
	}, nil
}

func normalizeLinearWorkItem(
	claim Claim,
	payload linearWorkItemPayload,
	normalizedAt time.Time,
) (linearWorkItemRow, []linearWorkItemTransitionRow, error) {
	if claim.Validate() != nil || claim.Provider != "linear" ||
		claim.Dataset != "work-items" || strings.TrimSpace(payload.Identifier) == "" ||
		strings.TrimSpace(payload.Title) == "" || normalizedAt.IsZero() {
		return linearWorkItemRow{}, nil, providerfoundation.ErrNormalizationInvalid
	}
	createdAt := parseLinearTime(payload.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt
		createdAt = &fallback
	}
	updatedAt := parseLinearTime(payload.UpdatedAt)
	if updatedAt == nil {
		fallback := *createdAt
		updatedAt = &fallback
	}
	stateName, stateType := "", ""
	if payload.State != nil {
		stateName, stateType = payload.State.Name, payload.State.Type
	}
	labels := make([]string, 0, len(payload.Labels.Nodes))
	for _, label := range payload.Labels.Nodes {
		if label.Name != "" {
			labels = append(labels, label.Name)
		}
	}
	assignees := make([]string, 0, 1)
	if payload.Assignee != nil {
		if identity := linearIdentity(*payload.Assignee); identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if payload.Creator != nil {
		if identity := linearIdentity(*payload.Creator); identity != "unknown" {
			reporter = &identity
		}
	}
	status := linearStatus(stateType)
	itemType := linearType(labels)
	priorityRaw, serviceClass := linearPriority(payload.Priority)
	cycleID, cycleName := "", ""
	if payload.Cycle != nil {
		cycleID, cycleName = payload.Cycle.ID, payload.Cycle.Name
		if cycleName == "" && payload.Cycle.Number != nil {
			cycleName = strconv.Itoa(*payload.Cycle.Number)
		}
	}
	var sprintID, sprintName *string
	if cycleID != "" {
		value := "linear:cycle:" + cycleID
		sprintID = &value
		if cycleName != "" {
			sprintName = &cycleName
		}
	}
	var parentID *string
	if payload.Parent != nil && payload.Parent.Identifier != "" {
		value := "linear:" + payload.Parent.Identifier
		parentID = &value
	}
	var nativeTeamKey *string
	if payload.Team != nil && payload.Team.Key != "" {
		nativeTeamKey = &payload.Team.Key
	}
	var projectID, projectName *string
	if payload.Project != nil {
		if payload.Project.ID != "" {
			projectID = &payload.Project.ID
		}
		if payload.Project.Name != "" {
			projectName = &payload.Project.Name
		}
	}
	var description *string
	if payload.Description != nil && *payload.Description != "" {
		description = payload.Description
	}
	createdUTC, updatedUTC := createdAt.UTC(), updatedAt.UTC()
	item := linearWorkItemRow{
		WorkItemID: "linear:" + payload.Identifier, Provider: "linear", Title: payload.Title,
		Type: itemType, Status: status, StatusRaw: nullableLinearString(stateName),
		Description: description, RepoID: nil, NativeTeamKey: nativeTeamKey, ProjectKey: nil,
		ProjectID: projectID, ProjectName: projectName, Assignees: assignees, Reporter: reporter,
		CreatedAt: createdUTC, UpdatedAt: updatedUTC, StartedAt: parseLinearTimePtr(payload.StartedAt),
		CompletedAt: parseLinearTimePtr(payload.CompletedAt), ClosedAt: nil, Labels: labels,
		StoryPoints: payload.Estimate, SprintID: sprintID, SprintName: sprintName,
		ParentID: parentID, EpicID: nil, URL: payload.URL, PriorityRaw: priorityRaw,
		ServiceClass: serviceClass, DueAt: parseLinearTimePtr(payload.DueDate),
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	if item.CompletedAt != nil {
		closed := item.CompletedAt.UTC()
		item.ClosedAt = &closed
	}
	if item.ClosedAt == nil && payload.CanceledAt != nil {
		canceled := parseLinearTime(*payload.CanceledAt)
		if canceled != nil {
			item.ClosedAt = canceled
		}
	}
	if err := item.validate(claim); err != nil {
		return linearWorkItemRow{}, nil, err
	}
	transitions := normalizeLinearTransitions(
		claim, item.WorkItemID, payload.History.Nodes, normalizedAt,
	)
	return item, transitions, nil
}

func (row linearWorkItemRow) validate(claim Claim) error {
	if row.Provider != "linear" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.Title == "" || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil ||
		row.Labels == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row linearWorkItemTransitionRow) validate(claim Claim) error {
	if row.Provider != "linear" || row.WorkItemID == "" || row.OccurredAt.IsZero() ||
		row.FromStatus == "" || row.ToStatus == "" || row.OrgID == "" ||
		row.OrgID != claim.OrgID || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func normalizeLinearTransitions(
	claim Claim,
	workItemID string,
	history []linearHistoryEntry,
	normalizedAt time.Time,
) []linearWorkItemTransitionRow {
	transitions := make([]linearWorkItemTransitionRow, 0, len(history))
	for _, entry := range history {
		if entry.FromState == nil && entry.ToState == nil {
			continue
		}
		if entry.ToState == nil || entry.ToState.Name == "" {
			continue
		}
		occurredAt := parseLinearTime(entry.CreatedAt)
		if occurredAt == nil {
			fallback := normalizedAt
			occurredAt = &fallback
		}
		fromRaw, fromStatus := (*string)(nil), "unknown"
		if entry.FromState != nil {
			fromRaw = nullableLinearString(entry.FromState.Name)
			fromStatus = linearStatus(entry.FromState.Type)
		}
		toRaw := nullableLinearString(entry.ToState.Name)
		actor := (*string)(nil)
		if entry.Actor != nil {
			if identity := linearIdentity(*entry.Actor); identity != "unknown" {
				actor = &identity
			}
		}
		row := linearWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "linear", OccurredAt: occurredAt.UTC(),
			FromStatusRaw: fromRaw, ToStatusRaw: toRaw, FromStatus: fromStatus,
			ToStatus: linearStatus(entry.ToState.Type), Actor: actor,
			OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
		}
		if row.validate(claim) == nil {
			transitions = append(transitions, row)
		}
	}
	return transitions
}

func parseLinearTime(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(value, "Z", "+00:00", 1))
	if err != nil {
		parsed, err = time.Parse("2006-01-02", value)
	}
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func parseLinearTimePtr(value *string) *time.Time {
	if value == nil {
		return nil
	}
	return parseLinearTime(*value)
}

func nullableLinearString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func linearIdentity(identity linearIdentityPayload) string {
	if email := strings.TrimSpace(strings.ToLower(identity.Email)); email != "" {
		return email
	}
	if name := strings.TrimSpace(identity.Name); name != "" {
		return name
	}
	return "unknown"
}

func linearStatus(stateType string) string {
	switch strings.ToLower(strings.TrimSpace(stateType)) {
	case "backlog":
		return "backlog"
	case "unstarted":
		return "todo"
	case "started":
		return "in_progress"
	case "completed":
		return "done"
	case "canceled", "cancelled":
		return "canceled"
	default:
		return "unknown"
	}
}

func linearType(labels []string) string {
	for _, label := range labels {
		switch strings.ToLower(strings.TrimSpace(label)) {
		case "bug", "type:bug":
			return "bug"
		case "incident":
			return "incident"
		case "epic":
			return "epic"
		case "story", "feature":
			return "story"
		case "chore", "maintenance":
			return "chore"
		}
	}
	return "task"
}

func linearPriority(priority *int) (*string, *string) {
	if priority == nil {
		return nil, nil
	}
	var raw, service string
	switch *priority {
	case 0:
		raw, service = "none", "intangible"
	case 1:
		raw, service = "urgent", "expedite"
	case 2:
		raw, service = "high", "fixed_date"
	case 3:
		raw, service = "medium", "standard"
	case 4:
		raw, service = "low", "intangible"
	default:
		return nil, nil
	}
	return &raw, &service
}

var _ CompleteRouteHandler = LinearWorkItemsRouteHandler{}
