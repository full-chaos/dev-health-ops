package providersync

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	linearWorkItemsDefaultPerPage = 50
	linearWorkItemsMaxPerPage     = 100
	linearWorkItemsDefaultPages   = 100
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
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

type linearWorkItemPayload struct {
	ID          string                 `json:"id"`
	Identifier  string                 `json:"identifier"`
	Title       string                 `json:"title"`
	Description *string                `json:"description"`
	Priority    *int                   `json:"priority"`
	Estimate    *float64               `json:"estimate"`
	CreatedAt   string                 `json:"createdAt"`
	UpdatedAt   string                 `json:"updatedAt"`
	StartedAt   *string                `json:"startedAt"`
	CompletedAt *string                `json:"completedAt"`
	CanceledAt  *string                `json:"canceledAt"`
	DueDate     *string                `json:"dueDate"`
	URL         *string                `json:"url"`
	ArchivedAt  *string                `json:"archivedAt"`
	State       *linearStatePayload    `json:"state"`
	Assignee    *linearIdentityPayload `json:"assignee"`
	Creator     *linearIdentityPayload `json:"creator"`
	Labels      linearLabelsPayload    `json:"labels"`
	Parent      *linearParentPayload   `json:"parent"`
	Project     *linearProjectPayload  `json:"project"`
	Cycle       *linearCyclePayload    `json:"cycle"`
	Team        *linearTeamPayload     `json:"team"`
	History     linearHistoryPayload   `json:"history"`
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
	ID     string `json:"id"`
	Number *int   `json:"number"`
	Name   string `json:"name"`
}

type linearTeamPayload struct {
	Key string `json:"key"`
}

type linearHistoryPayload struct {
	Nodes []linearHistoryEntry `json:"nodes"`
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

type linearWorkItemRows struct {
	WorkItems         []linearWorkItemRow
	StatusTransitions []linearWorkItemTransitionRow
}

// LinearWorkItemsRouteHandler is the provider-only canonical work-items
// vertical slice. It is intentionally not registered or activated here; the
// family planner and route gate remain separate work.
type LinearWorkItemsRouteHandler struct {
	PerPage  int
	MaxPages int
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
	filter := map[string]any{
		"team": map[string]any{"key": map[string]any{
			"in": []string{claim.SourceExternalID},
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
	page, err := providerfoundation.CollectLinearGraphQLPages(
		ctx, client, providerfoundation.LinearPageOptions{
			Query:          linearWorkItemsQuery,
			Variables:      map[string]any{"filter": filter},
			ConnectionPath: []string{"issues"}, PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if page.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	rows := linearWorkItemRows{
		WorkItems:         make([]linearWorkItemRow, 0, len(page.Items)),
		StatusTransitions: make([]linearWorkItemTransitionRow, 0),
	}
	for _, raw := range page.Items {
		var payload linearWorkItemPayload
		decoder := json.Unmarshal(raw, &payload)
		if decoder != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		if payload.ArchivedAt != nil {
			continue
		}
		item, transitions, normalizeErr := normalizeLinearWorkItem(
			claim, payload, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
	}
	effects, err := buildLinearWorkItemEffectsFromRows(rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt.UTC()
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"work_items_synced":  len(rows.WorkItems),
			"transitions_synced": len(rows.StatusTransitions),
		},
		Watermark: &watermark,
		Evidence: FetchEvidence{
			Provider: "linear", Dataset: "work-items", Requests: page.Pages,
			Pages: page.Pages, Records: len(rows.WorkItems) + len(rows.StatusTransitions),
		},
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
