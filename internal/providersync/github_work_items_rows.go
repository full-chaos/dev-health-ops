package providersync

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// These rows mirror the seven semantic lists emitted by Python's
// GitHubProvider._ingest_with_client. They deliberately model the provider
// batch, not ClickHouse's insert projection: the later effect layer owns the
// same sink coercions Python applies in ClickHouseStore.
type githubWorkItemRow struct {
	WorkItemID    string     `json:"work_item_id"`
	Provider      string     `json:"provider"`
	Title         string     `json:"title"`
	Type          string     `json:"type"`
	Status        string     `json:"status"`
	StatusRaw     *string    `json:"status_raw"`
	Description   *string    `json:"description"`
	RepoID        *uuid.UUID `json:"repo_id"`
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
}

type githubWorkItemTransitionRow struct {
	WorkItemID    string    `json:"work_item_id"`
	Provider      string    `json:"provider"`
	OccurredAt    time.Time `json:"occurred_at"`
	FromStatusRaw *string   `json:"from_status_raw"`
	ToStatusRaw   *string   `json:"to_status_raw"`
	FromStatus    string    `json:"from_status"`
	ToStatus      string    `json:"to_status"`
	Actor         *string   `json:"actor"`
	OrgID         string    `json:"org_id"`
}

type githubWorkItemDependencyRow struct {
	SourceWorkItemID             string     `json:"source_work_item_id"`
	TargetWorkItemID             string     `json:"target_work_item_id"`
	RelationshipType             string     `json:"relationship_type"`
	RelationshipTypeRaw          string     `json:"relationship_type_raw"`
	RelationshipSemanticsVersion string     `json:"relationship_semantics_version"`
	LastSynced                   time.Time  `json:"last_synced"`
	OrgID                        string     `json:"org_id"`
	SourceID                     *uuid.UUID `json:"source_id"`
}

type githubWorkItemReopenRow struct {
	WorkItemID    string    `json:"work_item_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	FromStatus    string    `json:"from_status"`
	ToStatus      string    `json:"to_status"`
	FromStatusRaw *string   `json:"from_status_raw"`
	ToStatusRaw   *string   `json:"to_status_raw"`
	Actor         *string   `json:"actor"`
	LastSynced    time.Time `json:"last_synced"`
	OrgID         string    `json:"org_id"`
}

type githubWorkItemInteractionRow struct {
	WorkItemID      string    `json:"work_item_id"`
	Provider        string    `json:"provider"`
	InteractionType string    `json:"interaction_type"`
	OccurredAt      time.Time `json:"occurred_at"`
	Actor           *string   `json:"actor"`
	BodyLength      int       `json:"body_length"`
	LastSynced      time.Time `json:"last_synced"`
	OrgID           string    `json:"org_id"`
}

type githubSprintRow struct {
	Provider      string     `json:"provider"`
	SprintID      string     `json:"sprint_id"`
	Name          *string    `json:"name"`
	State         *string    `json:"state"`
	StartedAt     *time.Time `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at"`
	CompletedAt   *time.Time `json:"completed_at"`
	NativeTeamKey *string    `json:"native_team_key"`
	LastSynced    time.Time  `json:"last_synced"`
	OrgID         string     `json:"org_id"`
}

type githubAIAttributionRow struct {
	RecordID     uuid.UUID      `json:"record_id"`
	OrgID        uuid.UUID      `json:"org_id"`
	Provider     string         `json:"provider"`
	SubjectType  string         `json:"subject_type"`
	SubjectID    string         `json:"subject_id"`
	RepoID       *uuid.UUID     `json:"repo_id"`
	Kind         string         `json:"kind"`
	Source       string         `json:"source"`
	Confidence   float64        `json:"confidence"`
	Actor        *string        `json:"actor"`
	Evidence     map[string]any `json:"evidence"`
	ObservedAt   time.Time      `json:"observed_at"`
	IngestedAt   time.Time      `json:"ingested_at"`
	SupersededBy *uuid.UUID     `json:"superseded_by"`
}

type githubWorkItemRows struct {
	WorkItems         []githubWorkItemRow
	StatusTransitions []githubWorkItemTransitionRow
	Dependencies      []githubWorkItemDependencyRow
	ReopenEvents      []githubWorkItemReopenRow
	Interactions      []githubWorkItemInteractionRow
	Sprints           []githubSprintRow
	AIAttributions    []githubAIAttributionRow
}

type githubWorkItemUserPayload struct {
	Email *string `json:"email"`
	Login *string `json:"login"`
	Name  *string `json:"name"`
}

type githubWorkItemLabelPayload struct {
	Name string `json:"name"`
}

type githubIssueWorkItemPayload struct {
	Number    int                          `json:"number"`
	Title     string                       `json:"title"`
	Body      *string                      `json:"body"`
	State     *string                      `json:"state"`
	CreatedAt *string                      `json:"created_at"`
	UpdatedAt *string                      `json:"updated_at"`
	ClosedAt  *string                      `json:"closed_at"`
	Labels    []githubWorkItemLabelPayload `json:"labels"`
	Assignees []githubWorkItemUserPayload  `json:"assignees"`
	User      *githubWorkItemUserPayload   `json:"user"`
	HTMLURL   *string                      `json:"html_url"`
	URL       *string                      `json:"url"`
}

type githubMilestonePayload struct {
	ID        any     `json:"id"`
	Number    any     `json:"number"`
	Title     *string `json:"title"`
	State     *string `json:"state"`
	CreatedAt *string `json:"created_at"`
	DueOn     *string `json:"due_on"`
}

type githubIdentityResolver func(githubWorkItemUserPayload) string

func normalizeGitHubIssueWorkItem(
	claim Claim,
	repoFullName string,
	repoID uuid.UUID,
	raw json.RawMessage,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) (githubWorkItemRow, error) {
	if claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || strings.TrimSpace(repoFullName) == "" ||
		repoID == uuid.Nil || normalizedAt.IsZero() {
		return githubWorkItemRow{}, ErrInvalidConfiguration
	}
	var issue githubIssueWorkItemPayload
	if json.Unmarshal(raw, &issue) != nil || issue.Number < 1 || strings.TrimSpace(issue.Title) == "" {
		return githubWorkItemRow{}, providerfoundation.ErrNormalizationInvalid
	}
	createdAt := parseGitHubWorkItemTime(issue.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitHubWorkItemTime(issue.UpdatedAt)
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := parseGitHubWorkItemTime(issue.ClosedAt)
	labels := make([]string, 0, len(issue.Labels))
	for _, label := range issue.Labels {
		if label.Name != "" {
			labels = append(labels, label.Name)
		}
	}
	state := ""
	if issue.State != nil {
		state = *issue.State
	}
	status := githubIssueStatus(state, labels)
	itemType := githubIssueType(labels)
	assignees := make([]string, 0, len(issue.Assignees))
	for _, assignee := range issue.Assignees {
		identity := resolveGitHubWorkItemIdentity(assignee, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if issue.User != nil {
		identity := resolveGitHubWorkItemIdentity(*issue.User, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	url := issue.HTMLURL
	if url == nil || *url == "" {
		url = issue.URL
	}
	description := issue.Body
	if description != nil && *description == "" {
		description = nil
	}
	projectID := repoFullName
	priority, serviceClass := githubPriorityFromLabels(labels)
	row := githubWorkItemRow{
		WorkItemID: "gh:" + repoFullName + "#" + strconv.Itoa(issue.Number),
		Provider:   "github", Title: issue.Title, Type: itemType, Status: status,
		StatusRaw: nullableString(state), Description: description, RepoID: &repoID,
		ProjectID: &projectID, Assignees: assignees, Reporter: reporter,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(), ClosedAt: closedAt,
		Labels: labels, URL: url, PriorityRaw: priority, ServiceClass: serviceClass,
		OrgID: claim.OrgID,
	}
	if closedAt != nil {
		copy := closedAt.UTC()
		row.CompletedAt = &copy
	}
	if err := row.validate(claim); err != nil {
		return githubWorkItemRow{}, err
	}
	return row, nil
}

func normalizeGitHubSprint(
	claim Claim,
	repoFullName string,
	raw json.RawMessage,
	normalizedAt time.Time,
) (githubSprintRow, error) {
	if claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || strings.TrimSpace(repoFullName) == "" ||
		normalizedAt.IsZero() {
		return githubSprintRow{}, ErrInvalidConfiguration
	}
	var milestone githubMilestonePayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&milestone) != nil {
		return githubSprintRow{}, providerfoundation.ErrNormalizationInvalid
	}
	id := stringValue(milestone.ID)
	if id == "" || id == "0" {
		id = stringValue(milestone.Number)
	}
	if id == "" || id == "0" {
		return githubSprintRow{}, providerfoundation.ErrNormalizationInvalid
	}
	createdAt := parseGitHubWorkItemTime(milestone.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	dueOn := parseGitHubWorkItemTime(milestone.DueOn)
	state := "active"
	if milestone.State != nil && *milestone.State == "closed" {
		state = "closed"
	}
	name := ""
	if milestone.Title != nil {
		name = *milestone.Title
	}
	row := githubSprintRow{
		Provider: "github", SprintID: "ghms:" + repoFullName + ":" + id,
		Name: &name, State: &state, StartedAt: createdAt, EndedAt: dueOn,
		LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
	}
	if state == "closed" {
		row.CompletedAt = dueOn
	}
	if err := row.validate(claim); err != nil {
		return githubSprintRow{}, err
	}
	return row, nil
}

func isWorkItemFamilyDataset(dataset string) bool {
	switch dataset {
	case "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments":
		return true
	default:
		return false
	}
}

func resolveGitHubWorkItemIdentity(user githubWorkItemUserPayload, resolver githubIdentityResolver) string {
	if resolver != nil {
		return resolver(user)
	}
	if user.Email != nil && strings.TrimSpace(*user.Email) != "" {
		return strings.ToLower(strings.TrimSpace(*user.Email))
	}
	if user.Login != nil && strings.TrimSpace(*user.Login) != "" {
		return "github:" + strings.TrimSpace(*user.Login)
	}
	if user.Name != nil {
		return strings.TrimSpace(*user.Name)
	}
	return "unknown"
}

func githubIssueStatus(state string, labels []string) string {
	priority := []string{"done", "canceled", "blocked", "in_review", "in_progress", "todo"}
	matches := map[string]bool{}
	for _, label := range labels {
		switch normalizeWorkItemLabel(label) {
		case "in progress", "doing", "wip":
			matches["in_progress"] = true
		case "in review", "needs review":
			matches["in_review"] = true
		case "blocked":
			matches["blocked"] = true
		case "todo", "to do", "ready":
			matches["todo"] = true
		case "done":
			matches["done"] = true
		case "wontfix", "duplicate":
			matches["canceled"] = true
		}
	}
	for _, status := range priority {
		if matches[status] {
			return status
		}
	}
	switch normalizeWorkItemLabel(state) {
	case "closed", "done", "merged":
		return "done"
	case "open", "opened":
		return "todo"
	default:
		return "unknown"
	}
}

func githubIssueType(labels []string) string {
	for _, wanted := range []string{"incident", "bug", "chore"} {
		for _, label := range labels {
			normalized := normalizeWorkItemLabel(label)
			if (wanted == "bug" && (normalized == "bug" || normalized == "type: bug")) ||
				(wanted == "incident" && normalized == "incident") ||
				(wanted == "chore" && (normalized == "chore" || normalized == "maintenance")) {
				return wanted
			}
		}
	}
	return "issue"
}

func githubPriorityFromLabels(labels []string) (*string, *string) {
	for _, label := range labels {
		switch normalizeWorkItemLabel(label) {
		case "priority::critical", "critical", "blocker", "urgent", "p0", "priority-critical", "critical-priority":
			return stringPointer("critical"), stringPointer("expedite")
		case "priority::high", "high", "p1", "priority-high", "high-priority":
			return stringPointer("high"), stringPointer("fixed_date")
		case "priority::medium", "medium", "p2", "priority-medium", "medium-priority":
			return stringPointer("medium"), stringPointer("standard")
		case "priority::low", "low", "p3", "p4", "priority-low", "low-priority":
			return stringPointer("low"), stringPointer("intangible")
		}
	}
	return nil, nil
}

func normalizeWorkItemLabel(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}

// parseGitHubWorkItemTime preserves the provider dataclass's full timestamp
// precision. The ClickHouse effect layer later owns DateTime64(3) coercion;
// reusing parseGitHubPullTime here would truncate before the semantic batch
// and diverge from Python's _to_utc normalization boundary.
func parseGitHubWorkItemTime(value *string) *time.Time {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*value))
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringPointer(value string) *string { return &value }

func (row githubWorkItemRow) validate(claim Claim) error {
	if row.Provider != "github" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.RepoID == nil || *row.RepoID == uuid.Nil ||
		row.Title == "" || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil || row.Labels == nil {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row githubSprintRow) validate(claim Claim) error {
	if row.Provider != "github" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.SprintID == "" || row.Name == nil || row.State == nil ||
		row.StartedAt == nil || row.StartedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}
