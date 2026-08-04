package providersync

import (
	"bytes"
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	Email   *string `json:"email"`
	Login   *string `json:"login"`
	Name    *string `json:"name"`
	Type    *string `json:"type"`
	AppSlug *string `json:"app_slug"`
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

type githubPullRequestWorkItemPayload struct {
	githubIssueWorkItemPayload
	Merged   bool    `json:"merged"`
	Draft    bool    `json:"draft"`
	MergedAt *string `json:"merged_at"`
	Head     struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

type githubWorkItemEventPayload struct {
	Event     string                      `json:"event"`
	CreatedAt *string                     `json:"created_at"`
	Actor     *githubWorkItemUserPayload  `json:"actor"`
	Label     *githubWorkItemLabelPayload `json:"label"`
}

type githubWorkItemCommentPayload struct {
	ID        any                        `json:"id"`
	Body      string                     `json:"body"`
	CreatedAt *string                    `json:"created_at"`
	User      *githubWorkItemUserPayload `json:"user"`
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

func normalizeGitHubIssueBundle(
	claim Claim,
	repoFullName string,
	repoID uuid.UUID,
	raw json.RawMessage,
	events []json.RawMessage,
	comments []json.RawMessage,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) (githubWorkItemRows, error) {
	item, err := normalizeGitHubIssueWorkItem(
		claim, repoFullName, repoID, raw, resolveIdentity, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	// The base row models the no-events fallback. The active Python producer
	// prefers the first terminal transition and uses closed_at only when no
	// such transition exists, so recompute the two derived timestamps here.
	item.StartedAt = nil
	item.CompletedAt = nil
	transitions, reopens, err := normalizeGitHubWorkItemEvents(
		claim, item.WorkItemID, false, events, item.CreatedAt,
		resolveIdentity, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	for _, transition := range transitions {
		if item.StartedAt == nil && transition.ToStatus == "in_progress" {
			started := transition.OccurredAt
			item.StartedAt = &started
		}
		if item.CompletedAt == nil &&
			(transition.ToStatus == "done" || transition.ToStatus == "canceled") {
			completed := transition.OccurredAt
			item.CompletedAt = &completed
			break
		}
	}
	if item.CompletedAt == nil && item.ClosedAt != nil {
		completed := item.ClosedAt.UTC()
		item.CompletedAt = &completed
	}
	interactions, err := normalizeGitHubWorkItemComments(
		claim, item.WorkItemID, comments, resolveIdentity, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	var issue githubIssueWorkItemPayload
	if json.Unmarshal(raw, &issue) != nil {
		return githubWorkItemRows{}, providerfoundation.ErrNormalizationInvalid
	}
	body := ""
	if issue.Body != nil {
		body = *issue.Body
	}
	dependencies, err := extractGitHubWorkItemDependencies(
		claim, item.WorkItemID, repoFullName, body, "", comments, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	return githubWorkItemRows{
		WorkItems: []githubWorkItemRow{item}, StatusTransitions: transitions,
		Dependencies: dependencies, ReopenEvents: reopens, Interactions: interactions,
		Sprints: []githubSprintRow{}, AIAttributions: []githubAIAttributionRow{},
	}, nil
}

func normalizeGitHubPullRequestBundle(
	claim Claim,
	repoFullName string,
	repoID uuid.UUID,
	raw json.RawMessage,
	events []json.RawMessage,
	comments []json.RawMessage,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) (githubWorkItemRows, error) {
	if claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || strings.TrimSpace(repoFullName) == "" ||
		repoID == uuid.Nil || normalizedAt.IsZero() {
		return githubWorkItemRows{}, ErrInvalidConfiguration
	}
	var pull githubPullRequestWorkItemPayload
	if json.Unmarshal(raw, &pull) != nil || pull.Number < 1 || strings.TrimSpace(pull.Title) == "" {
		return githubWorkItemRows{}, providerfoundation.ErrNormalizationInvalid
	}
	createdAt := parseGitHubWorkItemTime(pull.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitHubWorkItemTime(pull.UpdatedAt)
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := parseGitHubWorkItemTime(pull.ClosedAt)
	mergedAt := parseGitHubWorkItemTime(pull.MergedAt)
	labels := make([]string, 0, len(pull.Labels))
	for _, label := range pull.Labels {
		if label.Name != "" {
			labels = append(labels, label.Name)
		}
	}
	state := ""
	if pull.State != nil {
		state = *pull.State
	}
	statusRaw := state
	status := githubIssueStatus(state, labels)
	switch {
	case pull.Merged || mergedAt != nil:
		statusRaw, status = "merged", "done"
	case state == "closed":
		statusRaw, status = "closed", "canceled"
	case state == "open" && pull.Draft:
		statusRaw, status = "open", "todo"
	case state == "open":
		statusRaw, status = "open", "in_progress"
	}
	assignees := make([]string, 0, len(pull.Assignees))
	for _, assignee := range pull.Assignees {
		identity := resolveGitHubWorkItemIdentity(assignee, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if pull.User != nil {
		identity := resolveGitHubWorkItemIdentity(*pull.User, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	description := pull.Body
	if description != nil && *description == "" {
		description = nil
	}
	urlValue := pull.HTMLURL
	if urlValue == nil || *urlValue == "" {
		urlValue = pull.URL
	}
	projectID := repoFullName
	priority, serviceClass := githubPriorityFromLabels(labels)
	startedAt := createdAt.UTC()
	item := githubWorkItemRow{
		WorkItemID: "ghpr:" + repoFullName + "#" + strconv.Itoa(pull.Number),
		Provider:   "github", Title: pull.Title, Type: "pr", Status: status,
		StatusRaw: nullableString(statusRaw), Description: description, RepoID: &repoID,
		ProjectID: &projectID, Assignees: assignees, Reporter: reporter,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(), StartedAt: &startedAt,
		ClosedAt: closedAt, Labels: labels, URL: urlValue,
		PriorityRaw: priority, ServiceClass: serviceClass, OrgID: claim.OrgID,
	}
	if mergedAt != nil {
		completed := mergedAt.UTC()
		item.CompletedAt = &completed
	} else if closedAt != nil && (status == "done" || status == "canceled") {
		completed := closedAt.UTC()
		item.CompletedAt = &completed
	}
	if err := item.validate(claim); err != nil {
		return githubWorkItemRows{}, err
	}
	transitions, reopens, err := normalizeGitHubWorkItemEvents(
		claim, item.WorkItemID, true, events, item.CreatedAt,
		resolveIdentity, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	interactions, err := normalizeGitHubWorkItemComments(
		claim, item.WorkItemID, comments, resolveIdentity, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	body := ""
	if pull.Body != nil {
		body = *pull.Body
	}
	dependencies, err := extractGitHubWorkItemDependencies(
		claim, item.WorkItemID, repoFullName, body, pull.Head.Ref, comments, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	attributions, err := detectGitHubPullRequestAttributions(
		claim, repoID, pull, normalizedAt,
	)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	return githubWorkItemRows{
		WorkItems: []githubWorkItemRow{item}, StatusTransitions: transitions,
		Dependencies: dependencies, ReopenEvents: reopens, Interactions: interactions,
		Sprints: []githubSprintRow{}, AIAttributions: attributions,
	}, nil
}

func normalizeGitHubWorkItemEvents(
	claim Claim,
	workItemID string,
	pullRequest bool,
	rawEvents []json.RawMessage,
	createdAt time.Time,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) ([]githubWorkItemTransitionRow, []githubWorkItemReopenRow, error) {
	type eventWithIndex struct {
		payload githubWorkItemEventPayload
		index   int
	}
	events := make([]eventWithIndex, 0, len(rawEvents))
	for index, raw := range rawEvents {
		var event githubWorkItemEventPayload
		if json.Unmarshal(raw, &event) != nil {
			return nil, nil, providerfoundation.ErrNormalizationInvalid
		}
		events = append(events, eventWithIndex{payload: event, index: index})
	}
	sort.SliceStable(events, func(left, right int) bool {
		leftAt := parseGitHubWorkItemTime(events[left].payload.CreatedAt)
		rightAt := parseGitHubWorkItemTime(events[right].payload.CreatedAt)
		switch {
		case leftAt == nil && rightAt == nil:
			return events[left].index < events[right].index
		case leftAt == nil:
			return true
		case rightAt == nil:
			return false
		default:
			return leftAt.Before(*rightAt)
		}
	})
	previous := "unknown"
	if pullRequest {
		previous = "in_progress"
	}
	mergedInHistory := false
	transitions := make([]githubWorkItemTransitionRow, 0, len(events))
	reopens := make([]githubWorkItemReopenRow, 0)
	for _, wrapped := range events {
		event := wrapped.payload
		eventType := strings.ToLower(strings.TrimSpace(event.Event))
		occurredAt := parseGitHubWorkItemTime(event.CreatedAt)
		transitionAt := createdAt.UTC()
		if occurredAt != nil {
			transitionAt = occurredAt.UTC()
		}
		toStatus, toRaw := "", ""
		if pullRequest {
			switch eventType {
			case "merged":
				mergedInHistory, toStatus, toRaw = true, "done", "merged"
			case "closed":
				toStatus, toRaw = "canceled", "closed"
				if mergedInHistory {
					toStatus = "done"
				}
			case "reopened":
				toStatus, toRaw = "in_progress", "reopened"
			}
		} else {
			switch eventType {
			case "closed":
				toStatus, toRaw = "done", "closed"
			case "reopened":
				toStatus, toRaw = "todo", "reopened"
			case "labeled":
				if event.Label != nil {
					mapped := githubIssueStatus("", []string{event.Label.Name})
					if mapped != "unknown" {
						toStatus, toRaw = mapped, event.Label.Name
					}
				}
			}
		}
		if toStatus != "" {
			transition := githubWorkItemTransitionRow{
				WorkItemID: workItemID, Provider: "github", OccurredAt: transitionAt,
				ToStatusRaw: nullableString(toRaw), FromStatus: previous,
				ToStatus: toStatus, OrgID: claim.OrgID,
			}
			if err := transition.validate(claim); err != nil {
				return nil, nil, err
			}
			transitions = append(transitions, transition)
			previous = toStatus
		}
		if eventType != "reopened" || occurredAt == nil {
			continue
		}
		var actor *string
		if event.Actor != nil {
			identity := resolveGitHubWorkItemIdentity(*event.Actor, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		reopen := githubWorkItemReopenRow{
			WorkItemID: workItemID, OccurredAt: occurredAt.UTC(), FromStatus: "done",
			ToStatus: "todo", FromStatusRaw: stringPointer("closed"),
			ToStatusRaw: stringPointer("reopened"), Actor: actor,
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		}
		if err := reopen.validate(claim); err != nil {
			return nil, nil, err
		}
		reopens = append(reopens, reopen)
	}
	return transitions, reopens, nil
}

func normalizeGitHubWorkItemComments(
	claim Claim,
	workItemID string,
	rawComments []json.RawMessage,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) ([]githubWorkItemInteractionRow, error) {
	rows := make([]githubWorkItemInteractionRow, 0, len(rawComments))
	for _, raw := range rawComments {
		var comment githubWorkItemCommentPayload
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&comment) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		id := stringValue(comment.ID)
		occurredAt := parseGitHubWorkItemTime(comment.CreatedAt)
		if id == "" || id == "0" || occurredAt == nil {
			continue
		}
		var actor *string
		if comment.User != nil {
			identity := resolveGitHubWorkItemIdentity(*comment.User, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		row := githubWorkItemInteractionRow{
			WorkItemID: workItemID, Provider: "github", InteractionType: "comment",
			OccurredAt: occurredAt.UTC(), Actor: actor,
			BodyLength: utf8.RuneCountInString(comment.Body),
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		}
		if err := row.validate(claim); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

var (
	githubIssueReferencePattern = regexp.MustCompile(
		`(?im)(?:^|[\t ])(depends\s+on|blocked\s+by|blocks|fixes|closes|resolves)\s*:?\s*(?:#([0-9]+)|(?:(?:https?://)?github\.com/)?([a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+)#([0-9]+))`,
	)
	githubExternalBodyPattern = regexp.MustCompile(
		`(?i)(depends\s+on|blocked\s+by|blocks|fixes|closes|resolves|relates\s+to|part\s+of|see)\s*:?\s*([A-Za-z]{2,}-[0-9]+)\b`,
	)
	githubExternalBranchPattern = regexp.MustCompile(`(?i)([A-Za-z]{2,}-[0-9]+)`)
	githubLinearCommentPattern  = regexp.MustCompile(
		`(?i)linear\.app/[^/\s]+/issue/([A-Za-z][A-Za-z0-9]+-[0-9]+)`,
	)
)

func extractGitHubWorkItemDependencies(
	claim Claim,
	workItemID string,
	repoFullName string,
	body string,
	branch string,
	rawComments []json.RawMessage,
	normalizedAt time.Time,
) ([]githubWorkItemDependencyRow, error) {
	rows := make([]githubWorkItemDependencyRow, 0)
	appendRow := func(source, target, relationship, raw string) error {
		row := githubWorkItemDependencyRow{
			SourceWorkItemID: source, TargetWorkItemID: target,
			RelationshipType: relationship, RelationshipTypeRaw: raw,
			RelationshipSemanticsVersion: "canonical-blocks.v2",
			LastSynced:                   normalizedAt.UTC(), OrgID: claim.OrgID,
		}
		if err := row.validate(claim); err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	}
	for _, match := range githubIssueReferencePattern.FindAllStringSubmatch(body, -1) {
		keyword := strings.ToLower(strings.TrimSpace(match[1]))
		targetID := ""
		if match[2] != "" {
			targetID = "gh:" + repoFullName + "#" + match[2]
		} else if match[3] != "" && match[4] != "" {
			targetID = "gh:" + match[3] + "#" + match[4]
		}
		if targetID == "" {
			continue
		}
		relationship, reverse := "relates_to", false
		switch keyword {
		case "depends on", "blocked by":
			relationship, reverse = "blocks", true
		case "blocks":
			relationship = "blocks"
		}
		source, target := workItemID, targetID
		if reverse {
			source, target = targetID, workItemID
		}
		if err := appendRow(source, target, relationship, strings.ToLower(strings.TrimSpace(match[0]))); err != nil {
			return nil, err
		}
	}
	seenExternal := map[string]struct{}{}
	appendExternal := func(key, relationship, raw string) error {
		key = strings.ToUpper(strings.TrimSpace(key))
		if key == "" {
			return nil
		}
		if _, exists := seenExternal[key]; exists {
			return nil
		}
		seenExternal[key] = struct{}{}
		return appendRow(workItemID, "extkey:"+key, relationship, raw)
	}
	for _, match := range githubExternalBodyPattern.FindAllStringSubmatch(body, -1) {
		if err := appendExternal(match[2], githubExternalRelationship(match[1]), "external_issue_key"); err != nil {
			return nil, err
		}
	}
	for _, match := range githubExternalBranchPattern.FindAllStringSubmatch(branch, -1) {
		if err := appendExternal(match[1], "external_issue_key", "external_issue_key"); err != nil {
			return nil, err
		}
	}
	trustedBots := githubLinearLinkbackBots()
	type commentRelationship struct{ relationship, raw string }
	best := map[string]commentRelationship{}
	rank := map[string]int{"blocked_by": 3, "blocks": 2, "relates_to": 1}
	for _, raw := range rawComments {
		var comment githubWorkItemCommentPayload
		if json.Unmarshal(raw, &comment) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		login := ""
		if comment.User != nil && comment.User.Login != nil {
			login = strings.ToLower(strings.TrimSpace(*comment.User.Login))
		}
		if comment.Body == "" {
			continue
		}
		if _, trusted := trustedBots[login]; !trusted {
			continue
		}
		for _, indexes := range githubLinearCommentPattern.FindAllStringSubmatchIndex(comment.Body, -1) {
			key := strings.ToUpper(comment.Body[indexes[2]:indexes[3]])
			start := indexes[0] - 40
			if start < 0 {
				start = 0
			}
			preceding := strings.ToLower(comment.Body[start:indexes[0]])
			relationship := "relates_to"
			if strings.Contains(preceding, "blocked by") || strings.Contains(preceding, "depends on") {
				relationship = "blocked_by"
			} else if strings.Contains(preceding, "blocks") {
				relationship = "blocks"
			}
			current, exists := best[key]
			if !exists || rank[relationship] > rank[current.relationship] {
				best[key] = commentRelationship{relationship: relationship, raw: "github_comment_linear_url"}
			}
		}
	}
	commentKeys := make([]string, 0, len(best))
	for key := range best {
		commentKeys = append(commentKeys, key)
	}
	// Python preserves insertion order. Sorting is the deterministic equivalent
	// for provider payloads whose comment ordering can otherwise vary by page.
	sort.Strings(commentKeys)
	for _, key := range commentKeys {
		value := best[key]
		if err := appendExternal(key, value.relationship, value.raw); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func githubExternalRelationship(keyword string) string {
	switch strings.ToLower(strings.TrimSpace(keyword)) {
	case "blocks":
		return "blocks"
	case "blocked by", "depends on":
		return "blocked_by"
	default:
		return "relates_to"
	}
}

func githubLinearLinkbackBots() map[string]struct{} {
	configured := os.Getenv("GITHUB_LINEAR_LINKBACK_BOTS")
	if configured == "" {
		configured = "linear[bot]"
	}
	result := map[string]struct{}{}
	for _, value := range strings.Split(configured, ",") {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			result[value] = struct{}{}
		}
	}
	return result
}

type githubAIAttributionSignal struct {
	Kind       string
	Source     string
	Confidence float64
	Actor      *string
	Evidence   map[string]any
}

type githubAIPattern struct {
	pattern *regexp.Regexp
	raw     string
	kind    string
	actor   string
}

var githubAIBranchPatterns = []githubAIPattern{
	newGitHubAIPattern(`(?:^|[-/])copilot(?:[-/]|$)`, "ai_assisted", "copilot"),
	newGitHubAIPattern(`(?:^|[-/])claude(?:[-/]|$)`, "ai_assisted", "claude"),
	newGitHubAIPattern(`(?:^|[-/])cursor(?:[-/]|$)`, "ai_assisted", "cursor"),
	newGitHubAIPattern(`(?:^|[-/])codex(?:[-/]|$)`, "ai_assisted", "codex"),
	newGitHubAIPattern(`(?:^|[-/])windsurf(?:[-/]|$)`, "ai_assisted", "windsurf"),
	newGitHubAIPattern(`(?:^|[-/])devin(?:[-/]|$)`, "agent_created", "devin"),
	newGitHubAIPattern(`(?:^|[-/])agent(?:[-/]|$)`, "agent_created", "agent"),
	newGitHubAIPattern(`(?:^|[-/])ai(?:[-/]|$)`, "ai_assisted", "ai"),
}

var githubAIBodyPatterns = []githubAIPattern{
	newGitHubAIPattern(`\b(?:generated|created|authored|written)\s+(?:by|with|using)\s+(?:copilot|claude|codex|cursor|windsurf|ai|an\s+ai)\b`, "ai_assisted", ""),
	newGitHubAIPattern(`\bai[\s-]assisted\b`, "ai_assisted", ""),
	newGitHubAIPattern(`\bagent[\s-]created\b`, "agent_created", ""),
	newGitHubAIPattern(`\bcopilot\b`, "ai_assisted", "copilot"),
	newGitHubAIPattern(`\bclaude\b`, "ai_assisted", "claude"),
	newGitHubAIPattern(`\bcodex\b`, "ai_assisted", "codex"),
	newGitHubAIPattern(`\bcursor\b`, "ai_assisted", "cursor"),
}

func newGitHubAIPattern(pattern, kind, actor string) githubAIPattern {
	return githubAIPattern{pattern: regexp.MustCompile(`(?i)` + pattern), raw: pattern, kind: kind, actor: actor}
}

func detectGitHubPullRequestAttributions(
	claim Claim,
	repoID uuid.UUID,
	pull githubPullRequestWorkItemPayload,
	normalizedAt time.Time,
) ([]githubAIAttributionRow, error) {
	signals := make([]githubAIAttributionSignal, 0)
	labelKinds := map[string]string{
		"ai-assisted": "ai_assisted", "agent-created": "agent_created",
		"ai-review": "ai_review", "copilot": "ai_assisted",
		"claude-code": "ai_assisted", "codex": "ai_assisted",
		"cursor": "ai_assisted", "windsurf": "ai_assisted",
	}
	for _, label := range pull.Labels {
		normalized := strings.ToLower(strings.TrimSpace(label.Name))
		if kind, ok := labelKinds[normalized]; ok {
			signals = append(signals, githubAIAttributionSignal{
				Kind: kind, Source: "pr_label", Confidence: 0.95,
				Evidence: map[string]any{"label": label.Name},
			})
		}
	}
	if pull.User != nil {
		login, userType, appSlug := "", "", ""
		if pull.User.Login != nil {
			login = *pull.User.Login
		}
		if pull.User.Type != nil {
			userType = *pull.User.Type
		}
		if pull.User.AppSlug != nil {
			appSlug = *pull.User.AppSlug
		}
		if signal := detectGitHubAIAuthor(login, userType, appSlug); signal != nil {
			signals = append(signals, *signal)
		}
	}
	body := ""
	if pull.Body != nil {
		body = *pull.Body
	}
	trailerFired := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		colon := strings.Index(line, ":")
		if colon <= 0 {
			continue
		}
		key, value := strings.ToLower(strings.TrimSpace(line[:colon])), strings.TrimSpace(line[colon+1:])
		if key == "ai-assisted-by" || key == "generated-by" || key == "x-ai-generated" {
			actor := nullableString(value)
			signals = append(signals, githubAIAttributionSignal{
				Kind: "ai_assisted", Source: "commit_trailer", Confidence: 0.85,
				Actor: actor, Evidence: map[string]any{
					"trailer_key": strings.TrimSpace(line[:colon]), "trailer_value": value,
				},
			})
			trailerFired = true
			continue
		}
		if key == "co-authored-by" && githubAICoauthorPattern.MatchString(value) {
			actor := nullableString(value)
			signals = append(signals, githubAIAttributionSignal{
				Kind: "ai_assisted", Source: "commit_trailer", Confidence: 0.80,
				Actor: actor, Evidence: map[string]any{
					"trailer_key": "Co-authored-by", "trailer_value": value,
				},
			})
			trailerFired = true
		}
	}
	if signal := detectGitHubAITextPattern(pull.Head.Ref, githubAIBranchPatterns, "branch_name", 0.35, "branch"); signal != nil {
		signals = append(signals, *signal)
	}
	if !trailerFired {
		if signal := detectGitHubAITextPattern(body, githubAIBodyPatterns, "pr_body", 0.25, "matched_text"); signal != nil {
			signals = append(signals, *signal)
		}
	}
	if len(signals) == 0 {
		return []githubAIAttributionRow{}, nil
	}
	orgID, err := uuid.Parse(claim.OrgID)
	if err != nil {
		return nil, providerfoundation.ErrInvalidScope
	}
	observedAt := parseGitHubWorkItemTime(pull.CreatedAt)
	if observedAt == nil {
		fallback := normalizedAt.UTC()
		observedAt = &fallback
	}
	rows := make([]githubAIAttributionRow, 0, len(signals))
	for index, signal := range signals {
		evidence, marshalErr := json.Marshal(signal.Evidence)
		if marshalErr != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		subjectID := strconv.Itoa(pull.Number)
		identity := strings.Join([]string{
			claim.OrgID, repoID.String(), subjectID, signal.Source,
			strconv.Itoa(index), string(evidence),
		}, "|")
		row := githubAIAttributionRow{
			RecordID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)),
			OrgID:    orgID, Provider: "github", SubjectType: "pull_request",
			SubjectID: subjectID, RepoID: &repoID, Kind: signal.Kind,
			Source: signal.Source, Confidence: signal.Confidence, Actor: signal.Actor,
			Evidence: signal.Evidence, ObservedAt: observedAt.UTC(),
			IngestedAt: normalizedAt.UTC(),
		}
		if err := row.validate(claim); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

var githubAICoauthorPattern = regexp.MustCompile(
	`(?i)(copilot@github\.com|noreply\+copilot@github\.com|claude.*@anthropic\.com|<[\w.+-]*copilot[\w.+-]*@|<[\w.+-]*claude[\w.+-]*@|cursor-agent|chatgpt-codex|sweep-ai|devin@)`,
)

func detectGitHubAIAuthor(login, userType, appSlug string) *githubAIAttributionSignal {
	lower := strings.ToLower(strings.TrimSpace(login))
	ciBots := map[string]bool{
		"github-actions[bot]": true, "dependabot[bot]": true, "renovate[bot]": true,
	}
	if ciBots[lower] {
		return nil
	}
	known := map[string]bool{
		"copilot[bot]": true, "claude-code[bot]": true, "cursor-agent[bot]": true,
		"chatgpt-codex[bot]": true, "sweep-ai[bot]": true,
		"coderabbit[bot]": true, "devin[bot]": true,
	}
	confidence := 0.0
	knownBot := false
	if known[lower] {
		confidence, knownBot = 0.90, true
	} else if strings.EqualFold(userType, "bot") && strings.HasSuffix(lower, "[bot]") {
		confidence = 0.55
	}
	if confidence == 0 {
		return nil
	}
	actor := login
	return &githubAIAttributionSignal{
		Kind: "agent_created", Source: "bot_author", Confidence: confidence,
		Actor: &actor, Evidence: map[string]any{
			"login": login, "user_type": nullableString(userType),
			"app_slug": nullableString(appSlug), "known_ai_bot": knownBot,
		},
	}
}

func detectGitHubAITextPattern(
	value string,
	patterns []githubAIPattern,
	source string,
	confidence float64,
	textKey string,
) *githubAIAttributionSignal {
	for _, candidate := range patterns {
		indexes := candidate.pattern.FindStringIndex(value)
		if indexes == nil {
			continue
		}
		var actor *string
		if candidate.actor != "" {
			actorValue := candidate.actor
			actor = &actorValue
		}
		evidence := map[string]any{"matched_pattern": candidate.raw}
		if source == "branch_name" {
			evidence["branch"] = value
		} else {
			evidence[textKey] = value[indexes[0]:indexes[1]]
		}
		return &githubAIAttributionSignal{
			Kind: candidate.kind, Source: source, Confidence: confidence,
			Actor: actor, Evidence: evidence,
		}
	}
	return nil
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

func (row githubWorkItemTransitionRow) validate(claim Claim) error {
	if row.WorkItemID == "" || row.Provider != "github" || row.OccurredAt.IsZero() ||
		row.FromStatus == "" || row.ToStatus == "" || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row githubWorkItemDependencyRow) validate(claim Claim) error {
	if row.SourceWorkItemID == "" || row.TargetWorkItemID == "" ||
		row.RelationshipType == "" || row.RelationshipTypeRaw == "" ||
		row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
		row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row githubWorkItemReopenRow) validate(claim Claim) error {
	if row.WorkItemID == "" || row.OccurredAt.IsZero() || row.FromStatus == "" ||
		row.ToStatus == "" || row.LastSynced.IsZero() ||
		row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row githubWorkItemInteractionRow) validate(claim Claim) error {
	if row.WorkItemID == "" || row.Provider != "github" ||
		row.InteractionType != "comment" || row.OccurredAt.IsZero() ||
		row.BodyLength < 0 || row.LastSynced.IsZero() ||
		row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (row githubAIAttributionRow) validate(claim Claim) error {
	if row.RecordID == uuid.Nil || row.OrgID == uuid.Nil || row.OrgID.String() != claim.OrgID ||
		row.Provider != "github" || row.SubjectType != "pull_request" || row.SubjectID == "" ||
		row.RepoID == nil || *row.RepoID == uuid.Nil || row.Kind == "" || row.Source == "" ||
		row.Confidence < 0 || row.Confidence > 1 || row.Evidence == nil ||
		row.ObservedAt.IsZero() || row.IngestedAt.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}
