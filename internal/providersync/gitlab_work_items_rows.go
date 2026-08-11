package providersync

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// GitLab's work-item provider batch owns the six raw fact projections below.
// The row shapes intentionally reuse the direct work-graph projection already
// proven against ClickHouse for GitHub.  Provider-specific collection and
// normalization remain here; the shared sink projection prevents a second
// implementation of column order, null coercion, and FINAL readback.
type gitlabWorkItemRow = githubWorkItemRow
type gitlabWorkItemTransitionRow = githubWorkItemTransitionRow
type gitlabWorkItemDependencyRow = githubWorkItemDependencyRow
type gitlabWorkItemReopenRow = githubWorkItemReopenRow
type gitlabWorkItemInteractionRow = githubWorkItemInteractionRow
type gitlabSprintRow = githubSprintRow
type gitlabAIAttributionRow = githubAIAttributionRow

type gitlabWorkItemRows struct {
	WorkItems         []gitlabWorkItemRow
	StatusTransitions []gitlabWorkItemTransitionRow
	Dependencies      []gitlabWorkItemDependencyRow
	ReopenEvents      []gitlabWorkItemReopenRow
	Interactions      []gitlabWorkItemInteractionRow
	Sprints           []gitlabSprintRow
	AIAttributions    []gitlabAIAttributionRow
}

type gitlabWorkItemUserPayload struct {
	Email    *string `json:"email"`
	Username *string `json:"username"`
	Name     *string `json:"name"`
	Bot      bool    `json:"bot"`
}

type gitlabIssueMilestonePayload struct {
	ID        json.Number `json:"id"`
	Title     string      `json:"title"`
	State     string      `json:"state"`
	StartDate *string     `json:"start_date"`
	DueDate   *string     `json:"due_date"`
}

type gitlabIssueWorkItemPayload struct {
	IID         int                          `json:"iid"`
	Title       string                       `json:"title"`
	Description *string                      `json:"description"`
	State       string                       `json:"state"`
	CreatedAt   *string                      `json:"created_at"`
	UpdatedAt   *string                      `json:"updated_at"`
	ClosedAt    *string                      `json:"closed_at"`
	Labels      []string                     `json:"labels"`
	Assignees   []gitlabWorkItemUserPayload  `json:"assignees"`
	Author      *gitlabWorkItemUserPayload   `json:"author"`
	WebURL      *string                      `json:"web_url"`
	URL         *string                      `json:"url"`
	Weight      *float64                     `json:"weight"`
	Milestone   *gitlabIssueMilestonePayload `json:"milestone"`
}

type gitlabMergeRequestWorkItemPayload struct {
	IID          int                          `json:"iid"`
	Title        string                       `json:"title"`
	Description  *string                      `json:"description"`
	State        string                       `json:"state"`
	CreatedAt    *string                      `json:"created_at"`
	UpdatedAt    *string                      `json:"updated_at"`
	ClosedAt     *string                      `json:"closed_at"`
	MergedAt     *string                      `json:"merged_at"`
	Labels       []string                     `json:"labels"`
	Assignees    []gitlabWorkItemUserPayload  `json:"assignees"`
	Author       *gitlabWorkItemUserPayload   `json:"author"`
	WebURL       *string                      `json:"web_url"`
	URL          *string                      `json:"url"`
	Weight       *float64                     `json:"weight"`
	Milestone    *gitlabIssueMilestonePayload `json:"milestone"`
	SourceBranch string                       `json:"source_branch"`
}

type gitlabLabelEventPayload struct {
	Action    string  `json:"action"`
	CreatedAt *string `json:"created_at"`
	Label     struct {
		Name string `json:"name"`
	} `json:"label"`
	LabelName string `json:"label_name"`
}

type gitlabStateEventPayload struct {
	State     string                     `json:"state"`
	CreatedAt *string                    `json:"created_at"`
	User      *gitlabWorkItemUserPayload `json:"user"`
}

type gitlabNotePayload struct {
	System    bool                       `json:"system"`
	Body      string                     `json:"body"`
	CreatedAt *string                    `json:"created_at"`
	Author    *gitlabWorkItemUserPayload `json:"author"`
}

type gitlabIssueLinkPayload struct {
	LinkType   string      `json:"link_type"`
	IID        json.Number `json:"iid"`
	References struct {
		Full string `json:"full"`
	} `json:"references"`
}

type gitlabIdentityResolver func(gitlabWorkItemUserPayload) string

func resolveGitLabWorkItemIdentity(
	user gitlabWorkItemUserPayload,
	resolver gitlabIdentityResolver,
) string {
	if resolver != nil {
		return resolver(user)
	}
	if user.Email != nil && strings.TrimSpace(*user.Email) != "" {
		return strings.ToLower(strings.TrimSpace(*user.Email))
	}
	if user.Username != nil && strings.TrimSpace(*user.Username) != "" {
		return "gitlab:" + strings.TrimSpace(*user.Username)
	}
	if user.Name != nil && strings.TrimSpace(*user.Name) != "" {
		return strings.TrimSpace(*user.Name)
	}
	return "unknown"
}

func parseGitLabWorkItemTime(value *string) *time.Time {
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

func gitlabWorkItemLabels(labels []string) []string {
	result := make([]string, 0, len(labels))
	for _, label := range labels {
		// Python's `[str(lbl) for lbl in labels if lbl]` keeps whitespace and
		// preserves provider order; only an actually empty value is omitted.
		if label != "" {
			result = append(result, label)
		}
	}
	return result
}

func normalizeGitLabIssueWorkItem(
	claim Claim,
	fullName string,
	repoID uuid.UUID,
	payload gitlabIssueWorkItemPayload,
	labelEvents []gitlabLabelEventPayload,
	statusMapping *StatusMapping,
	resolveIdentity gitlabIdentityResolver,
	normalizedAt time.Time,
) (gitlabWorkItemRow, []gitlabWorkItemTransitionRow, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		strings.TrimSpace(fullName) == "" || repoID == uuid.Nil || payload.IID < 1 ||
		strings.TrimSpace(payload.Title) == "" || statusMapping == nil || normalizedAt.IsZero() {
		return gitlabWorkItemRow{}, nil, ErrInvalidConfiguration
	}
	createdAt := parseGitLabWorkItemTime(payload.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitLabWorkItemTime(payload.UpdatedAt)
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := parseGitLabWorkItemTime(payload.ClosedAt)
	labels := gitlabWorkItemLabels(payload.Labels)
	status := statusMapping.NormalizeStatus("gitlab", "", labels, payload.State)
	typeValue := statusMapping.NormalizeType("gitlab", "", labels)
	assignees := make([]string, 0, len(payload.Assignees))
	for _, user := range payload.Assignees {
		identity := resolveGitLabWorkItemIdentity(user, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if payload.Author != nil {
		identity := resolveGitLabWorkItemIdentity(*payload.Author, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	urlValue := payload.WebURL
	if urlValue == nil || *urlValue == "" {
		urlValue = payload.URL
	}
	description := payload.Description
	if description != nil && *description == "" {
		description = nil
	}
	var milestoneID, milestoneName *string
	if payload.Milestone != nil {
		if id := payload.Milestone.ID.String(); id != "" && id != "0" {
			milestoneID = &id
		}
		name := payload.Milestone.Title
		milestoneName = &name
	}
	row := gitlabWorkItemRow{
		WorkItemID: "gitlab:" + fullName + "#" + strconv.Itoa(payload.IID),
		Provider:   "gitlab", Title: payload.Title, Type: typeValue, Status: status,
		StatusRaw: nullableString(payload.State), Description: description, RepoID: &repoID,
		ProjectID: stringPointer(fullName), Assignees: assignees, Reporter: reporter,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(), ClosedAt: closedAt,
		Labels: labels, StoryPoints: payload.Weight, SprintID: milestoneID,
		SprintName: milestoneName, URL: urlValue,
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	transitions := normalizeGitLabIssueLabelTransitions(
		claim, row.WorkItemID, labelEvents, createdAt.UTC(), statusMapping, normalizedAt,
	)
	row.StartedAt, row.CompletedAt = gitlabWorkItemLifecycle(transitions, row.ClosedAt)
	if err := validateGitLabWorkItemRow(row, claim); err != nil {
		return gitlabWorkItemRow{}, nil, err
	}
	return row, transitions, nil
}

func normalizeGitLabIssueLabelTransitions(
	claim Claim,
	workItemID string,
	events []gitlabLabelEventPayload,
	createdAt time.Time,
	statusMapping *StatusMapping,
	normalizedAt time.Time,
) []gitlabWorkItemTransitionRow {
	sorted := append([]gitlabLabelEventPayload(nil), events...)
	sort.SliceStable(sorted, func(left, right int) bool {
		return gitlabEventTime(sorted[left].CreatedAt).Before(gitlabEventTime(sorted[right].CreatedAt))
	})
	transitions := make([]gitlabWorkItemTransitionRow, 0, len(sorted))
	previous := "unknown"
	for _, event := range sorted {
		action := strings.ToLower(strings.TrimSpace(event.Action))
		if action != "add" && action != "remove" {
			continue
		}
		labelName := event.Label.Name
		if labelName == "" {
			labelName = event.LabelName
		}
		if labelName == "" {
			continue
		}
		labels := []string{}
		if action == "add" {
			labels = []string{labelName}
		}
		mapped := statusMapping.NormalizeStatus("gitlab", "", labels, "")
		if mapped == "unknown" {
			continue
		}
		occurred := parseGitLabWorkItemTime(event.CreatedAt)
		if occurred == nil {
			occurred = &createdAt
		}
		row := gitlabWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "gitlab", OccurredAt: occurred.UTC(),
			FromStatusRaw: nil, ToStatusRaw: stringPointer(labelName), FromStatus: previous,
			ToStatus: mapped, OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
		}
		if validateGitLabTransitionRow(row, claim) == nil {
			transitions = append(transitions, row)
		}
		previous = mapped
	}
	return transitions
}

func normalizeGitLabMergeRequestWorkItem(
	claim Claim,
	fullName string,
	repoID uuid.UUID,
	payload gitlabMergeRequestWorkItemPayload,
	stateEvents []gitlabStateEventPayload,
	resolveIdentity gitlabIdentityResolver,
	normalizedAt time.Time,
) (gitlabWorkItemRow, []gitlabWorkItemTransitionRow, []gitlabWorkItemReopenRow, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		strings.TrimSpace(fullName) == "" || repoID == uuid.Nil || payload.IID < 1 ||
		strings.TrimSpace(payload.Title) == "" || normalizedAt.IsZero() {
		return gitlabWorkItemRow{}, nil, nil, ErrInvalidConfiguration
	}
	createdAt := parseGitLabWorkItemTime(payload.CreatedAt)
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitLabWorkItemTime(payload.UpdatedAt)
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	mergedAt := parseGitLabWorkItemTime(payload.MergedAt)
	closedAt := parseGitLabWorkItemTime(payload.ClosedAt)
	labels := gitlabWorkItemLabels(payload.Labels)
	statusRaw := payload.State
	status := "unknown"
	switch statusRaw {
	case "merged":
		status = "done"
	case "closed":
		status = "canceled"
	case "opened":
		status = "in_progress"
	}
	assignees := make([]string, 0, len(payload.Assignees))
	for _, user := range payload.Assignees {
		identity := resolveGitLabWorkItemIdentity(user, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if payload.Author != nil {
		identity := resolveGitLabWorkItemIdentity(*payload.Author, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	urlValue := payload.WebURL
	if urlValue == nil || *urlValue == "" {
		urlValue = payload.URL
	}
	description := payload.Description
	if description != nil && *description == "" {
		description = nil
	}
	var milestoneID, milestoneName *string
	if payload.Milestone != nil {
		if id := payload.Milestone.ID.String(); id != "" && id != "0" {
			milestoneID = &id
		}
		name := payload.Milestone.Title
		milestoneName = &name
	}
	priority, serviceClass := gitlabPriorityFromLabels(labels)
	completedAt := mergedAt
	if completedAt == nil {
		completedAt = closedAt
	}
	closedValue := closedAt
	if closedValue == nil {
		closedValue = mergedAt
	}
	row := gitlabWorkItemRow{
		WorkItemID: "gitlab:" + fullName + "!" + strconv.Itoa(payload.IID),
		Provider:   "gitlab", Title: payload.Title, Type: "merge_request", Status: status,
		StatusRaw: nullableString(statusRaw), Description: description, RepoID: &repoID,
		ProjectID: stringPointer(fullName), Assignees: assignees, Reporter: reporter,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(), StartedAt: createdAt,
		CompletedAt: completedAt, ClosedAt: closedValue, Labels: labels,
		StoryPoints: payload.Weight, SprintID: milestoneID, SprintName: milestoneName,
		URL: urlValue, PriorityRaw: priority, ServiceClass: serviceClass,
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	transitions, reopens := normalizeGitLabMRStateEvents(
		claim, row.WorkItemID, stateEvents, createdAt.UTC(), resolveIdentity, normalizedAt,
	)
	if err := validateGitLabWorkItemRow(row, claim); err != nil {
		return gitlabWorkItemRow{}, nil, nil, err
	}
	return row, transitions, reopens, nil
}

// normalizeGitLabMRAIAttributions mirrors gitlab_mr_ai_attributions at the
// same provider-normalization boundary that still owns the original merge
// request payload. The six raw ClickHouse facts deliberately do not retain
// source_branch or author.bot, so postponing this producer until the derived
// compute boundary would lose authoritative signals.
func normalizeGitLabMRAIAttributions(
	claim Claim,
	repoID uuid.UUID,
	payload gitlabMergeRequestWorkItemPayload,
	normalizedAt time.Time,
) ([]gitlabAIAttributionRow, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "work-items" || repoID == uuid.Nil || payload.IID < 1 ||
		normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	pull := githubPullRequestWorkItemPayload{}
	pull.Number = payload.IID
	pull.Body = payload.Description
	pull.CreatedAt = payload.CreatedAt
	if parseGitLabWorkItemTime(pull.CreatedAt) == nil {
		pull.CreatedAt = payload.UpdatedAt
	}
	pull.UpdatedAt = payload.UpdatedAt
	pull.Head.Ref = payload.SourceBranch
	pull.Labels = make([]githubWorkItemLabelPayload, 0, len(payload.Labels))
	for _, label := range payload.Labels {
		if label != "" {
			pull.Labels = append(pull.Labels, githubWorkItemLabelPayload{Name: label})
		}
	}
	if payload.Author != nil {
		user := githubWorkItemUserPayload{Login: payload.Author.Username}
		if payload.Author.Bot {
			user.Type = stringPointer("Bot")
		}
		pull.User = &user
	}
	rows, err := detectGitHubPullRequestAttributions(claim, repoID, pull, normalizedAt)
	if err != nil {
		return nil, err
	}
	for index := range rows {
		rows[index].Provider = "gitlab"
		if err := validateGitLabAIAttributionRow(rows[index], claim); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func validateGitLabAIAttributionRow(row gitlabAIAttributionRow, claim Claim) error {
	if claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		row.RecordID == uuid.Nil || row.OrgID == uuid.Nil || row.OrgID.String() != claim.OrgID ||
		row.Provider != "gitlab" || row.SubjectType != "pull_request" || row.SubjectID == "" ||
		row.RepoID == nil || *row.RepoID == uuid.Nil || row.Kind == "" || row.Source == "" ||
		row.Confidence < 0 || row.Confidence > 1 || row.Evidence == nil ||
		row.ObservedAt.IsZero() || row.IngestedAt.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func normalizeGitLabMRStateEvents(
	claim Claim,
	workItemID string,
	events []gitlabStateEventPayload,
	createdAt time.Time,
	resolveIdentity gitlabIdentityResolver,
	normalizedAt time.Time,
) ([]gitlabWorkItemTransitionRow, []gitlabWorkItemReopenRow) {
	sorted := append([]gitlabStateEventPayload(nil), events...)
	sort.SliceStable(sorted, func(left, right int) bool {
		return gitlabEventTime(sorted[left].CreatedAt).Before(gitlabEventTime(sorted[right].CreatedAt))
	})
	transitions := make([]gitlabWorkItemTransitionRow, 0, len(sorted))
	reopens := make([]gitlabWorkItemReopenRow, 0)
	previous := "unknown"
	for _, event := range sorted {
		state := strings.ToLower(strings.TrimSpace(event.State))
		toStatus := ""
		switch state {
		case "merged":
			toStatus = "done"
		case "closed":
			toStatus = "canceled"
		case "opened", "reopened":
			toStatus = "in_progress"
		default:
			continue
		}
		occurred := parseGitLabWorkItemTime(event.CreatedAt)
		if occurred == nil {
			occurred = &createdAt
		}
		var actor *string
		if event.User != nil {
			identity := resolveGitLabWorkItemIdentity(*event.User, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		transition := gitlabWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "gitlab", OccurredAt: occurred.UTC(),
			FromStatusRaw: nil, ToStatusRaw: stringPointer(state), FromStatus: previous,
			ToStatus: toStatus, Actor: actor, OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
		}
		if validateGitLabTransitionRow(transition, claim) == nil {
			transitions = append(transitions, transition)
		}
		if state == "reopened" {
			reopen := gitlabWorkItemReopenRow{
				WorkItemID: workItemID, OccurredAt: occurred.UTC(), FromStatus: "done",
				ToStatus: "in_progress", FromStatusRaw: stringPointer("closed"),
				ToStatusRaw: stringPointer("reopened"), Actor: actor,
				OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
			}
			if validateGitLabReopenRow(reopen, claim) == nil {
				reopens = append(reopens, reopen)
			}
		}
		previous = toStatus
	}
	return transitions, reopens
}

func normalizeGitLabIssueReopens(
	claim Claim,
	workItemID string,
	events []gitlabStateEventPayload,
	resolveIdentity gitlabIdentityResolver,
	normalizedAt time.Time,
) []gitlabWorkItemReopenRow {
	rows := make([]gitlabWorkItemReopenRow, 0)
	for _, event := range events {
		if strings.ToLower(strings.TrimSpace(event.State)) != "reopened" {
			continue
		}
		occurred := parseGitLabWorkItemTime(event.CreatedAt)
		if occurred == nil {
			continue
		}
		var actor *string
		if event.User != nil {
			identity := resolveGitLabWorkItemIdentity(*event.User, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		row := gitlabWorkItemReopenRow{
			WorkItemID: workItemID, OccurredAt: occurred.UTC(), FromStatus: "done",
			ToStatus: "in_progress", FromStatusRaw: stringPointer("closed"),
			ToStatusRaw: stringPointer("reopened"), Actor: actor,
			OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
		}
		if validateGitLabReopenRow(row, claim) == nil {
			rows = append(rows, row)
		}
	}
	return rows
}

func normalizeGitLabNotes(
	claim Claim,
	workItemID string,
	notes []gitlabNotePayload,
	resolveIdentity gitlabIdentityResolver,
	normalizedAt time.Time,
) []gitlabWorkItemInteractionRow {
	rows := make([]gitlabWorkItemInteractionRow, 0, len(notes))
	for _, note := range notes {
		if note.System {
			continue
		}
		occurred := parseGitLabWorkItemTime(note.CreatedAt)
		if occurred == nil {
			continue
		}
		var actor *string
		if note.Author != nil {
			identity := resolveGitLabWorkItemIdentity(*note.Author, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		row := gitlabWorkItemInteractionRow{
			WorkItemID: workItemID, Provider: "gitlab", InteractionType: "comment",
			OccurredAt: occurred.UTC(), Actor: actor, BodyLength: utf8.RuneCountInString(note.Body),
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		}
		if validateGitLabInteractionRow(row, claim) == nil {
			rows = append(rows, row)
		}
	}
	return rows
}

func normalizeGitLabSprint(
	claim Claim,
	fullName string,
	payload gitlabIssueMilestonePayload,
	normalizedAt time.Time,
) (gitlabSprintRow, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		strings.TrimSpace(fullName) == "" || normalizedAt.IsZero() || payload.ID.String() == "" ||
		payload.ID.String() == "0" {
		return gitlabSprintRow{}, providerfoundation.ErrInvalidScope
	}
	state := "future"
	switch strings.ToLower(strings.TrimSpace(payload.State)) {
	case "closed":
		state = "closed"
	case "active":
		state = "active"
	}
	name := payload.Title
	row := gitlabSprintRow{
		Provider: "gitlab", SprintID: "gitlab:" + fullName + ":milestone:" + payload.ID.String(),
		Name: &name, State: &state, StartedAt: parseGitLabWorkItemTime(payload.StartDate),
		EndedAt: parseGitLabWorkItemTime(payload.DueDate), LastSynced: normalizedAt.UTC(),
		OrgID: claim.OrgID,
	}
	if state == "closed" {
		row.CompletedAt = row.EndedAt
	}
	if err := validateGitLabSprintRow(row, claim); err != nil {
		return gitlabSprintRow{}, err
	}
	return row, nil
}

func gitlabWorkItemLifecycle(
	transitions []gitlabWorkItemTransitionRow,
	closedAt *time.Time,
) (*time.Time, *time.Time) {
	var startedAt, completedAt *time.Time
	for _, transition := range transitions {
		if startedAt == nil && transition.ToStatus == "in_progress" {
			value := transition.OccurredAt
			startedAt = &value
		}
		if completedAt == nil && (transition.ToStatus == "done" || transition.ToStatus == "canceled") {
			value := transition.OccurredAt
			completedAt = &value
			break
		}
	}
	if completedAt == nil && closedAt != nil {
		value := closedAt.UTC()
		completedAt = &value
	}
	return startedAt, completedAt
}

func gitlabEventTime(value *string) time.Time {
	parsed := parseGitLabWorkItemTime(value)
	if parsed == nil {
		return time.Unix(0, 0).UTC()
	}
	return parsed.UTC()
}

func gitlabPriorityFromLabels(labels []string) (*string, *string) {
	for _, label := range labels {
		switch strings.ToLower(strings.TrimSpace(label)) {
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

var (
	gitlabIssueReferencePattern = regexp.MustCompile(`(?m)(?:^|[^\w])(?:(?:([\w/-]+))?#(\d+))`)
	gitlabExternalKeyPattern    = regexp.MustCompile(`(?i)(depends\s+on|blocked\s+by|blocks|fixes|closes|resolves|relates\s+to|part\s+of|see)\s*:?\s*([A-Za-z]{2,}-\d+)\b`)
)

func normalizeGitLabDependencies(
	claim Claim,
	workItemID string,
	fullName string,
	description string,
	links []gitlabIssueLinkPayload,
	normalizedAt time.Time,
) []gitlabWorkItemDependencyRow {
	rows := make([]gitlabWorkItemDependencyRow, 0)
	seenTargets := map[string]struct{}{}
	appendRow := func(source, target, relationship, raw string) {
		if source == "" || target == "" || relationship == "" || raw == "" {
			return
		}
		row := gitlabWorkItemDependencyRow{
			SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: relationship,
			RelationshipTypeRaw: raw, RelationshipSemanticsVersion: "canonical-blocks.v2",
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		}
		if validateGitLabDependencyRow(row, claim) == nil {
			rows = append(rows, row)
		}
	}
	for _, link := range links {
		targetIID := link.IID.String()
		if targetIID == "" || targetIID == "0" {
			continue
		}
		targetPath := link.References.Full
		if index := strings.Index(targetPath, "#"); index >= 0 {
			targetPath = targetPath[:index]
		}
		if strings.TrimSpace(targetPath) == "" {
			targetPath = fullName
		}
		targetID := "gitlab:" + targetPath + "#" + targetIID
		if _, seen := seenTargets[targetID]; seen {
			continue
		}
		seenTargets[targetID] = struct{}{}
		linkType := strings.ToLower(strings.TrimSpace(link.LinkType))
		relationship, source, target := "relates_to", workItemID, targetID
		if linkType == "blocks" || linkType == "is_blocked_by" {
			relationship = "blocks"
			if linkType == "is_blocked_by" {
				source, target = targetID, workItemID
			}
		}
		appendRow(source, target, relationship, linkType)
	}
	for _, match := range gitlabIssueReferencePattern.FindAllStringSubmatch(description, -1) {
		project := match[1]
		if project == "" {
			project = fullName
		}
		targetID := "gitlab:" + project + "#" + match[2]
		if targetID == workItemID {
			continue
		}
		if _, seen := seenTargets[targetID]; seen {
			continue
		}
		seenTargets[targetID] = struct{}{}
		start := strings.Index(description, match[0]) - 50
		if start < 0 {
			start = 0
		}
		contextText := strings.ToLower(description[start : strings.Index(description[start:], match[0])+start+len(match[0])])
		relationship, raw, source, target := "relates_to", "description_reference", workItemID, targetID
		for _, keyword := range []string{"blocked by", "is blocked by", "blocking", "blocks"} {
			if strings.Contains(contextText, keyword) {
				relationship, raw = "blocks", keyword
				if strings.Contains(keyword, "by") {
					source, target = targetID, workItemID
				}
				break
			}
		}
		appendRow(source, target, relationship, raw)
	}
	seenExternal := map[string]struct{}{}
	for _, match := range gitlabExternalKeyPattern.FindAllStringSubmatch(description, -1) {
		key := strings.ToUpper(strings.TrimSpace(match[2]))
		if key == "" {
			continue
		}
		if _, seen := seenExternal[key]; seen {
			continue
		}
		seenExternal[key] = struct{}{}
		relationship := "relates_to"
		switch strings.ToLower(strings.TrimSpace(match[1])) {
		case "blocks":
			relationship = "blocks"
		case "blocked by", "depends on":
			relationship = "blocked_by"
		}
		appendRow(workItemID, "extkey:"+key, relationship, "external_issue_key")
	}
	return rows
}

func validateGitLabWorkItemRow(row gitlabWorkItemRow, claim Claim) error {
	if row.Provider != "gitlab" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.RepoID == nil || *row.RepoID == uuid.Nil ||
		row.Title == "" || row.Type == "" || row.Status == "" || row.CreatedAt.IsZero() ||
		row.UpdatedAt.IsZero() || row.Assignees == nil || row.Labels == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateGitLabTransitionRow(row gitlabWorkItemTransitionRow, claim Claim) error {
	if row.WorkItemID == "" || row.Provider != "gitlab" || row.OccurredAt.IsZero() ||
		row.FromStatus == "" || row.ToStatus == "" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateGitLabDependencyRow(row gitlabWorkItemDependencyRow, claim Claim) error {
	if row.SourceWorkItemID == "" || row.TargetWorkItemID == "" || row.RelationshipType == "" ||
		row.RelationshipTypeRaw == "" || row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
		row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateGitLabReopenRow(row gitlabWorkItemReopenRow, claim Claim) error {
	if row.WorkItemID == "" || row.OccurredAt.IsZero() || row.FromStatus == "" || row.ToStatus == "" ||
		row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateGitLabInteractionRow(row gitlabWorkItemInteractionRow, claim Claim) error {
	if row.WorkItemID == "" || row.Provider != "gitlab" || row.InteractionType != "comment" ||
		row.OccurredAt.IsZero() || row.BodyLength < 0 || row.LastSynced.IsZero() ||
		row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateGitLabSprintRow(row gitlabSprintRow, claim Claim) error {
	if row.Provider != "gitlab" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.SprintID == "" || row.Name == nil || row.State == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}
