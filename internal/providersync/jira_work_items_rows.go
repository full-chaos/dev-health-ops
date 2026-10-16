package providersync

// Jira work-item rows intentionally reuse the already verified direct
// work-item projections.  The Jira producer owns the provider normalization
// and the six-row-family assembly here; the ClickHouse adapter remains the
// single implementation of the Python sink's column coercions.

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

type jiraWorkItemRow = githubWorkItemRow
type jiraWorkItemTransitionRow = githubWorkItemTransitionRow
type jiraWorkItemDependencyRow = githubWorkItemDependencyRow
type jiraWorkItemReopenRow = githubWorkItemReopenRow
type jiraWorkItemInteractionRow = githubWorkItemInteractionRow
type jiraSprintRow = githubSprintRow

type jiraWorkItemRows struct {
	WorkItems          []jiraWorkItemRow
	Transitions        []jiraWorkItemTransitionRow
	Dependencies       []jiraWorkItemDependencyRow
	ReopenEvents       []jiraWorkItemReopenRow
	Interactions       []jiraWorkItemInteractionRow
	Sprints            []jiraSprintRow
	ProjectMemberships []projectmembership.Row
	Projects           []projectmembership.CatalogRow
	// MembershipCreation counts the CHAOS-7361 creation-time ADD decisions.
	MembershipCreation membershipCreationStats
}

type jiraIdentityResolver func(email, accountID, displayName string) string

// jiraWorkItemFixtureInput is also used by the semantic oracle.  The live
// REST path is always JSON-shaped; objectShape exists only to exercise the
// shipped Atlassian adapter-object path in the oracle without hand-authoring a
// second expected row.
type jiraWorkItemFixtureInput struct {
	Raw         map[string]any
	ObjectShape bool
	// AtlassianShape marks JSON decoded from iter_issues_via_rest. The
	// Atlassian canonical mapper validates users as objects before invoking the
	// Python normalizer, so account/email/display identity must be preserved on
	// this route even though the legacy JSON compatibility path intentionally
	// retains its historical getattr(dict, ...) behavior.
	AtlassianShape bool
	// PointsFields are the discovered story point field ids in preference
	// order; empty when story_points_field is configured or none was found.
	PointsFields []string
}

func normalizeJiraWorkItem(
	claim Claim,
	issue jiraWorkItemFixtureInput,
	statusMapping *StatusMapping,
	resolveIdentity jiraIdentityResolver,
	normalizedAt time.Time,
) (jiraWorkItemRow, []jiraWorkItemTransitionRow, error) {
	if claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) || statusMapping == nil ||
		normalizedAt.IsZero() {
		return jiraWorkItemRow{}, nil, ErrInvalidConfiguration
	}
	raw := issue.Raw
	key := stringFrom(raw["key"])
	fields, ok := jiraMapValue(raw["fields"])
	if key == "" || !ok {
		return jiraWorkItemRow{}, nil, providerfoundation.ErrNormalizationInvalid
	}
	projectKey, projectID, projectName := jiraProject(fields["project"], issue.ObjectShape)
	title := stringFrom(jiraField(fields, "summary", issue.ObjectShape))
	if title == "" {
		return jiraWorkItemRow{}, nil, providerfoundation.ErrNormalizationInvalid
	}
	description := jiraDescriptionText(jiraField(fields, "description", issue.ObjectShape))
	statusRaw, statusCategory := jiraStatus(fields["status"], issue.ObjectShape)
	labels := jiraLabels(jiraField(fields, "labels", issue.ObjectShape))
	typeRaw := jiraNamedValue(jiraField(fields, "issuetype", issue.ObjectShape), "name", issue.ObjectShape)
	priorityRaw := jiraNamedValue(jiraField(fields, "priority", issue.ObjectShape), "name", issue.ObjectShape)
	status := statusMapping.NormalizeStatus("jira", statusRaw, labels, "")
	if status != "done" && status != "canceled" && strings.EqualFold(statusCategory, "done") {
		status = "done"
	}
	typeName := statusMapping.NormalizeType("jira", typeRaw, labels)

	assignees := make([]string, 0, 1)
	identityShape := issue.ObjectShape || issue.AtlassianShape
	if value := jiraField(fields, "assignee", issue.ObjectShape); value != nil {
		if resolved := jiraResolveUser(value, identityShape, resolveIdentity); resolved != "" && resolved != "unknown" {
			assignees = append(assignees, resolved)
		}
	}
	var reporter *string
	if value := jiraField(fields, "reporter", issue.ObjectShape); value != nil {
		if resolved := jiraResolveUser(value, identityShape, resolveIdentity); resolved != "" && resolved != "unknown" {
			reporter = &resolved
		}
	}
	createdAt := jiraTime(jiraField(fields, "created", issue.ObjectShape))
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := jiraTime(jiraField(fields, "updated", issue.ObjectShape))
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := jiraTime(jiraField(fields, "resolutiondate", issue.ObjectShape))
	urlValue := stringPointerIfNonEmpty(raw["self"])
	storyPoints := jiraStoryPoints(claim, fields, issue.PointsFields, issue.ObjectShape)
	sprintID, sprintName := jiraSprint(jiraField(fields, jiraOptionString(claim, "sprint_field", "customfield_10020"), issue.ObjectShape), issue.ObjectShape)
	serviceClass := jiraServiceClass(priorityRaw)
	changelog, _ := jiraMapValue(jiraField(raw, "changelog", issue.ObjectShape))
	transitions := normalizeJiraTransitions(
		claim, key, changelog, labels, *createdAt, statusMapping, resolveIdentity,
		issue.ObjectShape, identityShape, normalizedAt,
	)
	startedAt, completedAt := deriveJiraLifecycle(transitions, status, closedAt, *updatedAt)
	row, err := buildWorkItemRow(claim, jiraIssueSource{
		claim: claim, fields: fields, objectShape: issue.ObjectShape,
		workItemID: "jira:" + key, title: title, typeName: typeName, status: status,
		statusRaw: nullableString(statusRaw), description: description,
		projectKey: nullableString(projectKey), projectID: nullableString(projectID),
		projectName: nullableString(projectName), assignees: assignees, reporter: reporter,
		createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC(), startedAt: startedAt,
		completedAt: completedAt, closedAt: closedAt, labels: labels, storyPoints: storyPoints,
		sprintID: nullableString(sprintID), sprintName: nullableString(sprintName), url: urlValue,
		priorityRaw: nullableString(priorityRaw), serviceClass: &serviceClass,
		dueAt: jiraTime(jiraField(fields, "duedate", issue.ObjectShape)),
	}, normalizedAt)
	if err != nil {
		return jiraWorkItemRow{}, nil, err
	}
	return row, transitions, nil
}

const jiraIssueSourceKind = "jira_issue"

// jiraIssueSource is how a Jira issue fills the shared work-item row.
type jiraIssueSource struct {
	claim       Claim
	fields      map[string]any
	objectShape bool

	workItemID, title, typeName, status                        string
	statusRaw, description, projectKey, projectID, projectName *string
	reporter, sprintID, sprintName, url, priorityRaw           *string
	serviceClass                                               *string
	assignees, labels                                          []string
	createdAt, updatedAt                                       time.Time
	startedAt, completedAt, closedAt, dueAt                    *time.Time
	storyPoints                                                *float64
}

func (s jiraIssueSource) sourceKind() string { return jiraIssueSourceKind }
func (s jiraIssueSource) validateRow(claim Claim, row workItemRow) error {
	return validateJiraWorkItem(row, claim)
}
func (s jiraIssueSource) WorkItemID() string      { return s.workItemID }
func (s jiraIssueSource) Provider() string        { return "jira" }
func (s jiraIssueSource) Title() string           { return s.title }
func (s jiraIssueSource) Type() string            { return s.typeName }
func (s jiraIssueSource) Status() string          { return s.status }
func (s jiraIssueSource) StatusRaw() *string      { return s.statusRaw }
func (s jiraIssueSource) Description() *string    { return s.description }
func (s jiraIssueSource) RepoID() *uuid.UUID      { return nil }
func (s jiraIssueSource) NativeTeamKey() *string  { return nil }
func (s jiraIssueSource) ProjectKey() *string     { return s.projectKey }
func (s jiraIssueSource) ProjectID() *string      { return s.projectID }
func (s jiraIssueSource) ProjectName() *string    { return s.projectName }
func (s jiraIssueSource) Assignees() []string     { return s.assignees }
func (s jiraIssueSource) Reporter() *string       { return s.reporter }
func (s jiraIssueSource) CreatedAt() time.Time    { return s.createdAt }
func (s jiraIssueSource) UpdatedAt() time.Time    { return s.updatedAt }
func (s jiraIssueSource) StartedAt() *time.Time   { return s.startedAt }
func (s jiraIssueSource) CompletedAt() *time.Time { return s.completedAt }
func (s jiraIssueSource) ClosedAt() *time.Time    { return s.closedAt }
func (s jiraIssueSource) Labels() []string        { return s.labels }
func (s jiraIssueSource) StoryPoints() *float64   { return s.storyPoints }
func (s jiraIssueSource) SprintID() *string       { return s.sprintID }
func (s jiraIssueSource) SprintName() *string     { return s.sprintName }
func (s jiraIssueSource) URL() *string            { return s.url }
func (s jiraIssueSource) PriorityRaw() *string    { return s.priorityRaw }
func (s jiraIssueSource) ServiceClass() *string   { return s.serviceClass }
func (s jiraIssueSource) DueAt() *time.Time       { return s.dueAt }

// ParentID reads fields.parent.key. Both the object and the JSON shapes carry
// the key (CHAOS-9835).
func (s jiraIssueSource) ParentID() workItemRef {
	parent, ok := jiraMapValue(jiraField(s.fields, "parent", s.objectShape))
	if !ok {
		return workItemRefAbsent()
	}
	return workItemRefKey(stringFrom(parent["key"]))
}

// EpicID reads the site's Epic Link field when the dataset option names one.
func (s jiraIssueSource) EpicID() workItemRef {
	epicField := jiraOptionString(s.claim, "epic_link_field", "")
	if epicField == "" {
		return workItemRefAbsent()
	}
	value := stringFrom(jiraField(s.fields, epicField, s.objectShape))
	if value == "" {
		return workItemRefAbsent()
	}
	return workItemRefKey(value)
}

// jiraIssueProjectMoves is jiraProjectMoveItems' entry point for a raw issue
// payload, mirroring normalizeJiraWorkItem's own changelog/createdAt
// derivation exactly (same fields, same object-shape branching) rather than
// threading a fourth return value through normalizeJiraWorkItem itself --
// that function has three other call sites (jira_atlassian_route.go and two
// test files) this change does not touch. Called separately, once per issue,
// by jira_work_items_route.go's Collect.
func jiraIssueProjectMoves(
	issue jiraWorkItemFixtureInput,
	resolveIdentity jiraIdentityResolver,
) []jiraProjectMoveItem {
	raw := issue.Raw
	fields, ok := jiraMapValue(raw["fields"])
	if !ok {
		return nil
	}
	createdAt := jiraTime(jiraField(fields, "created", issue.ObjectShape))
	fallback := time.Time{}
	if createdAt == nil {
		createdAt = &fallback
	}
	identityShape := issue.ObjectShape || issue.AtlassianShape
	changelog, _ := jiraMapValue(jiraField(raw, "changelog", issue.ObjectShape))
	return jiraProjectMoveItems(changelog, *createdAt, issue.ObjectShape, identityShape, resolveIdentity)
}

func validateJiraWorkItem(row jiraWorkItemRow, claim Claim) error {
	if row.Provider != "jira" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.Title == "" || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil ||
		row.Labels == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraTransition(row jiraWorkItemTransitionRow, claim Claim) error {
	if row.Provider != "jira" || row.WorkItemID == "" || row.OccurredAt.IsZero() ||
		row.FromStatus == "" || row.ToStatus == "" || row.OrgID == "" ||
		row.OrgID != claim.OrgID || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraDependency(row jiraWorkItemDependencyRow, claim Claim) error {
	if row.SourceWorkItemID == "" || row.TargetWorkItemID == "" ||
		row.RelationshipType == "" || row.RelationshipTypeRaw == "" ||
		row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
		row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraReopen(row jiraWorkItemReopenRow, claim Claim) error {
	if row.WorkItemID == "" || row.OccurredAt.IsZero() || row.FromStatus == "" ||
		row.ToStatus == "" || row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraInteraction(row jiraWorkItemInteractionRow, claim Claim) error {
	if row.WorkItemID == "" || row.Provider != "jira" || row.InteractionType != "comment" ||
		row.OccurredAt.IsZero() || row.BodyLength < 0 || row.LastSynced.IsZero() || row.InteractionID == "" ||
		row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraProjectMembership(row projectmembership.Row, claim Claim) error {
	if row.Provider != "jira" || row.SubjectKind != projectmembership.SubjectWorkItem ||
		row.SubjectID == "" || row.ToProjectID == "" || row.OccurredAt.IsZero() ||
		row.EventID == "" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateJiraSprint(row jiraSprintRow, claim Claim) error {
	if row.Provider != "jira" || row.SprintID == "" ||
		row.LastSynced.IsZero() || row.OrgID == "" || row.OrgID != claim.OrgID {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func normalizeJiraTransitions(
	claim Claim,
	key string,
	changelog map[string]any,
	labels []string,
	createdAt time.Time,
	statusMapping *StatusMapping,
	resolveIdentity jiraIdentityResolver,
	objectShape bool,
	identityObjectShape bool,
	normalizedAt time.Time,
) []jiraWorkItemTransitionRow {
	histories := mapSlice(changelog["histories"])
	type historyWithIndex struct {
		value map[string]any
		index int
		at    time.Time
	}
	ordered := make([]historyWithIndex, 0, len(histories))
	for index, value := range histories {
		mapped, ok := jiraMapValue(value)
		if !ok {
			continue
		}
		at := jiraTime(jiraField(mapped, "created", objectShape))
		if at == nil {
			at = &time.Time{}
		}
		ordered = append(ordered, historyWithIndex{value: mapped, index: index, at: *at})
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].at.Equal(ordered[right].at) {
			return ordered[left].index < ordered[right].index
		}
		return ordered[left].at.Before(ordered[right].at)
	})
	rows := make([]jiraWorkItemTransitionRow, 0)
	previous := "unknown"
	for _, history := range ordered {
		occurred := history.at
		if occurred.IsZero() {
			occurred = createdAt
		}
		author := jiraField(history.value, "author", objectShape)
		actor := jiraResolveUser(author, identityObjectShape, resolveIdentity)
		if !objectShape {
			actor = jiraResolveMapUser(author, resolveIdentity)
		}
		actorPtr := nullableString(actor)
		for _, itemValue := range mapSlice(jiraField(history.value, "items", objectShape)) {
			item, ok := jiraMapValue(itemValue)
			if !ok || !strings.EqualFold(stringFrom(jiraField(item, "field", objectShape)), "status") {
				continue
			}
			fromRaw := stringFrom(jiraField(item, "fromString", objectShape))
			toRaw := stringFrom(jiraField(item, "toString", objectShape))
			fromStatus := statusMapping.NormalizeStatus("jira", fromRaw, labels, "")
			toStatus := statusMapping.NormalizeStatus("jira", toRaw, labels, "")
			row := jiraWorkItemTransitionRow{
				WorkItemID: "jira:" + key, Provider: "jira", OccurredAt: occurred,
				FromStatusRaw: nullableString(fromRaw), ToStatusRaw: nullableString(toRaw),
				FromStatus: fromStatus, ToStatus: toStatus, Actor: actorPtr,
				OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
			}
			if row.FromStatus == "" {
				row.FromStatus = previous
			}
			if row.ToStatus == "" {
				row.ToStatus = "unknown"
			}
			rows = append(rows, row)
			previous = row.ToStatus
		}
	}
	return rows
}

// jiraProjectMoveItem is one changelog "project" item, unresolved: ids and the
// changelog's own display names only, pure and network-free. Resolving the
// REAL project key (jira_work_items_route.go's Collect) needs a live lookup
// this package's row-normalization functions deliberately do not have access
// to, so that step lives in the route, not here.
type jiraProjectMoveItem struct {
	FromProjectID   string
	FromProjectName string
	ToProjectID     string
	ToProjectName   string
	Actor           string
	OccurredAt      time.Time
	EventID         string // native changelog history id, "jira:"+id; "" if absent
}

// jiraProjectMoveItems extracts CHAOS-4193's project-membership facts from a
// jira issue's changelog.
//
// A SEPARATE loop from normalizeJiraTransitions (status), reading the SAME
// histories: a "project" item's fromString/toString carry the project NAME,
// not a status, so sharing one loop body would feed project names into status
// normalization. Confirmed live (CHAOS-4193 probe, real Move OPS-9 -> API-24,
// 2026-08-24): a Move logs a "Key" item (old/new ISSUE key -- carries no
// project data, not parsed here) and a "project" item (from/to are internal
// numeric project ids; fromString/toString are the project NAME, never
// necessarily the key -- API's name happened to equal its key, Billing/BILL
// from the same tenant's project list does not) together on ONE history entry
// sharing its native `id`.
//
// A Jira issue always belongs to exactly one project, so unlike GitHub's
// board-membership snapshot (which has a genuine "added with no prior board"
// case) fromProjectID is never actually empty here in practice -- but nothing
// in the changelog schema GUARANTEES that, so an empty `from` is passed
// through rather than assumed impossible; the row's own "" first-assignment
// convention already covers it correctly either way.
func jiraProjectMoveItems(
	changelog map[string]any,
	createdAt time.Time,
	objectShape bool,
	identityObjectShape bool,
	resolveIdentity jiraIdentityResolver,
) []jiraProjectMoveItem {
	histories := mapSlice(changelog["histories"])
	items := make([]jiraProjectMoveItem, 0)
	for _, value := range histories {
		mapped, ok := jiraMapValue(value)
		if !ok {
			continue
		}
		occurred := createdAt
		if at := jiraTime(jiraField(mapped, "created", objectShape)); at != nil {
			occurred = *at
		}
		historyID := stringFrom(jiraField(mapped, "id", objectShape))
		author := jiraField(mapped, "author", objectShape)
		actor := jiraResolveUser(author, identityObjectShape, resolveIdentity)
		if !objectShape {
			actor = jiraResolveMapUser(author, resolveIdentity)
		}
		for _, itemValue := range mapSlice(jiraField(mapped, "items", objectShape)) {
			item, ok := jiraMapValue(itemValue)
			if !ok || !strings.EqualFold(stringFrom(jiraField(item, "field", objectShape)), "project") {
				continue
			}
			toID := stringFrom(jiraField(item, "to", objectShape))
			if toID == "" {
				// A "project" item that names no destination is malformed --
				// every real Move has one. Not the ruled unresolvable/drop
				// counter (that is for a RESOLUTION failure on a real id);
				// this is a defensive skip of a payload shape that should
				// never occur.
				continue
			}
			eventID := ""
			if historyID != "" {
				eventID = "jira:" + historyID
			}
			items = append(items, jiraProjectMoveItem{
				FromProjectID:   stringFrom(jiraField(item, "from", objectShape)),
				FromProjectName: stringFrom(jiraField(item, "fromString", objectShape)),
				ToProjectID:     toID,
				ToProjectName:   stringFrom(jiraField(item, "toString", objectShape)),
				Actor:           actor, OccurredAt: occurred, EventID: eventID,
			})
		}
	}
	return items
}

func deriveJiraLifecycle(
	transitions []jiraWorkItemTransitionRow,
	status string,
	closedAt *time.Time,
	updatedAt time.Time,
) (*time.Time, *time.Time) {
	var started, completed *time.Time
	for _, transition := range transitions {
		if started == nil && transition.ToStatus == "in_progress" {
			value := transition.OccurredAt
			started = &value
		}
		if completed == nil && (transition.ToStatus == "done" || transition.ToStatus == "canceled") {
			value := transition.OccurredAt
			completed = &value
			break
		}
	}
	if completed == nil && (status == "done" || status == "canceled") {
		if closedAt != nil {
			value := *closedAt
			completed = &value
		} else {
			value := updatedAt
			completed = &value
		}
	}
	return started, completed
}

func normalizeJiraDependencies(
	claim Claim,
	workItemID string,
	issue map[string]any,
	normalizedAt time.Time,
) []jiraWorkItemDependencyRow {
	fields, _ := jiraMapValue(issue["fields"])
	dependencies := make([]jiraWorkItemDependencyRow, 0)
	linkedAt := jiraLinkStartTimes(issue)
	for _, linkValue := range mapSlice(jiraField(fields, "issuelinks", false)) {
		link, ok := jiraMapValue(linkValue)
		if !ok {
			continue
		}
		linkType, _ := jiraMapValue(link["type"])
		outwardRaw := stringFrom(linkType["outward"])
		if outwardRaw == "" {
			outwardRaw = stringFrom(linkType["name"])
		}
		inwardRaw := stringFrom(linkType["inward"])
		if inwardRaw == "" {
			inwardRaw = stringFrom(linkType["name"])
		}
		if outward, ok := jiraMapValue(link["outwardIssue"]); ok {
			if target := stringFrom(outward["key"]); target != "" {
				source, destination := workItemID, "jira:"+target
				relationship := jiraRelationship(outwardRaw)
				if relationship == "blocked_by" {
					source, destination, relationship = destination, source, "blocks"
				}
				row := jiraDependencyRow(claim, source, destination, relationship, outwardRaw, normalizedAt)
				row.RelationStartedAt = linkStart(linkedAt, target, outwardRaw, normalizedAt)
				dependencies = append(dependencies, row)
			}
		}
		if inward, ok := jiraMapValue(link["inwardIssue"]); ok {
			if sourceKey := stringFrom(inward["key"]); sourceKey != "" {
				source, destination := "jira:"+sourceKey, workItemID
				relationship := jiraRelationship(inwardRaw)
				if relationship == "blocked_by" {
					relationship = "blocks"
				} else if relationship == "blocks" {
					source, destination = destination, source
				}
				row := jiraDependencyRow(claim, source, destination, relationship, inwardRaw, normalizedAt)
				row.RelationStartedAt = linkStart(linkedAt, sourceKey, inwardRaw, normalizedAt)
				dependencies = append(dependencies, row)
			}
		}
	}

	return dependencies
}

var jiraIssueKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*-[0-9]+$`)

// jiraLinkItemKey is the key of the linked issue a changelog "Link" item names: the `to` (or `from`) value,
// else the last word of its description ("This issue blocks PAY-11"). The phrase is the link type's wording
// in that description ("blocks", "relates to", "is blocked by"), lower-cased, or "" when the description does
// not have the "This issue <phrase> <key>" form.
func jiraLinkItemKey(item map[string]any, valueField, textField string) (key, phrase string) {
	text := strings.TrimSpace(stringFrom(item[textField]))
	words := strings.Fields(text)
	if value := strings.TrimSpace(stringFrom(item[valueField])); jiraIssueKeyPattern.MatchString(value) {
		key = value
	} else if len(words) > 0 && jiraIssueKeyPattern.MatchString(words[len(words)-1]) {
		key = words[len(words)-1]
	}
	if key == "" {
		return "", ""
	}
	if rest, found := strings.CutPrefix(strings.ToLower(text), "this issue "); found {
		phrase = strings.TrimSpace(strings.TrimSuffix(rest, strings.ToLower(key)))
	}
	return key, phrase
}

func jiraLinkStartKey(key, phrase string) string {
	return key + "\x00" + strings.ToLower(strings.TrimSpace(phrase))
}

// jiraLinkStartTimes is, by the linked issue's key and the link type's wording, the time of the changelog entry
// that added that issue link on this issue: the provider's own time of the link. Jira's issuelinks carry no
// time; the changelog does (a "Link" item, `to` set when a link is added, `from` set when it is removed).
// Two links of different types to one issue keep their own times. A link removed and added again starts at
// its last addition; a removed link has no entry. A link without a changelog entry is absent from the map,
// and its row keeps a NULL start: no time is invented.
func jiraLinkStartTimes(issue map[string]any) map[string]time.Time {
	changelog, _ := jiraMapValue(issue["changelog"])
	type linkEvent struct {
		at    time.Time
		index int
		key   string
		added bool
	}
	var events []linkEvent
	for index, historyValue := range mapSlice(changelog["histories"]) {
		history, ok := jiraMapValue(historyValue)
		if !ok {
			continue
		}
		at := jiraTime(history["created"])
		if at == nil {
			continue
		}
		for _, itemValue := range mapSlice(history["items"]) {
			item, ok := jiraMapValue(itemValue)
			if !ok || !strings.EqualFold(stringFrom(item["field"]), "link") {
				continue
			}
			if key, phrase := jiraLinkItemKey(item, "from", "fromString"); key != "" {
				events = append(events, linkEvent{at: *at, index: index, key: jiraLinkStartKey(key, phrase)})
			}
			if key, phrase := jiraLinkItemKey(item, "to", "toString"); key != "" {
				events = append(events, linkEvent{at: *at, index: index, key: jiraLinkStartKey(key, phrase), added: true})
			}
		}
	}
	sort.SliceStable(events, func(left, right int) bool {
		if events[left].at.Equal(events[right].at) {
			return events[left].index < events[right].index
		}
		return events[left].at.Before(events[right].at)
	})
	started := map[string]time.Time{}
	for _, event := range events {
		if event.added {
			started[event.key] = event.at
		} else {
			delete(started, event.key)
		}
	}
	return started
}

// linkStart is the start time of the link of that type to the issue key, or nil: none in the changelog, or one
// after the sync clock (a clock that disagrees with the provider is not a start). An entry whose description
// names no link type applies to a link of any type to that issue.
func linkStart(started map[string]time.Time, key, phrase string, normalizedAt time.Time) *time.Time {
	at, ok := started[jiraLinkStartKey(key, phrase)]
	if !ok {
		at, ok = started[jiraLinkStartKey(key, "")]
	}
	if !ok || at.After(normalizedAt) {
		return nil
	}
	value := at.UTC()
	return &value
}

func jiraReopenEvents(
	claim Claim,
	transitions []jiraWorkItemTransitionRow,
	normalizedAt time.Time,
) []jiraWorkItemReopenRow {
	rows := make([]jiraWorkItemReopenRow, 0)
	for _, transition := range transitions {
		if (transition.FromStatus != "done" && transition.FromStatus != "canceled") ||
			(transition.ToStatus == "done" || transition.ToStatus == "canceled") {
			continue
		}
		rows = append(rows, jiraWorkItemReopenRow{
			WorkItemID: transition.WorkItemID, OccurredAt: transition.OccurredAt,
			FromStatus: transition.FromStatus, ToStatus: transition.ToStatus,
			FromStatusRaw: transition.FromStatusRaw, ToStatusRaw: transition.ToStatusRaw,
			Actor: transition.Actor, LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
		})
	}
	return rows
}

// Bounds on the provider-supplied strings of an interaction row (CHAOS-8806):
// an Atlassian account id is at most 128 characters and an email at most 254;
// 256 runes holds either, and a numeric comment id is far below 64 bytes.
const (
	jiraInteractionActorMaxRunes = 256
	jiraInteractionIDMaxBytes    = 64
)

func truncateRunes(value string, limit int) string {
	if utf8RuneCount(value) <= limit {
		return value
	}
	return string([]rune(value)[:limit])
}

func normalizeJiraInteractions(
	claim Claim,
	workItemID string,
	comments []map[string]any,
	resolveIdentity jiraIdentityResolver,
	normalizedAt time.Time,
) []jiraWorkItemInteractionRow {
	rows := make([]jiraWorkItemInteractionRow, 0, len(comments))
	for _, comment := range comments {
		id := interactionIDFrom(comment["id"])
		if id == "" || len(id) > jiraInteractionIDMaxBytes {
			// A comment id is numeric text of a few digits; a longer value is
			// not one (and an unbounded provider string must not reach the row).
			skipInteractionWithoutID("jira", claim.OrgID, workItemID)
			continue
		}
		occurredAt := jiraTime(comment["created"])
		if occurredAt == nil {
			continue
		}
		actor := (*string)(nil)
		if author := comment["author"]; author != nil {
			resolved := jiraResolveMapUser(author, resolveIdentity)
			if resolved != "" && resolved != "unknown" {
				resolved = truncateRunes(resolved, jiraInteractionActorMaxRunes)
				actor = &resolved
			}
		}
		body := comment["body"]
		bodyLength := 0
		if bodyString, ok := body.(string); ok {
			bodyLength = utf8RuneCount(bodyString)
		}
		rows = append(rows, jiraWorkItemInteractionRow{
			WorkItemID: workItemID, Provider: "jira", InteractionType: "comment",
			OccurredAt: *occurredAt, Actor: actor, BodyLength: bodyLength,
			LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID, InteractionID: id,
		})
	}
	return rows
}

func normalizeJiraSprint(claim Claim, payload map[string]any, normalizedAt time.Time) (jiraSprintRow, error) {
	id := stringFrom(payload["id"])
	if id == "" {
		return jiraSprintRow{}, providerfoundation.ErrNormalizationInvalid
	}
	name := nullableJiraString(payload["name"])
	state := nullableJiraString(payload["state"])
	return jiraSprintRow{
		Provider: "jira", SprintID: id, Name: name, State: state,
		StartedAt: jiraTime(payload["startDate"]), EndedAt: jiraTime(payload["endDate"]),
		CompletedAt: jiraTime(payload["completeDate"]), LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
	}, nil
}

func jiraDependencyRow(
	claim Claim, source, target, relationship, raw string, normalizedAt time.Time,
) jiraWorkItemDependencyRow {
	return jiraWorkItemDependencyRow{
		SourceWorkItemID: source, TargetWorkItemID: target, RelationshipType: relationship,
		RelationshipTypeRaw: raw, RelationshipSemanticsVersion: "canonical-blocks.v2",
		LastSynced: normalizedAt.UTC(), OrgID: claim.OrgID,
	}
}

func jiraRelationship(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if jiraBlockedByPattern.MatchString(normalized) {
		return "blocked_by"
	}
	if jiraBlocksPattern.MatchString(normalized) {
		return "blocks"
	}
	if strings.Contains(normalized, "relate") {
		return "relates_to"
	}
	if strings.Contains(normalized, "duplicate") {
		return "duplicates"
	}
	return "other"
}

var (
	jiraBlockedByPattern = regexp.MustCompile(`\b(?:is\s+)?blocked by\b`)
	jiraBlocksPattern    = regexp.MustCompile(`\bblocks?\b`)
)

func jiraLabels(value any) []string {
	labels := make([]string, 0)
	for _, item := range mapSlice(value) {
		if label := stringFrom(item); label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

func jiraProject(value any, objectShape bool) (string, string, string) {
	return jiraNamedValue(value, "key", objectShape), jiraNamedValue(value, "id", objectShape), jiraNamedValue(value, "name", objectShape)
}

func jiraStatus(value any, objectShape bool) (string, string) {
	statusRaw := jiraNamedValue(value, "name", objectShape)
	category := ""
	if mapped, ok := jiraMapValue(value); ok {
		if nested, ok := jiraMapValue(mapped["statusCategory"]); ok {
			category = stringFrom(nested["key"])
		}
	}
	return statusRaw, category
}

func jiraNamedValue(value any, field string, objectShape bool) string {
	if objectShape {
		// Test fixtures represent object fields as maps, but this branch mirrors
		// getattr() on SimpleNamespace by reading the corresponding member.
		if mapped, ok := jiraMapValue(value); ok {
			return stringFrom(mapped[field])
		}
		return ""
	}
	if mapped, ok := jiraMapValue(value); ok {
		return stringFrom(mapped[field])
	}
	return ""
}

func jiraField(fields map[string]any, name string, objectShape bool) any {
	if fields == nil {
		return nil
	}
	return fields[name]
}

func jiraResolveUser(value any, objectShape bool, resolver jiraIdentityResolver) string {
	if value == nil {
		return ""
	}
	var email, accountID, displayName string
	if objectShape {
		mapped, _ := jiraMapValue(value)
		email, accountID, displayName = stringFrom(mapped["emailAddress"]), stringFrom(mapped["accountId"]), stringFrom(mapped["displayName"])
		if displayName == "" {
			displayName = stringFrom(mapped["name"])
		}
	} else {
		// This is deliberate: the shipped JSON branch calls getattr(dict,...)
		// and therefore passes three None values to IdentityResolver.
	}
	if resolver != nil {
		return resolver(email, accountID, displayName)
	}
	if email != "" {
		return strings.ToLower(strings.TrimSpace(email))
	}
	if accountID != "" {
		return "jira:" + strings.TrimSpace(accountID)
	}
	return strings.TrimSpace(displayName)
}

func jiraResolveMapUser(value any, resolver jiraIdentityResolver) string {
	mapped, ok := jiraMapValue(value)
	if !ok {
		return ""
	}
	email := stringFrom(mapped["emailAddress"])
	accountID := stringFrom(mapped["accountId"])
	displayName := stringFrom(mapped["displayName"])
	if displayName == "" {
		displayName = stringFrom(mapped["name"])
	}
	if resolver != nil {
		return resolver(email, accountID, displayName)
	}
	if email != "" {
		return strings.ToLower(strings.TrimSpace(email))
	}
	if accountID != "" {
		return "jira:" + strings.TrimSpace(accountID)
	}
	return strings.TrimSpace(displayName)
}

func jiraSprint(value any, objectShape bool) (string, string) {
	if value == nil || value == "" {
		return "", ""
	}
	if values := mapSlice(value); len(values) > 0 {
		value = values[len(values)-1]
	}
	if mapped, ok := jiraMapValue(value); ok {
		// The object-shaped oracle turns this mapping into a
		// SimpleNamespace.  The production parser then stringifies that object
		// and applies a deliberately narrow ``id=digits`` regex.  Our fixture's
		// quoted sprint id therefore does not match, just as Python's does not.
		if objectShape {
			return "", ""
		}
		return stringFrom(mapped["id"]), stringFrom(mapped["name"])
	}
	raw := stringFrom(value)
	if raw == "" {
		return "", ""
	}
	id, name := "", ""
	for _, part := range strings.Split(raw, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch strings.TrimSpace(key) {
		case "id":
			id = strings.TrimSpace(value)
		case "name":
			name = strings.TrimSpace(value)
		}
	}
	return id, name
}

func jiraServiceClass(priority string) string {
	normalized := strings.ToLower(strings.TrimSpace(priority))
	if normalized == "" {
		return "standard"
	}
	for _, marker := range []string{"highest", "critical", "blocker", "urgent", "p0", "p1"} {
		if jiraMarkerMatch(normalized, marker) {
			return "expedite"
		}
	}
	for _, marker := range []string{"low", "lowest", "p4", "p5"} {
		if jiraMarkerMatch(normalized, marker) {
			return "background"
		}
	}
	return "standard"
}

func jiraMarkerMatch(value, marker string) bool {
	if value == marker {
		return true
	}
	for _, part := range strings.FieldsFunc(value, func(r rune) bool { return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) }) {
		if part == marker {
			return true
		}
	}
	return false
}

func jiraTime(value any) *time.Time {
	raw := strings.TrimSpace(stringFrom(value))
	if raw == "" {
		return nil
	}
	if len(raw) == len("2006-01-02") {
		if parsed, err := time.Parse("2006-01-02", raw); err == nil {
			parsed = parsed.UTC()
			return &parsed
		}
	}
	raw = normalizeJiraOffset(raw)
	for _, layout := range jiraTimestampLayouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			parsed = parsed.UTC()
			return &parsed
		}
	}
	return nil
}

// jiraTimestampLayouts are the shapes Jira REST returns for datetime fields.
var jiraTimestampLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05.000-07:00"}

// normalizeJiraOffset rewrites Jira Cloud's numeric offset into the colon form
// RFC3339 requires: the API returns "2026-07-22T10:00:00.000+0000", which
// time.Parse(time.RFC3339Nano, ...) rejects outright. A "Z" suffix becomes an
// explicit zero offset so one layout set covers every shape.
//
// This lived only on the work-items path; the incidents route parsed strict
// RFC3339 and so rejected every real Jira Cloud incident (CHAOS-3869). Route
// fixtures were Z-suffixed, which is why CI could not catch it.
func normalizeJiraOffset(raw string) string {
	if strings.HasSuffix(raw, "Z") {
		raw = strings.TrimSuffix(raw, "Z") + "+00:00"
	}
	if len(raw) >= 5 && (raw[len(raw)-5] == '+' || raw[len(raw)-5] == '-') && raw[len(raw)-3] != ':' {
		raw = raw[:len(raw)-2] + ":" + raw[len(raw)-2:]
	}
	return raw
}

func jiraFloat(value any) *float64 {
	if value == nil {
		return nil
	}
	var parsed float64
	switch number := value.(type) {
	case json.Number:
		parsed, _ = strconv.ParseFloat(string(number), 64)
	case float64:
		parsed = number
	case float32:
		parsed = float64(number)
	case int:
		parsed = float64(number)
	case string:
		parsed, _ = strconv.ParseFloat(strings.TrimSpace(number), 64)
	default:
		return nil
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil
	}
	return &parsed
}

// jiraDescriptionText is the plain text of a Jira description. Jira Cloud v3
// sends an Atlassian Document Format tree, Server and v2 a string. A string is
// kept as given; a tree is flattened (flattenJiraDocument); a value of another
// shape (list, number, boolean) states no description, so a held one is kept.
func jiraDescriptionText(value any) *string {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		text := flattenJiraDocument(typed)
		if text == "" {
			return nil
		}
		return &text
	case string:
		return nullableJiraString(typed)
	default:
		return nil
	}
}

const jiraDocumentMaxDepth = 32

// flattenJiraDocument joins the text nodes of an Atlassian Document Format
// tree: block nodes (paragraphs, headings, list items, code blocks) end in a
// newline, a hard break is a newline, a mention, emoji, status or card reads as
// its own text, and media reads as nothing. It stops once the text holds the
// most the description bound can keep (four bytes a rune at most), so a
// provider-sized tree costs a bounded amount.
func flattenJiraDocument(root map[string]any) string {
	var text strings.Builder
	limit := 4 * workItemDescriptionMaxRunes
	write := func(value string) {
		room := limit - text.Len()
		if len(value) > room {
			for room > 0 && !utf8.RuneStart(value[room]) {
				room--
			}
			value = value[:max(room, 0)]
		}
		text.WriteString(value)
	}
	var walk func(node map[string]any, depth int)
	walk = func(node map[string]any, depth int) {
		if depth > jiraDocumentMaxDepth || text.Len() >= limit {
			return
		}
		kind, _ := node["type"].(string)
		attrs, _ := node["attrs"].(map[string]any)
		switch kind {
		case "text":
			if value, ok := node["text"].(string); ok {
				write(value)
			}
			return
		case "hardBreak":
			text.WriteByte('\n')
			return
		case "mention", "emoji", "status":
			if value, ok := attrs["text"].(string); ok {
				write(value)
			}
			return
		case "inlineCard", "blockCard", "embedCard":
			if value, ok := attrs["url"].(string); ok {
				write(value)
			} else if data, ok := attrs["data"].(map[string]any); ok {
				if value, ok := data["url"].(string); ok {
					write(value)
				}
			}
			return
		case "media", "mediaSingle", "mediaGroup", "mediaInline":
			return
		}
		children, _ := node["content"].([]any)
		for _, child := range children {
			if mapped, ok := child.(map[string]any); ok {
				walk(mapped, depth+1)
			}
		}
		switch kind {
		case "doc", "bulletList", "orderedList", "table", "tableRow", "tableCell", "tableHeader":
		default:
			if text.Len() > 0 && !strings.HasSuffix(text.String(), "\n") {
				text.WriteByte('\n')
			}
		}
	}
	walk(root, 0)
	return strings.TrimSpace(text.String())
}

func nullableJiraString(value any) *string {
	if value == nil {
		return nil
	}
	valueString := stringFrom(value)
	if valueString == "" {
		return nil
	}
	return &valueString
}

func stringPointerIfNonEmpty(value any) *string { return nullableJiraString(value) }

func stringFrom(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func jiraMapValue(value any) (map[string]any, bool) {
	mapped, ok := value.(map[string]any)
	return mapped, ok
}

func mapSlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case nil:
		return nil
	default:
		return nil
	}
}

func jiraOptionString(claim Claim, key, fallback string) string {
	if claim.DatasetOptions != nil {
		if value, ok := claim.DatasetOptions[key]; ok {
			if option := strings.TrimSpace(stringFrom(value)); option != "" {
				return option
			}
		}
	}
	return fallback
}

func utf8RuneCount(value string) int {
	count := 0
	for range value {
		count++
	}
	return count
}

// jiraIssueCreatedAt is fields.created and nothing else. normalizeJiraWorkItem
// falls back to the sync clock for a missing value; the creation ADD must not,
// because occurred_at is a sorting-key member.
func jiraIssueCreatedAt(issue jiraWorkItemFixtureInput) *time.Time {
	fields, ok := jiraMapValue(issue.Raw["fields"])
	if !ok {
		return nil
	}
	return jiraTime(jiraField(fields, "created", issue.ObjectShape))
}

// jiraCreationMembership is CHAOS-7361's creation-time ADD for one issue.
// history is the issue's project rows already derived from the FULL changelog.
// The key written is the key work_items.project_key carries whenever the
// creation project is the issue's current project, so the presence view's
// project_key does not change when the item moves from the column arm to the
// transition arm.
func jiraCreationMembership(
	claim Claim,
	item jiraWorkItemRow,
	issue jiraWorkItemFixtureInput,
	history []projectmembership.Row,
	stats *membershipCreationStats,
	normalizedAt time.Time,
) (projectmembership.Row, bool) {
	createdAt := jiraIssueCreatedAt(issue)
	if createdAt == nil {
		if derefString(item.ProjectID) != "" || len(history) > 0 {
			stats.skip(projectmembership.SkipCreatedAtUnparseable)
		}
		return projectmembership.Row{}, false
	}
	template := projectmembership.Row{
		OrgID: claim.OrgID, RepoID: uuid.Nil, SubjectKind: projectmembership.SubjectWorkItem,
		SubjectID: item.WorkItemID, Provider: "jira", LastSynced: normalizedAt.UTC(),
	}
	row, outcome := projectmembership.CreationAdd(
		template, history, derefString(item.ProjectID), derefString(item.ProjectKey), *createdAt,
	)
	stats.record(outcome)
	if outcome != projectmembership.CreationAdded || validateJiraProjectMembership(row, claim) != nil {
		return projectmembership.Row{}, false
	}
	return row, true
}

// jiraCreationProjectName is the name for the creation project's catalog row:
// the issue's own project name when it is the current project, "" otherwise
// (a from-side project that no live lookup was made for).
func jiraCreationProjectName(item jiraWorkItemRow, projectID string) string {
	if projectID != "" && projectID == derefString(item.ProjectID) {
		return derefString(item.ProjectName)
	}
	return ""
}
