package providersync

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// workItemRow is the one work_items row every provider writes (CHAOS-9852).
// github, gitlab and jira write it directly; linear converts it with
// linearWorkItemRowFromBase. buildWorkItemRow is its only constructor:
// TestWorkItemRowBaseCensus fails on a row literal built anywhere else.
type workItemRow = githubWorkItemRow

// workItemSource is HOW one provider reads each field of a work_items row from
// its own payload. There is one method per row field, named after the field;
// the census fails when a row field has no method here, so a new column cannot
// be left unread by any provider. OrgID, LastSynced and DueAtAbsent are owned
// by the base (workItemBaseOwnedFields).
type workItemSource interface {
	// sourceKind names the adapter kind for logs and for the hierarchy matrix
	// (workItemHierarchyExceptions).
	sourceKind() string
	// validateRow is the provider's own row rule, run after the shared one.
	validateRow(claim Claim, row workItemRow) error

	WorkItemID() string
	Provider() string
	Title() string
	Type() string
	Status() string
	StatusRaw() *string
	Description() *string
	RepoID() *uuid.UUID
	NativeTeamKey() *string
	ProjectKey() *string
	ProjectID() *string
	ProjectName() *string
	Assignees() []string
	Reporter() *string
	CreatedAt() time.Time
	UpdatedAt() time.Time
	StartedAt() *time.Time
	CompletedAt() *time.Time
	ClosedAt() *time.Time
	Labels() []string
	StoryPoints() *float64
	SprintID() *string
	SprintName() *string
	ParentID() workItemRef
	EpicID() workItemRef
	URL() *string
	PriorityRaw() *string
	ServiceClass() *string
	DueAt() *time.Time
}

// workItemBaseOwnedFields are the row fields no provider reads.
var workItemBaseOwnedFields = map[string]string{
	"OrgID":       "the claim's org, stamped by the base",
	"LastSynced":  "the unit's normalizedAt, stamped by the base",
	"DueAtAbsent": "set only when an effect is decoded, never when a row is built",
}

// workItemRef is a provider's reading of one hierarchy field (parent_id or
// epic_id). Key is the provider-native key without the provider prefix; the
// base builds the stored id "<prefix>:<key>". Present marks that the payload
// carried the reference object at all, so a present reference with a blank key
// is logged as dropped instead of being stored as a bare "<prefix>:".
// Exception, when set, is the declared reason this adapter does not read the
// field (workItemHierarchyExceptions); it is never stored.
type workItemRef struct {
	Present   bool
	Key       string
	Exception *workItemFieldException
}

// workItemFieldException is a named, tested reason why one adapter kind does
// not read one hierarchy field. Ticket is the Linear issue that owns it.
type workItemFieldException struct {
	Ticket string
	Reason string
}

func workItemRefKey(key string) workItemRef {
	return workItemRef{Present: true, Key: key}
}

func workItemRefAbsent() workItemRef { return workItemRef{} }

// workItemRefFromPointer reads an optional provider key.
func workItemRefFromPointer(key *string) workItemRef {
	if key == nil {
		return workItemRef{}
	}
	return workItemRefKey(*key)
}

// workItemRefPrefix is the provider prefix of every work_item_id that a
// parent_id or epic_id of that provider can point at.
var workItemRefPrefix = map[string]string{
	"github": "gh:",
	"gitlab": "gitlab:",
	"jira":   "jira:",
	"linear": "linear:",
}

const (
	workItemFieldParentID = "parent_id"
	workItemFieldEpicID   = "epic_id"
)

// workItemHierarchyExceptions is the declared matrix of adapter kinds that do
// not read a hierarchy field, each with its ticket. Every other (kind, field)
// pair must be proven to fill the field by TestWorkItemHierarchyMatrix.
var workItemHierarchyExceptions = map[string]map[string]workItemFieldException{
	githubIssueSourceKind: {
		workItemFieldParentID: {Ticket: "CHAOS-9856", Reason: "the GitHub sub-issue parent is not collected yet (GraphQL Issue.parent)"},
		workItemFieldEpicID:   {Ticket: "CHAOS-9852", Reason: "GitHub has no epic object"},
	},
	githubPullRequestSourceKind: {
		workItemFieldParentID: {Ticket: "CHAOS-9852", Reason: "a GitHub pull request has no parent work item"},
		workItemFieldEpicID:   {Ticket: "CHAOS-9852", Reason: "GitHub has no epic object"},
	},
	githubProjectV2ItemSourceKind: {
		workItemFieldParentID: {Ticket: "CHAOS-9856", Reason: "the GitHub sub-issue parent is not collected yet (GraphQL Issue.parent)"},
		workItemFieldEpicID:   {Ticket: "CHAOS-9852", Reason: "GitHub has no epic object"},
	},
	gitlabIssueSourceKind: {
		workItemFieldParentID: {Ticket: "CHAOS-9858", Reason: "the GitLab work-item hierarchy parent is not collected yet"},
		workItemFieldEpicID:   {Ticket: "CHAOS-9854", Reason: "the issue payload epic is not read yet (Python build_epic_id_for_issue)"},
	},
	gitlabMergeRequestSourceKind: {
		workItemFieldParentID: {Ticket: "CHAOS-9852", Reason: "a GitLab merge request has no parent work item"},
		workItemFieldEpicID:   {Ticket: "CHAOS-9852", Reason: "a GitLab merge request belongs to no epic"},
	},
	linearIssueSourceKind: {
		workItemFieldEpicID: {Ticket: "CHAOS-9852", Reason: "Linear has no epic object; Python writes None (providers/linear/normalize.py:406)"},
	},
}

// declaredWorkItemException is how an adapter states a declared exception: it
// returns the matrix entry, so code and census cannot disagree.
func declaredWorkItemException(kind, field string) workItemRef {
	exception, ok := workItemHierarchyExceptions[kind][field]
	if !ok {
		panic("providersync: undeclared work item hierarchy exception " + kind + "/" + field)
	}
	return workItemRef{Exception: &exception}
}

// buildWorkItemRow is the only constructor of a work_items row. It reads every
// field through the provider's source, stamps the base-owned fields, builds the
// hierarchy ids with one grammar, and runs the shared and provider rules.
func buildWorkItemRow(claim Claim, source workItemSource, normalizedAt time.Time) (workItemRow, error) {
	provider := source.Provider()
	if provider != claim.Provider || workItemRefPrefix[provider] == "" || normalizedAt.IsZero() {
		return workItemRow{}, ErrInvalidConfiguration
	}
	row := workItemRow{
		WorkItemID: source.WorkItemID(), Provider: provider, Title: source.Title(),
		Type: source.Type(), Status: source.Status(), StatusRaw: source.StatusRaw(),
		Description: source.Description(), RepoID: source.RepoID(),
		NativeTeamKey: source.NativeTeamKey(), ProjectKey: source.ProjectKey(),
		ProjectID: source.ProjectID(), ProjectName: source.ProjectName(),
		Assignees: source.Assignees(), Reporter: source.Reporter(),
		CreatedAt: source.CreatedAt(), UpdatedAt: source.UpdatedAt(),
		StartedAt: source.StartedAt(), CompletedAt: source.CompletedAt(),
		ClosedAt: source.ClosedAt(), Labels: source.Labels(),
		StoryPoints: source.StoryPoints(), SprintID: source.SprintID(),
		SprintName: source.SprintName(), URL: source.URL(),
		PriorityRaw: source.PriorityRaw(), ServiceClass: source.ServiceClass(),
		DueAt: source.DueAt(), OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	row.ParentID = workItemHierarchyID(source, row.WorkItemID, workItemFieldParentID, source.ParentID())
	row.EpicID = workItemHierarchyID(source, row.WorkItemID, workItemFieldEpicID, source.EpicID())
	if err := validateWorkItemRow(claim, row); err != nil {
		return workItemRow{}, err
	}
	if err := source.validateRow(claim, row); err != nil {
		return workItemRow{}, err
	}
	return row, nil
}

// workItemHierarchyID applies the one parent/epic key grammar of every
// provider: "<provider prefix>:<key>" with the key trimmed; a blank key is no
// reference (never a bare "<prefix>:", CHAOS-9835).
func workItemHierarchyID(source workItemSource, workItemID, field string, ref workItemRef) *string {
	if ref.Exception != nil {
		return nil
	}
	key := strings.TrimSpace(ref.Key)
	if key == "" {
		if ref.Present {
			slog.Debug(workItemHierarchyDroppedEvent, "provider", source.Provider(),
				"kind", source.sourceKind(), "field", field, "work_item_id", workItemID)
		}
		return nil
	}
	value := workItemRefPrefix[source.Provider()] + key
	return &value
}

// validateWorkItemRow is the rule every provider's row meets. It is the common
// part of the provider rules it replaced; a title is checked by the provider
// rules, because a GitHub Projects v2 item was never required to have one.
func validateWorkItemRow(claim Claim, row workItemRow) error {
	prefix := workItemRefPrefix[row.Provider]
	if row.Provider != claim.Provider || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil ||
		row.Labels == nil || row.LastSynced.IsZero() ||
		!workItemHierarchyIDValid(row.ParentID, prefix) || !workItemHierarchyIDValid(row.EpicID, prefix) {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func workItemHierarchyIDValid(value *string, prefix string) bool {
	return value == nil || (strings.HasPrefix(*value, prefix) && strings.TrimSpace(strings.TrimPrefix(*value, prefix)) != "")
}

const (
	workItemHierarchyDroppedEvent = "providersync.work_items.hierarchy_ref_dropped_blank_key"
	workItemHierarchyEvent        = "providersync.work_items.hierarchy"
)

// logWorkItemHierarchy is the per-write summary of the hierarchy fields:
// counts only, no ids. A reader checks it to see whether a provider's batch
// carried parents and epics at all.
func logWorkItemHierarchy(ctx context.Context, rows []workItemStoredRow) {
	byProvider := map[string][3]int{}
	for _, row := range rows {
		counts := byProvider[row.Provider]
		counts[0]++
		if row.ParentID != "" {
			counts[1]++
		}
		if row.EpicID != "" {
			counts[2]++
		}
		byProvider[row.Provider] = counts
	}
	for provider, counts := range byProvider {
		slog.DebugContext(ctx, workItemHierarchyEvent, "provider", provider,
			"rows", counts[0], "with_parent_id", counts[1], "with_epic_id", counts[2])
	}
}

// linearWorkItemRowFromBase is the one conversion of a base row into the
// Linear effect row; Linear rows carry no repository.
func linearWorkItemRowFromBase(row workItemRow) linearWorkItemRow {
	return linearWorkItemRow{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Title: row.Title, Type: row.Type,
		Status: row.Status, StatusRaw: row.StatusRaw, Description: row.Description,
		RepoID: nil, NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName, Assignees: row.Assignees,
		Reporter: row.Reporter, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		StartedAt: row.StartedAt, CompletedAt: row.CompletedAt, ClosedAt: row.ClosedAt,
		Labels: row.Labels, StoryPoints: row.StoryPoints, SprintID: row.SprintID,
		SprintName: row.SprintName, ParentID: row.ParentID, EpicID: row.EpicID, URL: row.URL,
		PriorityRaw: row.PriorityRaw, ServiceClass: row.ServiceClass, DueAt: row.DueAt,
		OrgID: row.OrgID, LastSynced: row.LastSynced,
	}
}
