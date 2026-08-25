package providersync

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
)

// The raw Linear destinations use the same ClickHouse projection as the
// provider-neutral direct work-graph sinks. These wrappers keep provider
// ownership explicit: Linear supplies collection and normalization, while the
// existing adapters remain the single implementation of column order,
// ReplacingMergeTree readback, deduplication, and empty-row behavior.

func linearRawEffectIdentity(identity LinearWorkItemEffectIdentity) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: identity.OrgID, Provider: "linear", Dataset: identity.Dataset,
		Generation: identity.Generation, Destination: identity.Destination,
		ContentDigest: identity.ContentDigest, RowCount: identity.RowCount,
	}
}

func linearRawIdentityValid(identity LinearWorkItemEffectIdentity, destination string) bool {
	return identity.Provider == "linear" && identity.Dataset == "work-items" &&
		identity.OrgID != "" && identity.Generation != "" && identity.Destination == destination
}

type LinearWorkItemDependenciesClickHouseAdapter struct {
	Delegate GitHubWorkItemDependenciesClickHouseAdapter
}

func (adapter LinearWorkItemDependenciesClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "work_item_dependencies") || effect.Destination != "work_item_dependencies" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemDependencyRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
			row.SourceWorkItemID == "" || row.TargetWorkItemID == "" || row.RelationshipType == "" {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

func (adapter LinearWorkItemDependenciesClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "work_item_dependencies") || effect.Destination != "work_item_dependencies" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemDependencyRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
			row.SourceWorkItemID == "" || row.TargetWorkItemID == "" || row.RelationshipType == "" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

type LinearWorkItemReopenEventsClickHouseAdapter struct {
	Delegate GitHubWorkItemReopenEventsClickHouseAdapter
}

func (adapter LinearWorkItemReopenEventsClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "work_item_reopen_events") || effect.Destination != "work_item_reopen_events" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemReopenRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.WorkItemID == "" || row.FromStatus == "" || row.ToStatus == "" {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

func (adapter LinearWorkItemReopenEventsClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "work_item_reopen_events") || effect.Destination != "work_item_reopen_events" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemReopenRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.WorkItemID == "" || row.FromStatus == "" || row.ToStatus == "" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

type LinearWorkItemInteractionsClickHouseAdapter struct {
	Delegate GitHubWorkItemInteractionsClickHouseAdapter
}

func (adapter LinearWorkItemInteractionsClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "work_item_interactions") || effect.Destination != "work_item_interactions" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemInteractionRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.InteractionType != "comment" || row.BodyLength < 0 {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

func (adapter LinearWorkItemInteractionsClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "work_item_interactions") || effect.Destination != "work_item_interactions" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearWorkItemInteractionRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.InteractionType != "comment" || row.BodyLength < 0 {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

type LinearSprintsClickHouseAdapter struct {
	Delegate GitHubSprintsClickHouseAdapter
}

func (adapter LinearSprintsClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "sprints") || effect.Destination != "sprints" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearSprintRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.SprintID == "" || row.Name == nil || row.State == nil {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

func (adapter LinearSprintsClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "sprints") || effect.Destination != "sprints" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[linearSprintRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.SprintID == "" || row.Name == nil || row.State == nil {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

// CHAOS-4193: the two project-membership destinations, delegating to the
// SAME provider-neutral adapters CHAOS-4194's github producer already uses --
// projectmembership's two tables have one storage implementation, not one
// per provider.

type LinearProjectMembershipClickHouseAdapter struct {
	Delegate GitHubProjectMembershipClickHouseAdapter
}

// linearProjectMembershipRowValid mirrors linearProjectMembershipValid's
// (row.ToProjectID != "" || row.FromProjectID != "") acceptance at the sink
// boundary. A prior draft required ToProjectID alone, which rejected every
// valid removal (from=P, to="") the normalizer emits -- a single removal
// then aborted the whole effect and stalled the Linear sync watermark
// (codex review finding, CHAOS-4193).
func linearProjectMembershipRowValid(row projectmembership.Row, orgID string) bool {
	return row.OrgID == orgID && row.Provider == "linear" &&
		row.SubjectKind == projectmembership.SubjectWorkItem &&
		row.SubjectID != "" && (row.ToProjectID != "" || row.FromProjectID != "") &&
		row.EventID != ""
}

func (adapter LinearProjectMembershipClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "project_membership_transitions") || effect.Destination != "project_membership_transitions" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[projectmembership.Row](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if !linearProjectMembershipRowValid(row, identity.OrgID) {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

func (adapter LinearProjectMembershipClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "project_membership_transitions") || effect.Destination != "project_membership_transitions" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[projectmembership.Row](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if !linearProjectMembershipRowValid(row, identity.OrgID) {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

type LinearProjectCatalogClickHouseAdapter struct {
	Delegate GitHubProjectCatalogClickHouseAdapter
}

func (adapter LinearProjectCatalogClickHouseAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if !linearRawIdentityValid(identity, "projects") || effect.Destination != "projects" {
		return ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[projectmembership.CatalogRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.ID == "" {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, linearRawEffectIdentity(identity), effect)
}

// InspectLinearWorkItemEffect deliberately does NOT delegate to the GitHub
// catalog adapter's readback. That readback compares the stored row's
// updated_at against the row we wrote (workItemVersionVerdict) and reports
// EffectConflict whenever the stored version is newer. Our ensure-rows are
// epoch-anchored (linearEnsureProjectsRow) so they never win a real sync --
// which means ANY row Linear's own richer catalogue writer has ever placed at
// this key is, by construction, "newer" than ours, and the delegated readback
// would report EffectConflict on essentially every normal ensure, not just
// during crash recovery (codex review finding, CHAOS-4193). existenceOnlyProjectCatalogReadback
// verdicts on presence alone, which is the ensure-row's actual job.
func (adapter LinearProjectCatalogClickHouseAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if !linearRawIdentityValid(identity, "projects") || effect.Destination != "projects" {
		return EffectConflict, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[projectmembership.CatalogRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "linear" || row.ID == "" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return existenceOnlyProjectCatalogReadback(ctx, adapter.Delegate.Conn, rows)
}

var _ LinearWorkItemEffectAdapter = LinearWorkItemDependenciesClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearWorkItemReopenEventsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearWorkItemInteractionsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearSprintsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearProjectMembershipClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearProjectCatalogClickHouseAdapter{}
