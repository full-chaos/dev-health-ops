package providersync

import (
	"context"
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

var _ LinearWorkItemEffectAdapter = LinearWorkItemDependenciesClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearWorkItemReopenEventsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearWorkItemInteractionsClickHouseAdapter{}
var _ LinearWorkItemEffectAdapter = LinearSprintsClickHouseAdapter{}
