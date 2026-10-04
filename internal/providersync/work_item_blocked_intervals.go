package providersync

import (
	"context"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// loadWorkItemBlockedIntervalsForProvider returns, per work item id, the
// spans in which that item has an open blocker (CHAOS-8493), for the items
// of one sync unit.
//
// The decision is workitemmetrics.BlockedIntervalsByItem -- the ONE rule this
// deriver shares with the work_item_state daily family, the other writer of
// work_item_state_durations_daily. This function only assembles its inputs,
// from two places:
//
//   - the STORE, through the source: the blocking relations that name one of
//     the unit's items, and the stored items that are the other ends;
//   - the UNIT itself: the relations and the item rows this sync just
//     normalized. They are not in the store yet when the deriver runs, and
//     they are NEWER than what is, so they are merged in rather than read
//     back: of two rows for one relation the one synced last is the
//     relation, and of two rows for one item the rule keeps the one synced
//     last.
//
// Merging the fresh rows is what makes "the provider no longer reports this
// relation" work at sync time: an item re-synced without a link it used to
// have carries a last_synced newer than the stored relation row.
//
// A failed stored read fails the unit, for the reason
// LoadStoredBlockingFacts gives.
func loadWorkItemBlockedIntervalsForProvider(
	ctx context.Context,
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
) (map[string][]workitemmetrics.BlockedInterval, error) {
	if ctx == nil || source == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "work-items" {
		return nil, ErrInvalidConfiguration
	}
	if len(rows.WorkItems) == 0 {
		return nil, nil
	}
	idSet := make(map[string]struct{}, len(rows.WorkItems))
	ends := make([]workitemmetrics.RelationEnd, 0, len(rows.WorkItems))
	for _, item := range rows.WorkItems {
		if item.OrgID != claim.OrgID {
			return nil, providerfoundation.ErrInvalidScope
		}
		idSet[item.WorkItemID] = struct{}{}
		ends = append(ends, workitemmetrics.RelationEnd{
			WorkItemID: item.WorkItemID, Provider: item.Provider, Status: item.Status,
			CreatedAt: item.CreatedAt.UTC(), CompletedAt: item.CompletedAt, LastSynced: item.LastSynced.UTC(),
		})
	}
	itemIDs := make([]string, 0, len(idSet))
	for id := range idSet {
		itemIDs = append(itemIDs, id)
	}
	sort.Strings(itemIDs)

	fresh := make([]workitemmetrics.BlockingRelation, 0, len(rows.Dependencies))
	for _, dependency := range rows.Dependencies {
		if dependency.OrgID != claim.OrgID {
			return nil, providerfoundation.ErrInvalidScope
		}
		fresh = append(fresh, workitemmetrics.BlockingRelation{
			SourceID: dependency.SourceWorkItemID, TargetID: dependency.TargetWorkItemID,
			RelationshipType: dependency.RelationshipType,
			SemanticsVersion: dependency.RelationshipSemanticsVersion,
			LastSynced:       dependency.LastSynced.UTC(),
		})
	}

	stored, storedEnds, err := source.LoadStoredBlockingFacts(ctx, claim, itemIDs, fresh)
	if err != nil {
		return nil, err
	}
	return workitemmetrics.BlockedIntervalsByItem(
		mergeBlockingRelations(stored, fresh), append(storedEnds, ends...),
	), nil
}

// mergeBlockingRelations returns one row per (source, target, type): of a
// stored and a fresh row for the same relation, the one synced last. The
// result is in key order, so it does not depend on the order of its inputs.
//
// The two times a relation start comes from are carried across the merge:
//
//   - FirstSeenAt is the STORED first-seen time when the relation is stored.
//     A fresh relation that is not stored yet was first seen by THIS sync, so
//     its first-seen time is its own last_synced -- the value the store will
//     hold for it once this unit's rows are written. A stored relation with
//     no stored first-seen time keeps none: its start is not known.
//   - StartedAt (the provider's own link time) is the surviving row's, else
//     the other row's: a provider that reported the time once is not
//     forgotten because a later payload omitted it.
func mergeBlockingRelations(stored, fresh []workitemmetrics.BlockingRelation) []workitemmetrics.BlockingRelation {
	type key struct{ source, target, relationship string }
	keyOf := func(relation workitemmetrics.BlockingRelation) key {
		return key{relation.SourceID, relation.TargetID, relation.RelationshipType}
	}
	newest := make(map[key]workitemmetrics.BlockingRelation, len(stored)+len(fresh))
	for _, relation := range stored {
		k := keyOf(relation)
		if existing, seen := newest[k]; seen && !relation.LastSynced.After(existing.LastSynced) {
			continue
		}
		newest[k] = relation
	}
	for _, relation := range fresh {
		k := keyOf(relation)
		existing, isStored := newest[k]
		merged := relation
		if !isStored {
			// First written by this sync (or by an earlier row of this same
			// unit): the earliest of them is when the relation was first seen.
			firstSeen := relation.LastSynced.UTC()
			merged.FirstSeenAt = &firstSeen
			newest[k] = merged
			continue
		}
		other := existing
		if !relation.LastSynced.After(existing.LastSynced) {
			merged, other = existing, relation
		}
		merged.FirstSeenAt = existing.FirstSeenAt
		if merged.StartedAt == nil {
			merged.StartedAt = other.StartedAt
		}
		newest[k] = merged
	}
	keys := make([]key, 0, len(newest))
	for k := range newest {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(left, right int) bool {
		a, b := keys[left], keys[right]
		if a.source != b.source {
			return a.source < b.source
		}
		if a.target != b.target {
			return a.target < b.target
		}
		return a.relationship < b.relationship
	})
	merged := make([]workitemmetrics.BlockingRelation, 0, len(keys))
	for _, k := range keys {
		merged = append(merged, newest[k])
	}
	return merged
}
