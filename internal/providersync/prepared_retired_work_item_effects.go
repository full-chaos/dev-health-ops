package providersync

import (
	"context"
	"log/slog"
	"maps"
	"slices"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// retiredWorkItemSyncDestinations are the nine tables computed from stored
// work-item rows. A work-items sync unit wrote them once; the daily job is
// their one writer now. The names stay here for one reason: a prepared
// manifest that an earlier binary stored for a unit in flight across the
// deploy still holds an effect for each of them.
var retiredWorkItemSyncDestinations = map[string]struct{}{
	"estimate_coverage_metrics_daily":  {},
	"investment_classifications_daily": {},
	"investment_metrics_daily":         {},
	"issue_type_metrics_daily":         {},
	"work_item_cycle_times":            {},
	"work_item_metrics_daily":          {},
	"work_item_state_durations_daily":  {},
	"work_item_team_attributions":      {},
	"work_item_user_metrics_daily":     {},
}

// retiredWorkItemSyncResultKeys are the result entries the derivation of
// those tables attached to a unit result. A unit that completes a manifest
// without writing the tables must not report that it wrote them.
var retiredWorkItemSyncResultKeys = []string{
	"team_inheritance", "team_attribution_written",
	"derived_destinations_implemented", "derived_destinations_unimplemented",
	"watermark_held_for_derived_gap",
}

// retiredWorkItemEffectsSkipper completes a prepared work-items manifest that
// an earlier binary stored. Its raw effects go to the real sink and readback;
// an effect for a retired destination is settled without a store call.
//
// WHY THE MANIFEST IS COMPLETED AND NOT DISCARDED. The discard path for a
// superseded manifest re-runs the route from the claim, and it refuses when
// any effect of the document already committed or a writing effect cannot be
// read back. A unit in flight across this deploy is in exactly that state as
// soon as its first effect landed, and a readback of a retired destination
// can never be made: the sync sinks refuse those tables. The unit would stop
// with ErrEffectRecoveryUnsafe and need a person, for a document whose only
// difference is rows this unit no longer owes. Completing it loses nothing:
// every raw row of the document is applied, and the retired tables are
// recomputed from stored rows by the daily job.
//
// The stored document is not changed. The ledger entries, the payload digest
// and the per-effect digests are checked exactly as for any replay; the
// retired effects stay in the list handed to CommitPrepared, so the ledger
// and the manifest still agree entry by entry. Their ledger entries are
// settled so the generation can complete; the log line and the counter are
// the record that no row was written for them.
type retiredWorkItemEffectsSkipper struct {
	sink     EffectSink
	readback EffectReadback
	retired  []string
	skipped  int
}

// newRetiredWorkItemEffectsSkipper answers ok only for the one case it
// exists for: a work-items claim of a provider whose manifest holds every
// destination the route emits today, each once, plus one or more retired
// destinations, each once, and nothing else. Any other difference is not this
// deploy and keeps the discard decision.
func newRetiredWorkItemEffectsSkipper(
	claim Claim,
	descriptor CompleteRouteDescriptor,
	manifest PreparedRouteManifest,
	committer EffectCommitter,
) (*retiredWorkItemEffectsSkipper, bool) {
	if claim.Dataset != "work-items" || len(descriptor.Destinations) == 0 {
		return nil, false
	}
	switch claim.Provider {
	case "github", "gitlab", "jira", "linear":
	default:
		return nil, false
	}
	current := make(map[string]int, len(descriptor.Destinations))
	for _, destination := range descriptor.Destinations {
		if _, retired := retiredWorkItemSyncDestinations[destination]; retired {
			return nil, false
		}
		current[destination] = 0
	}
	if len(current) != len(descriptor.Destinations) {
		return nil, false
	}
	retired := make([]string, 0, len(retiredWorkItemSyncDestinations))
	for _, effect := range manifest.Batch.Effects {
		if _, known := current[effect.Destination]; known {
			current[effect.Destination]++
			continue
		}
		if _, isRetired := retiredWorkItemSyncDestinations[effect.Destination]; !isRetired ||
			slices.Contains(retired, effect.Destination) {
			return nil, false
		}
		retired = append(retired, effect.Destination)
	}
	for _, seen := range current {
		if seen != 1 {
			return nil, false
		}
	}
	if len(retired) == 0 {
		return nil, false
	}
	slices.Sort(retired)
	return &retiredWorkItemEffectsSkipper{
		sink: committer.Sink, readback: committer.Readback, retired: retired,
	}, true
}

func (skipper *retiredWorkItemEffectsSkipper) isRetired(destination string) bool {
	return slices.Contains(skipper.retired, destination)
}

// WriteEffect writes nothing for a retired destination.
func (skipper *retiredWorkItemEffectsSkipper) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if skipper.isRetired(effect.Destination) {
		skipper.skipped++
		return nil
	}
	if skipper.sink == nil {
		return ErrInvalidConfiguration
	}
	return skipper.sink.WriteEffect(ctx, claim, effect)
}

// InspectEffect reports a retired destination as absent, so an effect the
// earlier binary left in the writing state goes back to pending and is then
// settled by WriteEffect without a store call. Whether its rows landed does
// not matter: the daily job replaces them.
func (skipper *retiredWorkItemEffectsSkipper) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if skipper.isRetired(effect.Destination) {
		return EffectAbsent, nil
	}
	if skipper.readback == nil {
		return EffectConflict, ErrEffectRecoveryAmbiguous
	}
	return skipper.readback.InspectEffect(ctx, claim, effect)
}

// currentBatch is the manifest batch as the route emits it today: without
// the retired effects, and without the result entries their derivation
// attached. The input is not modified.
func (skipper *retiredWorkItemEffectsSkipper) currentBatch(batch CompleteRouteBatch) CompleteRouteBatch {
	effects := make([]EffectBatch, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		if !skipper.isRetired(effect.Destination) {
			effects = append(effects, effect)
		}
	}
	result := maps.Clone(batch.Result)
	for _, key := range retiredWorkItemSyncResultKeys {
		delete(result, key)
	}
	batch.Effects, batch.Result = effects, result
	return batch
}

// observe is the one scalar log line and the one bounded counter of a unit
// that completed such a manifest.
func (skipper *retiredWorkItemEffectsSkipper) observe(
	metrics *providerfoundation.Metrics, claim Claim, committed EffectCommitResult,
) {
	metrics.RecordPreparedRetiredWorkItemEffectsSkipped(claim.Provider, skipper.skipped)
	slog.Warn("provider_sync.prepared_snapshot_retired_effects_skipped",
		"provider", claim.Provider, "dataset", claim.Dataset,
		"unit", claim.ID, "generation", claim.GenerationKey(),
		"retired_effects", len(skipper.retired),
		"retired_effects_skipped", skipper.skipped,
		"effects_written", committed.Written,
		"effects_already_committed", committed.Skipped,
	)
}

var _ EffectSink = (*retiredWorkItemEffectsSkipper)(nil)
var _ EffectReadback = (*retiredWorkItemEffectsSkipper)(nil)
