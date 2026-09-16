package workitemcontract

import "time"

// Three independent paths write work_item_team_attributions: the daily
// `work_item_attribution` metrics family, the remaining-family staleness
// backstop, and providersync's sync-time deriver. They resolve the same
// cascade over the same keys into a ReplacingMergeTree(computed_at) whose
// sorting key carries no producer column, so two producers' rows for one key
// are indistinguishable once they are stored.
//
// These names are the vocabulary the `writer` column stores. They live here,
// in the package that already owns this table's destination declaration,
// because the writing packages do not import each other -- a second spelling
// in one of them would silently split the column's value space, and a query
// filtering on one spelling would report zero rows for a path that is
// writing normally.
const (
	// AttributionWriterDaily is the daily metrics family, scoped to a
	// partition's (org, repos, day) window.
	AttributionWriterDaily = "daily"
	// AttributionWriterBackstop is the remaining-family backstop, scoped to
	// the work items whose ownership changed since its last run.
	AttributionWriterBackstop = "backstop"
	// AttributionWriterSync is providersync's sync-time deriver, scoped to the
	// work items one sync claim just ingested.
	AttributionWriterSync = "sync"
)

// AttributionWriters returns the writer names in declaration order. A reader
// grouping rows by producer uses this rather than a literal list, so a fourth
// path added above cannot be missed by a dashboard or a divergence check.
func AttributionWriters() []string {
	return []string{
		AttributionWriterDaily,
		AttributionWriterBackstop,
		AttributionWriterSync,
	}
}

// attributionWriterRank is the fixed producer tie-break this table's
// ReplacingMergeTree(computed_at) has no column for: at an EXACT computed_at
// tie between two producers' rows for one key, the higher rank survives.
// It is a total order over the three writers, not a two-way flag, because
// all three target the same sorting key and any pair can collide.
//
// AttributionWriterDaily ranks highest: the daily family OWNS
// work_item_team_attributions for its (org, repo, day) window (see
// work_item_attribution_native_executor.go's "Dual writers" note) and is the
// comprehensive recompute the other three metrics.daily families wait on.
// AttributionWriterBackstop ranks below it: the backstop only narrows to
// staleness OUTSIDE that window -- a supplementary patch over the daily
// family's coverage, not a second source of the same authority.
// AttributionWriterSync ranks lowest and carries rank 0 (no offset): it is
// an independent, per-item real-time producer with no ownership relationship
// to the other two, and giving it the zero rank keeps its stored computed_at
// byte-identical to the unfolded value, so this table's existing sync-only
// exact-value fixtures are unaffected by the fold.
var attributionWriterRank = map[string]int{
	AttributionWriterSync:     0,
	AttributionWriterBackstop: 1,
	AttributionWriterDaily:    2,
}

// AttributionWriterRank returns writer's fixed tie-break rank, or -1 for a
// name outside the shared vocabulary.
func AttributionWriterRank(writer string) int {
	rank, ok := attributionWriterRank[writer]
	if !ok {
		return -1
	}
	return rank
}

// AttributionVersionFold folds writer's fixed rank into the value this
// table's ReplacingMergeTree(computed_at) stores as its version -- there is
// no separate version column, so the fold has to live in computed_at itself.
// computedAt is truncated to precision first, matching the column's stored
// precision and every writer's own stamp truncation, then advanced by
// writer's rank in whole units of precision.
//
// This only changes the outcome between two producers whose computed_at
// values are IDENTICAL at that precision: computed_at is stamped once per
// run (not per row, not per microsecond), so two different producers'
// independent wall-clock stamps landing within a few units of precision of
// each other, without being identical, is not a real production scenario
// this table's writers approach. An unrecognized writer name is left
// unfolded; WorkItemAttributionProducer.Validate and the equivalent
// providersync checks refuse those before a row is ever built.
func AttributionVersionFold(computedAt time.Time, writer string, precision time.Duration) time.Time {
	truncated := computedAt.UTC().Truncate(precision)
	rank := AttributionWriterRank(writer)
	if rank <= 0 {
		return truncated
	}
	return truncated.Add(time.Duration(rank) * precision)
}
