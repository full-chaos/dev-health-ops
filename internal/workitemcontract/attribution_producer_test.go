package workitemcontract

import (
	"testing"
	"time"
)

// TestAttributionWriterRankIsATotalOrderOverTheSharedVocabulary pins that
// every name AttributionWriters returns has a rank, the ranks are pairwise
// distinct, and daily outranks backstop -- the pair the demonstrated
// collision (the daily family vs. the remaining-family backstop) actually
// reaches.
func TestAttributionWriterRankIsATotalOrderOverTheSharedVocabulary(t *testing.T) {
	seen := map[int]string{}
	for _, writer := range AttributionWriters() {
		rank := AttributionWriterRank(writer)
		if rank < 0 {
			t.Fatalf("AttributionWriterRank(%q) = %d, want a non-negative rank for every "+
				"writer in the shared vocabulary", writer, rank)
		}
		if existing, ok := seen[rank]; ok {
			t.Fatalf("writers %q and %q share rank %d -- ranks must be a total order, "+
				"or an exact-computed_at collision between them stays non-deterministic",
				existing, writer, rank)
		}
		seen[rank] = writer
	}
	if AttributionWriterRank(AttributionWriterDaily) <= AttributionWriterRank(AttributionWriterBackstop) {
		t.Fatalf("daily rank %d must exceed backstop rank %d -- the daily family owns this "+
			"table for its window and must win an exact tie against the backstop",
			AttributionWriterRank(AttributionWriterDaily), AttributionWriterRank(AttributionWriterBackstop))
	}
}

// TestAttributionWriterRankRejectsAnUnknownName pins the fail-open-to-unfolded
// default: an unrecognized writer name gets no rank and AttributionVersionFold
// leaves it untouched rather than guessing. Callers still refuse an unknown
// writer before a row is built (WorkItemAttributionProducer.Validate and the
// providersync equivalent) -- this is a defense against a name added to one
// side's vocabulary without this map, not a path any current writer reaches.
func TestAttributionWriterRankRejectsAnUnknownName(t *testing.T) {
	if rank := AttributionWriterRank("not-a-real-writer"); rank != -1 {
		t.Fatalf("AttributionWriterRank(unknown) = %d, want -1", rank)
	}
}

// TestAttributionVersionFoldBreaksAnExactTieByRank pins the fold's core
// contract: two producers stamping the IDENTICAL instant diverge only by
// rank, and the higher-ranked producer's folded value is always the larger
// one, regardless of which producer is folded first.
func TestAttributionVersionFoldBreaksAnExactTieByRank(t *testing.T) {
	tie := time.Date(2026, 9, 16, 2, 18, 36, 147*int(time.Millisecond), time.UTC)
	precision := time.Millisecond

	daily := AttributionVersionFold(tie, AttributionWriterDaily, precision)
	backstop := AttributionVersionFold(tie, AttributionWriterBackstop, precision)
	sync := AttributionVersionFold(tie, AttributionWriterSync, precision)

	if !daily.After(backstop) {
		t.Fatalf("daily-folded %v must be after backstop-folded %v at an exact tie", daily, backstop)
	}
	if !daily.After(sync) {
		t.Fatalf("daily-folded %v must be after sync-folded %v at an exact tie", daily, sync)
	}
	if !backstop.After(sync) {
		t.Fatalf("backstop-folded %v must be after sync-folded %v at an exact tie", backstop, sync)
	}
}

// TestAttributionVersionFoldIsZeroForSync pins that the lowest-ranked writer's
// folded value is byte-identical to the plain truncated stamp: sync's rank is
// 0 by construction, so this table's existing sync-only exact-value fixtures
// see no change from the fold.
func TestAttributionVersionFoldIsZeroForSync(t *testing.T) {
	stamp := time.Date(2026, 9, 16, 2, 18, 36, 147123456, time.UTC)
	precision := time.Millisecond
	want := stamp.UTC().Truncate(precision)
	if got := AttributionVersionFold(stamp, AttributionWriterSync, precision); !got.Equal(want) {
		t.Fatalf("AttributionVersionFold(sync) = %v, want the plain truncated stamp %v", got, want)
	}
}

// TestAttributionVersionFoldPreservesOrderForDistinctStamps pins that the
// fold never changes the outcome between two ALREADY-DISTINCT computed_at
// values: a later stamp from the lowest-ranked writer still beats an earlier
// stamp from the highest-ranked one, as long as the real gap between them
// exceeds the rank spread -- the case every actual run of this table's
// writers falls into, since computed_at is stamped once per run.
func TestAttributionVersionFoldPreservesOrderForDistinctStamps(t *testing.T) {
	precision := time.Millisecond
	earlier := time.Date(2026, 9, 16, 2, 18, 36, 0, time.UTC)
	later := earlier.Add(time.Minute)

	dailyEarlier := AttributionVersionFold(earlier, AttributionWriterDaily, precision)
	syncLater := AttributionVersionFold(later, AttributionWriterSync, precision)
	if !syncLater.After(dailyEarlier) {
		t.Fatalf("sync's later stamp %v must still beat daily's earlier stamp %v -- rank must "+
			"only decide an EXACT tie, not overturn a real gap", syncLater, dailyEarlier)
	}
}
