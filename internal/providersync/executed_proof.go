package providersync

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExecutedProofWaiver is an explicit, dated, reasoned operator decision that
// a route may keep planning new work on fixture/golden proof alone (CHAOS-4060),
// recorded as a code fact on the descriptor that carries it -- never inferred,
// never defaulted, and never set by a runtime read.
type ExecutedProofWaiver struct {
	// Reason is the operator's explanation, human-reviewed at the point this
	// waiver was added.
	Reason string
	// Ticket names the Linear issue the waiver decision is recorded against.
	Ticket string
	// RecordedAt is when the waiver was granted. It exists so a stale waiver
	// is discoverable by inspection rather than living forever unexamined.
	RecordedAt time.Time
}

// ExecutedProofEvidence is the durable, observed-state snapshot behind the
// CHAOS-4060 executed-proof gate. It is deliberately TRI-STATE per
// provider/dataset pair (matrixKey form), not a single proven/not-proven
// boolean:
//
//   - Proven: at least one live sync_run_units row completed status='success'
//     and reported a nonzero persisted ROW count (see executedProofEvidenceSQL
//     for why that must be a row count, not a batch count). This is the
//     ticket's "at least one live unit that completed with persisted>0".
//   - Attempted: at least one live sync_run_units row exists AT ALL, any
//     status. This is what makes a pair NOT "never-attempted" the moment
//     planning mints its first unit -- durably, the instant that row is
//     written, independent of how the unit eventually resolves.
//
// The gate (ExecutedProofSatisfied) blocks a pair only when it is Attempted
// but not Proven -- the pagerduty/teams shape (CHAOS-4048/CHAOS-4049): tried,
// repeatedly, and never once produced a real row. A pair that is neither
// Attempted nor Proven (a brand-new route, or any pair with zero
// sync_run_units history) is allowed through exactly like nil evidence is:
// its first plan mints the row that makes it Attempted, and from the next
// planning cycle on it converges to either Proven (passes permanently) or
// Attempted-unproven (blocks until it does). No operator attestation or
// canary machinery is needed for that convergence -- it is the same durable
// state the gate already reads, observed one cycle later.
//
// A nil *ExecutedProofEvidence is the "not wired" sentinel: a caller that has
// not composed QueryExecutedProofEvidence (most existing planner tests, and
// any context with no Postgres access) gets the CHAOS-4054 behavior
// unchanged -- ExecutedProofSatisfied treats "unknown" as "not this gate's
// business to block." Once a caller composes real evidence -- always
// non-nil, even when both sets are empty -- the gate is live. Composing the
// production caller with a perpetually-nil pointer would silently defeat the
// gate, so the production wiring (NativeMaterializer.RefreshExecutedProof)
// always assigns a non-nil snapshot, including on the zero-evidence case.
type ExecutedProofEvidence struct {
	Proven    map[string]bool
	Attempted map[string]bool
	// Degraded marks a snapshot installed because the evidence QUERY itself
	// failed before any real snapshot ever existed for this process -- not
	// because it succeeded and genuinely found nothing. That distinction
	// matters precisely because "found nothing" is now a legitimate,
	// permissive state (never-attempted bootstraps through): if a query
	// failure produced the same empty Proven/Attempted shape, a database
	// outage at startup would read exactly like a fresh, healthy database
	// and every non-waived pair would bootstrap through un-gated -- silently
	// destroying the "fail closed on error" guarantee. Degraded forces
	// ExecutedProofSatisfied to block every non-waived pair outright,
	// Proven/Attempted contents notwithstanding, until a real query
	// succeeds.
	Degraded bool
}

// HasExecutedProof reports whether evidence proves at least one live executed
// run for the given provider/dataset pair. A nil receiver or absent key is
// "not proven", never "proven": the gate never manufactures a positive from
// missing data.
func (evidence *ExecutedProofEvidence) HasExecutedProof(provider, dataset string) bool {
	if evidence == nil {
		return false
	}
	return evidence.Proven[matrixKey(provider, dataset)]
}

// HasBeenAttempted reports whether at least one sync_run_units row exists at
// all for the given provider/dataset pair, regardless of outcome. A nil
// receiver or absent key means "never attempted" -- the bootstrap-friendly
// case ExecutedProofSatisfied lets through.
func (evidence *ExecutedProofEvidence) HasBeenAttempted(provider, dataset string) bool {
	if evidence == nil {
		return false
	}
	return evidence.Attempted[matrixKey(provider, dataset)]
}

// ExecutedProofSatisfied reports whether this descriptor's CHAOS-4060
// executed-proof requirement is met: an explicit waiver, live PROVEN
// evidence for the identity's canonical (plannable) dataset, the pair never
// having been attempted at all (bootstrap convergence -- see
// ExecutedProofEvidence), or the gate not being wired by this caller at all
// (nil evidence). A Degraded snapshot blocks every non-waived pair
// unconditionally: it means the evidence query itself failed, not that it
// succeeded and found nothing, so "never attempted" cannot be trusted.
//
// Evidence is looked up under CanonicalDataset, not RequestedDataset: an
// alias identity (pr-reviews, tests, ...) is never independently planned or
// executed (CHAOS-4054), so it can never accumulate its own sync_run_units
// rows under its own name going forward. Its proof is whatever the canonical
// writer it folds onto has proven -- and QueryExecutedProofEvidence
// canonicalizes legacy alias-keyed rows onto that same writer, so pre-4054
// history recorded under an alias still counts.
func (descriptor CompleteRouteDescriptor) ExecutedProofSatisfied(evidence *ExecutedProofEvidence) bool {
	if descriptor.ExecutedProofWaiver != nil {
		return true
	}
	if evidence == nil {
		return true
	}
	if evidence.Degraded {
		return false
	}
	canonical := descriptor.CanonicalDataset
	if canonical == "" {
		canonical = descriptor.RequestedDataset
	}
	if evidence.HasExecutedProof(descriptor.Provider, canonical) {
		return true
	}
	return !evidence.HasBeenAttempted(descriptor.Provider, canonical)
}

// executedProofEvidenceSQL asks Postgres itself, not Go, to decide what
// counts as a persisted row: a numeric, positive count under either the Go
// completion payload's go_provider_route.records key or the legacy Python
// persisted key (CHAOS-4049) that pre-cutover rows still carry.
//
// records, not effects_written, is the correct Go-side signal: effects_written
// (EffectCommitResult.Written, cmd/dev-health-worker payload shape written by
// providerunit.Handler.Work) counts committed EFFECT BATCHES -- one per
// destination table -- not rows. A route can commit a batch with zero rows in
// it (an optional upstream API returning nothing this window is a legitimate
// success, not a failure) and effects_written would still count it, which is
// exactly the CHAOS-4049 "succeeded but persisted nothing" shape this gate
// exists to catch, reintroduced by the gate's own evidence source. records
// (ProductionContractComparator.CompareCompleteRoute's row count,
// result.Comparison.NativeRecords, or the chunked executor's CommittedRows)
// is a true per-row count summed across every committed batch, mandatory on
// every completed unit -- so a batch that wrote nothing contributes 0, not 1.
//
// Each cast is guarded by a regex bounded to at most 18 digits: bigint's
// range is +-9223372036854775807 (19 digits), so an unbounded `^[0-9]+$`
// still lets a 19+ digit value reach `::bigint` and raise "value out of
// range", which errors the WHOLE query rather than just skipping that row.
// 18 digits is always in range, so the bound trades an implausible
// (999+ quadrillion row) count for keeping the query alive -- exactly the
// same "one malformed historical result blob cannot fail the whole evidence
// query" contract the regex already existed to provide, extended to cover
// magnitude as well as shape.
//
// Deliberately no WHERE clause: a row of ANY status counts toward
// "attempted" (a unit was planned/claimed for this pair, whatever became of
// it), while "proven" is folded in per-group via bool_or over the
// success-and-positive-count predicate. One full scan computes both sets --
// CHAOS-4080 ("Executed-proof evidence query: unbounded full scan of
// sync_run_units, no shaped index") tracks giving this an indexed/maintained
// projection instead of a periodic full scan; the bounded refresh interval
// in NativeMaterializer caps query FREQUENCY in the meantime, not per-scan
// cost.
//
// Each disjunct is wrapped in its own COALESCE(...,false): a row whose
// go_provider_route.records key is present-but-zero and whose legacy
// persisted key is simply ABSENT (a real Go completion shape -- the payload
// only ever carries one or the other) evaluates the absent side's regex
// match as NULL, not false. bool_or is an AGGREGATE, not a WHERE-clause
// filter -- unlike WHERE, where a NULL predicate just excludes the row the
// same as false would, bool_or folds a NULL CONTRIBUTION into the running
// three-valued OR: false OR NULL is NULL, not false, so one such row without
// any other row in its group leaves the whole group's "proven" NULL instead
// of false, which fails the pgx scan into *bool outright. Coalescing each
// disjunct closed makes every row contribute a real true/false, exactly the
// same semantics the original WHERE-clause version of this query had.
const executedProofEvidenceSQL = `
SELECT
  lower(provider) AS provider,
  lower(dataset_key) AS dataset_key,
  bool_or(
    status = 'success'
    AND (
      COALESCE(
        (result #>> '{go_provider_route,records}') ~ '^[0-9]{1,18}$'
        AND (result #>> '{go_provider_route,records}')::bigint > 0,
        false
      )
      OR COALESCE(
        (result ->> 'persisted') ~ '^[0-9]{1,18}$'
        AND (result ->> 'persisted')::bigint > 0,
        false
      )
    )
  ) AS proven
FROM public.sync_run_units
GROUP BY 1, 2
`

// QueryExecutedProofEvidence computes the durable executed-proof snapshot
// (CHAOS-4060) from the authoritative sync_run_units table. It always returns
// a non-nil *ExecutedProofEvidence on success -- including one with both sets
// empty, which is a legitimate "nothing has ever been attempted yet" result,
// not a sentinel for "gate disabled" (see ExecutedProofEvidence).
//
// Every scanned row's dataset_key is canonicalized (canonicalRouteIdentity)
// before being folded into either set: a pre-CHAOS-4054 row planned directly
// under an alias identity (github/pr-reviews, github/tests, ...) still
// counts as evidence for the canonical writer it would fold onto today
// (github/prs, github/cicd, ...), since ExecutedProofSatisfied only ever
// looks a pair up by its canonical identity and that history is real,
// durable proof of exactly the writer the alias shares.
func QueryExecutedProofEvidence(ctx context.Context, db *pgxpool.Pool) (*ExecutedProofEvidence, error) {
	if db == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	rows, err := db.Query(ctx, executedProofEvidenceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	evidence := &ExecutedProofEvidence{
		Proven:    make(map[string]bool),
		Attempted: make(map[string]bool),
	}
	for rows.Next() {
		var provider, dataset string
		var proven bool
		if err := rows.Scan(&provider, &dataset, &proven); err != nil {
			return nil, err
		}
		key := matrixKey(provider, canonicalRouteIdentity(dataset))
		evidence.Attempted[key] = true
		if proven {
			evidence.Proven[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return evidence, nil
}
