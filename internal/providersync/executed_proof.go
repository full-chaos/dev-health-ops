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
// CHAOS-4060 executed-proof gate: the set of provider/dataset pairs (matrixKey
// form) with at least one live sync_run_units row that completed
// status='success' and reported nonzero persisted/effects-written rows. It
// says an execution actually happened and its artifacts/rows exist -- never a
// timing window, a retry count, or any other heuristic.
//
// A nil map is the "not wired" sentinel: a caller that has not composed
// QueryExecutedProofEvidence (most existing planner tests, and any context
// with no Postgres access) gets the CHAOS-4054 behavior unchanged --
// ExecutedProofSatisfied treats "unknown" as "not this gate's business to
// block." Once a caller composes real evidence -- always non-nil, even when
// empty -- the gate is live: a pair absent from the map has no proof and is
// not satisfied absent a waiver. Composing the production caller with a
// perpetually-nil map would silently defeat the gate, so the production
// wiring (NativeMaterializer.RefreshExecutedProof) always assigns a non-nil
// map, including on the zero-evidence case.
type ExecutedProofEvidence map[string]bool

// HasExecutedProof reports whether evidence proves at least one live executed
// run for the given provider/dataset pair. Absent or nil evidence is "not
// proven", never "proven": the gate never manufactures a positive from
// missing data.
func (evidence ExecutedProofEvidence) HasExecutedProof(provider, dataset string) bool {
	if evidence == nil {
		return false
	}
	return evidence[matrixKey(provider, dataset)]
}

// ExecutedProofSatisfied reports whether this descriptor's CHAOS-4060
// executed-proof requirement is met: an explicit waiver, live evidence for
// the identity's canonical (plannable) dataset, or the gate not being wired
// by this caller at all (nil evidence -- see ExecutedProofEvidence).
//
// Evidence is looked up under CanonicalDataset, not RequestedDataset: an
// alias identity (pr-reviews, tests, ...) is never independently planned or
// executed (CHAOS-4054), so it can never accumulate its own sync_run_units
// rows. Its proof is whatever the canonical writer it folds onto has proven.
func (descriptor CompleteRouteDescriptor) ExecutedProofSatisfied(evidence ExecutedProofEvidence) bool {
	if descriptor.ExecutedProofWaiver != nil {
		return true
	}
	if evidence == nil {
		return true
	}
	canonical := descriptor.CanonicalDataset
	if canonical == "" {
		canonical = descriptor.RequestedDataset
	}
	return evidence.HasExecutedProof(descriptor.Provider, canonical)
}

// executedProofEvidenceSQL asks Postgres itself, not Go, to decide what
// counts as a persisted row: a numeric, positive count under either the Go
// completion payload's go_provider_route.effects_written key
// (cmd/dev-health-worker payload shape written by providerunit.Handler.Work)
// or the legacy Python persisted key (CHAOS-4049) that pre-cutover rows still
// carry. The regex guards each cast so one malformed historical result blob
// cannot fail the whole evidence query -- an unparseable value simply
// contributes no evidence, exactly like a genuinely absent key would.
const executedProofEvidenceSQL = `
SELECT lower(provider), lower(dataset_key)
FROM public.sync_run_units
WHERE status = 'success'
  AND (
    (
      (result #>> '{go_provider_route,effects_written}') ~ '^[0-9]+$'
      AND (result #>> '{go_provider_route,effects_written}')::bigint > 0
    )
    OR (
      (result ->> 'persisted') ~ '^[0-9]+$'
      AND (result ->> 'persisted')::bigint > 0
    )
  )
GROUP BY 1, 2
`

// QueryExecutedProofEvidence computes the durable executed-proof snapshot
// (CHAOS-4060) from the authoritative sync_run_units table. It always returns
// a non-nil map on success -- including an empty one, which is a legitimate
// "nothing has ever proven itself yet" result, not a sentinel for "gate
// disabled" (see ExecutedProofEvidence).
func QueryExecutedProofEvidence(ctx context.Context, db *pgxpool.Pool) (ExecutedProofEvidence, error) {
	if db == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	rows, err := db.Query(ctx, executedProofEvidenceSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	evidence := make(ExecutedProofEvidence)
	for rows.Next() {
		var provider, dataset string
		if err := rows.Scan(&provider, &dataset); err != nil {
			return nil, err
		}
		evidence[matrixKey(provider, dataset)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return evidence, nil
}
