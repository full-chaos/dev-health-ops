package providersync

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
// (nil evidence).
//
// Proven is checked BEFORE Degraded, deliberately: it is a durable, permanent
// fact (a row that already exists and already proved itself does not stop
// being true because a LATER query failed), so it survives a degraded
// snapshot unchanged. What a Degraded snapshot revokes is only the
// never-attempted pass-through -- once the evidence query itself is failing,
// "this pair has no history" can no longer be trusted as a true negative, so
// it stops being treated as a green light. This is the codex round-4 fix: a
// snapshot that was healthy, then degraded by a LATER refresh failure (not
// just the very first load), must not keep authorizing pairs on the strength
// of what it last successfully observed being "nothing yet".
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
	canonical := descriptor.CanonicalDataset
	if canonical == "" {
		canonical = descriptor.RequestedDataset
	}
	if evidence.HasExecutedProof(descriptor.Provider, canonical) {
		return true
	}
	if evidence.Degraded {
		return false
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
// success-and-positive-count predicate. One full scan computes both sets.
//
// CHAOS-4114/CHAOS-4124: this query is NO LONGER what the refresh runs. It
// outgrew both its deadlines and a startup timeout installed an empty
// Degraded snapshot that blocked every non-waived pair for eight hours. It
// survives here as the DEFINITION the durable ledger projects -- the alembic
// 0109 backfill (ExecutedProofLedgerBackfillSQL) and the per-unit stamps are
// built from the same executedProofProvenPredicateSQL, and the integration
// tests assert this scan and the ledger read agree on identical fixtures.
// CHAOS-4080 ("Executed-proof evidence query: unbounded full scan of
// sync_run_units, no shaped index") asked for exactly this maintained
// projection; the ledger is it.
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
  lower(unit.provider) AS provider,
  lower(unit.dataset_key) AS dataset_key,
  bool_or(` + executedProofProvenPredicateSQL + `) AS proven
FROM public.sync_run_units AS unit
GROUP BY 1, 2
`

// executedProofProvenPredicateSQL is THE single definition of "this one
// sync_run_units row is live executed proof" (CHAOS-4114). Every consumer --
// the legacy whole-table scan above, the per-unit ledger stamp
// executedProofLedgerTerminalSQL writes on terminalization, and the alembic
// 0109 one-time backfill -- evaluates this identical expression, so the
// ledger cannot silently mean something different from the query it replaces.
// It is deliberately a SQL fragment and not a Go (or Python) reimplementation
// of the same rules: the JSON-path/regex/bigint semantics live in exactly one
// place, in the one language that already had to get them right. The
// migration copy is pinned to this text by
// tests/test_executed_proof_ledger_predicate_parity.py.
//
// It is written against the alias `unit`, so every consumer must alias
// public.sync_run_units AS unit.
const executedProofProvenPredicateSQL = `
    unit.status = 'success'
    AND (
      COALESCE(
        (unit.result #>> '{go_provider_route,records}') ~ '^[0-9]{1,18}$'
        AND (unit.result #>> '{go_provider_route,records}')::bigint > 0,
        false
      )
      OR COALESCE(
        (unit.result ->> 'persisted') ~ '^[0-9]{1,18}$'
        AND (unit.result ->> 'persisted')::bigint > 0,
        false
      )
    )
  `

// executedProofLedgerReadSQL is what the refresh actually runs now. The
// whole-table GROUP BY above outgrew both of its deadlines (CHAOS-4114: the
// 15s scheduler StepTimeout on the periodic refresh, and the 30s startup
// budget whose failure caused the CHAOS-4124 eight-hour total planning
// outage), because it scanned every sync_run_units row and extracted JSON
// from each one -- monotonically slower forever, since that table only grows.
// The ledger holds at most one row per (provider, dataset_key) pair -- ~100
// rows for the entire route matrix, independent of sync history -- so this
// read is bounded by the SIZE OF THE ROUTE MATRIX rather than by the size of
// the run history. The house rule the ticket names applies: a timeout never
// fixes capacity; durable truth does.
//
// Note this reads the RAW provider/dataset_key the units were planned under.
// Alias canonicalization stays a read-time transform in
// QueryExecutedProofEvidence, byte-identical to the pre-ledger behavior, so
// the ledger is a faithful projection of sync_run_units and not a second,
// subtly different opinion about route identity.
const executedProofLedgerReadSQL = `
SELECT provider, dataset_key, proven_at IS NOT NULL AS proven
FROM public.sync_executed_proof_ledger
`

// executedProofLedgerAttemptedSQL records that a pair has been ATTEMPTED. It
// is issued in the same transaction that INSERTs the sync_run_units rows,
// because "attempted" is a statement about ROW EXISTENCE, not about
// terminalization: executedProofEvidenceSQL has no WHERE clause, so a pair
// became Attempted the instant planning minted its first row, whatever became
// of it afterwards. Writing it anywhere later would leave a window in which
// the gate reads a freshly-planned pair as never-attempted and bootstraps it
// through -- the fail-OPEN direction.
//
// DO NOTHING, never DO UPDATE: attempted_at records when the pair FIRST
// became attempted, and monotone means the first writer wins. It must never
// clobber a proven_at either.
const executedProofLedgerAttemptedSQL = `
INSERT INTO public.sync_executed_proof_ledger (provider, dataset_key, attempted_at)
SELECT DISTINCT btrim(lower(pair.provider)), btrim(lower(pair.dataset_key)), $3::timestamptz
FROM unnest($1::text[], $2::text[]) AS pair(provider, dataset_key)
WHERE btrim(pair.provider) <> '' AND btrim(pair.dataset_key) <> ''
ON CONFLICT (provider, dataset_key) DO NOTHING
`

// executedProofLedgerTerminalSQL stamps PROVEN from the unit row the caller
// just terminalized, looked up by primary key. Deriving the verdict from the
// persisted row rather than from the caller's in-memory result payload is the
// point: the row is what the legacy scan read, and
// executedProofProvenPredicateSQL is the identical expression it read it
// with, so the two can never disagree about what counts.
//
// The upsert is monotone in both columns. attempted_at is only ever supplied
// by the INSERT arm (a terminalizing unit's pair is necessarily already
// attempted, so in practice the conflict arm always runs); proven_at is
// COALESCEd so an already-proven pair keeps its original proving instant and
// a later unproven completion can never un-prove it. That mirrors bool_or's
// semantics in the query this replaces: proof is permanent.
const executedProofLedgerTerminalSQL = `
INSERT INTO public.sync_executed_proof_ledger AS ledger
  (provider, dataset_key, attempted_at, proven_at)
SELECT
  btrim(lower(unit.provider)),
  btrim(lower(unit.dataset_key)),
  $2::timestamptz,
  CASE WHEN ` + executedProofProvenPredicateSQL + ` THEN $2::timestamptz END
FROM public.sync_run_units AS unit
WHERE unit.id = $1::uuid
  AND btrim(unit.provider) <> '' AND btrim(unit.dataset_key) <> ''
ON CONFLICT (provider, dataset_key) DO UPDATE
  SET proven_at = COALESCE(ledger.proven_at, EXCLUDED.proven_at)
`

// ExecutedProofLedgerBackfillSQL rebuilds the whole ledger from
// sync_run_units in one pass. It is the one-time alembic 0109 backfill (run
// offline, in migration context, where neither the 15s nor the 30s deadline
// exists), and it is what the integration tests run to prove the ledger read
// answers EXACTLY what the whole-table scan answered on the same fixture.
//
// attempted_at/proven_at are derived from the units themselves rather than
// stamped with now(), so a backfilled ledger reads as the honest history it
// is. Both aggregates are monotone-merged into whatever the ledger already
// holds, so running the backfill twice is a no-op.
const ExecutedProofLedgerBackfillSQL = `
INSERT INTO public.sync_executed_proof_ledger AS ledger
  (provider, dataset_key, attempted_at, proven_at)
SELECT
  btrim(lower(unit.provider)),
  btrim(lower(unit.dataset_key)),
  min(unit.created_at),
  min(unit.updated_at) FILTER (WHERE ` + executedProofProvenPredicateSQL + `)
FROM public.sync_run_units AS unit
GROUP BY 1, 2
ON CONFLICT (provider, dataset_key) DO UPDATE
  SET attempted_at = LEAST(ledger.attempted_at, EXCLUDED.attempted_at),
      proven_at = COALESCE(ledger.proven_at, EXCLUDED.proven_at)
`

// QueryExecutedProofEvidence loads the durable executed-proof snapshot
// (CHAOS-4060) from the maintained sync_executed_proof_ledger projection
// (CHAOS-4114) rather than scanning sync_run_units. It always returns
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
	rows, err := db.Query(ctx, executedProofLedgerReadSQL)
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

// executedProofLedgerExecutor is the narrow write surface the ledger stamps
// need. Both *pgxpool.Pool and pgx.Tx satisfy it, so a caller can enlist the
// stamp in the transaction that is already writing the units -- which is the
// only correct place for it. A stamp committed separately from the unit row
// it describes can survive a rolled-back unit (ledger says attempted, no row
// exists) or be lost while the unit commits (row exists, ledger silent, gate
// fails OPEN on a pair it should be watching).
type executedProofLedgerExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// RecordExecutedProofAttempted marks every supplied provider/dataset pair as
// ATTEMPTED. Call it from inside the transaction that inserts the
// sync_run_units rows, with the pairs those rows carry.
//
// Pairs are deduplicated and empty ones dropped by the statement itself, so a
// caller may pass a plan's units verbatim. Calling it with no pairs is a
// no-op rather than an error: a zero-unit plan is a legitimate outcome and
// must not fail the materialization it belongs to.
func RecordExecutedProofAttempted(
	ctx context.Context,
	executor executedProofLedgerExecutor,
	providers, datasets []string,
	now time.Time,
) error {
	if ctx == nil || executor == nil || len(providers) != len(datasets) {
		return ErrInvalidConfiguration
	}
	if len(providers) == 0 {
		return nil
	}
	_, err := executor.Exec(
		ctx, executedProofLedgerAttemptedSQL, providers, datasets, now.UTC(),
	)
	return err
}

// RecordExecutedProofTerminal stamps PROVEN for the unit the caller just
// terminalized, deriving the verdict from the persisted row by primary key.
// Call it from inside the terminalizing transaction, AFTER the status/result
// write, so the row it reads is the terminal one.
//
// It is safe (and a deliberate no-op) on a failure terminalization: the
// predicate requires status='success' with a positive row count, so a failed
// unit merely re-asserts attempted_at, which is already set. That is why the
// unreclaimable sweep and the terminal-delivery repair do not call this --
// they cannot move either bit.
func RecordExecutedProofTerminal(
	ctx context.Context,
	executor executedProofLedgerExecutor,
	unitID string,
	now time.Time,
) error {
	if ctx == nil || executor == nil || strings.TrimSpace(unitID) == "" {
		return ErrInvalidConfiguration
	}
	_, err := executor.Exec(ctx, executedProofLedgerTerminalSQL, unitID, now.UTC())
	return err
}
