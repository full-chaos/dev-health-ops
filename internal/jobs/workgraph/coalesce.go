package workgraph

import (
	"encoding/json"
	"fmt"
	"time"
)

// MaxMaterializeLookbackDays is the widest start an investment.materialize
// request may be enqueued with when its scope names no from_date.
//
// It is NOT a new policy number. It is the window the native executor has
// always applied to a start-less scope itself
// (internal/jobs/investment/nativeexecutor.go's defaultWindowDays, which is in
// turn run_investment_materialize's `window_days: int = 30`). Writing it into
// the scope AT ENQUEUE TIME rather than leaving it to be re-derived at
// execution time is what makes the request self-describing, and a
// self-describing request is the precondition for coalescing: two requests can
// only be proven to name the same work if the work they name does not depend
// on WHEN each of them happens to run.
//
// investment's own constant is pinned against this one by a test in that
// package rather than being deleted in favour of it, because the dependency
// only runs one way -- investment imports workgraph, never the reverse -- and
// a silent divergence between the enqueue-time bound and the execution-time
// default would reopen exactly the "same key, different work" gap this
// constant closes.
const MaxMaterializeLookbackDays = 30

// WriteOutcome reports what one WriteRequestTx actually did beyond inserting
// the row, so the CALLER -- which owns a logger, as this writer deliberately
// does not -- can report both effects.
//
// Both fields follow the same rule the outbox strand repair follows for its
// refusals: an effect that is not counted cannot be told apart from an effect
// that never fired. A supersede that logs nothing is indistinguishable from a
// coalescing key that never matches, and a silently bounded start is
// indistinguishable from a producer that always sent one.
type WriteOutcome struct {
	// SupersededRequestIDs names every PENDING request this write moved to
	// 'canceled' because it named the same work. Empty is the ordinary case.
	SupersededRequestIDs []string
	// BoundedFromDate is the from_date this write ADDED to a materialize
	// scope that named none but did name a to_date, as a DATE-only string.
	// Empty when the producer supplied a start, which is the ordinary case.
	BoundedFromDate string
	// BoundedWindowDays is the window_days this write WROTE into a materialize
	// scope that named neither a start nor an anchor to compute one from, or
	// that named a window wider than the maximum lookback. Zero when the
	// producer's own window stood.
	BoundedWindowDays int
}

// supersedePendingSQL makes the superseded request TERMINAL before anything
// else can observe it.
//
// ORDER IS THE WHOLE POINT. internal/joboutbox/strand_repair.go re-arms a
// delivered outbox row whenever its work-graph request is still 'pending' (or
// 'running' past its lease). Cancelling a River job while the request row
// stayed pending would therefore not supersede anything: the next reconciler
// pass would resurrect it, and the backlog this coalescing exists to end would
// rebuild itself one repair tick at a time. Moving the row to 'canceled' --
// which is neither of the two states that query accepts, and which alembic
// 0060's forbid_work_graph_terminal_mutation trigger then makes immutable
// forever -- is what removes it from every re-arm path at once. The stale
// River job is left alone deliberately and no-ops when it runs: PostgresStore.
// Claim reports a canceled request as a finished one, so the handler retires
// the job without executing it.
//
// THE PREDICATE IS THE COALESCING KEY, and every conjunct earns its place:
//
//   - org_id + kind + scope IS (org, day-range, scope). from_date/to_date live
//     inside scope, so jsonb equality compares the whole key at once and does
//     it semantically -- jsonb does not preserve key order or number spelling
//     across a round-trip, so a text comparison would miss real matches.
//   - state='pending' with no claim token and no lease is the "not started"
//     set. A running request is never superseded: its work is already
//     underway, and cancelling it out from under its own lease is the strand
//     this package exists to prevent.
//   - the correlation_id PREFIX scopes the supersede to ONE producer. The
//     producers of work-graph requests each stamp their own prefix and no two
//     of them collide: "ext-recompute:" (internal/externalrecompute/enqueue.go,
//     shared with workerctl's replay of the same seam), "post-sync:"
//     (cmd/dev-health-worker/sync_dispatch.go), "fixed-schedule:"
//     (internal/scheduler/fixed/producers.go) and "manual-trigger:"
//     (cmd/dev-health-workerctl/trigger_investment.go). That separation is
//     load-bearing rather than tidy: a post-sync materialize request can be the
//     prerequisite half of a completion fence a later handoff waits on, and a
//     manual-trigger request is an operator's explicit backfill. Cancelling
//     either would destroy work nobody asked to destroy, so a producer may only
//     ever supersede its own queued work.
//   - id <> the incoming request keeps an idempotent re-write harmless. The
//     drain's correlation is deterministic per bridge row, so a lease reclaim
//     re-runs this write with the SAME request id; without this conjunct that
//     retry would cancel the very row it is re-confirming and leave nothing
//     pending at all.
const supersedePendingSQL = `
UPDATE public.work_graph_execution_requests
SET state = 'canceled', claim_token = NULL, lease_expires_at = NULL,
    updated_at = statement_timestamp()
WHERE org_id = $1::uuid
  AND kind = $2
  AND state = 'pending'
  AND claim_token IS NULL
  AND lease_expires_at IS NULL
  AND scope = $3::jsonb
  AND split_part(correlation_id, ':', 1) = split_part($4::text, ':', 1)
  AND id <> $5::uuid
RETURNING id::text`

// boundMaterializeStart pins the start of a materialize scope that names none,
// so an unbounded-start request cannot be enqueued.
//
// A start-less scope is not WRONG -- the executor runs it over its own 30-day
// default. It is UNPINNED, which is a different defect: the work such a
// request names cannot be read off the request. That makes it impossible to
// prove two of them name the same work, so nothing can be coalesced, and it
// makes the recorded intent of a queued job unreadable to an operator. Writing
// the bound INTO the scope fixes both at once.
//
// NO CLOCK IS INVOLVED, and that is a correctness requirement rather than a
// style preference. Producers derive their request ids deterministically (a
// lease reclaim re-runs the same write with the same id), and writeRowTx
// refuses a conflicting row whose stored scope differs from the incoming one.
// A bound anchored on the wall clock would therefore turn a retry that crossed
// midnight into a permanent ErrInvalidState -- a request that can never be
// re-confirmed, which is a strictly worse failure than the unpinned scope this
// exists to fix. Every branch below is a pure function of the scope.
//
// TWO BRANCHES, because a scope can be unpinned in two different ways:
//
//   - to_date PRESENT. The window has an anchor, so the start is computed from
//     it and written as an explicit from_date. The offset is
//     `+1 - MaxMaterializeLookbackDays` rather than `-MaxMaterializeLookbackDays`
//     because the executor advances a supplied to_date by one day to make the
//     window end-exclusive over whole days (materializeWindow); reproducing
//     that +1 here is what makes the explicit from_date name the SAME window
//     the default would have produced, instead of one a day short of it.
//   - to_date ABSENT. There is no anchor in the scope at all: the window ends
//     whenever the job runs, and no value written now can change that. What
//     CAN be written is the LENGTH, so window_days is pinned to the maximum
//     lookback. A scope that already names a window_days within the bound is
//     left alone; one that names a wider window is CLAMPED, which is the only
//     branch here that narrows anything a producer asked for, and the only one
//     that is a rejection rather than a clarification.
//
// A scope that is not a JSON OBJECT is refused rather than passed through. The
// executor already refuses it (materializeScope's probe), so this only moves
// the same rejection from execution time to enqueue time, where it costs one
// producer error instead of a job that can never succeed.
func boundMaterializeStart(scope []byte) ([]byte, string, int, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(scope, &fields); err != nil || fields == nil {
		return nil, "", 0, fmt.Errorf(
			"%w: investment.materialize scope must be a JSON object", ErrInvalidState)
	}
	if from, ok := decodeScopeString(fields["from_date"]); ok && from != "" {
		return scope, "", 0, nil
	}
	if to, ok := decodeScopeString(fields["to_date"]); ok && to != "" {
		if anchor, err := time.Parse(time.DateOnly, to); err == nil {
			bounded := anchor.UTC().
				AddDate(0, 0, 1-MaxMaterializeLookbackDays).
				Format(time.DateOnly)
			encoded, err := json.Marshal(bounded)
			if err != nil {
				return nil, "", 0, fmt.Errorf("%w: encode bounded from_date", ErrInvalidState)
			}
			fields["from_date"] = encoded
			reEncoded, err := json.Marshal(fields)
			if err != nil {
				return nil, "", 0, fmt.Errorf(
					"%w: encode bounded materialize scope", ErrInvalidState)
			}
			return reEncoded, bounded, 0, nil
		}
	}
	if days, ok := decodeScopeInt(fields["window_days"]); ok &&
		days >= 1 && days <= MaxMaterializeLookbackDays {
		return scope, "", 0, nil
	}
	encoded, err := json.Marshal(MaxMaterializeLookbackDays)
	if err != nil {
		return nil, "", 0, fmt.Errorf("%w: encode bounded window_days", ErrInvalidState)
	}
	fields["window_days"] = encoded
	reEncoded, err := json.Marshal(fields)
	if err != nil {
		return nil, "", 0, fmt.Errorf("%w: encode bounded materialize scope", ErrInvalidState)
	}
	return reEncoded, "", MaxMaterializeLookbackDays, nil
}

// decodeScopeString reads one scope field as a string. A field that is absent,
// JSON null, or some other type reads as "not supplied" rather than as an
// error: the executor's own decoder is the contract for scope TYPES, and
// duplicating that judgement here would give one seam two opinions about the
// same malformed scope.
func decodeScopeString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	return value, true
}

// decodeScopeInt is decodeScopeString's rule for window_days.
func decodeScopeInt(raw json.RawMessage) (int, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	return value, true
}
