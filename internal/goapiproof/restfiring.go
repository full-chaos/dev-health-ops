package goapiproof

import (
	"context"
	"fmt"
	"time"
)

// This file adds go-api-rest-prove's per-declaration firing history:
// not what one request's own comparison found this run
// (RESTReceipt.BaselineDefects already carries that, per request, per
// run), but what EVERY run go_api_rest_proof_run still remembers for
// that exact corpus request has found, taken together. A BaselineDefect
// that has never once covered a real difference reads, in a single
// run's own receipt, exactly like one doing nothing that run for an
// ordinary reason -- there is nothing in one row that distinguishes
// "quiet because the mechanism is intermittent and absent this time"
// from "quiet on every run this table has ever recorded". This file
// answers that question by reading back what is already persisted.
//
// RunsLive, RunsFired and LastFiredBuild are derived from columns
// go_api_rest_proof_run already carried before this file existed --
// method, path, request_identity, candidate_build, observed_at,
// baseline_defect. The five-run NEVER_FIRED determination additionally
// reads baseline_defect_declared (alembic 0135): the full set of
// tickets a request DECLARED that run, not merely which of them fired.
// Before 0135, this table could prove a declaration FIRED in a past run
// (its ticket is literally in baseline_defect) but could not prove one
// was declared-and-silent rather than not-yet-declared-at-all -- both
// read identically, absent from a column that was never asked the
// question. That gap is exactly why a direction-of-error matters here:
// treating "receipt exists" as "was declared" (the only reading
// available before 0135) makes the flag fire EARLIER than the truth,
// never later -- a declaration added to the corpus reads as
// live-and-silent starting from its very first run, not from when it
// was actually added. Early is not the safe direction for a signal
// whose purpose is to trigger a remove-the-declaration review: a reader
// asked to justify a declaration that has had no real chance to fire
// learns to wave the flag away, and a flag that cries wolf is worth
// less than no flag. A NULL-era row (see baseline_defect_declared's own
// doc comment) still carries exactly that old ambiguity -- it predates
// 0135 and was never asked what it declared -- and every reader below
// treats it as UNKNOWN, never as evidence of either liveness or
// silence, for that reason. A HUMAN reader of the report inherits the
// same obligation: a NEVER_FIRED word is only as trustworthy as its own
// WindowKnownRuns/5 -- confirm the window actually reached 5 known runs
// (not merely 5 receipts) before treating the flag as a reason to open
// a remove-the-declaration review, and never generalise a pre-0135
// observation ("this looked quiet before the column existed") into
// evidence either way.
//
// baseline_defect_declared is additive-only and required no backfill:
// NULL means unknown, and ComputeFiringHistory excludes an unknown run
// from the five-run window entirely (neither counts toward it nor
// breaks it) rather than guessing. The window's clock therefore starts
// at the first run recorded after 0135 shipped; nothing recorded before
// it is retroactively accused.

// FiringHistory is one declared BaselineDefect's own record across
// every run go_api_rest_proof_run still remembers for its corpus
// request.
type FiringHistory struct {
	// Ticket is the BaselineDefect's own Ticket field this history was
	// built for.
	Ticket string `json:"ticket"`
	// RunsLive counts every recorded run KNOWN to have this declaration
	// live: either baseline_defect_declared names Ticket (a post-0135
	// row that says so directly), or -- for any row, pre- or post-0135
	// -- Ticket appears in that row's own baseline_defect (matched)
	// array, which is self-evident proof it was both declared and fired
	// that run regardless of whether the declared column exists for it.
	// A row that is merely silent AND pre-0135 (declared column NULL)
	// is UNKNOWN, not counted here either way. This is intentionally
	// wider than WindowKnownRuns below: an old, silent, pre-0135 run
	// never counts toward the five-run window even when a LATER row
	// proves the ticket was already declared back then, because the
	// window's own clock starts fresh at 0135 by design -- but a
	// FIRED old row is unambiguous evidence on its own terms, so it
	// still counts here.
	RunsLive int `json:"runs_live"`
	// RunsFired counts, of RunsLive, how many runs actually named
	// Ticket in their own baseline_defect array -- the same fact
	// classifyBaselineDefects (compare.go) already computes per
	// comparison, read back rather than re-derived by a second rule.
	RunsFired int `json:"runs_fired"`
	// LastFiredBuild is the candidate_build of the most recent run
	// RunsFired counted. Empty when RunsFired is zero: this declaration
	// has never fired in any recorded run.
	LastFiredBuild string `json:"last_fired_build,omitempty"`
	// WindowKnownRuns is how many of the most recent GLOBAL runs this
	// declaration was confirmed live in, walking back from now and
	// stopping at the first run that is not confirmed live (see
	// ComputeFiringHistory's own doc comment) -- capped at firingWindow
	// (5). Printed on every declaration's report line, flagged or not:
	// a reader seeing no NEVER_FIRED flag must be able to tell "the
	// window has not filled yet" apart from "every known run fired",
	// or an incomplete window reproduces, in miniature, the exact
	// silence-reads-as-health failure this file exists to close.
	WindowKnownRuns int `json:"window_known_runs"`
	// NeverFired is true when WindowKnownRuns reached firingWindow (5)
	// and Ticket fired in none of those five known-live runs.
	NeverFired bool `json:"never_fired"`
}

// DeclarationRun is one recorded go_api_rest_proof_run row for a single
// corpus request: when it ran, what candidate build it ran against,
// which declared tickets its own comparison fired, and -- when known --
// which tickets it declared.
type DeclarationRun struct {
	ObservedAt     time.Time
	CandidateBuild string
	// FiredTickets is that run's own baseline_defect array: the tickets
	// this comparison actually matched. Always known -- this column
	// predates 0135 and is never NULL for tickets that fired.
	FiredTickets []string
	// DeclaredKnown is true when this row's own baseline_defect_declared
	// column is non-NULL: a row written after 0135 shipped, which always
	// populates it (possibly with an empty array, meaning "declared
	// nothing"). False for a NULL-era row -- see this file's own package
	// doc comment for why that row is UNKNOWN rather than "not live".
	DeclaredKnown bool
	// DeclaredTickets is the full set of tickets this request declared
	// that run. Meaningful only when DeclaredKnown is true.
	DeclaredTickets []string
}

// firingWindow is the five-consecutive-known-run bar the determination
// is judged against. FiringWindow is the same value, exported so a
// caller formatting the report (cmd/go-api-rest-prove) prints "N of
// FiringWindow known runs" without hand-copying the literal 5 a second
// place it would drift from.
const (
	firingWindow = 5
	FiringWindow = firingWindow
)

// ComputeFiringHistory derives ticket's FiringHistory from its own
// request's recorded runs (ownRuns, newest first -- every
// go_api_rest_proof_run row for this exact (method, path,
// request_identity)) against the GLOBAL run timeline (globalRuns,
// newest first -- every DISTINCT observed_at go_api_rest_proof_run has
// ever recorded, for ANY route: one value per execution of the tool,
// because a single run computes its own observed_at exactly once and
// writes that same value, unchanged, into every receipt it produces).
//
// The window walks globalRuns from the newest, classifying THIS
// request's own state in each one:
//
//   - no receipt at all for this request that run -- a real gap (refused,
//     or not yet part of the corpus): the window stops filling here.
//   - a receipt exists, baseline_defect_declared is NULL (pre-0135,
//     unknown): this run is excluded from the window entirely -- it
//     neither advances the count nor stops it -- and the walk continues
//     to the next, older run looking for a replacement.
//   - a receipt exists, the declared column is known, and it does NOT
//     name Ticket: this run genuinely did not declare Ticket yet (the
//     corpus added it later). Real, known evidence against liveness --
//     the window stops filling here, exactly like a true gap.
//   - a receipt exists, the declared column is known, and it names
//     Ticket: a confirmed live run. The window advances; whether it also
//     fired is recorded.
//
// The walk stops as soon as the window reaches firingWindow (5) confirmed
// live runs, or hits a stopping case above, or runs out of globalRuns --
// whichever comes first. NeverFired is true only when the window filled
// to 5 and none of those five fired.
func ComputeFiringHistory(ticket string, ownRuns []DeclarationRun, globalRuns []time.Time) FiringHistory {
	history := FiringHistory{Ticket: ticket}
	for _, run := range ownRuns {
		fired := containsTicket(run.FiredTickets, ticket)
		live := fired || (run.DeclaredKnown && containsTicket(run.DeclaredTickets, ticket))
		if !live {
			continue
		}
		history.RunsLive++
		if fired {
			history.RunsFired++
			if history.LastFiredBuild == "" {
				history.LastFiredBuild = run.CandidateBuild
			}
		}
	}

	fired := false
	for _, at := range globalRuns {
		run, hasReceipt := runAt(ownRuns, at)
		switch {
		case !hasReceipt:
			// A real gap: this request was not live at all that run.
			// The window stops filling here -- see the doc comment
			// above for why this is never silently skipped.
			history.NeverFired = false
			return history
		case !run.DeclaredKnown:
			// Unknown era: excluded from the window entirely, neither
			// counted nor a stop. Keep looking further back.
			continue
		case !containsTicket(run.DeclaredTickets, ticket):
			// Known, and this run genuinely did not declare Ticket --
			// real evidence against liveness, same treatment as a gap.
			history.NeverFired = false
			return history
		}
		history.WindowKnownRuns++
		if containsTicket(run.FiredTickets, ticket) {
			fired = true
		}
		if history.WindowKnownRuns == firingWindow {
			history.NeverFired = !fired
			return history
		}
	}
	// Ran out of globalRuns before the window filled to five.
	history.NeverFired = false
	return history
}

func runAt(runs []DeclarationRun, at time.Time) (DeclarationRun, bool) {
	for _, run := range runs {
		if run.ObservedAt.Equal(at) {
			return run, true
		}
	}
	return DeclarationRun{}, false
}

func containsTicket(tickets []string, want string) bool {
	for _, ticket := range tickets {
		if ticket == want {
			return true
		}
	}
	return false
}

// globalRunLookback bounds how many GLOBAL runs ReadRESTFiringHistory
// fetches to feed the window walk above. It is deliberately larger than
// firingWindow: an unknown (pre-0135) run is skipped rather than
// stopping the walk, so filling a 5-run window can require looking past
// more than 5 raw global runs whenever the unknown/known boundary falls
// inside the fetched range. A generous, bounded constant -- not every
// row this table has ever recorded -- because looking further back than
// this can only ever find MORE unknown rows once the walk is already
// past the boundary (every row before 0135 shipped is unknown, and nothing
// after it can un-happen), so it can only under-report WindowKnownRuns
// while the window is still filling, never mis-report NeverFired.
const globalRunLookback = 25

// ReadRESTDeclarationRuns reads every recorded go_api_rest_proof_run row
// for one exact corpus request -- (method, path, requestIdentity) --
// newest first: ComputeFiringHistory's own ownRuns argument.
func ReadRESTDeclarationRuns(ctx context.Context, db Querier, method, path, requestIdentity string) ([]DeclarationRun, error) {
	rows, err := db.Query(ctx,
		`SELECT observed_at, candidate_build, COALESCE(baseline_defect, '{}'), baseline_defect_declared
		   FROM go_api_rest_proof_run
		  WHERE method = $1 AND path = $2 AND request_identity = $3
		  ORDER BY observed_at DESC`,
		method, path, requestIdentity,
	)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read REST declaration runs for %s %s: %w", method, path, err)
	}
	defer rows.Close()

	var runs []DeclarationRun
	for rows.Next() {
		var run DeclarationRun
		var declared *[]string
		if err := rows.Scan(&run.ObservedAt, &run.CandidateBuild, &run.FiredTickets, &declared); err != nil {
			return nil, fmt.Errorf("goapiproof: scan REST declaration run: %w", err)
		}
		if declared != nil {
			run.DeclaredKnown = true
			run.DeclaredTickets = *declared
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// ReadRESTGlobalRuns reads the most recent limit DISTINCT observed_at
// values go_api_rest_proof_run has recorded for ANY route, newest
// first -- ComputeFiringHistory's own globalRuns argument, and the run
// timeline a gap in one request's own history is judged against.
func ReadRESTGlobalRuns(ctx context.Context, db Querier, limit int) ([]time.Time, error) {
	rows, err := db.Query(ctx,
		`SELECT DISTINCT observed_at FROM go_api_rest_proof_run ORDER BY observed_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read REST global run timeline: %w", err)
	}
	defer rows.Close()

	var runs []time.Time
	for rows.Next() {
		var at time.Time
		if err := rows.Scan(&at); err != nil {
			return nil, fmt.Errorf("goapiproof: scan REST global run: %w", err)
		}
		runs = append(runs, at)
	}
	return runs, rows.Err()
}

// ReadRESTFiringHistory is the one call a REST corpus request that
// declares at least one BaselineDefect needs: every named ticket's own
// FiringHistory, in the order given. Returns (nil, nil) for an empty
// tickets list rather than issuing two queries for nothing.
func ReadRESTFiringHistory(ctx context.Context, db Querier, method, path, requestIdentity string, tickets []string) ([]FiringHistory, error) {
	if len(tickets) == 0 {
		return nil, nil
	}
	ownRuns, err := ReadRESTDeclarationRuns(ctx, db, method, path, requestIdentity)
	if err != nil {
		return nil, err
	}
	globalRuns, err := ReadRESTGlobalRuns(ctx, db, globalRunLookback)
	if err != nil {
		return nil, err
	}
	histories := make([]FiringHistory, 0, len(tickets))
	for _, ticket := range tickets {
		histories = append(histories, ComputeFiringHistory(ticket, ownRuns, globalRuns))
	}
	return histories, nil
}
