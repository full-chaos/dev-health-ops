package goapiproof

// CHAOS-5486: the Go port of `dev-hops go-api routing disable` -- the
// rollback half of the rollout.
//
// `enable` shipped without an off-ramp. For several hours on 2026-09-07
// fifteen operations were live on the Go plane, four measurably
// divergent, and there was no way to turn them off short of hand-written
// SQL -- the practice this whole surface exists to abolish. Plan section
// 5's rule is "rollback is a registry change, not an image rollback";
// this is the verb that makes that change possible.
//
// THE ASYMMETRY WITH `enable` IS DELIBERATE AND IS THE PYTHON CONTRACT.
// `disable` runs FEWER preflights than `enable`, and that is not an
// oversight to be tidied up:
//
//   - it never contacts query-api. It has to work when the planes
//     disagree and when the deployed process is DOWN, which is exactly
//     when it is needed. `enable`'s preflights exist to stop traffic
//     moving TO an unproven plane; none of that reasoning applies to
//     moving traffic back.
//   - it never writes `current_candidate_build`. `--candidate-build` is
//     a GUARD -- "refuse if someone repointed this row since I looked" --
//     and go_api_cli.py documents it "Never written -- disable changes
//     mode only". Re-pointing a row is `repoint`'s job, and a verb that
//     could do both would be able to change what a row means while
//     claiming only to have turned it off.
//   - it never INSERTS. A ModeChange for an operation with no row is
//     reported as "nothing to disable", never written: manufacturing a
//     `python` row for something that was never enabled invents history.
//
// The guard lives in the UPDATE's WHERE, not in a separate earlier read:
// a row repointed between the plan and the write simply does not match,
// and is reported by its ABSENCE from the applied list rather than as a
// silent success (go_api_routing_admin.apply_disable).

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DisableModes are the only modes `disable` may set. All three make an
// operation UNREACHABLE to a real client:
//
//   - python   -- the documented safe default; identical in effect to
//     having no row at all.
//   - disabled -- same reachability, but records a deliberate decision
//     rather than a default, so a reader can tell "turned off" from
//     "never on".
//   - shadow   -- the client still receives Python's response (plan §5
//     stage 4) and the dispatcher logs loudly that the shadow executor
//     does not exist.
//
// canary/primary are absent on purpose: turning an operation ON is
// `enable`'s job, and it has preflights this path does not.
var DisableModes = []string{"python", "disabled", "shadow"}

// ErrDisableGuardMismatch reports that a row points at a different
// candidate build than the -candidate-build guard named -- somebody
// repointed it since the operator looked.
var ErrDisableGuardMismatch = errors.New("goapiproof: a routing row points at a different candidate build than the guard named")

// ErrDisableRefusesEnablingMode is returned when a caller asks this verb
// to set a REACHABLE mode.
//
// Enforced at the write and not only at the flag parse: an invariant
// checked only by the caller is an invariant the next caller breaks. The
// Python off-ramp learned this the hard way: a hand-built
// ModeChange(new_mode="primary") reached apply_disable and turned routing
// ON -- the one thing an off-ramp must never be able to do.
var ErrDisableRefusesEnablingMode = errors.New("goapiproof: disable may only set an unreachable mode")

// DisableChange is one row `disable` would change, or did.
type DisableChange struct {
	Operation      string
	DocumentDigest string
	// CurrentMode is empty when no row exists at the live digest --
	// reported as "nothing to disable", never invented.
	CurrentMode string
	NewMode     string
	// CandidateBuild is what the row points at. Reported, NEVER written.
	CandidateBuild string
	Applied        bool
	// StaleSchemaDigests names every OTHER schema digest (not the live
	// one) at which this operation currently has a row: `disable`
	// computes `SchemaDigest` from THIS BINARY's
	// own embedded SDL, and unlike the Python verb (which runs INSIDE the
	// deployed edge image, so its SDL is the edge's by construction),
	// `go-api-routing` ships in no image and is built from an operator
	// checkout by design -- so a stale checkout produces a schema digest
	// nothing at the deployed process actually uses, and `disable -apply`
	// against it printed `applied: 0`, exit 0, with the row it was
	// actually trying to reach left completely untouched and NOTHING
	// saying so. `status`'s census (`rows by schema_digest`) already has
	// this fact; `disable` never consulted it before this fix.
	StaleSchemaDigests []string
}

// IsNoop is true when there is no row, or the row is already in the
// requested mode.
func (c DisableChange) IsNoop() bool {
	return c.CurrentMode == "" || c.CurrentMode == c.NewMode
}

// DisableRequest is one invocation.
type DisableRequest struct {
	SchemaDigest string
	// Operations is the resolved, catalog-validated list.
	Operations []string
	// DocumentDigest maps operation -> the catalog's document digest.
	// `disable` cannot ask the deployed process for it (it must work when
	// that process is down), so the checked-in catalog is the source --
	// and a row whose document digest has drifted from the catalog is
	// simply not matched, which is correct: the edge could not dispatch
	// to it either.
	DocumentDigest map[string]string
	NewMode        string
	// ExpectedCandidateBuild, when non-empty, is a guard: a row pointing
	// somewhere else is refused. Never written.
	ExpectedCandidateBuild string
	RecordedBy             string
	ReviewEvidence         string
	// Apply writes. Without it nothing is written and the plan is
	// returned for the operator to read.
	Apply bool
}

func (r DisableRequest) validate() error {
	switch {
	case r.SchemaDigest == "":
		return errors.New("goapiproof: schema digest is required")
	case len(r.Operations) == 0:
		return errors.New("goapiproof: no operations selected")
	}
	if !contains(DisableModes, r.NewMode) {
		return fmt.Errorf("%w: %v, got %q", ErrDisableRefusesEnablingMode, DisableModes, r.NewMode)
	}
	if r.Apply {
		if r.RecordedBy == "" {
			return errors.New("goapiproof: recorded-by is required to apply")
		}
		if r.ReviewEvidence == "" {
			return errors.New("goapiproof: review-evidence is required to apply: a mode change is a decision, and a decision with no durable reason is unreadable weeks later")
		}
	}
	return nil
}

// disableRoutingRowSQL changes reachability and provenance, and NOTHING
// else. current_candidate_build, owner, rollout_percentage and
// eligible_orgs are absent from the SET list on purpose -- see the
// package comment's asymmetry note. The optional guard is $6: when it is
// NULL the predicate is unconditional, so one statement serves both the
// guarded and unguarded call.
const disableRoutingRowSQL = `
UPDATE public.go_api_routing_state
   SET mode = $4,
       review_evidence = $5,
       recorded_by = $7,
       updated_at = $8
 WHERE schema_digest = $1
   AND document_digest = $2
   AND selected_operation = $3
   AND ($6::text IS NULL OR current_candidate_build = $6)`

// selectOtherSchemaDigestRowsSQL names every row a requested operation
// has at a schema digest OTHER than the live one. Read-only,
// never used to decide what gets written -- purely so the plan can tell
// an operator "you asked to disable X and I found nothing, but X DOES
// have a row, just not at the digest THIS checkout computed" rather than
// staying silent about it.
const selectOtherSchemaDigestRowsSQL = `
SELECT selected_operation, schema_digest
  FROM public.go_api_routing_state
 WHERE selected_operation = ANY($1)
   AND schema_digest <> $2`

// Disable plans and (with Apply) writes the mode changes, in ONE
// transaction.
//
// Returns the FULL plan -- including the rows it will not touch -- so an
// operator sees "three of the five you named have no row" rather than a
// count that quietly omits them.
func Disable(ctx context.Context, pool *pgxpool.Pool, request DisableRequest) ([]DisableChange, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// EVERY row for an operation, not one per operation. The
	// routing table's primary key is (schema_digest, document_digest,
	// selected_operation), so ONE operation can legitimately have several
	// rows at the live digest under different document digests -- and a
	// map keyed by operation alone silently keeps whichever happened to
	// come last. For an off-ramp that is the worst possible failure: it
	// reports success while leaving a reachable row behind. Both planes
	// had this shape; Go stops carrying it.
	type liveRow struct{ documentDigest, mode, candidateBuild string }
	live := map[string][]liveRow{}
	found, err := readRoutingRows(ctx, tx, selectDisableCandidatesSQL, request.SchemaDigest)
	if err != nil {
		return nil, err
	}
	for _, row := range found {
		live[row.operation] = append(live[row.operation], liveRow{
			documentDigest: row.documentDigest,
			mode:           row.mode,
			candidateBuild: row.build,
		})
	}

	operations := append([]string(nil), request.Operations...)
	sort.Strings(operations)

	// Every OTHER schema digest at which a named
	// operation currently has a row -- the fact `status`'s census
	// (`rows by schema_digest`) already carries. See
	// DisableChange.StaleSchemaDigests.
	staleDigests := map[string][]string{}
	{
		rows, err := tx.Query(ctx, selectOtherSchemaDigestRowsSQL, operations, request.SchemaDigest)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: read stale-digest rows: %w", err)
		}
		for rows.Next() {
			var operation, digest string
			if err := rows.Scan(&operation, &digest); err != nil {
				rows.Close()
				return nil, fmt.Errorf("goapiproof: scan stale-digest row: %w", err)
			}
			staleDigests[operation] = append(staleDigests[operation], digest)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("goapiproof: read stale-digest rows: %w", err)
		}
		for operation := range staleDigests {
			sort.Strings(staleDigests[operation])
			staleDigests[operation] = dedupeSorted(staleDigests[operation])
		}
	}

	changes := make([]DisableChange, 0, len(operations))
	var guardProblems []string
	for _, operation := range operations {
		rows := live[operation]
		if len(rows) == 0 {
			// A named operation with no row at this checkout's own live
			// digest but a row at ANOTHER digest means this checkout's
			// embedded SDL is stale relative to the deployed process
			// (`go-api-routing` ships in no image and is built from an
			// operator checkout by design, unlike the Python verb, which
			// runs INSIDE the deployed edge image). This is NEVER a
			// refusal, named explicitly or picked up by `-operations
			// all-registered`: refusing here contradicts this verb's own
			// documented contract (disable.go's package comment: "must
			// work when the planes disagree and when the deployed process
			// is down"). An off-ramp that ABORTS ENTIRELY, leaving every
			// operation's row -- including ones with a perfectly live row
			// -- untouched, over ONE unrelated operation's leftover
			// old-digest row (which every SDL move leaves behind; no verb
			// deletes old rows) is the off-ramp failing exactly when it is
			// most needed. The operation is SKIPPED instead, reported as
			// "nothing to disable HERE" with `StaleSchemaDigests` still
			// attached so the plan names it, and every OTHER named
			// operation -- including this same one, at whatever digest IS
			// live -- is still acted on.
			changes = append(changes, DisableChange{
				Operation:          operation,
				DocumentDigest:     request.DocumentDigest[operation],
				NewMode:            request.NewMode,
				StaleSchemaDigests: staleDigests[operation],
			})
			continue
		}
		hasCatalogRow := false
		for _, row := range rows {
			if row.documentDigest == request.DocumentDigest[operation] {
				hasCatalogRow = true
				break
			}
		}
		// The guard is defined to ignore dead rows for
		// the COMPARISON, full stop -- it was never meant to read a dead
		// row's build at all, live-catalog-row-present or not. When an
		// operation has NO catalog row, there is therefore NOTHING for a
		// set guard to compare against, and that absence -- not a
		// build value -- is what refuses: an operator who named a guard
		// asked this verb to check something, and there is no row it is
		// entitled to check. Never falls back to reading a dead row's
		// build, matching or not.
		if hasCatalogRow == false && request.ExpectedCandidateBuild != "" {
			guardProblems = append(guardProblems, fmt.Sprintf(
				"%s: -candidate-build was named, but every row this operation has at the live schema digest is DEAD (no catalog document digest) -- there is no row a guard can check; re-run `status` and decide again without the guard, or on an operation with a live row",
				operation))
			continue
		}
		for _, row := range rows {
			// The guard is scoped to the row `status` would actually
			// report: the one whose document digest matches the catalog's
			// for this operation. A DEAD row (document digest not the
			// catalog's -- the edge could never dispatch to it, and
			// `status` never shows its build) is still eligible to be
			// disabled, just never guarded against a build nobody could
			// have compared it to -- an operator who copied
			// `-candidate-build` from `status`'s own output must never be
			// refused "somebody has repointed it since you looked" for a
			// row they were never shown and never asked about. (The
			// operation-has-no-catalog-row case is handled ABOVE, before
			// this loop, so this loop only ever runs its guard check
			// against the catalog row.)
			isCatalogRow := row.documentDigest == request.DocumentDigest[operation]
			if isCatalogRow && request.ExpectedCandidateBuild != "" && row.candidateBuild != request.ExpectedCandidateBuild {
				guardProblems = append(guardProblems, fmt.Sprintf(
					"%s (document digest %s) points at %s, not the %s you named -- somebody has repointed it since you looked; re-run `status` and decide again",
					operation, row.documentDigest, row.candidateBuild, request.ExpectedCandidateBuild))
				continue
			}
			changes = append(changes, DisableChange{
				Operation:          operation,
				DocumentDigest:     row.documentDigest,
				CurrentMode:        row.mode,
				NewMode:            request.NewMode,
				CandidateBuild:     row.candidateBuild,
				StaleSchemaDigests: staleDigests[operation],
			})
		}
	}
	if len(guardProblems) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrDisableGuardMismatch, guardProblems)
	}
	if !request.Apply {
		return changes, nil
	}

	now := time.Now().UTC()
	for index := range changes {
		change := &changes[index]
		// No row means nothing to turn off. Never an INSERT: turning
		// something off must not be able to turn something on, and it is
		// enforced twice -- by the mode check in validate and by this
		// path using UPDATE rather than an upsert.
		if change.CurrentMode == "" {
			continue
		}
		// The WRITE-time guard, matching the plan-
		// time one above -- applied ONLY to the row `status` would report
		// (the catalog's document digest for this operation). A dead row
		// (different document digest) is written UNGUARDED: it was never
		// checked against -candidate-build at plan time either, so
		// holding its write to that same guard would refuse a write the
		// plan already promised, on a build nobody compared it to.
		//
		// An operation with NO
		// catalog row at all is refused entirely at PLAN time (see
		// `hasCatalogRow` above) whenever a guard is set -- `changes`
		// therefore never contains a row for such an operation on this
		// path (guardProblems being non-empty returns before
		// `request.Apply` is even read), so this write-time guard needs
		// no fallback of its own: reaching here at all already proves
		// the operation HAS a catalog row.
		var guard any
		if request.ExpectedCandidateBuild != "" && change.DocumentDigest == request.DocumentDigest[change.Operation] {
			guard = request.ExpectedCandidateBuild
		}
		// This used to check `tag.RowsAffected() == 0`
		// and skip marking the row Applied, with a comment describing a
		// row "repointed between the read and the write". That race is
		// IMPOSSIBLE: the `FOR UPDATE` plan read above locks
		// every targeted row continuously from the read through this
		// exact write, and the guard is now checked at PLAN time too
		// (`isCatalogRow` above refuses a mismatch before this loop ever
		// runs) -- so nothing can move a row, or make this guarded UPDATE
		// miss, between the plan and this write. Dead code removed rather
		// than left as unreachable defensive dressing around a comment
		// that described a scenario which cannot occur.
		if _, err := tx.Exec(ctx, disableRoutingRowSQL,
			request.SchemaDigest, change.DocumentDigest, change.Operation,
			request.NewMode, request.ReviewEvidence, guard, request.RecordedBy, now); err != nil {
			return nil, fmt.Errorf("goapiproof: disable %s: %w", change.Operation, err)
		}
		change.Applied = true
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return changes, nil
}

// DisableSummary counts what a plan or an apply did, including the zeros.
type DisableSummary struct {
	Total int
	// Actionable is the number of rows that exist AND are not already in
	// the requested mode.
	Actionable int
	Applied    int
	// NoRow is how many named operations have no row at the live digest.
	// Reported separately from "already in that mode" because they are
	// different facts an operator acts on differently.
	NoRow int
}

// SummarizeDisable counts changes. Every counter is reported even at
// zero, so "nothing needed changing" and "nothing was looked at" cannot
// read alike.
func SummarizeDisable(changes []DisableChange) DisableSummary {
	summary := DisableSummary{Total: len(changes)}
	for _, change := range changes {
		if change.CurrentMode == "" {
			summary.NoRow++
		}
		if !change.IsNoop() {
			summary.Actionable++
		}
		if change.Applied {
			summary.Applied++
		}
	}
	return summary
}
