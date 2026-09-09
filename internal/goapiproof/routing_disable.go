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
// silent success (go_api_routing_admin.apply_disable, codex r1 P1).

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
// Python off-ramp learned this from a codex r1 P1 where a hand-built
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

	// EVERY row for an operation, not one per operation (r1 F5). The
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

	changes := make([]DisableChange, 0, len(operations))
	var guardProblems []string
	for _, operation := range operations {
		rows := live[operation]
		if len(rows) == 0 {
			// No row anywhere at the live digest: reported as "nothing to
			// disable", never invented. The catalog's digest is carried
			// only so the plan can name the row it would have targeted.
			changes = append(changes, DisableChange{
				Operation:      operation,
				DocumentDigest: request.DocumentDigest[operation],
				NewMode:        request.NewMode,
			})
			continue
		}
		for _, row := range rows {
			if request.ExpectedCandidateBuild != "" && row.candidateBuild != request.ExpectedCandidateBuild {
				guardProblems = append(guardProblems, fmt.Sprintf(
					"%s (document digest %s) points at %s, not the %s you named -- somebody has repointed it since you looked; re-run `status` and decide again",
					operation, row.documentDigest, row.candidateBuild, request.ExpectedCandidateBuild))
				continue
			}
			changes = append(changes, DisableChange{
				Operation:      operation,
				DocumentDigest: row.documentDigest,
				CurrentMode:    row.mode,
				NewMode:        request.NewMode,
				CandidateBuild: row.candidateBuild,
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
	var guard any
	if request.ExpectedCandidateBuild != "" {
		guard = request.ExpectedCandidateBuild
	}
	for index := range changes {
		change := &changes[index]
		// No row means nothing to turn off. Never an INSERT: turning
		// something off must not be able to turn something on, and it is
		// enforced twice -- by the mode check in validate and by this
		// path using UPDATE rather than an upsert.
		if change.CurrentMode == "" {
			continue
		}
		tag, err := tx.Exec(ctx, disableRoutingRowSQL,
			request.SchemaDigest, change.DocumentDigest, change.Operation,
			request.NewMode, request.ReviewEvidence, guard, request.RecordedBy, now)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: disable %s: %w", change.Operation, err)
		}
		if tag.RowsAffected() == 0 {
			// Only reachable with the guard on: the row moved between the
			// read and the write. Reported by omission from the applied
			// set -- never as a silent success.
			continue
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
