package goapiproof

// CHAOS-5486: the Go port of `dev-hops go-api routing enable`.
//
// WHAT IS PRESERVED FROM THE PYTHON VERB, exactly. `enable` is a
// mutation that refuses on any doubt at all, and every refusal below
// answers a question the 2026-09-01 six-day outage (CHAOS-5416) could
// not:
//
//  1. the running query-api must be reachable and must agree with this
//     checkout on the SCHEMA digest -- rows follow the deployed image,
//     so a row written from a checkout that has moved ahead is
//     unreachable the moment it is written. Checked by the CALLER, which
//     is the half that owns HTTP; Enable then writes the digest the
//     RUNNING process reported, so a caller that skipped the check still
//     cannot write a row at a digest no binary computes.
//  2. the running query-api must register every named operation, under
//     the same DOCUMENT digest the edge's catalog carries.
//  3. the exact candidate build must have a `deployed_executed`/`match`
//     proof run for each operation, or --acknowledge-unproven must be
//     passed -- in which case the row carries an ACKNOWLEDGED-UNPROVEN
//     review_evidence prefix DURABLY, so `status` can report it for as
//     long as the enablement is in force rather than only in a log line
//     written at the moment it happened.
//
// WHAT IS DELIBERATELY DIFFERENT. The Python verb takes the candidate
// build as a flag, documented "by CONVENTION, unverified" -- the fifteen
// live rows carry a sha nothing ever checked. Here the build is READ from
// the deployed process's /buildinfo and the flag is a cross-check that can
// only FAIL a run (team-lead ruling R51). A row's provenance must not rest
// on somebody having typed the right thing.
//
// WHY THERE IS NO PRE-READ OF THE ROW, AND THE LIVE DEADLOCK THIS LEAVES
// (r6 P3, reproduced -- CORRECTED from an earlier version of this
// comment that claimed the opposite of the executed behaviour).
//
// Enable inserts the candidate build and then upserts the routing row,
// per operation, in that order -- CB then RS. It does NOT first
// SELECT ... FOR UPDATE the routing rows the way `repoint` does
// (routing_rows.go's selectRepointCandidatesSQL locks EVERY routing row
// at a schema digest, UP FRONT, before it registers a single candidate
// build -- RS then, per row, CB then RS again). That is a genuine
// LOCK-ORDER INVERSION, not the deadlock-AVOIDING opposite this comment
// used to claim: the textbook rule for avoiding a deadlock between two
// transactions is that they acquire contended resources in the SAME
// order, and CB-then-RS vs RS-then-CB is exactly the shape that produces
// one when they run concurrently over the same rows.
//
// Executed with the two real binaries: a third session holds one routing
// row for 4s while `repoint` (queued first, so its FOR UPDATE lock lands
// first) races `enable` (registering a candidate build the same
// operation names). Result: `repoint` exit=0; `enable`:
// `ERROR: deadlock detected (SQLSTATE 40P01)` exit=2. Postgres:
// `Process 139 waits for ShareLock on transaction 799; blocked by
// process 137. Process 137 waits for ShareLock on transaction 798;
// blocked by process 139.` Both sides roll back cleanly -- Postgres
// aborts one, and no row is left half-written -- so this is a FAILED
// WRITE an operator retries, not corruption, which is why it is P3 and
// not P1/P2.
//
// NOT FIXED IN THIS PR (RISK-NOTES names it explicitly, corrected to
// describe the actual defect rather than a "convention" that does not
// exist): the real fix is making `enable` acquire the SAME lock in the
// SAME order `repoint` does -- pre-locking the target routing rows
// (where one already exists) via routing_rows.go's shared reader BEFORE
// touching go_api_candidate_build -- and it is deferred to a dedicated
// follow-up PR rather than folded in here, per team-lead ruling.
// CHAOS-5507 is the ticket that fix belongs to; this PR's contribution is
// correcting the false claim and pinning the reproduced behaviour with a
// test (r7 F8, reproduced: corrected the test's own name here --
// TestEnableAndRepointLockOrderInversionDeadlocks, in
// routing_repoint_integration_test.go -- this comment used to cite a name
// that was never the test's actual name), so the follow-up starts from
// what the code actually does.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnableModes are the only modes `enable` may set. Both make an operation
// reachable to a real client, which is the whole point of the verb;
// python/disabled/shadow do not, so they belong to `disable`.
//
// Mirrors go_api_cli.py's `--mode` choices exactly. A shadow row is
// re-pointed by the `repoint` verb, never by widening this set: an
// enablement that could also leave an operation unreachable would make
// "enable" a name that does not describe what ran.
var EnableModes = []string{"canary", "primary"}

// ErrEnableUnproven reports that an operation has no deployed-executed
// proof for this candidate build and --acknowledge-unproven was not
// passed.
var ErrEnableUnproven = errors.New("goapiproof: no deployed_executed/match proof run recorded for this candidate build")

// UnprovenEvidencePrefix is prepended to review_evidence on any row
// enabled without a proof run.
//
// Durable on the ROW, not only in a log line: on 2026-09-07 fifteen
// operations were enabled on an explicit ruling that lived in a chat
// message, which is precisely the "unreadable six weeks later" problem
// `status`'s UNPROVEN marker exists to flag. Byte-identical to
// go_api_cli.py's `_enable_review_evidence` prefix so a row written by
// either plane reads the same.
const UnprovenEvidencePrefix = "ACKNOWLEDGED-UNPROVEN: "

// EnableRequest is one invocation.
type EnableRequest struct {
	// SchemaDigest and RunningBuild are what the DEPLOYED process
	// reported. Neither is ever operator-supplied.
	SchemaDigest string
	RunningBuild string

	// Operations is the resolved, catalog-validated list. Empty is an
	// error, never "everything": the caller resolves `all-registered`.
	Operations []string

	// DocumentDigest maps operation -> the document digest the RUNNING
	// process registers. Written to the row, so a row can never name a
	// document the deployed binary does not serve.
	DocumentDigest map[string]string

	Mode              string
	RolloutPercentage int

	RecordedBy     string
	ReviewEvidence string

	// AcknowledgeUnproven enables operations with no proof run, marking
	// each one UNPROVEN durably.
	AcknowledgeUnproven bool

	DryRun bool
}

// EnableOutcome is what happened to one operation.
type EnableOutcome struct {
	Operation      string
	DocumentDigest string
	Mode           string
	CandidateBuild string
	// Proven is false when this row was enabled under
	// --acknowledge-unproven. It is reported even on success, because an
	// enablement and an ACKNOWLEDGED enablement must not print alike.
	Proven bool
	// ReviewEvidence is what was actually written, prefix included.
	ReviewEvidence string
	// ModeBefore/CandidateBuildBefore/HadRowBefore (r8 F1, reproduced) are
	// the row's own state, read with the SAME lock and the SAME
	// transaction as the write that replaces it -- see the SELECT ... FOR
	// UPDATE in Enable's write loop, below. HadRowBefore is false when no
	// row existed yet (ModeBefore/CandidateBuildBefore are then the zero
	// value); only set when Apply actually wrote (empty on a dry run,
	// which reads nothing).
	//
	// This REPLACES an earlier, unlocked, OUTSIDE-the-transaction pre-read
	// that used to live in cmd/go-api-routing/enable.go, purely for this
	// log line. Executed (opus r8, two real binaries): a third session
	// holds the target row for 3s; `disable -apply` (queued first) turns
	// it python; `enable` (queued second, unaware) turns it back on. The
	// OLD unlocked read ran before `disable`'s write landed, so the log
	// printed `mode_before=canary mode_after=canary` -- durably recording
	// "nothing happened" for the one event (a re-enable of a just-rolled-
	// back operation) a rollback investigation most needs to find. Reading
	// under the SAME FOR-UPDATE lock the write itself takes closes that
	// window: by the time this read runs, `disable`'s commit has already
	// happened or this transaction is already waiting behind it, so
	// ModeBefore is always the value the write ACTUALLY replaced, never a
	// stale snapshot from before a concurrent writer ran.
	ModeBefore           string
	CandidateBuildBefore string
	HadRowBefore         bool
}

// ErrEnableRequestRefused marks EVERY validate() refusal as a refusal.
//
// Executed evidence for why this exists (CHAOS-5486, the input-domain
// sweep run through the real binary): `enable -mode shadow` printed
//
//	go-api-routing: goapiproof: enable may only set [canary primary], got "shadow" ...
//
// where the SAME class of refusal on `disable` printed
//
//	go-api-routing: refused: goapiproof: disable may only set an unreachable mode: ...
//
// Both exited 2, so the machine-readable contract was already right --
// but the word an operator reads was missing on one verb and present on
// the other, and two spellings of one fact is how a reader learns to
// distrust both. The sentinel is on the REQUEST rather than on each
// message so a validation added later inherits it instead of having to
// remember to.
var ErrEnableRequestRefused = errors.New("goapiproof: enable refuses this request")

// enableRefusal carries ErrEnableRequestRefused WITHOUT putting its text
// in front of the message.
//
// `fmt.Errorf("%w: %w", ErrEnableRequestRefused, err)` is the obvious
// spelling and it stutters -- the CLI already prefixes "refused: ", so the
// operator reads "refused: enable refuses this request: enable may only
// set...". The sentinel is a CLASSIFICATION, not a sentence; it belongs in
// errors.Is and nowhere else. Unwrap() []error is how a value carries a
// sentinel it does not print (Go 1.20+).
type enableRefusal struct{ err error }

func (e enableRefusal) Error() string   { return e.err.Error() }
func (e enableRefusal) Unwrap() []error { return []error{e.err, ErrEnableRequestRefused} }

func (r EnableRequest) validate() error {
	if err := r.validateFields(); err != nil {
		return enableRefusal{err}
	}
	return nil
}

func (r EnableRequest) validateFields() error {
	switch {
	case r.SchemaDigest == "":
		return errors.New("goapiproof: schema digest is required and must come from the running process's /registry")
	case r.RunningBuild == "":
		return errors.New("goapiproof: candidate build is required and must come from /buildinfo, never a flag")
	case r.RecordedBy == "":
		return errors.New("goapiproof: recorded-by is required")
	case r.ReviewEvidence == "":
		return errors.New("goapiproof: review-evidence is required: an enablement is a decision, and a decision with no durable reason is unreadable weeks later")
	case len(r.Operations) == 0:
		return errors.New("goapiproof: no operations selected -- 'all-registered' must be resolved to a concrete list before it reaches Enable")
	case r.RolloutPercentage < 0 || r.RolloutPercentage > 100:
		return fmt.Errorf("goapiproof: rollout percentage %d is outside 0..100, which ck_go_api_routing_state_rollout_percentage rejects", r.RolloutPercentage)
	}
	if !contains(EnableModes, r.Mode) {
		return fmt.Errorf("goapiproof: enable may only set %v, got %q -- turning an operation OFF is `disable`'s job", EnableModes, r.Mode)
	}
	for _, operation := range r.Operations {
		if r.DocumentDigest[operation] == "" {
			return fmt.Errorf("goapiproof: no document digest for %s -- the RUNNING process's /registry is the only source for it", operation)
		}
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// upsertRoutingStateSQL mirrors go_api_routing_admin.upsert_routing_state
// exactly, including which columns the ON CONFLICT branch overwrites.
// `updated_at` is refreshed even on a no-change write, on purpose: it is
// the row's only timestamp, and "re-confirmed today" reads very
// differently from "nothing has touched this since September".
const upsertRoutingStateSQL = `
INSERT INTO public.go_api_routing_state
	(schema_digest, document_digest, selected_operation, current_candidate_build,
	 owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
VALUES ($1, $2, $3, $4, 'go', $5, $6, $7, $8, $9)
ON CONFLICT (schema_digest, document_digest, selected_operation) DO UPDATE
   SET current_candidate_build = EXCLUDED.current_candidate_build,
       owner = EXCLUDED.owner,
       mode = EXCLUDED.mode,
       rollout_percentage = EXCLUDED.rollout_percentage,
       review_evidence = EXCLUDED.review_evidence,
       recorded_by = EXCLUDED.recorded_by,
       updated_at = EXCLUDED.updated_at`

// selectEnableBeforeStateSQL reads the target row's mode/build UNDER THE
// SAME LOCK the upsert immediately below it takes (r8 F1, reproduced --
// see EnableOutcome.ModeBefore's own doc comment). Scoped to the row's
// FULL identity, matching the upsert's own ON CONFLICT columns exactly --
// there is no ambiguity to resolve with an ORDER BY the way the shared
// multi-row reads elsewhere in this package need one.
const selectEnableBeforeStateSQL = `
SELECT mode, current_candidate_build
  FROM public.go_api_routing_state
 WHERE schema_digest = $1 AND document_digest = $2 AND selected_operation = $3
   FOR UPDATE`

// Enable registers the candidate build and points the named routing rows
// at it, in ONE transaction.
//
// One transaction because a failure between the two writes would leave a
// candidate build registered for a rollout that never happened -- and
// because a partial enablement is a fleet half on each plane, which is
// the state neither `status` nor the dispatcher can describe.
func Enable(ctx context.Context, pool *pgxpool.Pool, request EnableRequest) ([]EnableOutcome, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}

	wanted := make(map[string]string, len(request.Operations))
	for _, operation := range request.Operations {
		wanted[operation] = request.DocumentDigest[operation]
	}

	proven, err := OperationsWithEnablementProof(ctx, pool, request.SchemaDigest, request.RunningBuild, wanted)
	if err != nil {
		return nil, err
	}
	var unproven []string
	for _, operation := range request.Operations {
		if !proven[operation] {
			unproven = append(unproven, operation)
		}
	}
	sort.Strings(unproven)
	if len(unproven) > 0 && !request.AcknowledgeUnproven {
		return nil, fmt.Errorf("%w (%s=%s stage=%s terminal_state=%s) for: %v\n"+
			"  Plan section 5 stage 3 requires the exact candidate build to have served the operation through real ingress, auth, parse/validate, dispatch and a real database -- a constructor, health check or bare 200 does not qualify.\n"+
			"  Record it with go-api-prove, or pass -acknowledge-unproven to enable anyway (the row is then reported UNPROVEN by `status` for as long as it is in force)",
			ErrEnableUnproven, "candidate_build", request.RunningBuild,
			EnablementProofStage, EnablementProofTerminalState, unproven)
	}

	outcomes := make([]EnableOutcome, 0, len(request.Operations))
	for _, operation := range request.Operations {
		evidence := request.ReviewEvidence
		if !proven[operation] {
			evidence = UnprovenEvidencePrefix + evidence
		}
		outcomes = append(outcomes, EnableOutcome{
			Operation:      operation,
			DocumentDigest: wanted[operation],
			Mode:           request.Mode,
			CandidateBuild: request.RunningBuild,
			Proven:         proven[operation],
			ReviewEvidence: evidence,
		})
	}
	if request.DryRun {
		return outcomes, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	// Paired with the explicit Commit below on purpose (Trap #110): a
	// t.Fatalf or an early return anywhere in this scope must not leave the
	// transaction holding its pooled connection.
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	for i := range outcomes {
		outcome := &outcomes[i]
		// Candidate build FIRST. The routing row's 4-column foreign key
		// makes the order mandatory -- but this is NOT a deadlock-avoiding
		// "lock-order convention" (r7 F8, reproduced: corrected, this
		// comment used to claim the opposite of what the package's own
		// doc comment above now says). `repoint` locks the routing row
		// FIRST, then registers the build; this locks the build FIRST,
		// then the routing row. That is the exact lock-order INVERSION
		// the package comment (above, "WHY THERE IS NO PRE-READ OF THE
		// ROW") documents as a genuine, executed, reproduced deadlock
		// between enable and repoint -- not something this order avoids.
		// The FK dependency (a routing row cannot name a build that does
		// not exist yet) is why THIS order is mandatory on its own; fixing
		// the inversion means making `repoint` acquire its lock in this
		// SAME order instead, tracked against the same follow-up PR named
		// above.
		if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
			request.SchemaDigest, outcome.DocumentDigest, outcome.Operation, request.RunningBuild); err != nil {
			return nil, fmt.Errorf("goapiproof: register candidate build for %s: %w", outcome.Operation, err)
		}
		// r8 F1 (reproduced): the row's BEFORE state, read under the SAME
		// FOR-UPDATE lock the upsert immediately below takes, in the SAME
		// CB-then-RS order the upsert already uses -- see
		// EnableOutcome.ModeBefore's own doc comment for why this
		// replaced an unlocked, outside-the-transaction pre-read that
		// used to live in the cmd layer. A genuine query error here
		// (never observed with a real role in either r7's or r8's own
		// attempts to reach it -- Postgres's privilege model requires the
		// SAME grant for the UPSERT immediately below, so a role that
		// cannot run this SELECT cannot run that UPSERT either) now
		// aborts the WHOLE enable the same way any other mid-transaction
		// database error already does, rather than degrading to a
		// silently-wrong logged value -- the CHAOS-5416 "a measurement
		// that did not happen is not a pass" rule applied to this read
		// too.
		switch err := tx.QueryRow(ctx, selectEnableBeforeStateSQL,
			request.SchemaDigest, outcome.DocumentDigest, outcome.Operation,
		).Scan(&outcome.ModeBefore, &outcome.CandidateBuildBefore); {
		case err == nil:
			outcome.HadRowBefore = true
		case errors.Is(err, pgx.ErrNoRows):
			// Genuinely no row yet -- not a failure.
		default:
			return nil, fmt.Errorf("goapiproof: read before-state for %s: %w", outcome.Operation, err)
		}
		tag, err := tx.Exec(ctx, upsertRoutingStateSQL,
			request.SchemaDigest, outcome.DocumentDigest, outcome.Operation, request.RunningBuild,
			request.Mode, request.RolloutPercentage, outcome.ReviewEvidence, request.RecordedBy, now)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: enable %s: %w", outcome.Operation, err)
		}
		// An upsert that matched nothing is a silent no-op an operator
		// would read as success -- the failure mode CHAOS-5416 spent six
		// days in. Refuse instead.
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("goapiproof: enable %s affected %d rows, want exactly 1", outcome.Operation, tag.RowsAffected())
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return outcomes, nil
}
