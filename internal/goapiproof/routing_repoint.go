package goapiproof

// CHAOS-5486: the mode-preserving re-point.
//
// WHY THIS EXISTS. After a redeploy every go_api_routing_state row still
// names the build it was enabled at, and VerifyCandidateBuild refuses a
// proof run while any row disagrees with the running process. The Python
// verbs cannot fix that without changing what they are asked to preserve:
//
//   - `dev-hops go-api routing enable` WRITES current_candidate_build but
//     its --mode is required and accepts only canary|primary
//     (go_api_cli.py), so it cannot re-point a shadow row without also
//     making that operation Go-serving through the product edge;
//   - `dev-hops go-api routing disable` accepts --mode shadow but its
//     --candidate-build is a guard documented "Never written -- disable
//     changes mode only", confirmed in go_api_routing_admin.py where
//     enable writes the column and disable only reads it.
//
// So a shadow row could not be re-pointed at all, measured on the compose
// stack 2026-09-09 (lane-stack-owner, JOB 4/JOB 5 prep): three rows stuck
// at b18e56fa7… while the process ran ffd9e5d5d…, and the proof refused
// on all fifteen because VerifyCandidateBuild iterates every row
// regardless of mode. This is the missing verb, in Go per the cutover
// rule that no new Python compute is written.
//
// WHAT IT DELIBERATELY DOES NOT DO. It never writes `mode`, `owner`,
// `rollout_percentage` or `eligible_orgs`. Re-pointing is a provenance
// correction — "this row is about the build that is actually running" —
// and a verb that could also change reachability would be an enablement
// wearing a provenance name. Reachability stays with enable/disable.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRepointBuildMismatch is returned when the caller asks to re-point at
// a build that is not the one the deployed process reports.
//
// The check lives here as well as in the command because the whole point
// of the receipt chain is that a build identity is READ from the running
// process, never typed: a re-point to a hand-supplied sha would recreate
// exactly the "somebody typed a sha" provenance the proof runner refuses.
var ErrRepointBuildMismatch = errors.New("goapiproof: re-point build does not match the running build")

// ErrRepointNoRows is returned when no routing row exists at the schema
// digest. It is an error rather than an empty success for the reason the
// whole subsystem exists: "the registry is unreachable or empty" and
// "every row is already correct" must not read alike.
var ErrRepointNoRows = errors.New("goapiproof: no routing rows at this schema digest")

// ErrRepointUnknownOperation reports an --operations name with no row at
// this schema digest (r2 R2-07).
//
// `repoint` takes raw operation names rather than catalog-validated ones,
// so a filter like `chosen,typo` used to re-point `chosen`, drop `typo`
// without a word, and report success -- an operator who named two
// operations was told nothing about the one that did not happen. Same
// class as the empty-filter widening: the verb quietly doing something
// other than what was asked. `enable` and `disable` already refuse an
// unknown name via ResolveOperations; this closes the third verb.
var ErrRepointUnknownOperation = errors.New("goapiproof: no routing row at this schema digest for a named operation")

// RepointOutcome is what happened to one operation's row.
type RepointOutcome struct {
	Operation string
	// DocumentDigest completes the row's identity. One operation can have
	// several rows at a schema digest under different document digests,
	// so an outcome that named only the operation could not be matched
	// back to the row it describes (codex r3, CONC-01).
	DocumentDigest string
	// Mode is read before the write and re-read after it. The two are
	// compared, so "mode unchanged" is asserted by the code rather than
	// promised by the SQL.
	ModeBefore string
	ModeAfter  string
	BuildFrom  string
	BuildTo    string
	// Changed is false when the row already named the running build.
	// Re-running a re-point is a no-op in effect, and must be able to say
	// so instead of reporting fifteen writes that changed nothing.
	Changed bool
}

// RepointRequest is one invocation.
type RepointRequest struct {
	SchemaDigest string
	// RunningBuild is the build identity read from the deployed
	// process's /buildinfo. It is the value written.
	RunningBuild string
	// ExpectBuild, when non-empty, must equal RunningBuild. It is an
	// operator cross-check, never the source of what is written.
	ExpectBuild string
	// Operations restricts the write; empty means every row at the digest.
	Operations []string
	RecordedBy string
	// ReviewEvidence is why, recorded durably on every row touched.
	ReviewEvidence string
	// PrincipalID is the effective-principal envelope's `sub` -- WHO THE
	// CREDENTIAL SAYS is acting, recorded on the CHAOS-5505 audit row.
	// The build this verb writes is READ from the authenticated
	// /buildinfo, which VERIFIES that envelope, so the row can and must
	// name the subject the verified credential carried.
	PrincipalID string
	// DryRun evaluates and reports without writing.
	DryRun bool
}

func (r RepointRequest) validate() error {
	switch {
	case r.SchemaDigest == "":
		return errors.New("goapiproof: schema digest is required")
	case r.RunningBuild == "":
		return errors.New("goapiproof: running build is required and must come from /buildinfo")
	case r.RecordedBy == "":
		return errors.New("goapiproof: recorded-by is required")
	case r.ReviewEvidence == "":
		return errors.New("goapiproof: review-evidence is required: a provenance write is still a decision")
	case r.ExpectBuild != "" && r.ExpectBuild != r.RunningBuild:
		// Checked BEFORE the principal id: a cross-check mismatch is the
		// more specific fact, and an operator who typed the wrong sha
		// should hear that rather than a message about an audit column.
		return fmt.Errorf("%w: cross-check %q, running %q", ErrRepointBuildMismatch, r.ExpectBuild, r.RunningBuild)
	case r.PrincipalID == "" && !r.DryRun:
		return errors.New("goapiproof: principal id is required: this verb reads the authenticated /buildinfo, so the envelope it presented was verified and the audit row must name the subject that credential carried")
	}
	return nil
}

// registerCandidateBuildSQL mirrors the Python admin layer's ordering
// exactly: the routing row carries a 4-column foreign key to
// go_api_candidate_build, so the build must be registered FIRST or the
// update violates it. ON CONFLICT DO NOTHING keeps a re-run idempotent.
const registerCandidateBuildSQL = `
INSERT INTO public.go_api_candidate_build
	(schema_digest, document_digest, selected_operation, candidate_build)
VALUES ($1, $2, $3, $4)
ON CONFLICT (schema_digest, document_digest, selected_operation, candidate_build) DO NOTHING`

// repointRoutingRowSQL writes provenance and NOTHING else. mode, owner,
// rollout_percentage and eligible_orgs are absent from the SET list on
// purpose; see the package comment.
const repointRoutingRowSQL = `
UPDATE public.go_api_routing_state
   SET current_candidate_build = $4,
       review_evidence = $5,
       recorded_by = $6,
       updated_at = $7
 WHERE schema_digest = $1
   AND document_digest = $2
   AND selected_operation = $3`

// ErrRepointRacedAnotherWriter reports that a routing row changed between
// this verb's unlocked survey and the locked read that follows it.
//
// It is a REFUSAL, not a wrong write: see the lock-order note on Repoint
// for why the survey has to be unlocked, and why the only safe answer to
// "the shape changed underneath me" is to start over rather than register
// a candidate build while already holding routing-row locks -- precisely
// the ordering CHAOS-5507 exists to remove.
var ErrRepointRacedAnotherWriter = errors.New("goapiproof: a routing row changed while this re-point was preparing")

// repointAttempts bounds the retry. The race it retries is a genuine
// concurrent writer, so a retry usually succeeds immediately; a bound
// keeps a pathological loop from becoming a hang, and the refusal after
// it names what happened rather than pretending the run was clean.
const repointAttempts = 3

// Repoint points every selected routing row at the running build, leaving
// every reachability column untouched.
//
// All rows move in ONE transaction. A partial re-point is the state the
// proof runner refuses on, so committing some rows and failing others
// would leave the deployment in exactly the condition this verb exists to
// clear.
//
// THE LOCK ORDER (CHAOS-5507). Every writer of go_api_routing_state in
// this package obeys ONE rule:
//
//	register the candidate build BEFORE taking any routing-row lock,
//	and visit routing rows in (selected_operation, document_digest) order.
//
// `enable` obeys it by construction -- it registers, then reads the
// before-state under the shared ordered predicate, then upserts.
// `disable` obeys it vacuously: it registers no build at all, so it can
// never hold a candidate-build lock while waiting for a row. This verb
// used to do the OPPOSITE: SELECT ... FOR UPDATE over the routing rows,
// THEN insert the build. Two transactions doing that concurrently on the
// same (schema_digest, document_digest, selected_operation,
// candidate_build) key each held what the other waited for, and Postgres
// broke the cycle by aborting one -- SQLSTATE 40P01, cleanly, with no
// partial write, which is exactly why nobody noticed until a review round
// read the two orders side by side.
//
// Obeying the rule costs a SECOND read. The build cannot be registered
// without each row's document_digest (it is one of the four key columns),
// and learning it is what the locking read used to do. So the survey pass
// reads it WITHOUT a lock, the build is registered, and only then is the
// locking read taken -- which is also the read the writes are driven
// from, so nothing is ever written from the unlocked snapshot.
//
// THE TOCTOU THAT OPENS, and how it is closed. Between the unlocked
// survey and the locked read another writer can change a row's
// document_digest, or move a row onto or off the running build. Every one
// of those is detected by comparing the locked read against the survey
// inside the transaction, and the answer is always to roll back and start
// over -- never to register a build while holding locks, which would put
// the original ordering back for the rare path only. A bounded number of
// attempts, then a named refusal.
func Repoint(ctx context.Context, pool *pgxpool.Pool, request RepointRequest) ([]RepointOutcome, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	var lastRace error
	for attempt := 0; attempt < repointAttempts; attempt++ {
		outcomes, err := repointOnce(ctx, pool, request)
		if err == nil {
			return outcomes, nil
		}
		if !errors.Is(err, ErrRepointRacedAnotherWriter) {
			return nil, err
		}
		lastRace = err
	}
	return nil, fmt.Errorf("%w after %d attempts: another writer is changing these rows continuously -- re-run once it settles (last: %v)",
		ErrRepointRacedAnotherWriter, repointAttempts, lastRace)
}

// rowKey is a routing row's full identity within one schema digest.
//
// Keyed by BOTH columns, never by operation alone: one operation can have
// several rows at a schema digest under different document digests, and
// three separate findings across three review rounds were all a map that
// forgot that (r1 F5, r2 R2-03, r3 CONC-01).
type rowKey struct{ operation, documentDigest string }

func repointOnce(ctx context.Context, pool *pgxpool.Pool, request RepointRequest) ([]RepointOutcome, error) {
	wanted := map[string]bool{}
	for _, operation := range request.Operations {
		wanted[operation] = true
	}
	selected := func(operation string) bool { return len(wanted) == 0 || wanted[operation] }

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// --- PASS ONE: survey, no locks. ---------------------------------
	surveyed, err := readRoutingRows(ctx, tx, surveyRoutingRowsSQL, request.SchemaDigest)
	if err != nil {
		return nil, err
	}
	if len(surveyed) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrRepointNoRows, request.SchemaDigest)
	}
	surveyedByKey := make(map[rowKey]routingRow, len(surveyed))
	surveyedOperations := map[string]bool{}
	for _, row := range surveyed {
		surveyedByKey[rowKey{row.operation, row.documentDigest}] = row
		surveyedOperations[row.operation] = true
	}

	// EVERY named operation must exist, not merely one of them (r2
	// R2-07), checked against the SURVEY -- before the registration below
	// and before any write -- so a filter with a typo in it registers
	// nothing and changes nothing.
	if len(wanted) > 0 {
		var missing []string
		for operation := range wanted {
			if !surveyedOperations[operation] {
				missing = append(missing, operation)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, fmt.Errorf("%w: %v at %s -- re-pointing the rest and saying nothing about these is how an operator learns too late that half a rollout moved",
				ErrRepointUnknownOperation, missing, request.SchemaDigest)
		}
	}

	// --- Register the candidate build, BEFORE any routing-row lock. ---
	// In the shared total order, which readRoutingRows preserves. Only
	// for rows that will actually be written: a dry run writes nothing at
	// all, and a row already naming the running build needs no
	// registration it does not already have.
	if !request.DryRun {
		for _, row := range surveyed {
			if !selected(row.operation) || row.build == request.RunningBuild {
				continue
			}
			if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
				request.SchemaDigest, row.documentDigest, row.operation, request.RunningBuild); err != nil {
				return nil, fmt.Errorf("goapiproof: register candidate build for %s: %w", row.operation, err)
			}
		}
	}

	// --- PASS TWO: the locking read the writes are driven from. -------
	locked, err := readRoutingRows(ctx, tx, selectRepointCandidatesSQL, request.SchemaDigest)
	if err != nil {
		return nil, err
	}
	if len(locked) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrRepointNoRows, request.SchemaDigest)
	}

	now := time.Now().UTC()
	audit := RoutingAudit{
		Action:          AuditActionRepoint,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  request.ReviewEvidence,
		SchemaDigest:    request.SchemaDigest,
	}

	outcomes := make([]RepointOutcome, 0, len(locked))
	for _, row := range locked {
		if !selected(row.operation) {
			continue
		}
		outcome := RepointOutcome{
			Operation:      row.operation,
			DocumentDigest: row.documentDigest,
			ModeBefore:     row.mode,
			ModeAfter:      row.mode,
			BuildFrom:      row.build,
			BuildTo:        request.RunningBuild,
			Changed:        row.build != request.RunningBuild,
		}
		if !outcome.Changed || request.DryRun {
			outcomes = append(outcomes, outcome)
			continue
		}
		// This row needs a write. The build was registered from the
		// SURVEY, so the survey has to still describe it: a row that
		// appeared, or whose document digest moved, has no registration
		// under the key this UPDATE is about to reference, and its
		// foreign key would refuse. Start over rather than register one
		// here, which would be the old lock order in a rare path.
		before, surveyedBefore := surveyedByKey[rowKey{row.operation, row.documentDigest}]
		switch {
		case !surveyedBefore:
			return nil, fmt.Errorf("%w: %s (document digest %s) appeared between the survey and the locked read",
				ErrRepointRacedAnotherWriter, row.operation, row.documentDigest)
		case before.build == request.RunningBuild:
			// The survey said this row was already correct, so nothing
			// was registered for it -- and now it is not.
			return nil, fmt.Errorf("%w: %s (document digest %s) left the running build between the survey and the locked read",
				ErrRepointRacedAnotherWriter, row.operation, row.documentDigest)
		}

		tag, err := tx.Exec(ctx, repointRoutingRowSQL,
			request.SchemaDigest, row.documentDigest, row.operation,
			request.RunningBuild, request.ReviewEvidence, request.RecordedBy, now)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: re-point %s: %w", row.operation, err)
		}
		// A re-point that matched no row is a silent no-op the operator
		// would read as success, which is the failure mode CHAOS-5416
		// spent six days in. Refuse instead.
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("goapiproof: re-point %s affected %d rows, want exactly 1", row.operation, tag.RowsAffected())
		}
		buildBefore, modeBefore := row.build, row.mode
		audit.Entries = append(audit.Entries, RoutingAuditEntry{
			DocumentDigest:       row.documentDigest,
			Operation:            row.operation,
			CandidateBuildBefore: &buildBefore,
			CandidateBuildAfter:  request.RunningBuild,
			// A re-point NEVER touches reachability, so before and after
			// are the same mode by contract -- and recording both turns
			// that contract into something a reader can check rather than
			// take on trust.
			ModeBefore: &modeBefore,
			ModeAfter:  row.mode,
		})
		outcomes = append(outcomes, outcome)
	}
	if len(outcomes) == 0 {
		return nil, fmt.Errorf("%w: none of the requested operations exist at %s", ErrRepointNoRows, request.SchemaDigest)
	}

	// Re-read the modes inside the same transaction and assert none moved.
	// The UPDATE cannot change mode -- the column is not in its SET list --
	// but a trigger, a rule or a later edit to that statement could, and
	// this verb's entire contract is that it does not touch reachability.
	if !request.DryRun {
		// Keyed by the row's FULL identity (r3 CONC-01): keyed by
		// operation alone this collapses duplicate rows and can compare
		// one row's mode against another row's before-value, which
		// establishes nothing. It runs the SAME ordered, LOCKING
		// predicate the writes were driven from, so it locks what it
		// asserts on and introduces no lock order of its own.
		after, err := readRoutingRows(ctx, tx, selectRepointCandidatesSQL, request.SchemaDigest)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: re-read modes: %w", err)
		}
		observed := make(map[rowKey]string, len(after))
		for _, row := range after {
			observed[rowKey{row.operation, row.documentDigest}] = row.mode
		}
		for index := range outcomes {
			key := rowKey{outcomes[index].Operation, outcomes[index].DocumentDigest}
			mode, ok := observed[key]
			if !ok {
				return nil, fmt.Errorf("goapiproof: %s (document digest %s) vanished during re-point",
					outcomes[index].Operation, outcomes[index].DocumentDigest)
			}
			if mode != outcomes[index].ModeBefore {
				return nil, fmt.Errorf("goapiproof: %s mode changed %q -> %q during a re-point, which must never touch reachability",
					outcomes[index].Operation, outcomes[index].ModeBefore, mode)
			}
			outcomes[index].ModeAfter = mode
		}
		// CHAOS-5505: only rows that actually MOVED are audited.
		if len(audit.Entries) > 0 {
			if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("goapiproof: commit: %w", err)
		}
	}

	sortOutcomes(outcomes)
	return outcomes, nil
}

// sortOutcomes puts the report in the row order this table is always read
// in: (selected_operation, document_digest).
//
// TOTAL, not by operation alone. `sort.Slice` is NOT stable, so a
// comparator that TIES leaves the order of the tied elements up to the
// algorithm -- and one operation TIES with itself whenever it has several
// rows at a schema digest under different document digests, which is the
// shape the whole re-read assertion above exists for. An operator diffing
// two runs of the same command must not see two rows swap for no reason;
// worse, a report whose row order is not a function of the data cannot be
// pinned by a test at all.
//
// It is the SAME order `selectRepointCandidatesSQL` reads in, deliberately:
// the report an operator reads and the order the rows were locked in are
// one fact, and two spellings of one fact drift.
func sortOutcomes(outcomes []RepointOutcome) {
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].Operation != outcomes[j].Operation {
			return outcomes[i].Operation < outcomes[j].Operation
		}
		return outcomes[i].DocumentDigest < outcomes[j].DocumentDigest
	})
}

// RepointSummary counts what a run did, including the zeros.
type RepointSummary struct {
	Total     int
	Changed   int
	Unchanged int
}

// Summarize counts outcomes. Every counter is reported even at zero, so
// "nothing needed changing" and "nothing was looked at" cannot read alike.
func Summarize(outcomes []RepointOutcome) RepointSummary {
	summary := RepointSummary{Total: len(outcomes)}
	for _, outcome := range outcomes {
		if outcome.Changed {
			summary.Changed++
			continue
		}
		summary.Unchanged++
	}
	return summary
}
