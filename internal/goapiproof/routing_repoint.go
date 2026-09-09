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

// RepointOutcome is what happened to one operation's row.
type RepointOutcome struct {
	Operation string
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
		return fmt.Errorf("%w: cross-check %q, running %q", ErrRepointBuildMismatch, r.ExpectBuild, r.RunningBuild)
	}
	return nil
}

const selectRepointCandidatesSQL = `
SELECT selected_operation, document_digest, mode, current_candidate_build
  FROM public.go_api_routing_state
 WHERE schema_digest = $1
 ORDER BY selected_operation
   FOR UPDATE`

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

// Repoint points every selected routing row at the running build, leaving
// every reachability column untouched.
//
// All rows move in ONE transaction. A partial re-point is the state the
// proof runner refuses on, so committing some rows and failing others
// would leave the deployment in exactly the condition this verb exists to
// clear.
func Repoint(ctx context.Context, pool *pgxpool.Pool, request RepointRequest) ([]RepointOutcome, error) {
	if pool == nil {
		return nil, errors.New("goapiproof: nil pool")
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	wanted := map[string]bool{}
	for _, operation := range request.Operations {
		wanted[operation] = true
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	type candidate struct {
		operation, documentDigest, mode, build string
	}
	var candidates []candidate
	rows, err := tx.Query(ctx, selectRepointCandidatesSQL, request.SchemaDigest)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.operation, &c.documentDigest, &c.mode, &c.build); err != nil {
			rows.Close()
			return nil, fmt.Errorf("goapiproof: scan routing row: %w", err)
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrRepointNoRows, request.SchemaDigest)
	}

	now := time.Now().UTC()
	outcomes := make([]RepointOutcome, 0, len(candidates))
	for _, c := range candidates {
		if len(wanted) > 0 && !wanted[c.operation] {
			continue
		}
		outcome := RepointOutcome{
			Operation:  c.operation,
			ModeBefore: c.mode,
			ModeAfter:  c.mode,
			BuildFrom:  c.build,
			BuildTo:    request.RunningBuild,
			Changed:    c.build != request.RunningBuild,
		}
		if !outcome.Changed || request.DryRun {
			outcomes = append(outcomes, outcome)
			continue
		}
		if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
			request.SchemaDigest, c.documentDigest, c.operation, request.RunningBuild); err != nil {
			return nil, fmt.Errorf("goapiproof: register candidate build for %s: %w", c.operation, err)
		}
		tag, err := tx.Exec(ctx, repointRoutingRowSQL,
			request.SchemaDigest, c.documentDigest, c.operation,
			request.RunningBuild, request.ReviewEvidence, request.RecordedBy, now)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: re-point %s: %w", c.operation, err)
		}
		// A re-point that matched no row is a silent no-op the operator
		// would read as success, which is the failure mode CHAOS-5416
		// spent six days in. Refuse instead.
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("goapiproof: re-point %s affected %d rows, want exactly 1", c.operation, tag.RowsAffected())
		}
		outcomes = append(outcomes, outcome)
	}
	if len(outcomes) == 0 {
		return nil, fmt.Errorf("%w: none of the requested operations exist at %s", ErrRepointNoRows, request.SchemaDigest)
	}

	// Re-read the modes inside the same transaction and assert none moved.
	// The UPDATE cannot change mode -- the column is not in its SET list --
	// but a trigger, a rule or a later edit to that statement could, and
	// this verb's entire contract is that it does not touch reachability.
	// Asserting it costs one query and turns the contract into a test the
	// production path runs every time.
	if !request.DryRun {
		observed := map[string]string{}
		modeRows, err := tx.Query(ctx, `SELECT selected_operation, mode FROM public.go_api_routing_state WHERE schema_digest = $1`, request.SchemaDigest)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: re-read modes: %w", err)
		}
		for modeRows.Next() {
			var operation, mode string
			if err := modeRows.Scan(&operation, &mode); err != nil {
				modeRows.Close()
				return nil, fmt.Errorf("goapiproof: scan mode: %w", err)
			}
			observed[operation] = mode
		}
		modeRows.Close()
		if err := modeRows.Err(); err != nil {
			return nil, fmt.Errorf("goapiproof: re-read modes: %w", err)
		}
		for index := range outcomes {
			after, ok := observed[outcomes[index].Operation]
			if !ok {
				return nil, fmt.Errorf("goapiproof: %s vanished during re-point", outcomes[index].Operation)
			}
			if after != outcomes[index].ModeBefore {
				return nil, fmt.Errorf("goapiproof: %s mode changed %q -> %q during a re-point, which must never touch reachability",
					outcomes[index].Operation, outcomes[index].ModeBefore, after)
			}
			outcomes[index].ModeAfter = after
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("goapiproof: commit: %w", err)
		}
	}

	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Operation < outcomes[j].Operation })
	return outcomes, nil
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
