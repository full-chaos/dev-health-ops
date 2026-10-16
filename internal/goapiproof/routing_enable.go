package goapiproof

// `enable` lights an MCP class root. It is a mutation that refuses on any doubt at all, and every refusal answers a
// question the 2026-09-01 six-day outage (CHAOS-5416) could not:
//
//  1. the running query-api must be reachable and must agree with this checkout on the SCHEMA digest. Checked by the
//     CALLER, which is the half that owns HTTP; Enable then writes the digest the RUNNING process reported, so a
//     caller that skipped the check still cannot record a digest no binary computes.
//  2. nothing else: the per-root proof receipt that used to gate a class root is retired (D6024). MCP and GraphQL parity
//     is held by the CI step TestMCPClassParityVenueOracle (CHAOS-9802), so enabling a root is the operator's
//     decision, recorded with its evidence.
//
// A catalog operation has no routing state: query-api serves every registered operation, so Enable refuses one
// (ErrDocumentOperationNotRouted).
//
// The candidate build is READ from the deployed process's /buildinfo and the flag is a cross-check that can only FAIL
// a run (team-lead ruling R51). A decision's provenance must not rest on somebody having typed the right thing.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// EnableModes are the only modes `enable` may set. Both make a class root reachable to a real client, which is the
// whole point of the verb; python/disabled/shadow do not, so they belong to `disable`.
var EnableModes = []string{"canary", "primary"}

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

	// OperationKinds is each operation's kind: OperationKindMCPClass for a class root. A request naming an operation
	// without a known kind is refused before anything is read or written.
	OperationKinds map[string]string

	Mode string
	// RolloutPercentage must be EnforcedRolloutPercentage: no plane obeys any
	// other value (CHAOS-6807), so Enable refuses it rather than record it.
	RolloutPercentage int

	RecordedBy     string
	ReviewEvidence string

	// PrincipalID is the effective-principal envelope's `sub` -- WHO THE
	// CREDENTIAL SAYS is acting, recorded on the CHAOS-5505 audit row.
	// Required: `enable` presents an envelope to /buildinfo, which
	// VERIFIES it, so a row here can and must name the subject that
	// verified credential carried. It is NOT RecordedBy, which is what
	// the operator typed about themselves and is verified by nothing.
	PrincipalID string

	DryRun bool
}

// EnableOutcome is what happened to one operation.
type EnableOutcome struct {
	Operation      string
	DocumentDigest string
	Mode           string
	CandidateBuild string
	// ReviewEvidence is what was actually written, prefix included.
	ReviewEvidence string
	// ModeBefore/CandidateBuildBefore/HadRowBefore are
	// the row's own state, read with the SAME lock and the SAME
	// transaction as the write that replaces it -- see the SELECT ... FOR
	// UPDATE in Enable's write loop, below. HadRowBefore is false when no
	// row existed yet (ModeBefore/CandidateBuildBefore are then the zero
	// value); only set when Apply actually wrote (empty on a dry run,
	// which reads nothing).
	//
	// Reading under the SAME FOR-UPDATE lock the write itself takes is
	// what makes this safe under concurrency: a naive read taken before
	// this call even starts can run BEFORE a racing writer's commit lands
	// -- executed (two real binaries): a third session holds the target
	// row for 3s; `disable -apply` (queued first) turns it python;
	// `enable` (queued second, unaware) turns it back on. An unlocked
	// pre-read taken before `disable`'s write landed would log
	// `mode_before=canary mode_after=canary` -- durably recording
	// "nothing happened" for the one event (a re-enable of a just-rolled-
	// back operation) a rollback investigation most needs to find. Under
	// the SAME lock the write itself takes, that window closes: by the
	// time this read runs, `disable`'s commit has already happened or
	// this transaction is already waiting behind it, so ModeBefore is
	// always the value the write ACTUALLY replaced, never a stale
	// snapshot from before a concurrent writer ran.
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
	case r.PrincipalID == "":
		return errors.New("goapiproof: principal id is required: `enable` reads the authenticated /buildinfo, so the envelope it presented was verified and the audit row must name the subject that credential carried")
	case len(r.Operations) == 0:
		return errors.New("goapiproof: no operations selected -- 'all-registered' must be resolved to a concrete list before it reaches Enable")
	case r.RolloutPercentage < 0 || r.RolloutPercentage > 100:
		return fmt.Errorf("goapiproof: rollout percentage %d is outside 0..100, which no plane could obey", r.RolloutPercentage)
	case r.RolloutPercentage != EnforcedRolloutPercentage:
		return ErrRolloutNotEnforced(r.RolloutPercentage)
	}
	if !contains(EnableModes, r.Mode) {
		return fmt.Errorf("goapiproof: enable may only set %v, got %q -- turning an operation OFF is `disable`'s job", EnableModes, r.Mode)
	}
	var classOps, documentOps int
	for _, operation := range r.Operations {
		if mcpclass.IsOperation(operation) {
			classOps++
		} else {
			documentOps++
		}
	}
	if classOps > 0 && documentOps > 0 {
		return errors.New("goapiproof: an MCP class operation (mcp:<root>) and a document operation cannot be enabled in one run -- the two are admitted by different receipts and checked against different facts; run them separately")
	}
	for _, operation := range r.Operations {
		if mcpclass.IsOperation(operation) {
			// A class row: allowlisted root, the class digest, the class kind, and
			// canary only (a class row has no edge route, so the primary route rule
			// could never admit it -- refusing by name beats a generic "unproven").
			switch {
			case !mcpclass.AllowedOperation(operation):
				return fmt.Errorf("goapiproof: %s is not an allowlisted MCP root field (allowed: %v)", operation, mcpclass.SortedRoots())
			case r.DocumentDigest[operation] != mcpclass.DocumentDigest():
				return fmt.Errorf("goapiproof: %s must carry the MCP class document digest %s, got %q", operation, mcpclass.DocumentDigest(), r.DocumentDigest[operation])
			case r.OperationKinds[operation] != OperationKindMCPClass:
				return fmt.Errorf("goapiproof: %s is an MCP class operation and needs kind %q, got %q", operation, OperationKindMCPClass, r.OperationKinds[operation])
			case r.Mode != TargetModeCanary:
				return fmt.Errorf("goapiproof: an MCP class row is enabled in mode %q only: it has no edge route, so %q could never be admitted and would say something about the row it cannot show", TargetModeCanary, r.Mode)
			}
			continue
		}
		if r.OperationKinds[operation] == OperationKindMCPClass {
			return fmt.Errorf("goapiproof: %s is not an MCP class operation but was given kind %q", operation, OperationKindMCPClass)
		}
		if r.DocumentDigest[operation] == "" {
			return fmt.Errorf("goapiproof: no document digest for %s -- the RUNNING process's /registry is the only source for it", operation)
		}
		if kind := r.OperationKinds[operation]; kind != OperationKindQuery && kind != OperationKindMutation {
			return fmt.Errorf("goapiproof: no known document kind for %s (got %q) -- enable needs each operation's kind from the catalog: a query is admitted by a two-plane receipt and a mutation only by a write receipt, and an unknown kind is refused rather than treated as a query", operation, kind)
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
	if _, document := splitClassOperations(request.Operations); len(document) > 0 {
		return nil, enableRefusal{refuseDocumentOperations("enable", document)}
	}

	wanted := make(map[string]string, len(request.Operations))
	for _, operation := range request.Operations {
		wanted[operation] = request.DocumentDigest[operation]
	}

	outcomes := make([]EnableOutcome, 0, len(request.Operations))
	for _, operation := range request.Operations {
		outcomes = append(outcomes, EnableOutcome{
			Operation:      operation,
			DocumentDigest: wanted[operation],
			Mode:           request.Mode,
			CandidateBuild: request.RunningBuild,
			ReviewEvidence: request.ReviewEvidence,
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
	audit := RoutingAudit{
		Action:          AuditActionEnable,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  request.ReviewEvidence,
		SchemaDigest:    request.SchemaDigest,
	}

	for i := range outcomes {
		outcome := &outcomes[i]
		// CHAOS-8735: a class root's decision is one row per operation (go_api_class_decision), written
		// with no candidate-build registration (no foreign key) and with the database's now() as its time.
		switch err := tx.QueryRow(ctx, classDecisionLockSQL, outcome.Operation).Scan(&outcome.ModeBefore, &outcome.CandidateBuildBefore); {
		case err == nil:
			outcome.HadRowBefore = true
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return nil, fmt.Errorf("goapiproof: read before-state for %s: %w", outcome.Operation, err)
		}
		tag, err := tx.Exec(ctx, classDecisionUpsertSQL, outcome.Operation, request.Mode, request.RunningBuild, request.SchemaDigest, outcome.ReviewEvidence, request.RecordedBy)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: enable %s: %w", outcome.Operation, err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("goapiproof: enable %s affected %d rows, want exactly 1", outcome.Operation, tag.RowsAffected())
		}
		entry := RoutingAuditEntry{
			DocumentDigest:      outcome.DocumentDigest,
			Operation:           outcome.Operation,
			CandidateBuildAfter: request.RunningBuild,
			ModeAfter:           request.Mode,
		}
		if outcome.HadRowBefore {
			buildBefore, modeBefore := outcome.CandidateBuildBefore, outcome.ModeBefore
			entry.CandidateBuildBefore = &buildBefore
			entry.ModeBefore = &modeBefore
		}
		audit.Entries = append(audit.Entries, entry)
	}
	// CHAOS-5505: the append-only record of the decision, committing with
	// the write it describes. `enable` upserts every named row -- even a
	// no-change one refreshes updated_at -- so every outcome is audited.
	if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return outcomes, nil
}

// EnforcedRolloutPercentage is the only rollout_percentage a row can carry
// honestly: canary and primary are both "on for every authenticated org",
// revocable only by mode (CHAOS-6807).
const EnforcedRolloutPercentage = 100

// ErrRolloutNotEnforced is the refusal for a rollout percentage no plane
// would obey. It says what the row would really do, so the operator does not
// believe in a staged rollout that is not there.
func ErrRolloutNotEnforced(percentage int) error {
	return fmt.Errorf("goapiproof: rollout percentage %d is refused: neither plane enforces rollout_percentage or eligible_orgs, so a canary or primary row is on for EVERY authenticated org whatever it records, and these operations have no Python resolver for an org outside a cohort to fall back to; the only control is the mode (turn it off with `disable`)", percentage)
}
