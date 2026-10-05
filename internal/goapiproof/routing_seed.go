package goapiproof

// `seed` writes the first decision for an MCP class root that has none, in the one mode that cannot route a client
// anywhere -- shadow. It never changes an existing decision and never writes another mode. A shadow decision does not
// make a root reachable: the gates on shadow->canary (`enable`'s per-root receipt) are untouched. A catalog
// operation has no routing state (ErrDocumentOperationNotRouted).

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SeedMode is the only mode `seed` writes.
const SeedMode = "shadow"

// Seed actions, one per operation. would-create is a dry run, or a created
// row that was rolled back because another operation in the run was refused.
const (
	SeedActionCreated        = "created"
	SeedActionAlreadyPresent = "already-present"
	SeedActionRefused        = "refused"
	SeedActionWouldCreate    = "would-create"
)

// ErrSeedRefused reports that at least one operation was refused. The whole
// invocation is one transaction: after a refusal nothing was written for any
// operation, and every outcome names its own reason.
var ErrSeedRefused = errors.New("goapiproof: seed refused")

// ErrSeedRequestRefused reports a request that is malformed before any row
// is read.
var ErrSeedRequestRefused = errors.New("goapiproof: seed request refused")

// SeedRequest is what the verb derived from the running system. Nothing in
// it is typed by the operator except the operation names, provenance, and
// the dry-run switch.
type SeedRequest struct {
	SchemaDigest   string
	RunningBuild   string
	Operations     []string
	DocumentDigest map[string]string
	RecordedBy     string
	ReviewEvidence string
	PrincipalID    string
	DryRun         bool
}

// SeedOutcome is one operation's result.
type SeedOutcome struct {
	Operation      string
	DocumentDigest string
	Action         string
	// Reason is set on a refusal, and on already-present (which mode/digest
	// was found).
	Reason string
	// CorrelationID ties every row this run created to its audit rows.
	CorrelationID string
}

func (r SeedRequest) validate() error {
	switch {
	case r.SchemaDigest == "" || r.RunningBuild == "":
		return fmt.Errorf("%w: schema digest and running build are required", ErrSeedRequestRefused)
	case len(r.Operations) == 0:
		return fmt.Errorf("%w: no operations to seed", ErrSeedRequestRefused)
	case r.RecordedBy == "" || r.ReviewEvidence == "":
		return fmt.Errorf("%w: recorded-by and review-evidence are required", ErrSeedRequestRefused)
	case r.PrincipalID == "":
		return fmt.Errorf("%w: the envelope subject is required -- every created row is audited", ErrSeedRequestRefused)
	}
	for _, operation := range r.Operations {
		if r.DocumentDigest[operation] == "" {
			return fmt.Errorf("%w: %s has no document digest", ErrSeedRequestRefused, operation)
		}
	}
	return nil
}

// SeedEvidencePrefix opens review_evidence on every row and audit entry seed
// writes, so a reader of either table can tell a seeded row from an enabled one
// (the audit action vocabulary admits only enable|disable|repoint).
const SeedEvidencePrefix = "seed: "

// Seed creates the first shadow row for each named operation in ONE
// transaction: if any operation is refused, nothing is written for any of them
// (a half-applied batch is a state an operator cannot describe). It returns a
// non-nil error wrapping ErrSeedRefused if any operation was refused; outcomes
// are always returned, and after a refusal none of them was written.
func Seed(ctx context.Context, pool *pgxpool.Pool, request SeedRequest) ([]SeedOutcome, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrSeedRequestRefused)
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	class, document := splitClassOperations(request.Operations)
	if len(document) > 0 {
		if len(class) > 0 {
			return nil, fmt.Errorf("%w: %w", ErrSeedRequestRefused, errMixedClassAndDocument)
		}
		return nil, fmt.Errorf("%w: %w", ErrSeedRequestRefused, refuseDocumentOperations("seed", document))
	}
	operations := class
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	evidence := SeedEvidencePrefix + request.ReviewEvidence
	outcomes := make([]SeedOutcome, 0, len(operations))
	var entries []RoutingAuditEntry
	refused := 0
	for _, operation := range operations {
		outcome, err := seedClassDecision(ctx, tx, request, evidence, operation)
		if err != nil {
			return outcomes, err
		}
		switch outcome.Action {
		case SeedActionRefused:
			refused++
		case SeedActionCreated:
			entries = append(entries, RoutingAuditEntry{
				DocumentDigest:      outcome.DocumentDigest,
				Operation:           operation,
				CandidateBuildAfter: request.RunningBuild,
				ModeAfter:           SeedMode,
			})
		}
		outcomes = append(outcomes, outcome)
	}
	if refused > 0 {
		for i := range outcomes {
			if outcomes[i].Action == SeedActionCreated {
				outcomes[i].Action = SeedActionWouldCreate
				outcomes[i].Reason = "not written: another operation in this run was refused"
			}
		}
		return outcomes, fmt.Errorf("%w: %d of %d operation(s) refused; nothing was written", ErrSeedRefused, refused, len(outcomes))
	}
	if len(entries) == 0 || request.DryRun {
		return outcomes, nil
	}
	correlationID, err := writeRoutingAudit(ctx, tx, RoutingAudit{
		Action:          AuditActionEnable,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  evidence,
		SchemaDigest:    request.SchemaDigest,
		Entries:         entries,
	}, now)
	if err != nil {
		return outcomes, err
	}
	if err := tx.Commit(ctx); err != nil {
		return outcomes, fmt.Errorf("goapiproof: commit: %w", err)
	}
	for i := range outcomes {
		if outcomes[i].Action == SeedActionCreated {
			outcomes[i].CorrelationID = correlationID
		}
	}
	return outcomes, nil
}
