package goapiproof

// CHAOS-7096's org allowlist for /query/proof-write: go_api_proof_orgs (the
// live, mutable row an org either has or does not) plus its own append-only
// audit table, go_api_proof_org_audits -- see alembic 0145's doc comment
// for why this is its OWN pair of tables rather than a widened
// go_api_routing_audits (CHAOS-5505's identical reasoning: a proof-org
// event has none of a routing row's fields to report).
//
// Every write here is CredentialClassOperatorDirect (see routing_audit.go's
// doc comment for the vocabulary): this command has no deployed-process
// envelope to verify, unlike enable/repoint, and recording a weaker claim
// accurately beats recording a stronger one nothing checked.
//
// D2976 condition (4): the CLI verb writes an actor + reason row, never a
// bare insert -- enforced twice over, structurally (go_api_proof_orgs.added_by
// and .reason are NOT NULL, bounded CHECK constraints, alembic 0145) and
// here (ProofOrgRequest's own validation refuses an empty actor/reason
// before any statement runs).

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrProofOrgRequestIncomplete reports that OrgID, RecordedBy or
// ReviewEvidence was empty -- refused before any Postgres statement runs,
// the same "a decision with no durable record is unreadable weeks later"
// principle commonFlags.requireProvenance already applies to every other
// routing verb.
var ErrProofOrgRequestIncomplete = errors.New("goapiproof: proof-org request is missing OrgID, RecordedBy or ReviewEvidence")

// ProofOrgRequest is one add or remove.
type ProofOrgRequest struct {
	OrgID          string
	RecordedBy     string
	ReviewEvidence string
}

func (r ProofOrgRequest) validate() error {
	if r.OrgID == "" || r.RecordedBy == "" || r.ReviewEvidence == "" {
		return ErrProofOrgRequestIncomplete
	}
	return nil
}

// AddProofOrg allowlists an org (upsert: re-adding an already-allowed org
// refreshes added_by/reason/added_at rather than refusing -- the same
// "state, not a one-shot event" shape go_api_routing_state itself has).
// The live row write and its audit row commit in ONE transaction: a rolled
// back write must never leave an audit entry claiming it happened.
func AddProofOrg(ctx context.Context, pool *pgxpool.Pool, req ProofOrgRequest) error {
	if err := req.validate(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // best-effort on the already-failed path; Commit's error is what's returned

	if _, err := tx.Exec(ctx,
		`INSERT INTO go_api_proof_orgs (org_id, added_by, reason)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (org_id) DO UPDATE SET added_by = EXCLUDED.added_by, reason = EXCLUDED.reason, added_at = now()`,
		req.OrgID, req.RecordedBy, req.ReviewEvidence,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO go_api_proof_org_audits (org_id, action, credential_class, recorded_by, reason)
		 VALUES ($1, 'add', 'operator_direct', $2, $3)`,
		req.OrgID, req.RecordedBy, req.ReviewEvidence,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RemoveProofOrg removes an org from the allowlist and reports whether a
// row actually existed to remove. Audited either way -- "an operator asked
// to remove an org that was never allowed" is itself worth a durable
// record, the same reporting bias `disable` already has for a no-op guard
// case (see disable.go's own doc comment).
func RemoveProofOrg(ctx context.Context, pool *pgxpool.Pool, req ProofOrgRequest) (removed bool, err error) {
	if err := req.validate(); err != nil {
		return false, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `DELETE FROM go_api_proof_orgs WHERE org_id = $1`, req.OrgID)
	if err != nil {
		return false, err
	}
	removed = tag.RowsAffected() > 0

	if _, err := tx.Exec(ctx,
		`INSERT INTO go_api_proof_org_audits (org_id, action, credential_class, recorded_by, reason)
		 VALUES ($1, 'remove', 'operator_direct', $2, $3)`,
		req.OrgID, req.RecordedBy, req.ReviewEvidence,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return removed, nil
}

// ProofOrgRow is one allowlist entry, as `list` reports it.
type ProofOrgRow struct {
	OrgID   string
	AddedBy string
	Reason  string
	AddedAt time.Time
}

// ListProofOrgs reads the whole allowlist, org_id order -- deterministic
// output for a script, and small by construction (an operator adds orgs
// one at a time; this table is never bulk-seeded).
func ListProofOrgs(ctx context.Context, pool *pgxpool.Pool) ([]ProofOrgRow, error) {
	rows, err := pool.Query(ctx, `SELECT org_id, added_by, reason, added_at FROM go_api_proof_orgs ORDER BY org_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProofOrgRow
	for rows.Next() {
		var r ProofOrgRow
		if err := rows.Scan(&r.OrgID, &r.AddedBy, &r.Reason, &r.AddedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
