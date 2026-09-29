package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ProofOrgAllowed reports whether orgID is on /query/proof-write's allowlist
// (CHAOS-7096, go_api_proof_orgs -- alembic 0145): a single, unqualified
// SELECT, matching queryAPIPosture()'s own manifest entry for this table
// (relation-only, no privilege beyond SELECT).
//
// The table is empty on every deployment until an operator explicitly runs
// `dho goapi routing proof-org add`, so the caller MUST treat a query error
// exactly like "not found" -- false, never true, and never propagated as
// "the check could not run, so let the request through". That is the
// caller's job (newDocumentDispatchHandler's orgAllowed closure), not this
// function's: this function still returns the error, so the caller's own
// fail-closed wrapper has something to log.
func ProofOrgAllowed(ctx context.Context, pool *pgxpool.Pool, orgID string) (bool, error) {
	if pool == nil {
		return false, errors.New("postgres: ProofOrgAllowed called with a nil pool")
	}
	if orgID == "" {
		// The caller already refuses an empty OrgID before reaching here
		// (claims.OrgID == "" is checked first) -- this is a second, cheap
		// floor so a future caller that skips that check still fails closed
		// rather than running a query with an empty parameter.
		return false, nil
	}
	var exists bool
	err := pool.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM go_api_proof_orgs WHERE org_id = $1)",
		orgID,
	).Scan(&exists)
	switch {
	case err == nil:
		return exists, nil
	case errors.Is(err, pgx.ErrNoRows):
		// EXISTS never returns zero rows, but treat it identically to "not
		// found" rather than as ambiguous, on the same fail-closed principle.
		return false, nil
	default:
		return false, fmt.Errorf("postgres: reading go_api_proof_orgs for org %q: %w", orgID, err)
	}
}
