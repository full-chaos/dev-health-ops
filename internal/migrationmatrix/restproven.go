package migrationmatrix

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// This file adds the REST surface's "proven" state -- the fourth column
// go-api-rest-prove's own package doc comment (goapiproof/restcorpus.go)
// exists to feed. Modelled on live.go's OperationRow.Proven, and built
// from the SAME rule class as the GraphQL Ops block: an admissible
// go_api_rest_proof_run row, judged through goapiproof.EnablementProofClause
// -- one predicate, reused unchanged (not a second copy of it) because
// go_api_rest_proof_run's stage/terminal_state/candidate_build/
// build_binding/baseline_defect columns are named identically to
// go_api_proof_run's own (alembic 0134's own doc comment). REST has its
// own dedicated table, not a row sharing go_api_proof_run under a
// sentinel key: a REST route carries no GraphQL schema/document digest to
// key a receipt by, so alembic 0134 keys go_api_rest_proof_run by
// (method, path, candidate_build) instead -- see that migration's own
// module doc comment for why no second, GraphQL-shaped registry table
// exists alongside it.
//
// It differs from the GraphQL column in one structural way. A GraphQL
// operation's admissible candidate build is READ from
// go_api_routing_state.current_candidate_build -- a live row this page
// already reads for the Ops block. A REST route has no such row:
// restendpoints.go's own package doc comment states the REST section is
// read ENTIRELY from committed source on every render, with no per-row
// ledger. So "proven at which build" has no live column to read it from
// here, and the caller must SUPPLY the candidate build to check against
// -- see ApplyRESTProof's own doc comment for why that build is
// deliberately left the caller's decision rather than guessed at by this
// package.

// RESTOperationName renders the routeswitch operation name one REST route
// carries -- "REST:<METHOD>:<path>", the SAME string cmd/query-api's own
// route files declare as a package constant (e.g. quadrantOperation) and
// goapiproof's restcorpus.go keys its corpus by. Built here from (method,
// path) rather than imported, because cmd/query-api is package main and
// cannot be imported; see restcorpus.go's own package doc comment for the
// same constraint on that side.
func RESTOperationName(method, path string) string {
	return fmt.Sprintf("REST:%s:%s", method, path)
}

// restProofQuery is ONE statement, same discipline as routingStateQuery:
// every admissible row for the given candidate_build in one snapshot,
// never a per-route round trip that could observe a mid-run write
// inconsistently.
func restProofQuery(clause string) string {
	return `
SELECT DISTINCT ON (p.method, p.path) p.method, p.path, p.id::text
FROM go_api_rest_proof_run AS p
WHERE p.candidate_build = $1
  AND ` + clause + `
ORDER BY p.method, p.path, p.observed_at DESC, p.id
`
}

// ReadRESTProof reads every admissible REST proof at candidateBuild from
// go_api_rest_proof_run, judged under goapiproof.TargetModeCanary -- the
// more permissive of EnablementProofClause's two route rules
// (TargetModeCanary admits any recorded measurement_route; see that
// function's own doc comment). REST has no primary/canary distinction of
// its own to select between: this page reports whether a route is
// PROVEN, not whether it is safe to promote to a specific routing mode,
// so the permissive arm is the right one to read against, not a guess.
//
// Returns operation name (RESTOperationName's own form) -> receipt id,
// ApplyRESTProof's own input shape.
func ReadRESTProof(ctx context.Context, dsn, candidateBuild string) (map[string]string, error) {
	clause, err := goapiproof.EnablementProofClause("p", goapiproof.TargetModeCanary)
	if err != nil {
		return nil, fmt.Errorf("build the REST enablement predicate: %w", err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, secrets.RedactedConnectError("connect to postgres", "dsn", "POSTGRES_URI")
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, restProofQuery(clause), candidateBuild)
	if err != nil {
		return nil, fmt.Errorf("read REST proof: %w", err)
	}
	defer rows.Close()

	proven := map[string]string{}
	for rows.Next() {
		var method, path, proofRunID string
		if err := rows.Scan(&method, &path, &proofRunID); err != nil {
			return nil, fmt.Errorf("scan REST proof row: %w", err)
		}
		proven[RESTOperationName(method, path)] = proofRunID
	}
	return proven, rows.Err()
}

// ApplyRESTProof promotes every RESTPorted row named in proven (operation
// -> proof_run_id, ReadRESTProof's own return shape) to RESTProven, and
// sets Proven to goapiproof-equivalent NoProof on every other row this
// section renders -- so a reader can tell "not proven" apart from "this
// derivation never ran" (rows.Proven stays "" until this function runs;
// see RESTEndpointRow.Proven's own doc comment).
//
// A row that is NOT RESTPorted (python-only, dead-by-design) is left
// alone: proof is a claim about a Go route actually being reachable, and
// neither status makes that claim.
//
// Returns a NEW slice; the input is never mutated, matching
// LoadRESTEndpoints' own "every call reads fresh, nothing is cached"
// contract.
func ApplyRESTProof(rows []RESTEndpointRow, proven map[string]string) []RESTEndpointRow {
	out := make([]RESTEndpointRow, len(rows))
	for i, row := range rows {
		out[i] = row
		if row.Status != RESTPorted {
			continue
		}
		operation := RESTOperationName(row.Method, row.Path)
		if id, ok := proven[operation]; ok && !goapiproof.NamesNothing(id) {
			out[i].Status = RESTProven
			out[i].Proven = id
			continue
		}
		out[i].Proven = NoProof
	}
	return out
}

// RESTProvenOperations lists the operations ApplyRESTProof promoted,
// sorted -- a small reporting helper, not used by rendering (the render
// reads Status per row directly).
func RESTProvenOperations(rows []RESTEndpointRow) []string {
	var out []string
	for _, row := range rows {
		if row.Status == RESTProven {
			out = append(out, RESTOperationName(row.Method, row.Path))
		}
	}
	sort.Strings(out)
	return out
}
