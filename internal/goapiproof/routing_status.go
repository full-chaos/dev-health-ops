package goapiproof

// CHAOS-5486: the Go port of `dev-hops go-api routing status` -- the
// diagnostic half.
//
// `status` NEVER refuses and never reports an unhealthy state as a
// failure, because it is what an operator runs when things are ALREADY
// broken -- including when query-api is down, which it reports as
// UNREACHABLE rather than dying on. A diagnostic that fails because the
// thing it diagnoses is down is useless exactly when it is needed; the
// Python verb learned that twice, from codex r1 (an unguarded database
// read) and codex r2 (an unguarded SDL read), and both guards are carried
// over here as independent, separately-reported failures.
//
// Reporting is driven by the CATALOG, not by the table: an operation with
// no row anywhere is reported MISSING rather than simply being absent
// from the output. "Nothing printed" is exactly how the six-day CHAOS-5416
// outage stayed invisible.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Digest states a routing row can be in, relative to the LIVE schema
// digest.
const (
	// DigestMatch: a row exists at the live digest. Reachable if its mode
	// says so.
	DigestMatch = "MATCH"
	// DigestStale: rows exist for this operation but NONE of them is
	// reachable -- either they sit at other SCHEMA digests, or they sit
	// at the live one under a DOCUMENT digest the catalog does not carry.
	// Both are the silent-death shape: present in psql, never consulted.
	DigestStale = "STALE"
	// DigestMissing: no row at any digest. Never enabled, or cleaned up.
	DigestMissing = "MISSING"
)

// OperationStatus is one row of `status`.
//
// Proven is orthogonal to DigestState: a row can be live and reachable
// while nothing ever proved the deployed build serves it. Rendering that
// as UNPROVEN is what keeps an acknowledged-unproven enablement visible
// for as long as it is in force, not just in the log line written the
// moment it happened.
type OperationStatus struct {
	Operation      string
	DocumentDigest string
	DigestState    string

	Mode                  string
	CurrentCandidateBuild string
	RolloutPercentage     *int
	Owner                 string
	UpdatedAt             *time.Time
	ReviewEvidence        string
	RecordedBy            string

	StaleDigests []string
	Proven       bool

	// UnreachableDocumentDigests names rows this operation has AT THE LIVE
	// SCHEMA DIGEST whose document digest is NOT the catalog's.
	//
	// The routing table's primary key is (schema_digest, document_digest,
	// selected_operation), so one operation can have several rows at the
	// live digest. Only ONE of them is reachable: the edge resolves a
	// request to an operation through the catalog, then looks the row up
	// by the CATALOG's document digest. Every other row is dead in exactly
	// the way a stale schema digest is dead -- present in psql, never
	// consulted.
	//
	// r1 fixed the arbitrary-pick half of this (a reader that kept
	// whichever row came last). r2 found the half that remained: a LONE
	// row at the live schema digest whose document digest differs was
	// still classified MATCH, and could be reported reachable. It cannot
	// be reached by anything. It is now STALE, and its digest is named
	// here.
	UnreachableDocumentDigests []string
}

// Reachable reports whether a real request would be served by Go right
// now.
//
// Mirrors go_api_dispatcher's _REACHABLE_MODES and routeswitch's
// PostgresSwitch.reachableModes exactly: canary and primary only. shadow
// is NOT reachable (the client still gets Python's response), and
// python/disabled are the safe default a missing row already gives.
func (s OperationStatus) Reachable() bool {
	return s.DigestState == DigestMatch && (s.Mode == "canary" || s.Mode == "primary")
}

// CountRowsBySchemaDigest returns {schema_digest: row_count} over the
// WHOLE routing table.
//
// Deliberately unfiltered: the table holds one row per (schema version,
// document, operation) triple -- tens of rows, not a scan risk -- and the
// whole point is to see the digests nobody asked about, including the dead
// ones.
func CountRowsBySchemaDigest(ctx context.Context, db Querier) (map[string]int, error) {
	if db == nil {
		return nil, errors.New("goapiproof: nil database handle")
	}
	rows, err := db.Query(ctx, `
		SELECT schema_digest, count(*)
		  FROM public.go_api_routing_state
		 GROUP BY schema_digest`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: count routing rows by digest: %w", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var digest string
		var count int
		if err := rows.Scan(&digest, &count); err != nil {
			return nil, fmt.Errorf("goapiproof: scan digest census row: %w", err)
		}
		counts[digest] = count
	}
	return counts, rows.Err()
}

type routingStateRow struct {
	schemaDigest   string
	documentDigest string
	operation      string
	candidateBuild string
	owner          string
	mode           string
	rollout        int
	reviewEvidence string
	recordedBy     string
	updatedAt      time.Time
}

// RoutingStatusRows reports, per CATALOG operation, its row at the live
// digest or why there isn't one.
//
// Rows in the table for operations NOT in the catalog are not reported
// here -- they cannot be dispatched (the edge resolves a request to an
// operation via the catalog), so they are stale by construction. The
// per-digest census from CountRowsBySchemaDigest is what surfaces those.
func RoutingStatusRows(ctx context.Context, db Querier, liveSchemaDigest string, catalog map[string]string) ([]OperationStatus, error) {
	if db == nil {
		return nil, errors.New("goapiproof: nil database handle")
	}
	if liveSchemaDigest == "" {
		// Without a live digest there is no key to classify rows AGAINST,
		// so MATCH/STALE/MISSING would have no truth value. Refusing here
		// is what lets the CALLER still print the census, rather than
		// printing a classification it invented.
		return nil, errors.New("goapiproof: cannot classify routing rows without a live schema digest")
	}

	rows, err := db.Query(ctx, `
		SELECT schema_digest, document_digest, selected_operation, current_candidate_build,
		       owner, mode, rollout_percentage,
		       COALESCE(review_evidence, ''), COALESCE(recorded_by, ''), updated_at
		  FROM public.go_api_routing_state`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	var all []routingStateRow
	for rows.Next() {
		var row routingStateRow
		if err := rows.Scan(&row.schemaDigest, &row.documentDigest, &row.operation, &row.candidateBuild,
			&row.owner, &row.mode, &row.rollout, &row.reviewEvidence, &row.recordedBy, &row.updatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("goapiproof: scan routing row: %w", err)
		}
		all = append(all, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}

	// A row is REACHABLE only if it sits at the live schema digest AND
	// carries the catalog's document digest -- both halves of the key the
	// edge looks it up by. Anything else at the live digest is dead in the
	// same way a stale schema digest is dead, so it is collected, not
	// promoted (r2 R2-03).
	liveByOperation := map[string]routingStateRow{}
	unreachable := map[string][]string{}
	staleDigests := map[string]map[string]bool{}
	for _, row := range all {
		if row.schemaDigest != liveSchemaDigest {
			if staleDigests[row.operation] == nil {
				staleDigests[row.operation] = map[string]bool{}
			}
			staleDigests[row.operation][row.schemaDigest] = true
			continue
		}
		if row.documentDigest != catalog[row.operation] {
			unreachable[row.operation] = append(unreachable[row.operation], row.documentDigest)
			continue
		}
		// Keyed by operation ALONE, and that is safe HERE for a reason
		// worth stating, because the same shape one file over was the r1
		// F5 defect. A row only reaches this line if its document digest
		// EQUALS the catalog's, and the table's primary key is
		// (schema_digest, document_digest, selected_operation) -- so at a
		// fixed live schema digest and a fixed catalog digest there is at
		// most ONE such row per operation, and this map cannot collapse
		// two rows into one. Every other row at the live digest went to
		// `unreachable` above, where each is kept. The catalog digest is
		// the missing third part of the key, supplied by the guard rather
		// than by the map.
		liveByOperation[row.operation] = row
	}
	for operation := range unreachable {
		sort.Strings(unreachable[operation])
	}

	// Proof is keyed by the exact (schema_digest, candidate_build,
	// operation, document_digest) tuple, so the query is grouped by the
	// build each row actually points at -- a row still naming an older
	// build must not borrow a newer build's proof. The row's OWN document
	// digest is used, not the catalog's, for the same reason: a row whose
	// document digest has drifted must not borrow the catalog's proof.
	buildsWanted := map[string]map[string]string{}
	for operation, row := range liveByOperation {
		if buildsWanted[row.candidateBuild] == nil {
			buildsWanted[row.candidateBuild] = map[string]string{}
		}
		buildsWanted[row.candidateBuild][operation] = row.documentDigest
	}
	proven := map[string]bool{}
	builds := make([]string, 0, len(buildsWanted))
	for build := range buildsWanted {
		builds = append(builds, build)
	}
	sort.Strings(builds)
	for _, build := range builds {
		found, err := OperationsWithEnablementProof(ctx, db, liveSchemaDigest, build, buildsWanted[build])
		if err != nil {
			return nil, err
		}
		for operation := range found {
			proven[operation] = true
		}
	}

	statuses := make([]OperationStatus, 0, len(catalog))
	for _, operation := range CatalogOperations(catalog) {
		stale := make([]string, 0, len(staleDigests[operation]))
		for digest := range staleDigests[operation] {
			stale = append(stale, digest)
		}
		sort.Strings(stale)

		status := OperationStatus{
			Operation:                  operation,
			DocumentDigest:             catalog[operation],
			StaleDigests:               stale,
			UnreachableDocumentDigests: unreachable[operation],
		}
		row, live := liveByOperation[operation]
		switch {
		case live:
			rollout := row.rollout
			updatedAt := row.updatedAt
			status.DigestState = DigestMatch
			status.Mode = row.mode
			status.CurrentCandidateBuild = row.candidateBuild
			status.RolloutPercentage = &rollout
			status.Owner = row.owner
			status.UpdatedAt = &updatedAt
			status.ReviewEvidence = row.reviewEvidence
			status.RecordedBy = row.recordedBy
			status.Proven = proven[operation]
		case len(stale) > 0 || len(unreachable[operation]) > 0:
			// STALE covers both shapes, because they are the same fact:
			// a row exists and nothing will ever look it up. Which kind
			// it is, is in StaleDigests / UnreachableDocumentDigests.
			status.DigestState = DigestStale
		default:
			status.DigestState = DigestMissing
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}
