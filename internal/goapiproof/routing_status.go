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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	// DigestPending: no row at the LIVE digest, but a row exists at the
	// digest the caller's own binary computes.
	//
	// Distinct from STALE because the two are opposite predictions about
	// the same table: STALE says nothing will ever read this row, PENDING
	// says something WILL read it as soon as the image the caller was
	// built from is deployed. During the pre-roll window `carry` creates,
	// every row it has just written is PENDING -- reporting those STALE
	// tells the operator the carry failed when it in fact succeeded.
	DigestPending = "PENDING"
)

// OperationStatus is one row of `status`.
//
// Proven is orthogonal to DigestState: a row can be live and reachable
// while nothing ever proved the deployed build serves it. Rendering that
// as UNPROVEN (or NAMED-LIMIT, when the row says the ledger's written limit
// admitted it) is what keeps an enablement with no proof visible for as
// long as it is in force, not just in the log line written the moment it
// happened.
type OperationStatus struct {
	Operation      string
	DocumentDigest string
	DigestState    string

	Mode                  string
	CurrentCandidateBuild string
	RolloutPercentage     *int
	// EligibleOrgs is the row's eligible_orgs column as text (nil = SQL NULL).
	EligibleOrgs   *string
	Owner          string
	UpdatedAt      *time.Time
	ReviewEvidence string
	RecordedBy     string

	StaleDigests []string
	Proven       bool
	// NamedLimit is true for a NOT store-proven live row whose own
	// review_evidence says the go-served ledger's written limit admitted it.
	// It is deliberately not Proven.
	NamedLimit bool
	// VenueProof is the class (VenueClassAdmin/VenueClassNoData) a NOT
	// store-proven live row's review_evidence claims when a venue receipt
	// written by the retired venue path admitted it; empty otherwise. It is
	// read only, so such a row keeps its own word until `enable` rewrites it.
	VenueProof string

	// PendingDigests names rows this operation has at the schema digest
	// the CALLER's own binary computes, when that is not the digest the
	// deployed process computes.
	//
	// They are not stale and they are not live: nothing reads them YET,
	// and the reason nothing reads them yet is that the image which will
	// is not deployed. Collapsing them into StaleDigests was the same
	// inversion censusMarker exists to prevent, one column over -- during
	// the pre-roll window `carry` creates, EVERY freshly carried row
	// would read STALE on the one command an operator runs to decide
	// whether to roll.
	PendingDigests []string

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

// UnenforcedControls names the routing controls a REACHABLE row carries that
// no plane obeys (CHAOS-6807): a rollout_percentage other than 100 and a
// non-empty eligible_orgs. canary and primary are both "on for every
// authenticated org, revocable only by mode": neither the Python edge
// dispatcher nor PostgresSwitch.Enabled receives an org, and the operations
// have no Python resolver to fall back to, so a cohort could not be honoured
// even if one were named. A row that is not reachable (python, disabled,
// shadow, or no live row) has no such claim to make. NULL, JSON null, [] and
// {} record no allowlist.
func (s OperationStatus) UnenforcedControls() []string {
	controls := []string{}
	if s.DigestState != DigestMatch || !s.Reachable() {
		return controls
	}
	if s.RolloutPercentage != nil && *s.RolloutPercentage != EnforcedRolloutPercentage {
		controls = append(controls, fmt.Sprintf("rollout_percentage=%d", *s.RolloutPercentage))
	}
	if s.EligibleOrgs != nil && !eligibleOrgsIsEmpty(*s.EligibleOrgs) {
		controls = append(controls, "eligible_orgs="+strings.TrimSpace(*s.EligibleOrgs))
	}
	return controls
}

// eligibleOrgsIsEmpty reports whether an eligible_orgs column value records no
// allowlist: SQL text of NULL, JSON null, an empty array or an empty object,
// however the json column spells them (it keeps insignificant whitespace, so
// `[ ]` is as empty as `[]`). The value is read as JSON, not compared as text.
// Text that is not JSON at all is not empty: better a false warning than a
// hidden cohort.
func eligibleOrgsIsEmpty(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return true
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return false
	}
	switch value := decoded.(type) {
	case nil:
		return true
	case []any:
		return len(value) == 0
	case map[string]any:
		return len(value) == 0
	}
	return false
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
	eligibleOrgs   *string
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
// liveSchemaDigest is the digest the DEPLOYED process computes -- never
// the caller's own, unless the caller has no way to learn the deployed
// one and says so. "Live" is a property of what is running.
//
// pendingSchemaDigest is the caller's own digest when it differs from the
// live one (empty otherwise). Rows there are reported PendingDigests
// rather than StaleDigests: see that field.
//
// No normalisation of the two coinciding, deliberately: a row at the live
// digest never reaches the pending bucket at all (it takes the live
// branch first), so passing the same value twice is already a no-op, and
// a guard whose removal changes nothing observable is a guard that reads
// as coverage without being any.
func RoutingStatusRows(ctx context.Context, db Querier, liveSchemaDigest, pendingSchemaDigest string, catalog map[string]string) ([]OperationStatus, error) {
	return RoutingStatusRowsWithKinds(ctx, db, liveSchemaDigest, pendingSchemaDigest, catalog, nil)
}

// RoutingStatusRowsWithKinds is RoutingStatusRows for a catalog that carries
// GraphQL mutations (CHAOS-6810): an operation named in mutationOperations is
// judged PROVEN only by a write_executed write receipt, every other operation by
// a deployed_executed one, the same rule `enable` applies. Nil judges every
// operation as a query.
func RoutingStatusRowsWithKinds(ctx context.Context, db Querier, liveSchemaDigest, pendingSchemaDigest string, catalog map[string]string, mutationOperations map[string]bool) ([]OperationStatus, error) {
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
		       owner, mode, rollout_percentage, eligible_orgs::text,
		       COALESCE(review_evidence, ''), COALESCE(recorded_by, ''), updated_at
		  FROM public.go_api_routing_state`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows: %w", err)
	}
	var all []routingStateRow
	for rows.Next() {
		var row routingStateRow
		if err := rows.Scan(&row.schemaDigest, &row.documentDigest, &row.operation, &row.candidateBuild,
			&row.owner, &row.mode, &row.rollout, &row.eligibleOrgs, &row.reviewEvidence, &row.recordedBy, &row.updatedAt); err != nil {
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
	pendingDigests := map[string]map[string]bool{}
	for _, row := range all {
		if row.schemaDigest != liveSchemaDigest {
			bucket := staleDigests
			// A row at the caller's own digest is PENDING, not stale:
			// "nothing reads this yet" and "nothing will ever read this"
			// are different facts and an operator acts differently on
			// each.
			if pendingSchemaDigest != "" && row.schemaDigest == pendingSchemaDigest {
				bucket = pendingDigests
			}
			if bucket[row.operation] == nil {
				bucket[row.operation] = map[string]bool{}
			}
			bucket[row.operation][row.schemaDigest] = true
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
	//
	// The route rule also depends on the row's OWN mode, matching Python's
	// routing_status_rows exactly: a `primary` row is held to the strict
	// edge-only rule (this is the mode `enable --mode primary` would use);
	// every other mode -- canary, but also python/shadow/disabled, none of
	// which `enable` can even target -- reads the permissive any-route
	// rule, the same one `enable --mode canary` uses. So the group key is
	// (build, target mode), not build alone.
	type buildModeKey struct{ build, targetMode string }
	buildsWanted := map[buildModeKey]map[string]string{}
	for operation, row := range liveByOperation {
		targetMode := TargetModeCanary
		if row.mode == TargetModePrimary {
			targetMode = TargetModePrimary
		}
		key := buildModeKey{build: row.candidateBuild, targetMode: targetMode}
		if buildsWanted[key] == nil {
			buildsWanted[key] = map[string]string{}
		}
		buildsWanted[key][operation] = row.documentDigest
	}
	proven := map[string]bool{}
	keys := make([]buildModeKey, 0, len(buildsWanted))
	for key := range buildsWanted {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].build != keys[j].build {
			return keys[i].build < keys[j].build
		}
		return keys[i].targetMode < keys[j].targetMode
	})
	for _, key := range keys {
		found, err := OperationsWithEnablementProofByKind(ctx, db, liveSchemaDigest, key.build, key.targetMode, buildsWanted[key], mutationOperations)
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
		pending := make([]string, 0, len(pendingDigests[operation]))
		for digest := range pendingDigests[operation] {
			pending = append(pending, digest)
		}
		sort.Strings(pending)

		status := OperationStatus{
			Operation:                  operation,
			DocumentDigest:             catalog[operation],
			StaleDigests:               stale,
			PendingDigests:             pending,
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
			status.EligibleOrgs = row.eligibleOrgs
			status.Owner = row.owner
			status.UpdatedAt = &updatedAt
			status.ReviewEvidence = row.reviewEvidence
			status.RecordedBy = row.recordedBy
			status.Proven = proven[operation]
			if !status.Proven {
				status.NamedLimit = HasNamedLimitEvidence(row.reviewEvidence)
				status.VenueProof = LegacyVenueEvidenceClass(row.reviewEvidence)
			}
		case len(stale) == 0 && len(unreachable[operation]) == 0 && len(pending) > 0:
			// ONLY pending rows: nothing is live, nothing is dead, and
			// the operator is mid-window. Named as its own state so the
			// report cannot say "stale" about a row that is waiting.
			status.DigestState = DigestPending
		case len(stale) > 0 || len(pending) > 0 || len(unreachable[operation]) > 0:
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
