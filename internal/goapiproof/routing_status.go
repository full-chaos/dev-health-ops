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
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// Digest states a routing row can be in, relative to the LIVE schema
// digest.
const (
	// DigestMatch: a row exists at the live digest. Reachable if its mode
	// says so.
	DigestMatch = "MATCH"
	// DigestStale: rows exist for this operation but NONE of them is
	// reachable -- either they sit at other SCHEMA digests, or they sit
	// at the live one under a DOCUMENT digest the operation does not accept.
	// Both are the silent-death shape: present in psql, never consulted.
	DigestStale = "STALE"
	// DigestMissing: no row at any digest. Never enabled, or cleaned up.
	// query-api serves a catalog operation in this state (CHAOS-8517): see
	// OperationStatus.ServedWithoutRow.
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
	// DigestUnregistered: a live row (at the live schema digest) of an operation
	// the catalog does not register. Nothing can dispatch to it -- the edge
	// resolves an operation through the catalog -- and nothing else names it, so
	// it is listed as an entry of its own, with the row's own document digest, or
	// it is invisible (missing is not healthy). Python's status lists these rows
	// under the same word.
	DigestUnregistered = "UNREGISTERED"
)

// Document classes of the row a MATCH reports (CHAOS-8649), named in OperationStatus.DocumentClass.
const (
	// DocumentClassCurrent: the row sits under the catalog's current document digest.
	DocumentClassCurrent = "current"
	// DocumentClassLegacy: the row sits under a document digest the catalog registers as a LEGACY text of
	// the operation (CHAOS-8000 dual accept). query-api's switch reads such a row exactly as it reads the
	// current one (routeswitch.AcceptedDigests), so in a served mode it serves the operation.
	DocumentClassLegacy = "legacy"
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

	// DocumentClass names which accepted document the row a MATCH reports sits under:
	// DocumentClassCurrent or DocumentClassLegacy (CHAOS-8649). Empty for every other DigestState.
	DocumentClass string
	// RowDocumentDigest is the document digest of the row a MATCH reports: DocumentDigest for a
	// current-class row, the legacy text's own digest for a legacy-class one. Empty for every other
	// DigestState.
	RowDocumentDigest string
	// AcceptedDocumentDigests names, sorted, the document digest of EVERY row this operation has at the
	// live schema digest under a document it accepts: its current text or a registered legacy one
	// (routeswitch.AcceptedDigests, the switch's own rule). query-api's switch reads each of them and
	// serves the operation when any one is in a served mode; the row a MATCH reports is the one that
	// decides that answer (see RoutingStatusRowsWithLegacy). Listed so that a second accepted row is never
	// invisible. Empty when there is none.
	AcceptedDocumentDigests []string
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
	// SCHEMA DIGEST whose document digest is NOT one the operation accepts:
	// neither the catalog's current digest nor a legacy digest the catalog
	// registers for it (CHAOS-8649).
	//
	// The routing table's primary key is (schema_digest, document_digest,
	// selected_operation), so one operation can have several rows at the
	// live digest. The edge resolves a request to an operation through the
	// catalog, then reads the rows under the documents that operation
	// ACCEPTS (routeswitch.AcceptedDigests): the current text and its
	// registered legacy texts (CHAOS-8000 dual accept); those rows are in
	// AcceptedDocumentDigests. Every other row is dead in exactly the way a
	// stale schema digest is dead -- present in psql, never consulted.
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

// Reachable reports whether a real request is served by this operation's
// routing ROW right now.
//
// Mirrors go_api_dispatcher's _REACHABLE_MODES and routeswitch's
// PostgresSwitch.reachableModes exactly: canary and primary only. shadow
// is NOT reachable (the client still gets Python's response), and
// python/disabled are not served.
//
// It is a statement about the row, and stays one: an operation with no
// row at all is not Reachable in this sense, and query-api serves it all
// the same (CHAOS-8517). ServedWithoutRow says that.
func (s OperationStatus) Reachable() bool {
	return s.DigestState == DigestMatch && servedMode(s.Mode)
}

// servedMode is the mode half of Reachable: canary and primary only.
func servedMode(mode string) bool {
	return mode == "canary" || mode == "primary"
}

// ServedWithoutRow reports the one state in which query-api serves a
// catalog operation that has no routing row (CHAOS-8517,
// queryapi/routeswitch/catalog_switch.go): the operation has NO row at
// any schema digest, under any document digest -- DigestMissing, which
// RoutingStatusRowsWithKinds assigns to exactly that state.
//
// Every other rowless-at-the-live-digest state (STALE, PENDING) is an
// operation that HAS a row somewhere, and query-api does not serve it:
// a row left at another digest still holds its operation dark.
func (s OperationStatus) ServedWithoutRow() bool {
	return s.DigestState == DigestMissing
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
	// A caller of THIS reader declares its catalog all queries, by calling it.
	kinds := make(map[string]string, len(catalog))
	for operation := range catalog {
		kinds[operation] = OperationKindQuery
	}
	return RoutingStatusRowsWithKinds(ctx, db, liveSchemaDigest, pendingSchemaDigest, catalog, kinds)
}

// RoutingStatusRowsWithKinds is RoutingStatusRows for a catalog that carries
// GraphQL mutations (CHAOS-6810): an operation of kind mutation is judged PROVEN
// only by a write_executed write receipt, a query by a deployed_executed one, the
// same rule `enable` applies; an operation with no known kind is never PROVEN.
func RoutingStatusRowsWithKinds(ctx context.Context, db Querier, liveSchemaDigest, pendingSchemaDigest string, catalog map[string]string, operationKinds map[string]string) ([]OperationStatus, error) {
	return RoutingStatusRowsWithLegacy(ctx, db, liveSchemaDigest, pendingSchemaDigest, catalog, operationKinds, nil)
}

// RoutingStatusRowsWithLegacy is RoutingStatusRowsWithKinds for a catalog that registers LEGACY texts
// (CHAOS-8000 dual accept; LoadOperationCatalogWithKindsAndLegacy reads them). legacy maps an operation to
// the document digests of its legacy texts; nil, or an operation absent from it, means the operation
// accepts its current text only, which is exactly RoutingStatusRowsWithKinds.
//
// CHAOS-8649: query-api's switch reads an operation's rows at the live schema digest under EVERY document
// the operation accepts (routeswitch.AcceptedDigests: the current digest, then the legacy ones) and serves
// the operation when any one of them is in a served mode. The census uses the same function on the same
// catalog data, so it counts exactly those rows: a row under a registered legacy digest is MATCH, with
// DocumentClass DocumentClassLegacy and its own mode and build, not UNREACHABLE. A row under a digest the
// operation does not accept stays in UnreachableDocumentDigests.
func RoutingStatusRowsWithLegacy(ctx context.Context, db Querier, liveSchemaDigest, pendingSchemaDigest string, catalog map[string]string, operationKinds map[string]string, legacy map[string][]string) ([]OperationStatus, error) {
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
	// carries a document digest the operation accepts -- both halves of the
	// key the edge looks it up by. Anything else at the live digest is dead
	// in the same way a stale schema digest is dead, so it is collected, not
	// promoted (r2 R2-03).
	acceptedByOperation := map[string][]routingStateRow{}
	var unregistered []routingStateRow
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
		// An MCP class row is not a catalog operation by construction: it has its
		// own table (MCPClassStatusRows), and listing it here as UNREGISTERED
		// would be false.
		if mcpclass.IsClassRow(row.operation, row.documentDigest) {
			continue
		}
		// A live row of an operation the catalog does not register has no catalog
		// row to hang off, so it becomes an entry of its own below.
		if _, registered := catalog[row.operation]; !registered {
			unregistered = append(unregistered, row)
			continue
		}
		// The switch's own rule, on the same catalog data: the rows it reads for this
		// operation are the ones under routeswitch.AcceptedDigests (CHAOS-8649).
		if !slices.Contains(routeswitch.AcceptedDigests(catalog[row.operation], legacy[row.operation]), row.documentDigest) {
			unreachable[row.operation] = append(unreachable[row.operation], row.documentDigest)
			continue
		}
		// Every accepted row is KEPT, never collapsed into one per operation
		// (the r1 F5 defect one file over was a map that did that). The
		// table's primary key is (schema_digest, document_digest,
		// selected_operation), so at the live schema digest there is at most
		// one row per accepted document; decidingRow picks the one that
		// decides the switch's answer, and AcceptedDocumentDigests names all.
		acceptedByOperation[row.operation] = append(acceptedByOperation[row.operation], row)
	}
	for operation := range unreachable {
		sort.Strings(unreachable[operation])
	}
	liveByOperation := make(map[string]routingStateRow, len(acceptedByOperation))
	acceptedDigestsByOperation := make(map[string][]string, len(acceptedByOperation))
	for operation, rows := range acceptedByOperation {
		liveByOperation[operation] = decidingRow(rows, routeswitch.AcceptedDigests(catalog[operation], legacy[operation]))
		digests := make([]string, 0, len(rows))
		for _, row := range rows {
			digests = append(digests, row.documentDigest)
		}
		sort.Strings(digests)
		acceptedDigestsByOperation[operation] = digests
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
		found, err := OperationsWithEnablementProofByKind(ctx, db, liveSchemaDigest, key.build, key.targetMode, buildsWanted[key], operationKinds)
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
			AcceptedDocumentDigests:    acceptedDigestsByOperation[operation],
		}
		row, live := liveByOperation[operation]
		switch {
		case live:
			rollout := row.rollout
			updatedAt := row.updatedAt
			status.DigestState = DigestMatch
			status.RowDocumentDigest = row.documentDigest
			status.DocumentClass = DocumentClassCurrent
			if row.documentDigest != catalog[operation] {
				status.DocumentClass = DocumentClassLegacy
			}
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
	// Every live row nobody registered, one entry per row, in a fixed order. Proven is
	// false: a receipt proves a catalog operation's document, and there is none here.
	sort.Slice(unregistered, func(i, j int) bool {
		if unregistered[i].operation != unregistered[j].operation {
			return unregistered[i].operation < unregistered[j].operation
		}
		return unregistered[i].documentDigest < unregistered[j].documentDigest
	})
	for _, row := range unregistered {
		rollout := row.rollout
		updatedAt := row.updatedAt
		statuses = append(statuses, OperationStatus{
			Operation:             row.operation,
			DocumentDigest:        row.documentDigest,
			DigestState:           DigestUnregistered,
			Mode:                  row.mode,
			CurrentCandidateBuild: row.candidateBuild,
			RolloutPercentage:     &rollout,
			EligibleOrgs:          row.eligibleOrgs,
			Owner:                 row.owner,
			UpdatedAt:             &updatedAt,
			ReviewEvidence:        row.reviewEvidence,
			RecordedBy:            row.recordedBy,
		})
	}
	return statuses, nil
}

// decidingRow is the one of an operation's accepted rows that decides the switch's answer, so the census
// reports the row the switch acts on. PostgresSwitch.Enabled serves the operation when ANY accepted row is
// in a served mode (anyReachable), so the deciding row is the first row in a served mode, in the order of
// accepted (the current digest, then the legacy ones as the catalog lists them); when none is served, the
// first row in that order, whose mode then says why the operation is not served. rows is never empty.
func decidingRow(rows []routingStateRow, accepted []string) routingStateRow {
	ordered := slices.Clone(rows)
	slices.SortStableFunc(ordered, func(a, b routingStateRow) int {
		return slices.Index(accepted, a.documentDigest) - slices.Index(accepted, b.documentDigest)
	})
	for _, row := range ordered {
		if servedMode(row.mode) {
			return row
		}
	}
	return ordered[0]
}
