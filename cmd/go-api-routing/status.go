package main

// The `status` verb: a diagnostic that NEVER refuses.
//
// It never exits non-zero for an unhealthy state, because it is what an
// operator runs when things are already broken -- including when
// query-api is down, which it reports as UNREACHABLE rather than dying
// on. Each of the four things it can read (this binary's SDL digest, the
// running process's registry, the catalog, the database) fails
// INDEPENDENTLY and is reported by name; one of them being unavailable
// never suppresses the other three.
//
// The Python verb learned this twice -- codex r1 (an unguarded database
// read exited 1 with a traceback) and codex r2 (an unguarded SDL read
// emitted nothing at all, not even invalid JSON) -- and both guards are
// carried over.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// statusReport is the --json shape. Every field that can be UNKNOWN is a
// pointer or a nullable string, so "we could not tell" is a value a
// reader can see rather than a zero that looks like an answer.
type statusReport struct {
	LocalSchemaDigest   string  `json:"local_schema_digest"`
	GoPlaneSchemaDigest *string `json:"go_plane_schema_digest"`
	GoPlaneError        *string `json:"go_plane_error"`
	PlanesAgree         *bool   `json:"planes_agree"`
	// TWO independent database facts, not one (r2 R2-04). The census
	// ("how many rows at which digests") and the per-operation
	// classification are separate reads with separate failure modes, and
	// collapsing them meant a classification failure printed "registry
	// database UNREACHABLE" and SUPPRESSED a census that had already
	// succeeded -- hiding the one number that says whether anything is
	// enabled at all.
	RegistryDBError     *string                 `json:"registry_db_error"`
	ClassificationError *string                 `json:"classification_error"`
	CatalogLoaded       bool                    `json:"catalog_loaded"`
	CatalogError        *string                 `json:"catalog_error"`
	RowsBySchemaDigest  map[string]int          `json:"rows_by_schema_digest"`
	Operations          []statusReportOperation `json:"operations"`
}

type statusReportOperation struct {
	Operation                  string   `json:"operation"`
	DocumentDigest             string   `json:"document_digest"`
	DigestState                string   `json:"digest_state"`
	Mode                       *string  `json:"mode"`
	CurrentCandidateBuild      *string  `json:"current_candidate_build"`
	RolloutPercentage          *int     `json:"rollout_percentage"`
	Owner                      *string  `json:"owner"`
	UpdatedAt                  *string  `json:"updated_at"`
	ReviewEvidence             *string  `json:"review_evidence"`
	RecordedBy                 *string  `json:"recorded_by"`
	StaleDigests               []string `json:"stale_digests"`
	UnreachableDocumentDigests []string `json:"unreachable_document_digests"`
	Proven                     bool     `json:"proven"`
	// r5 P1 (reproduced): a plain `bool` can only ever say "yes" or "no",
	// so the moment reachability genuinely CANNOT be told (the go plane
	// is unreachable) it defaulted to the local row's own classification
	// -- which reads as "yes". Tri-state, matching every other "we could
	// not tell" field on this report (see the type's own doc comment):
	// nil is UNKNOWN, not a silent true.
	Reachable *bool `json:"reachable"`
	// ReachableReason names WHY, whenever Reachable is not true (r6
	// observability (6), team-lead ruling): a JSON consumer reading
	// `reachable: false` or `null` had to cross-reference DigestState,
	// DeployedDigestState and PlanesAgree by hand to learn which of them
	// is the actual cause. nil when Reachable is true -- a healthy row
	// needs no explanation.
	ReachableReason *string `json:"reachable_reason"`

	// DeployedDigestState and DeployedDocumentDigest report what the
	// RUNNING go plane's own /registry says about this operation, cross-
	// checked against DocumentDigest (the catalog's, what a row must
	// carry to be reachable) -- the same comparison `enable`'s Preflight 3
	// makes before writing, now made here too so `status` cannot report an
	// operation healthier than a write verb would treat it (r4 P1).
	//
	//   AGREE        the deployed plane registers this operation under
	//                 DocumentDigest, matching the catalog.
	//   MISMATCH      the deployed plane registers this operation, but
	//                 under a DIFFERENT document digest.
	//   UNREGISTERED  the deployed plane does not register this operation
	//                 at all.
	//   UNKNOWN       the go plane could not be reached (GoPlaneError is
	//                 set) -- deployed agreement genuinely cannot be told.
	DeployedDigestState    string  `json:"deployed_digest_state"`
	DeployedDocumentDigest *string `json:"deployed_document_digest"`
}

func runStatus(argv []string) error {
	set := newVerbFlagSet("status")
	var common commonFlags
	var registryURL string
	var asJSON bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api. Unreachable is REPORTED, never fatal (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state. Unreachable is REPORTED, never fatal")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog")
	set.BoolVar(&asJSON, "json", false, "emit machine-readable JSON instead of the text table")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "per-request timeout")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	// The environment fallback happens HERE, after Parse, rather than as
	// the flag's default value -- see bindPostgresURI: a default value is
	// printed in full by `flag`'s usage text, and this one is a DSN with a
	// password in it.
	common.resolvePostgresURI()
	// r6 P2 (reproduced): `-registry-url` used to default to a HARDCODED
	// `http://localhost:8090/registry` -- an operator who never
	// configured this at all got a silent probe of whatever happened to
	// answer there, rather than the honest "nobody told me" this
	// diagnostic can afford to print (status never refuses). Parity with
	// Python's `status`, which also never refuses on a missing URL --
	// see resolveEndpointURL's doc comment (main.go).
	registryURLResolved, haveRegistryURL := resolveEndpointURL(registryURL, "/registry")
	if haveRegistryURL {
		// codex r3 SEC-01 / r4 CRED-01: a URL carrying userinfo, a query
		// or a fragment is printed verbatim by any transport error
		// downstream. The REBUILT value is what is used from here on.
		if sanitized, err := sanitizeEndpointURL("-registry-url", registryURLResolved); err != nil {
			return err
		} else {
			registryURL = sanitized
		}
	}

	ctx := context.Background()
	local := localSchemaDigest()
	report := statusReport{
		LocalSchemaDigest:  local,
		RowsBySchemaDigest: map[string]int{},
		Operations:         []statusReportOperation{},
	}

	catalog, catalogErr := goapiproof.LoadOperationCatalog(common.catalogPath)
	report.CatalogLoaded = catalogErr == nil
	if catalogErr != nil {
		report.CatalogError = stringPtr(catalogErr.Error())
		catalog = map[string]string{}
	}

	// No credential is read here, deliberately. /registry is
	// unauthenticated (only /buildinfo checks the envelope), and a
	// diagnostic that refused without a credential would be unusable in
	// exactly the situation it exists for. status therefore never reads
	// the build identity either -- that is a WRITE verb's preflight.
	//
	// r4 P1 (reproduced): `registry.DocumentDigest` used to be read ONLY
	// for its schema digest and then discarded -- so a deployed registry
	// that agreed on schema_digest but reported a DIFFERENT document
	// digest for one operation was invisible here, even though `enable`'s
	// preflight (cmd/go-api-routing/enable.go's Preflight 3, the same
	// map) refuses that exact case. Executed: with the go plane's
	// document digest for `flowMatrix` changed and schema_digest
	// unchanged, `status` printed `flowMatrix MATCH canary 100 ok` /
	// `planes_agree=true` while `enable -dry-run` refused with "document
	// digest MISMATCH". A diagnostic that looks healthier than the write
	// verb it exists to inform is the exact "output that merely looks
	// healthy" this package's own doc comment forbids. `deployedDigests`
	// is now threaded into the per-operation report below so it can say
	// the same thing `enable` would.
	var deployedDigests map[string]string
	if !haveRegistryURL {
		// Parity with Python's `status`: no HTTP attempt at all when
		// nothing names an endpoint (no probing whatever happens to
		// answer on a hardcoded default), and the SAME sentence Python's
		// own `go_error` fallback uses.
		report.GoPlaneError = stringPtr("no -registry-url and " + queryAPIURLEnvVar + " is unset")
	} else if registry, err := goapiproof.FetchRegistry(ctx, httpClient(common.timeout), registryURL); err != nil {
		report.GoPlaneError = stringPtr(err.Error())
	} else {
		report.GoPlaneSchemaDigest = stringPtr(registry.SchemaDigest)
		agree := registry.SchemaDigest == local
		report.PlanesAgree = &agree
		deployedDigests = registry.DocumentDigest
	}

	var statuses []goapiproof.OperationStatus
	if common.postgresURI == "" {
		report.RegistryDBError = stringPtr("no -postgres-uri and " + postgresURIEnvVar + " is unset")
	} else if pool, err := connectPostgres(ctx, common.postgresURI, common.timeout); err != nil {
		// REPORTED, never fatal -- and now bounded, so a blackholed
		// database makes this verb say "unreachable" instead of hanging
		// forever (r1 F10). A diagnostic that never returns is worse than
		// one that returns bad news.
		report.RegistryDBError = stringPtr(err.Error())
	} else {
		defer pool.Close()
		// EVERY database read gets the deadline, not just the dial.
		//
		// r1's third P1, executed: with `LOCK TABLE go_api_routing_state
		// IN ACCESS EXCLUSIVE MODE` held in another transaction,
		// `status -timeout 2s` was killed at 12s having printed NOTHING
		// -- not even the schema digests and the /registry comparison,
		// which had already succeeded and need no database at all. The
		// dial bound (r1 F10) only ever covered `pool.Ping`; a lock lets
		// the connection open and then blocks the QUERY, so the bound
		// never applied to the thing that actually waits.
		//
		// A diagnostic that never returns is worse than one that returns
		// bad news, and this is the verb whose entire contract is that an
		// unhealthy state is REPORTED. The deadline covers the census and
		// the classification together, so a database that hangs costs the
		// timeout once rather than once per query, and both failures land
		// in their own report fields where the half that still works
		// still prints.
		dbCtx, cancel := context.WithTimeout(ctx, common.timeout)
		defer cancel()
		counts, err := goapiproof.CountRowsBySchemaDigest(dbCtx, pool)
		if err != nil {
			report.RegistryDBError = stringPtr(err.Error())
		} else {
			report.RowsBySchemaDigest = counts
			// Only attempt the per-operation classification once the census
			// succeeded and there is a catalog to drive it: a failure here
			// must not erase the census, which is meaningful on its own.
			if len(catalog) > 0 {
				statuses, err = goapiproof.RoutingStatusRows(dbCtx, pool, local, catalog)
				if err != nil {
					// Its OWN field: the census above succeeded and must
					// still be printed (r2 R2-04).
					report.ClassificationError = stringPtr(err.Error())
				}
			}
		}
	}
	// r5 P1 (reproduced): schema-level disagreement must prevent a
	// positive `reachable` answer the same way a per-operation document
	// digest disagreement already does -- `enable`'s preflight 2 refuses
	// on it BEFORE preflight 3 (the per-operation check) ever runs, so
	// nothing under a schema mismatch is writable regardless of what an
	// individual row's document digest says.
	schemaMismatch := report.PlanesAgree != nil && !*report.PlanesAgree
	for _, status := range statuses {
		report.Operations = append(report.Operations, toReportOperation(status, deployedDigests, report.GoPlaneError != nil, schemaMismatch))
	}

	if asJSON {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			// Reaching this would mean the report itself is unrenderable.
			// Say so rather than printing nothing -- "no output" is the one
			// thing this command must never do.
			// A defect in THIS program's own report shape, not anything an
			// operator can fix -- one of the few genuine internal errors.
			// Still not fatal: status must emit SOMETHING.
			fmt.Fprintf(stderr, "go-api-routing: %v\n", internal("the status report could not be rendered as JSON: %w", err))
			return nil
		}
		fmt.Fprintln(stdout, string(encoded))
		return nil
	}
	printStatusText(report, local)
	return nil
}

func printStatusText(report statusReport, local string) {
	fmt.Fprintf(stdout, "local schema_digest        : %s\n", local)
	switch {
	case report.GoPlaneSchemaDigest == nil:
		fmt.Fprintf(stdout, "go plane schema_digest     : UNREACHABLE (%s)\n", derefOr(report.GoPlaneError, "unknown"))
	case *report.PlanesAgree:
		fmt.Fprintf(stdout, "go plane schema_digest     : %s  [AGREE]\n", *report.GoPlaneSchemaDigest)
	default:
		fmt.Fprintf(stdout, "go plane schema_digest     : %s  [MISMATCH]\n", *report.GoPlaneSchemaDigest)
		fmt.Fprintf(stdout, "  !! Rows follow the deployed image. Every row written at %s is unreachable to this binary. See %s\n", local, runbook)
	}
	if !report.CatalogLoaded {
		fmt.Fprintf(stdout, "!! CATALOG UNAVAILABLE: %s\n", derefOr(report.CatalogError, "unknown"))
		fmt.Fprintln(stdout, "   Nothing can be reported per operation, and NOTHING is Go-eligible on the edge. This is NOT the same as an empty catalog -- regenerate with scripts/go_api/generate_operation_catalog.py")
	}
	if report.RegistryDBError != nil {
		fmt.Fprintf(stdout, "registry database          : UNREACHABLE (%s)\n", *report.RegistryDBError)
		fmt.Fprintln(stdout, "  Routing rows cannot be read, so MATCH/STALE/MISSING is unknown -- this is NOT evidence that nothing is enabled.")
		return
	}

	fmt.Fprintln(stdout, "rows by schema_digest:")
	if len(report.RowsBySchemaDigest) == 0 {
		fmt.Fprintln(stdout, "  (table is empty -- nothing is enabled for Go)")
	} else {
		digests := make([]string, 0, len(report.RowsBySchemaDigest))
		for digest := range report.RowsBySchemaDigest {
			digests = append(digests, digest)
		}
		sort.Strings(digests)
		for _, digest := range digests {
			marker := "  <- STALE"
			if digest == local {
				marker = "  <- live"
			}
			fmt.Fprintf(stdout, "  %s  %d%s\n", digest, report.RowsBySchemaDigest[digest], marker)
		}
	}
	fmt.Fprintln(stdout)
	if report.ClassificationError != nil {
		// The census above HAS printed. Only the per-operation half
		// failed, and saying so beats printing nothing (r2 R2-04).
		fmt.Fprintf(stdout, "per-operation classification: UNAVAILABLE (%s)\n", *report.ClassificationError)
		fmt.Fprintln(stdout, "  The per-digest census above is still accurate; only the per-operation table could not be built.")
		return
	}
	// r5 P1 (reproduced): the PROOF column must degrade to MISMATCH on a
	// schema-level disagreement too, not only a per-operation document
	// digest one -- the top-of-report banner above already says [MISMATCH]
	// once; this is the same fact, per row, where an operator's eye
	// actually lands.
	schemaMismatch := report.PlanesAgree != nil && !*report.PlanesAgree
	fmt.Fprintf(stdout, "%-24s %-8s %-10s %-8s PROOF\n", "OPERATION", "DIGEST", "MODE", "ROLLOUT")
	for _, operation := range report.Operations {
		mode := derefOr(operation.Mode, "-")
		rollout := "-"
		if operation.RolloutPercentage != nil {
			rollout = fmt.Sprintf("%d", *operation.RolloutPercentage)
		}
		proof := "-"
		if operation.DigestState == goapiproof.DigestMatch {
			proof = "UNPROVEN"
			if operation.Proven {
				proof = "ok"
			}
			// r4 P1 (reproduced): a proof receipt names a build served at a
			// document digest -- it says nothing about whether the DEPLOYED
			// process still registers that digest right now. Printing "ok"
			// here regardless was the "output that merely looks healthy"
			// the reviewer's repro caught: `enable -dry-run` refused the
			// identical operation with a document digest MISMATCH while
			// this line still read "ok".
			if operation.DeployedDigestState == "MISMATCH" || operation.DeployedDigestState == "UNREGISTERED" || schemaMismatch {
				proof = "MISMATCH"
			}
		}
		fmt.Fprintf(stdout, "%-24s %-8s %-10s %-8s %s\n", operation.Operation, operation.DigestState, mode, rollout, proof)
		// r2 R2-05: these were computed and never printed, which made the
		// runbook's promise to name them false.
		if len(operation.StaleDigests) > 0 {
			fmt.Fprintf(stdout, "    rows at STALE schema digests: %v\n", operation.StaleDigests)
		}
		if len(operation.UnreachableDocumentDigests) > 0 {
			fmt.Fprintf(stdout, "    rows at the LIVE schema digest the edge can never reach, document digest: %v\n", operation.UnreachableDocumentDigests)
		}
		// r4 P1 (reproduced): the deployed plane's own per-operation
		// document digest, cross-checked against the catalog's -- the
		// exact comparison `enable`'s preflight makes before it will write
		// a row, now surfaced here too so this diagnostic cannot look
		// healthier than a write verb would treat the same operation.
		switch operation.DeployedDigestState {
		case "MISMATCH":
			fmt.Fprintf(stdout, "    !! DEPLOYED document digest MISMATCH: catalog=%s go=%s -- enable would refuse this operation right now\n", operation.DocumentDigest, derefOr(operation.DeployedDocumentDigest, "unknown"))
		case "UNREGISTERED":
			fmt.Fprintln(stdout, "    !! the deployed go plane does not register this operation at all -- enable would refuse it")
		}
	}
}

func toReportOperation(status goapiproof.OperationStatus, deployedDigests map[string]string, goPlaneUnreachable bool, schemaMismatch bool) statusReportOperation {
	reported := statusReportOperation{
		Operation:                  status.Operation,
		DocumentDigest:             status.DocumentDigest,
		DigestState:                status.DigestState,
		StaleDigests:               status.StaleDigests,
		UnreachableDocumentDigests: status.UnreachableDocumentDigests,
		Proven:                     status.Proven,
	}
	if reported.StaleDigests == nil {
		reported.StaleDigests = []string{}
	}
	if reported.UnreachableDocumentDigests == nil {
		reported.UnreachableDocumentDigests = []string{}
	}
	switch {
	case goPlaneUnreachable:
		reported.DeployedDigestState = "UNKNOWN"
	case deployedDigests == nil:
		reported.DeployedDigestState = "UNKNOWN"
	default:
		if deployed, ok := deployedDigests[status.Operation]; !ok {
			reported.DeployedDigestState = "UNREGISTERED"
		} else {
			reported.DeployedDocumentDigest = stringPtr(deployed)
			if deployed == status.DocumentDigest {
				reported.DeployedDigestState = "AGREE"
			} else {
				reported.DeployedDigestState = "MISMATCH"
			}
		}
	}
	// r5 P1 (reproduced): the r4 fix downgraded Reachable on a per-
	// operation document-digest disagreement, but left TWO other ways to
	// report a positive answer that `enable`'s own preflights would
	// refuse: an unreachable/down go plane (preflight 1) and a SCHEMA-
	// level digest disagreement (preflight 2, which runs and refuses
	// BEFORE preflight 3's per-operation check ever does). Executed: a
	// schema-mismatch fixture printed `reachable=true` in JSON while
	// `enable -dry-run` exited 2 with "schema digest MISMATCH", and a
	// down plane (HTTP 503) printed `reachable=true` too, both while
	// `deployed_digest_state=UNKNOWN` on the same row -- an admission
	// this binary could not tell, sitting next to a claim that it could.
	// Order matters: an unreachable go plane is checked FIRST, because it
	// makes the schema comparison itself impossible to have made (nothing
	// downstream of "we could not even ask" gets to claim a known state).
	// r6 F3(a) (reproduced): with the go plane down, EVERY row -- including
	// a `pr` with no row at all (MISSING) -- reported `reachable: null`.
	// That is wrong for a row this binary's OWN classification already
	// knows is unreachable regardless of what the go plane says: a
	// MISSING/STALE row, or one sitting in mode python/disabled/shadow,
	// cannot become reachable no matter how the deployed plane answers.
	// `!localReachable` is therefore checked FIRST -- a known false stays
	// false even when the deployed plane's own agreement is unknown.
	// Python reports `false` here, never `null`.
	localReachable := status.Reachable()
	switch {
	case !localReachable:
		reported.Reachable = boolPtr(false)
		reported.ReachableReason = stringPtr(fmt.Sprintf("this row's own digest_state/mode (%s) is not a live, dispatchable row", status.DigestState))
	case goPlaneUnreachable:
		reported.Reachable = nil
		reported.ReachableReason = stringPtr("the go plane could not be reached, so deployed agreement is genuinely unknown")
	case schemaMismatch:
		reported.Reachable = boolPtr(false)
		reported.ReachableReason = stringPtr("the two planes disagree on the SCHEMA digest -- enable's preflight 2 would refuse before ever checking this operation")
	case reported.DeployedDigestState == "MISMATCH":
		reported.Reachable = boolPtr(false)
		reported.ReachableReason = stringPtr("the deployed plane registers this operation under a DIFFERENT document digest than the catalog's -- enable's preflight 3 would refuse it")
	case reported.DeployedDigestState == "UNREGISTERED":
		reported.Reachable = boolPtr(false)
		reported.ReachableReason = stringPtr("the deployed plane does not register this operation at all -- enable's preflight 3 would refuse it")
	default:
		reported.Reachable = boolPtr(true)
	}
	if status.DigestState != goapiproof.DigestMatch {
		return reported
	}
	reported.Mode = stringPtr(status.Mode)
	reported.CurrentCandidateBuild = stringPtr(status.CurrentCandidateBuild)
	reported.RolloutPercentage = status.RolloutPercentage
	reported.Owner = stringPtr(status.Owner)
	reported.ReviewEvidence = stringPtr(status.ReviewEvidence)
	reported.RecordedBy = stringPtr(status.RecordedBy)
	if status.UpdatedAt != nil {
		reported.UpdatedAt = stringPtr(status.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	return reported
}

func stringPtr(value string) *string { return &value }

func boolPtr(value bool) *bool { return &value }

func derefOr(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}
