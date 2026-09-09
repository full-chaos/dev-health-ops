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
	Reachable                  bool     `json:"reachable"`
}

func runStatus(argv []string) error {
	set := newVerbFlagSet("status")
	var common commonFlags
	var registryURL string
	var asJSON bool
	set.StringVar(&registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api. Unreachable is REPORTED, never fatal")
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
	// codex r3 SEC-01 / r4 CRED-01: a URL carrying userinfo, a query or
	// a fragment is printed verbatim by any transport error downstream.
	// The REBUILT value is what is used from here on.
	if sanitized, err := sanitizeEndpointURL("-registry-url", registryURL); err != nil {
		return err
	} else {
		registryURL = sanitized
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
	if registry, err := goapiproof.FetchRegistry(ctx, httpClient(common.timeout), registryURL); err != nil {
		report.GoPlaneError = stringPtr(err.Error())
	} else {
		report.GoPlaneSchemaDigest = stringPtr(registry.SchemaDigest)
		agree := registry.SchemaDigest == local
		report.PlanesAgree = &agree
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
	for _, status := range statuses {
		report.Operations = append(report.Operations, toReportOperation(status))
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
	}
}

func toReportOperation(status goapiproof.OperationStatus) statusReportOperation {
	reported := statusReportOperation{
		Operation:                  status.Operation,
		DocumentDigest:             status.DocumentDigest,
		DigestState:                status.DigestState,
		StaleDigests:               status.StaleDigests,
		UnreachableDocumentDigests: status.UnreachableDocumentDigests,
		Proven:                     status.Proven,
		Reachable:                  status.Reachable(),
	}
	if reported.StaleDigests == nil {
		reported.StaleDigests = []string{}
	}
	if reported.UnreachableDocumentDigests == nil {
		reported.UnreachableDocumentDigests = []string{}
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

func derefOr(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}
