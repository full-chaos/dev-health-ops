package routing

// The `status` verb: a diagnostic that NEVER refuses.
//
// It never exits non-zero for an unhealthy state, because it is what an operator runs when things are already
// broken -- including when query-api is down, which it reports as UNREACHABLE rather than dying on. Each of the
// four things it can read (this binary's SDL digest, the running process's registry, the catalog, the database)
// fails INDEPENDENTLY and is reported by name; one of them being unavailable never suppresses the other three.
//
// What it reports: the two planes' schema digests, every catalog operation with whether the deployed process
// registers it (query-api serves every registered operation, and no row decides it), and the MCP class decisions
// (go_api_class_decision), the only routing state there is.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/jackc/pgx/v5/pgxpool"
)

// statusReport is the --json shape. Every field that can be UNKNOWN is a pointer or a nullable string, so "we could
// not tell" is a value a reader can see rather than a zero that looks like an answer.
type statusReport struct {
	// The JSON key names match Python's `status --json`: `python_plane_schema_digest` is this binary's own SDL
	// digest. PythonPlaneDigestError has no failure mode here (the digest is computed from the embedded SDL) and is
	// always nil -- present for key-set parity.
	LocalSchemaDigest      string  `json:"python_plane_schema_digest"`
	PythonPlaneDigestError *string `json:"python_plane_digest_error"`
	GoPlaneSchemaDigest    *string `json:"go_plane_schema_digest"`
	GoPlaneError           *string `json:"go_plane_error"`
	PlanesAgree            *bool   `json:"planes_agree"`
	RegistryDBError        *string `json:"registry_db_error"`
	CatalogLoaded          bool    `json:"catalog_loaded"`
	CatalogError           *string `json:"catalog_error"`
	// Operations is every catalog operation with what the deployed process says about it.
	Operations []statusReportOperation `json:"operations"`
	// MCPClass is the MCP caller class's per-root-field decision state (CHAOS-7214), Go-only: the class decisions
	// are not catalog operations, so they are not in Operations. MCPClassError is its own failure field so a
	// failure here never erases the operations table.
	MCPClass      []statusReportMCPRoot `json:"mcp_class"`
	MCPClassError *string               `json:"mcp_class_error"`
}

// statusReportMCPRoot is one allowlisted MCP root field's class-row state.
type statusReportMCPRoot struct {
	Root                  string   `json:"root"`
	Operation             string   `json:"operation"`
	ServedByBinary        bool     `json:"served_by_binary"`
	DigestState           string   `json:"digest_state"`
	Mode                  *string  `json:"mode"`
	CurrentCandidateBuild *string  `json:"current_candidate_build"`
	Reachable             bool     `json:"reachable"`
	Proven                bool     `json:"proven"`
	Dark                  bool     `json:"dark"`
	StaleDigests          []string `json:"stale_digests"`
	// ProofReference/ProofShapesCounted/ProofShapesMatched/ProofShapesExcluded: what the
	// admissible class receipt for the live build rests on (empty when unproven).
	ProofReference      string   `json:"proof_reference"`
	ProofShapesCounted  int      `json:"proof_shapes_counted"`
	ProofShapesMatched  int      `json:"proof_shapes_matched"`
	ProofShapesExcluded []string `json:"proof_shapes_excluded"`
	// ProofShapesStochastic names the shapes proven under the stochastic leaf class (counted, never matched).
	ProofShapesStochastic []string `json:"proof_shapes_stochastic"`
}

// statusReportOperation is one catalog operation. query-api serves every registered operation without a routing
// row, so there is no mode, row or digest state to report: only whether the deployed process registers it.
type statusReportOperation struct {
	Operation      string `json:"operation"`
	DocumentDigest string `json:"document_digest"`
	// Served is true when the deployed process registers the operation, false when it does not, and null when the
	// deployed process could not be asked.
	Served *bool `json:"served"`
	// DeployedDigestState is what the RUNNING go plane's /registry says about this operation, against the
	// catalog's DocumentDigest:
	//   AGREE        the deployed plane registers it under DocumentDigest.
	//   MISMATCH     it registers it under a DIFFERENT document digest.
	//   UNREGISTERED it does not register it at all.
	//   UNKNOWN      the go plane could not be reached.
	DeployedDigestState    string  `json:"deployed_digest_state"`
	DeployedDocumentDigest *string `json:"deployed_document_digest"`
}

func toReportOperation(operation, catalogDigest string, deployed map[string]string, goPlaneUnreachable bool) statusReportOperation {
	report := statusReportOperation{Operation: operation, DocumentDigest: catalogDigest, DeployedDigestState: "UNKNOWN"}
	if goPlaneUnreachable || deployed == nil {
		return report
	}
	registered, ok := deployed[operation]
	switch {
	case !ok:
		report.DeployedDigestState, report.Served = "UNREGISTERED", boolPtr(false)
	case registered != catalogDigest:
		report.DeployedDigestState, report.DeployedDocumentDigest, report.Served = "MISMATCH", stringPtr(registered), boolPtr(true)
	default:
		report.DeployedDigestState, report.DeployedDocumentDigest, report.Served = "AGREE", stringPtr(registered), boolPtr(true)
	}
	return report
}

func runStatus(argv []string) error {
	set := newVerbFlagSet("status")
	var common commonFlags
	var registryURL string
	var asJSON bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api. Unreachable is REPORTED, never fatal (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_class_decision. Unreachable is REPORTED, never fatal")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog")
	set.BoolVar(&asJSON, "json", false, "emit machine-readable JSON instead of the text table")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "per-request timeout")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	// The environment fallback happens HERE, after Parse, rather than as the flag's default value: a default value
	// is printed in full by `flag`'s usage text, and this one is a DSN with a password in it.
	common.resolvePostgresURI()
	registryURLResolved, haveRegistryURL := resolveEndpointURL(registryURL, "/registry")
	// An unusable GO_API_QUERY_API_URL inherited from the environment is exactly the already-broken state this verb
	// exists to report, not die on: the error is stashed and applied to the report once it exists.
	var registryURLSanitizeErr error
	if haveRegistryURL {
		// Name the SOURCE that actually supplied the value, so an operator who never typed the flag is not blamed.
		label := "-registry-url"
		if registryURL == "" {
			label = queryAPIURLEnvVar
		}
		if sanitized, err := sanitizeEndpointURL(label, registryURLResolved); err != nil {
			registryURLSanitizeErr = err
			haveRegistryURL = false
		} else {
			registryURL = sanitized
		}
	}

	ctx := context.Background()
	local := localSchemaDigest()
	report := statusReport{LocalSchemaDigest: local, Operations: []statusReportOperation{}, MCPClass: []statusReportMCPRoot{}}
	if registryURLSanitizeErr != nil {
		report.GoPlaneError = stringPtr(registryURLSanitizeErr.Error())
	}

	catalog, catalogErr := goapiproof.LoadOperationCatalog(common.catalogPath)
	report.CatalogLoaded = catalogErr == nil
	if catalogErr != nil {
		report.CatalogError = stringPtr(catalogErr.Error())
		catalog = map[string]string{}
	}

	// No credential is read here, deliberately. /registry is unauthenticated, and a diagnostic that refused without
	// a credential would be unusable in exactly the situation it exists for.
	var deployedDigests map[string]string
	if !haveRegistryURL && registryURLSanitizeErr != nil {
		// GoPlaneError is already set to the sanitize failure above.
	} else if !haveRegistryURL {
		report.GoPlaneError = stringPtr("no -registry-url and " + queryAPIURLEnvVar + " is unset")
	} else if registry, err := goapiproof.FetchRegistry(ctx, httpClient(common.timeout), registryURL); err != nil {
		report.GoPlaneError = stringPtr(err.Error())
	} else {
		report.GoPlaneSchemaDigest = stringPtr(registry.SchemaDigest)
		agree := registry.SchemaDigest == local
		report.PlanesAgree = &agree
		deployedDigests = registry.DocumentDigest
	}

	names := make([]string, 0, len(catalog))
	for operation := range catalog {
		names = append(names, operation)
	}
	sort.Strings(names)
	for _, operation := range names {
		report.Operations = append(report.Operations, toReportOperation(operation, catalog[operation], deployedDigests, report.GoPlaneError != nil))
	}

	// "Live" is a property of the DEPLOYED PROCESS; this binary's own digest stands in only when it could not be
	// asked, and that is reported by the plane lines above, never silent.
	liveDigest := local
	if report.GoPlaneSchemaDigest != nil {
		liveDigest = *report.GoPlaneSchemaDigest
	}
	if common.postgresURI == "" {
		report.RegistryDBError = stringPtr("no -postgres-uri and " + postgresURIEnvVar + " is unset")
	} else if pool, err := connectPostgres(ctx, common.postgresURI, common.timeout); err != nil {
		// REPORTED, never fatal -- and bounded, so a blackholed database makes this verb say "unreachable" instead
		// of hanging forever.
		report.RegistryDBError = stringPtr(redactCredentials(err.Error()))
	} else {
		defer pool.Close()
		// EVERY database read gets the deadline, not just the dial: a lock lets the connection open and then blocks
		// the QUERY, which a dial-only bound never covers.
		dbCtx, cancel := context.WithTimeout(ctx, common.timeout)
		defer cancel()
		classRows, classErr := mcpClassStatus(dbCtx, pool, liveDigest)
		if classErr != nil {
			report.MCPClassError = stringPtr(redactCredentials(classErr.Error()))
		}
		report.MCPClass = classRows
	}

	if asJSON {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			// Reaching this would mean the report itself is unrenderable. Say so rather than printing nothing.
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
		fmt.Fprintf(stdout, "  !! This binary's SDL is NOT the deployed one. `enable` and `seed` run from THIS binary refuse until they agree. See %s\n", runbook)
	}
	if !report.CatalogLoaded {
		fmt.Fprintf(stdout, "!! CATALOG UNAVAILABLE: %s\n", derefOr(report.CatalogError, "unknown"))
		fmt.Fprintln(stdout, "   Nothing can be reported per operation. This is NOT the same as an empty catalog -- regenerate with scripts/go_api/generate_operation_catalog.py.")
	}
	fmt.Fprintln(stdout, "catalog operations are served by query-api whenever the deployed process registers them; no routing row decides it")
	fmt.Fprintf(stdout, "%-30s %-13s %s\n", "OPERATION", "DEPLOYED", "SERVED")
	for _, operation := range report.Operations {
		served := "unknown"
		if operation.Served != nil {
			served = "no"
			if *operation.Served {
				served = "yes"
			}
		}
		fmt.Fprintf(stdout, "%-30s %-13s %s\n", operation.Operation, operation.DeployedDigestState, served)
		switch operation.DeployedDigestState {
		case "MISMATCH":
			fmt.Fprintf(stdout, "    !! DEPLOYED document digest MISMATCH: catalog=%s go=%s\n", operation.DocumentDigest, derefOr(operation.DeployedDocumentDigest, "unknown"))
		case "UNREGISTERED":
			fmt.Fprintln(stdout, "    !! the deployed go plane does not register this operation at all")
		}
	}
	if report.RegistryDBError != nil {
		fmt.Fprintf(stdout, "\nclass decisions            : UNREACHABLE (%s)\n", *report.RegistryDBError)
		fmt.Fprintln(stdout, "  The MCP class decisions cannot be read, so which roots serve is unknown -- this is NOT evidence that none is enabled.")
		return
	}
	printMCPClassStatus(report)
}

func stringPtr(value string) *string { return &value }

func boolPtr(value bool) *bool { return &value }

func derefOr(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}

// printMCPClassStatus prints the MCP class table after the operations table.
// DARK names a root with no reachable row at the live digest: after a schema
// digest move that is what a missed carry looks like, and "missing is not
// healthy" applies to it as to any operation.
func printMCPClassStatus(report statusReport) {
	fmt.Fprintln(stdout)
	if report.MCPClassError != nil {
		fmt.Fprintf(stdout, "mcp class rows: UNAVAILABLE (%s)\n", *report.MCPClassError)
		return
	}
	fmt.Fprintln(stdout, "(class rows are enabled in canary only: they have no edge route, so primary is never admitted)")
	fmt.Fprintf(stdout, "%-30s %-8s %-10s %-8s %s\n", "MCP CLASS ROOT", "DIGEST", "MODE", "PROOF", "LISTENER")
	for _, row := range report.MCPClass {
		proof := "-"
		if row.DigestState == goapiproof.DigestMatch {
			proof = "UNPROVEN"
			if row.Proven {
				proof = "ok"
			}
		}
		listener := "DARK (answers 404 root_field_not_enabled)"
		if row.Reachable {
			listener = "serves"
		}
		if !row.ServedByBinary {
			listener += "; NOT a Query root of this binary's SDL"
		}
		fmt.Fprintf(stdout, "%-30s %-8s %-10s %-8s %s\n", row.Operation, row.DigestState, derefOr(row.Mode, "-"), proof, listener)
		if len(row.StaleDigests) > 0 {
			fmt.Fprintf(stdout, "    rows at other schema or document digests, never read: %v\n", row.StaleDigests)
		}
		if row.Proven && (row.ProofReference != "" || row.ProofShapesCounted > 0 || len(row.ProofShapesExcluded) > 0) {
			// A receipt written before the reference was recorded still has counts and
			// exclusions worth showing; it says so rather than hiding them.
			reference := row.ProofReference
			if reference == "" {
				reference = "(not recorded)"
			}
			fmt.Fprintf(stdout, "    proof: reference=%s shapes counted=%d matched=%d excluded=%d\n", reference, row.ProofShapesCounted, row.ProofShapesMatched, len(row.ProofShapesExcluded))
			for _, stochastic := range row.ProofShapesStochastic {
				fmt.Fprintf(stdout, "      proven under the stochastic leaf class (not a match): %s\n", stochastic)
			}
			for _, excluded := range row.ProofShapesExcluded {
				fmt.Fprintf(stdout, "      excluded: %s\n", excluded)
			}
		}
	}
}

// mcpClassStatus builds the MCP class table: this binary's served roots (the class
// allowlist intersected with its SDL) against the rows at the live digest.
func mcpClassStatus(ctx context.Context, pool *pgxpool.Pool, liveDigest string) ([]statusReportMCPRoot, error) {
	served, err := mcpclass.ServedRoots(schemav1.SDL)
	if err != nil {
		return nil, err
	}
	rows, err := goapiproof.MCPClassStatusRows(ctx, pool, liveDigest, served)
	if err != nil {
		return nil, err
	}
	out := make([]statusReportMCPRoot, 0, len(rows))
	for _, row := range rows {
		entry := statusReportMCPRoot{Root: row.Root, Operation: row.Operation, ServedByBinary: row.ServedByBinary,
			DigestState: row.DigestState, Reachable: row.Reachable, Proven: row.Proven, Dark: row.Dark, StaleDigests: row.StaleDigests,
			ProofReference: row.ProofReference, ProofShapesCounted: row.ProofExecuted, ProofShapesMatched: row.ProofMatched, ProofShapesExcluded: row.ProofExcluded, ProofShapesStochastic: row.ProofStochastic}
		if entry.ProofShapesStochastic == nil {
			entry.ProofShapesStochastic = []string{}
		}
		if entry.ProofShapesExcluded == nil {
			entry.ProofShapesExcluded = []string{}
		}
		if row.DigestState == goapiproof.DigestMatch {
			entry.Mode, entry.CurrentCandidateBuild = stringPtr(row.Mode), stringPtr(row.CurrentCandidateBuild)
		}
		if entry.StaleDigests == nil {
			entry.StaleDigests = []string{}
		}
		out = append(out, entry)
	}
	return out, nil
}
