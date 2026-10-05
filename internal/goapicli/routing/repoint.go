package routing

// The `repoint` verb: CHAOS-5486's mode-preserving provenance write.
//
// WHAT enable/disable COULD NOT DO, measured on the compose stack
// 2026-09-09 (lane-stack-owner, JOB 4 and JOB 5 prep):
//
//   - `routing enable` writes current_candidate_build but requires
//     --mode canary|primary, so re-pointing a shadow row would also make
//     that operation Go-serving through the product edge;
//   - `routing disable` accepts --mode shadow but its --candidate-build
//     is a guard documented "Never written -- disable changes mode only".
//
// Three shadow rows were therefore stuck naming an old build while the
// process ran another, and the proof runner refused on all fifteen
// operations because it inspects every row regardless of mode.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// repointResult is -json's output shape for `repoint`, the same mechanism carry.go's
// carryResult provides -- see its doc comment for the reasoning (D2828/D2829). repoint's
// own refusal vocabulary is flatter than carry's: it has no equivalent of "digest_unchanged"
// or "stale_build" (nothing bigboy-cut.sh or the Helm hook currently branches on for
// repoint), so Reason is one of "repointed" (success, including a dry run, a run that
// repointed zero eligible rows, the no-op on an empty table, which EmptyTable marks, and
// the no-op on a table whose rows are all dark and elsewhere, which DarkRowsOnly marks),
// "refused" (any refusal), or "error" (an internal defect).
type repointResult struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	// EmptyTable is true on the one success that re-pointed nothing because
	// go_api_routing_state held no row at any schema digest (CHAOS-8543); see
	// carryResult.EmptyTable. Reason stays "repointed".
	EmptyTable bool `json:"empty_table,omitempty"`
	// DarkRowsOnly is carryResult.DarkRowsOnly for `repoint` (CHAOS-8586).
	DarkRowsOnly bool `json:"dark_rows_only,omitempty"`
}

// repointJSONPrefix mirrors carryJSONPrefix -- see its doc comment.
const repointJSONPrefix = "GOAPI_ROUTING_JSON "

func printRepointResult(w interface{ Write([]byte) (int, error) }, result repointResult) {
	payload, err := json.Marshal(result)
	if err != nil {
		payload, _ = json.Marshal(repointResult{Reason: "error", Message: "internal: could not encode the -json result: " + err.Error()})
	}
	fmt.Fprintf(w, "%s%s\n", repointJSONPrefix, payload)
}

func runRepoint(argv []string) (err error) {
	set := newVerbFlagSet("repoint")
	var common commonFlags
	var registryURL, buildInfoURL, expectBuild, documentDigest string
	var dryRun, jsonOut bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api -- the only authority on which schema digest is live (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	set.StringVar(&buildInfoURL, "buildinfo-url", "", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build every row is pointed at (falls back to "+queryAPIURLEnvVar+"+\"/buildinfo\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row touched (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row touched (required)")
	set.StringVar(&expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written")
	set.StringVar(&documentDigest, "document", "", "narrow -operations (exactly one name) to the ONE row whose OWN document digest equals this -- the only way to re-point just a DOCUMENT_DRIFT row (as `status` names it) and leave a sibling catalog row untouched. Exact-match")
	set.BoolVar(&dryRun, "dry-run", false, "report what would change and write NOTHING")
	set.BoolVar(&jsonOut, "json", false, "also print one machine-readable line to stdout, prefixed `"+repointJSONPrefix+"`, classifying the outcome by a stable `reason` field (\"repointed\", \"refused\", \"error\")")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds EACH HTTP request, the Postgres dial, and EACH database statement (server-side statement_timeout/lock_timeout) -- never the run as a whole")

	var emptyTable, darkRowsOnly bool
	defer func() {
		if !jsonOut {
			return
		}
		reason := "repointed"
		if err != nil {
			reason = "refused"
			if errors.Is(err, errInternal) {
				reason = "error"
			}
		}
		result := repointResult{Reason: reason, EmptyTable: emptyTable, DarkRowsOnly: darkRowsOnly}
		if err != nil {
			result.Message = redactCredentials(err.Error())
		}
		printRepointResult(stdout, result)
	}()

	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	// Same empty-value trap -candidate-build guards against in disable:
	// an interpolated but unset shell variable passed as `-document ""`
	// must never read as "no selector", which would silently widen a
	// deliberately narrow re-point back to every row the operation has.
	var documentPassedEmpty bool
	set.Visit(func(f *flag.Flag) {
		if f.Name == "document" && f.Value.String() == "" {
			documentPassedEmpty = true
		}
	})
	if documentPassedEmpty {
		return refuse("-document was passed but empty -- an empty value is silently the SAME as no selector at all, which this refuses rather than falls back to every row the operation has. Omit the flag entirely to re-point every row, or pass the exact digest `status` shows for the drifted row")
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if err := common.requireProvenance(); err != nil {
		return err
	}
	if err := common.requirePostgres(); err != nil {
		return err
	}
	credential, err := envelopeCredential()
	if err != nil {
		return err
	}

	ctx := context.Background()
	client := httpClient(common.timeout)

	// Same shape as enable's identical preflight --
	// see resolveEndpointURL's doc comment (main.go) for the executed
	// repro. Resolved HERE, after every other precondition (same ordering
	// `TestEveryVerbRefusesItsOwnMissingPreconditions` pins), and BEFORE
	// sanitizeEndpointURL, same reason as enable's.
	// Resolved TOGETHER, same reason as enable's
	// identical fix -- see resolveQueryAPIEndpoints's own doc comment.
	registryURL, buildInfoURL, err = resolveQueryAPIEndpoints(registryURL, buildInfoURL)
	if err != nil {
		return err
	}
	// codex r3 SEC-01 / r4 CRED-01: a URL carrying userinfo, a query or
	// a fragment is printed verbatim by any transport error downstream.
	// The REBUILT value is what is used from here on.
	if sanitized, err := sanitizeEndpointURL("-registry-url", registryURL); err != nil {
		return err
	} else {
		registryURL = sanitized
	}
	if sanitized, err := sanitizeEndpointURL("-buildinfo-url", buildInfoURL); err != nil {
		return err
	} else {
		buildInfoURL = sanitized
	}

	// Every one of these is a state the OPERATOR resolves -- an
	// unreachable process, a build the process cannot name, a filter that
	// names nothing -- so each exits 2, not 1. `enable` routes
	// them through refuse() too; never return one of them raw.
	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the running query-api's registry: %v.\n"+
			"  A measurement that did not happen is not a pass -- fix the deployment or point -registry-url at the right process.", err)
	}
	// Same shape as enable's identical preflight --
	// `FetchRegistry` itself does not refuse on an empty registry
	// (status needs the schema digest it still reports), so the write
	// verb refuses here instead.
	if len(registry.DocumentDigest) == 0 {
		return refuse("%s registers no operations -- there is nothing to prove", goapiproof.EndpointLabel(registryURL))
	}
	running, err := goapiproof.FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return refuse("%v\n  the deployed query-api must identify its build at %s before any row can be pointed at it", err, goapiproof.EndpointLabelWithPort(buildInfoURL))
		}
		return refuse("%v", err)
	}

	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	operations, err := requestedOperations(common.operations)
	if err != nil {
		return refuse("%v", err)
	}
	if documentDigest != "" && len(operations) != 1 {
		return refuse("-document selects one specific row and requires exactly one -operations name, got %d (%v)", len(operations), operations)
	}

	// Read only AFTER /buildinfo answered 200 -- the deployed verifier
	// accepting this exact token. See goapiproof.EnvelopeSubject.
	principalID, err := credential.EnvelopeSubject(ctx)
	if err != nil {
		return refuse("%v", err)
	}

	outcomes, err := goapiproof.Repoint(ctx, pool, goapiproof.RepointRequest{
		SchemaDigest:         registry.SchemaDigest,
		RunningBuild:         running,
		ExpectBuild:          expectBuild,
		Operations:           operations,
		SelectDocumentDigest: documentDigest,
		RecordedBy:           common.recordedBy,
		ReviewEvidence:       common.reviewEvidence,
		// CHAOS-5505: WHO THE CREDENTIAL SAYS is acting, distinct from
		// -recorded-by. Same envelope and same reasoning as `enable`.
		PrincipalID: principalID,
		DryRun:      dryRun,
	})
	// CHAOS-8543: an empty table is a valid state since the catalog rule, and the
	// post-upgrade hook of a stack that serves from one must not fail. Every
	// preflight above still ran, and so did the request's own checks -- the
	// -expect-build cross-check included, which is what the hook waits on -- so
	// only the answer to "there is no row at all" changed, from a refusal to a
	// no-op. Rows that exist only at other digests, all in a dark mode, are the
	// same no-op (CHAOS-8586); a served row at another digest is still
	// ErrRepointNoRows.
	emptyTable = errors.Is(err, goapiproof.ErrRoutingTableEmpty)
	darkRowsOnly = errors.Is(err, goapiproof.ErrRoutingRowsOnlyDark)
	if err != nil && !emptyTable && !darkRowsOnly {
		return classifyWriteError(err)
	}

	summary := goapiproof.Summarize(outcomes)
	verb := "repointed"
	if dryRun {
		verb = "would repoint"
	}
	// Same as enable's endpoint line, so a
	// wrong-process repoint looks the same way wrong on stdout.
	fmt.Fprintf(stdout, "go-api-routing: registry=%s buildinfo=%s\n",
		goapiproof.EndpointLabelWithPort(registryURL), goapiproof.EndpointLabelWithPort(buildInfoURL))
	// Every counter prints, including the zeros: "no row needed changing"
	// and "nobody looked" must not read alike.
	fmt.Fprintf(stdout, "go-api-routing: schema_digest=%s running_build=%s dry_run=%t\n", registry.SchemaDigest, running, dryRun)
	fmt.Fprintf(stdout, "go-api-routing: %s total=%d changed=%d unchanged=%d\n", verb, summary.Total, summary.Changed, summary.Unchanged)
	for _, outcome := range outcomes {
		state := "unchanged"
		if outcome.Changed {
			state = "repointed"
		}
		fmt.Fprintf(stdout, "go-api-routing:   %-24s mode=%-8s %s  %s -> %s  digest=%s\n",
			outcome.Operation, outcome.ModeAfter, state, outcome.BuildFrom, outcome.BuildTo, outcome.DocumentDigest)
		// r6 observability (1)/(5) (team-lead ruling): a structured line
		// PER CHANGED ROW, mirroring disable's own `go_api_routing.disabled`
		// line -- BEFORE-and-AFTER mode/build, not just "some rows moved".
		// Unchanged rows are not logged here: they are already the whole
		// point of `changed=0` in the summary line above, and a log line
		// for every unchanged row on a large rollout would bury the ones
		// that actually moved.
		if outcome.Changed && !dryRun {
			fmt.Fprintf(stderr, "go_api_routing.repointed operation=%s mode_before=%s mode_after=%s build_before=%s build_after=%s schema_digest=%s document_digest=%s recorded_by=%q\n",
				outcome.Operation, outcome.ModeBefore, outcome.ModeAfter, outcome.BuildFrom, outcome.BuildTo, registry.SchemaDigest, outcome.DocumentDigest, common.recordedBy)
		}
	}
	if emptyTable {
		fmt.Fprintln(stdout, routingTableEmptyNote("re-point"))
	}
	if darkRowsOnly {
		fmt.Fprintln(stdout, routingRowsOnlyDarkNote("re-point"))
		printDarkRowsOnly(err, "repoint")
	}
	return nil
}

// routingRowsOnlyDarkNote is the one line `repoint` prints when no row sits at the schema digest it
// works on and every row elsewhere is in a dark mode, so the verb did nothing (CHAOS-8586).
func routingRowsOnlyDarkNote(what string) string {
	return fmt.Sprintf("go-api-routing: NO-OP: go_api_routing_state has no row at this schema digest, and every row at another digest is in a dark mode (python, disabled or shadow), so there is nothing to %s and nothing was written.", what)
}

// printDarkRowsOnly names, on stderr, every row behind a dark-rows-only no-op, one structured line each.
func printDarkRowsOnly(err error, verb string) {
	var dark *goapiproof.RoutingRowsOnlyDarkError
	if !errors.As(err, &dark) {
		return
	}
	for _, row := range dark.Rows {
		fmt.Fprintf(stderr, "go_api_routing.noop_dark_rows_only verb=%s operation=%q mode=%q row_schema_digest=%q live_schema_digest=%s document_digest=%q\n",
			verb, row.Operation, row.Mode, row.SchemaDigest, dark.SchemaDigest, row.DocumentDigest)
	}
}

// routingTableEmptyNote is the one line `repoint` prints when go_api_routing_state has no row at any
// schema digest and the verb therefore did nothing (CHAOS-8543).
func routingTableEmptyNote(what string) string {
	return fmt.Sprintf("go-api-routing: NO-OP: go_api_routing_state has no row at any schema digest, so there is nothing to %s and nothing was written. "+
		"An empty table is a valid state: query-api serves every registered operation without a routing row, and no MCP class root is enabled.", what)
}
