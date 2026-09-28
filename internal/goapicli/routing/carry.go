package routing

// The `carry` verb: the one command that runs BEFORE a roll.
//
// Every other verb here writes rows at a digest something is ALREADY
// serving. `carry` writes rows at the digest the image about to roll
// will compute, so the operations enabled right now are still routed the
// moment the first new pod starts -- instead of every one of them
// falling back to Python (and the go-only ones answering the deletion
// error) until an operator notices.
//
// Its preflights are `enable`'s, with exactly one inversion and one
// addition, and both are the point of the verb:
//
//  1. The two planes' digests must DISAGREE. `enable` refuses when this
//     binary's SDL is not the deployed one; `carry` exists only for that
//     case and refuses when they agree.
//  2. The document a row is keyed to must be BYTE-IDENTICAL in the image
//     this binary was built from. The digest is recomputed here, from
//     that image's own registered-document dump, with the same function
//     the running process uses -- never trusted from a flag.
//  3. Every row being carried must already name the build /buildinfo
//     reports, because this verb COPIES provenance instead of re-reading
//     it. A row that needs `repoint` is refused by name rather than
//     having a stale claim copied forward.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// carryResult is -json's output shape: one line, prefixed carryJSONPrefix, printed to
// stdout on EVERY outcome (success or refusal) when -json is set.
//
// D2828/D2829 (CHAOS-7022 r1, CHAOS-7023 r1 P1): callers (bigboy-cut.sh, the Helm
// pre-upgrade hook) used to branch on grepping this command's human-readable TEXT for
// specific refusal wording -- twice wrong, in two different ways, because that text was
// never a contract (it changed shape between the CLI's own early preflight and the
// goapiproof package's differently-worded sentinel errors for the exact same condition,
// and a caller had no way to know which layer's wording it was reading). Reason is a
// SMALL, STABLE, CLOSED vocabulary a caller switches on -- never the prose, which is free
// to change without notice.
type carryResult struct {
	// Reason is exactly one of: "carried" (success, including a run that carried zero
	// eligible rows), "digest_unchanged" (Preflight 2: this binary's SDL is already live
	// -- an ordinary, expected no-op on a non-schema-changing roll), "stale_build" (a row
	// names a build the deployed process is not running -- the caller's cue to `repoint`
	// then retry, the rev196 exception), "refused" (any OTHER refusal -- catalog/document
	// load failure, an unreachable registry, an unresolvable operation filter, a
	// goapiproof-level write refusal not covered above), or "error" (an internal defect
	// in this command, never an operator-actionable state).
	Reason       string `json:"reason"`
	LiveDigest   string `json:"live_schema_digest,omitempty"`
	TargetDigest string `json:"target_schema_digest,omitempty"`
	Carried      int    `json:"carried,omitempty"`
	Message      string `json:"message,omitempty"`
}

// carryJSONPrefix marks the one line of a -json run's stdout that is machine-readable --
// every other line is the unchanged human-readable plan/summary text. A caller does
// `grep '^`+carryJSONPrefix+`'` then strips the prefix before handing the rest to a JSON
// parser, rather than requiring stdout to be JSON-only (which would have meant giving up
// the operator-facing text this command already prints, or duplicating it inside the
// JSON payload).
const carryJSONPrefix = "GOAPI_ROUTING_JSON "

func printCarryResult(w io.Writer, result carryResult) {
	payload, err := json.Marshal(result)
	if err != nil {
		// Never silently drop the machine-readable line: a caller parsing for it and
		// getting nothing is a false "digest_unchanged"/"carried" away from writing
		// nothing when it should have aborted -- so a marshal failure (which can only
		// happen here from a caller error, since carryResult's own fields are all
		// plain strings/ints) is itself reported through the SAME channel.
		payload, _ = json.Marshal(carryResult{Reason: "error", Message: "internal: could not encode the -json result: " + err.Error()})
	}
	fmt.Fprintf(w, "%s%s\n", carryJSONPrefix, payload)
}

// targetDocumentDigests computes what the image THIS binary was built
// from registers, from its registered-document dump.
//
// The dump carries document TEXT, and the digest is computed here with
// goapidigest.Document -- the same function the running query-api and
// registrydump use -- rather than read from a `digest` field in the
// file. A digest read from a file is a digest somebody could have typed;
// a digest computed over the text is a fact about the text, and the text
// is what the new image will serve.
func targetDocumentDigests(path string) (map[string]string, error) {
	documents, err := goapiproof.LoadDocuments(path)
	if err != nil {
		return nil, err
	}
	digests := make(map[string]string, len(documents))
	for operation, text := range documents {
		digests[operation] = goapidigest.Document(text)
	}
	return digests, nil
}

func runCarry(argv []string) (err error) {
	set := newVerbFlagSet("carry")
	var common commonFlags
	var registryURL, buildInfoURL, documentsPath, expectBuild string
	var dryRun, jsonOut bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED (pre-roll) query-api -- the ONLY source of the live schema digest and of what is reachable now (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	set.StringVar(&buildInfoURL, "buildinfo-url", "", "GET /buildinfo on the DEPLOYED query-api -- the build every carried row must already name (falls back to "+queryAPIURLEnvVar+"+\"/buildinfo\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&documentsPath, "documents", goapiproof.DefaultDocumentsPath, "registrydump JSON for the image THIS binary was built from -- the registered documents the new deployment will serve")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog for the image THIS binary was built from")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row written (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row written (required)")
	set.StringVar(&expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the deployed build is not this sha. Never the source of the value written")
	set.BoolVar(&dryRun, "dry-run", false, "run every preflight, print the plan and write NOTHING")
	set.BoolVar(&jsonOut, "json", false, "also print one machine-readable line to stdout, prefixed `"+carryJSONPrefix+"`, classifying the outcome by a stable `reason` field (\"carried\", \"digest_unchanged\", \"stale_build\", \"refused\", \"error\"). Callers branch on -json's `reason`, never on this command's human-readable text, which carries no stability contract")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds EACH HTTP request, the Postgres dial, and EACH database statement (server-side statement_timeout/lock_timeout) -- never the run as a whole")

	// D2828/D2829: -json's reason is classified HERE, at the exact call site that knows
	// WHY, not guessed afterward from err's text or type -- reason is set explicitly at
	// the two sites that need a specific value (Preflight 2, the stale-build refusal);
	// every other return falls through to the deferred default (err==nil -> "carried",
	// errInternal -> "error", anything else -> "refused"). Deferred so EVERY return path
	// -- including ones added later -- gets a JSON line without having to remember to
	// call printCarryResult at each one.
	var reason, liveDigest, targetDigest string
	var carriedCount int
	defer func() {
		if !jsonOut {
			return
		}
		if reason == "" {
			switch {
			case err == nil:
				reason = "carried"
			case errors.Is(err, errInternal):
				reason = "error"
			default:
				reason = "refused"
			}
		}
		result := carryResult{Reason: reason, LiveDigest: liveDigest, TargetDigest: targetDigest, Carried: carriedCount}
		if err != nil {
			// Same defense-in-depth as the command's other error-print sites (see
			// credentialBoundary's own doc comment): refuse/internal already redact
			// what they build, but this print applies it once more rather than
			// trusting every possible error on every return path already did.
			result.Message = redactCredentials(err.Error())
		}
		printCarryResult(stdout, result)
	}()

	if err := parseVerbFlags(set, argv); err != nil {
		return err
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

	catalog, err := goapiproof.LoadOperationCatalog(common.catalogPath)
	if err != nil {
		return refuse("%v -- refusing to carry anything on a catalog this process cannot read", err)
	}
	// Resolved against THIS image's catalog, so a name that is not part
	// of the deployment being rolled to is refused before any plane is
	// contacted. An empty or separator-only value is refused rather than
	// widened to everything (goapiproof.SplitOperations).
	operationFilter, err := goapiproof.SplitOperations(common.operations)
	if err != nil {
		return refuse("%v", err)
	}
	if operationFilter != nil {
		if _, err := goapiproof.ResolveOperations(common.operations, catalog); err != nil {
			return refuse("%v", err)
		}
	}
	targetDigests, err := targetDocumentDigests(documentsPath)
	if err != nil {
		return refuse("%v -- the registered-document dump for THIS image is what says the documents did not change; without it nothing can be carried safely", err)
	}

	ctx := context.Background()
	client := httpClient(common.timeout)

	registryURL, buildInfoURL, err = resolveQueryAPIEndpoints(registryURL, buildInfoURL)
	if err != nil {
		return err
	}
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

	// --- Preflight 1: the DEPLOYED process, which is the authority on
	// what is reachable right now. -----------------------------------
	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the deployed query-api's registry: %v.\n"+
			"  A measurement that did not happen is not a pass -- the live schema digest and the live document digests are the only record of what a roll would un-route.", err)
	}
	if len(registry.DocumentDigest) == 0 {
		return refuse("%s registers no operations -- there is nothing reachable to preserve", goapiproof.EndpointLabel(registryURL))
	}

	liveDigest, targetDigest = registry.SchemaDigest, localSchemaDigest()

	// --- Preflight 2: the digests must DISAGREE, the inversion of
	// `enable`'s own preflight 2. -------------------------------------
	target := targetDigest
	if registry.SchemaDigest == target {
		reason = "digest_unchanged"
		return refuse("this binary's SDL is the one the deployed process already computes (%s).\n"+
			"  `carry` moves rows to a digest that is not live YET -- build it from the commit about to roll, or use `enable` if the roll has already happened.\n"+
			"  See %s", target, runbook)
	}

	running, err := goapiproof.FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return refuse("%v\n  the deployed query-api must identify its build at %s before any row's provenance can be copied to a new digest", err, goapiproof.EndpointLabelWithPort(buildInfoURL))
		}
		return refuse("%v", err)
	}
	principalID, err := credential.EnvelopeSubject(ctx)
	if err != nil {
		return refuse("%v", err)
	}

	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	outcomes, carryErr := goapiproof.Carry(ctx, pool, goapiproof.CarryRequest{
		LiveSchemaDigest:   registry.SchemaDigest,
		TargetSchemaDigest: target,
		RunningBuild:       running,
		ExpectBuild:        expectBuild,
		Inputs: goapiproof.CarryInputs{
			LiveDocumentDigest:    registry.DocumentDigest,
			TargetDocumentDigest:  targetDigests,
			CatalogDocumentDigest: catalog,
		},
		Operations:     operationFilter,
		RecordedBy:     common.recordedBy,
		ReviewEvidence: common.reviewEvidence,
		PrincipalID:    principalID,
		DryRun:         dryRun,
	})
	// The plan is printed even on a refusal, and BEFORE the error is
	// classified: a refusal that names two operations is far more useful
	// beside the twelve that would have carried cleanly than on its own,
	// and an operator deciding whether to roll needs both halves.
	printCarryPlan(outcomes, registry.SchemaDigest, target, running, dryRun)
	carriedCount = goapiproof.SummarizeCarry(outcomes).Carried
	if carryErr != nil {
		if errors.Is(carryErr, goapiproof.ErrCarryBuildNotRunning) {
			// The rev196 exception, D2828/D2829's whole reason for a distinct
			// reason value here: this is the ONE refusal a caller is meant to
			// self-heal from (repoint, then retry), never treated the same as an
			// unclassified refusal it should just abort on.
			reason = "stale_build"
			return refuse("%v", carryErr)
		}
		if errors.Is(carryErr, goapiproof.ErrCarryRequestRefused) {
			return refuse("%v", carryErr)
		}
		return classifyWriteError(carryErr)
	}
	return nil
}

func printCarryPlan(outcomes []goapiproof.CarryOutcome, liveDigest, targetDigest, running string, dryRun bool) {
	summary := goapiproof.SummarizeCarry(outcomes)
	fmt.Fprintf(stdout, "go-api-routing: live schema_digest   = %s  (the deployed process)\n", liveDigest)
	fmt.Fprintf(stdout, "go-api-routing: target schema_digest = %s  (this binary's embedded SDL)\n", targetDigest)
	fmt.Fprintf(stdout, "go-api-routing: deployed build=%s dry_run=%t\n", running, dryRun)
	verb := "carried"
	if dryRun {
		verb = "would carry"
	}
	// Every counter prints even at zero: "nothing needed carrying" and
	// "nothing was looked at" must not read alike.
	fmt.Fprintf(stdout, "go-api-routing: %s=%d unchanged=%d skipped=%d refused=%d of %d row(s) at the live digest\n",
		verb, summary.Carried, summary.Unchanged, summary.Skipped, summary.Refused, summary.Total)
	for _, outcome := range outcomes {
		fmt.Fprintf(stdout, "go-api-routing:   %-10s %-24s mode=%-8s rollout=%-4d %s digest=%s\n",
			outcome.Action, outcome.Operation, outcome.Mode, outcome.RolloutPercentage, outcome.Build, outcome.DocumentDigest)
		if outcome.Reason != "" {
			fmt.Fprintf(stdout, "go-api-routing:              %s\n", outcome.Reason)
		}
		// One structured line PER ROW written, mirroring `enable`'s
		// go_api_routing.enabled and `repoint`'s go_api_routing.repointed:
		// a log search six weeks later must find WHICH operations moved to
		// the new digest, under whose name, not merely that some did.
		if outcome.Action == goapiproof.CarryActionCarry && !dryRun {
			fmt.Fprintf(stderr, "go_api_routing.carried operation=%s mode=%s rollout=%d build=%s from_schema_digest=%s to_schema_digest=%s document_digest=%s proof=none_at_target_digest\n",
				outcome.Operation, outcome.Mode, outcome.RolloutPercentage, outcome.Build,
				liveDigest, targetDigest, outcome.DocumentDigest)
		}
	}
	if summary.Carried > 0 && !dryRun {
		// The sentence an operator needs next, on the command that wrote
		// the rows rather than in a runbook they may not open: the carried
		// rows are UNPROVEN at the new digest by construction, and the
		// provenance they carry is corrected by `repoint` after the roll.
		fmt.Fprintf(stdout, "go-api-routing: NEXT: roll, then `repoint` (the carried rows still name the pre-roll build), then re-prove -- `status` reports these rows UNPROVEN at %s until go-api-prove runs against the new build\n", targetDigest)
	}
}
