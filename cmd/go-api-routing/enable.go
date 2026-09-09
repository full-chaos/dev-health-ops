package main

// The `enable` verb: a mutation that refuses -- exit 2, nothing written --
// on any doubt at all.
//
// The four preflights are the Python verb's, in the same order, and each
// answers a question the 2026-09-01 six-day outage (CHAOS-5416) could
// not:
//
//  1. Is query-api reachable? No answer is not a pass. A measurement that
//     did not happen must fail loudly (root AGENTS.md).
//  2. Do both planes hash the same SDL? The Python edge computes its
//     routing key from contracts/graphql/v1/schema.graphql; the deployed
//     Go binary hashes its own go:embed'ed copy; this command hashes the
//     copy IT was built from, with the same one implementation
//     (internal/goapidigest.Schema). Rows follow the IMAGE. Writing rows
//     from a checkout that has moved ahead of the deployed image produces
//     exactly the dead rows this command exists to recover from.
//  3. Does the running binary register the operation, under the same
//     document digest the edge's catalog carries? The deployed image is
//     the authority on what it serves; the catalog is the authority on
//     what the edge can dispatch. Both must say yes or the row is
//     unreachable from one side or the other.
//  4. Has this exact candidate build been proven? See
//     goapiproof.EnablementProofStage.

import (
	"context"
	"errors"
	"fmt"
	"time"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// localSchemaDigest is the digest of the SDL THIS BINARY was built from.
//
// It is the same function and the same file the deployed query-api hashes
// (goapidigest.Schema over contracts/graphql/v1/schema.graphql, embedded
// verbatim by schemav1.SDL) and the same value the Python edge computes
// from its own checkout of that file -- so comparing it against the
// running process's /registry is the cross-plane agreement check
// preflight 2 has always been. The residual it leaves is named in the
// refusal itself: this binary must be built from the same commit as the
// edge it is enabling rows for.
func localSchemaDigest() string { return goapidigest.Schema(schemav1.SDL) }

func runEnable(argv []string) error {
	set := newVerbFlagSet("enable")
	var common commonFlags
	var registryURL, buildInfoURL, expectBuild, mode string
	var rollout int
	var acknowledgeUnproven, dryRun bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	set.StringVar(&buildInfoURL, "buildinfo-url", "", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the candidate build written (falls back to "+queryAPIURLEnvVar+"+\"/buildinfo\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog -- what the Python dispatcher can map a request to")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row touched (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row touched (required)")
	set.StringVar(&mode, "mode", "", "routing mode: canary or primary. Only these two make an operation reachable, so they are the only ones an 'enable' verb offers (required)")
	set.IntVar(&rollout, "rollout", 100, "rollout_percentage written to the row. NOTE: neither plane enforces this yet -- canary means 'on for everyone, revocable'. Recorded, not obeyed")
	set.StringVar(&expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written")
	// No backticks around "status" here: Go's `flag`
	// package treats a back-quoted word as the FLAG'S OWN VALUE NAME (its
	// documented convention for choosing the placeholder shown in usage
	// text) -- one there made `enable -h` print `-acknowledge-unproven
	// status`, a boolean switch that reads as if it takes an argument
	// named "status". Following that literally is refused safely (an
	// unexpected operand), so the only real cost is a confused operator.
	set.BoolVar(&acknowledgeUnproven, "acknowledge-unproven", false, "enable operations with no deployed-executed proof run for this build. Each such row records ACKNOWLEDGED-UNPROVEN durably and is reported UNPROVEN by status for as long as it is in force")
	set.BoolVar(&dryRun, "dry-run", false, "run every preflight and write NOTHING")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "per-request timeout")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if mode == "" {
		return refuse("-mode is required and must be one of %v", goapiproof.EnableModes)
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
		return refuse("%v -- refusing to enable anything on a catalog this process cannot read", err)
	}
	operations, err := goapiproof.ResolveOperations(common.operations, catalog)
	if err != nil {
		return refuse("%v", err)
	}

	ctx := context.Background()
	client := httpClient(common.timeout)

	// Neither flag defaults to a HARDCODED
	// `http://localhost:8090/...` -- an operator who forgets ONE of the
	// two must not silently send that half of the preflight -- and the
	// effective-principal envelope -- to whatever happens to answer on
	// localhost:8090, rather than a refusal. See resolveEndpointURL's
	// doc comment (main.go) for the executed repro.
	// Resolved HERE, after every other precondition, so the FIRST thing
	// an operator missing several is told is still the cheapest one to
	// fix (-mode, provenance, -postgres-uri, the credential) -- the same
	// ordering `TestEveryVerbRefusesItsOwnMissingPreconditions` pins.
	// Resolved BEFORE sanitizeEndpointURL: an unresolved empty string
	// must produce THIS refusal, not sanitizeEndpointURL's "must be
	// http:// or https://" (a confusing message for an operator who typed
	// nothing at all).
	// Resolved TOGETHER, not per-route -- see
	// resolveQueryAPIEndpoints's own doc comment. A mix of one explicit
	// flag and one env-derived route must not silently split the preflight across two
	// different processes with no second flag named at all.
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

	// --- Preflight 1: is query-api reachable at all? -------------------
	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the running query-api's registry: %v.\n"+
			"  A measurement that did not happen is not a pass -- fix the deployment or point -registry-url at the right process.", err)
	}
	// `FetchRegistry` does not refuse on an empty
	// `operations` array by itself (that refusal lives here, and in
	// repoint's identical preflight) -- `status` shares the same function
	// and needs the schema_digest from an otherwise-empty registry for its
	// own diagnostic, which the old shared refusal destroyed. A WRITE verb
	// still has nothing to prove or write against an empty registry, so it
	// still refuses, just at its own call site rather than inside the
	// shared reader.
	if len(registry.DocumentDigest) == 0 {
		return refuse("%s registers no operations -- there is nothing to prove", goapiproof.EndpointLabel(registryURL))
	}

	// --- Preflight 2: do both planes hash the same SDL? ----------------
	local := localSchemaDigest()
	if registry.SchemaDigest != local {
		return refuse("schema digest MISMATCH between planes.\n"+
			"  this binary's embedded SDL: %s\n"+
			"  running query-api (go)    : %s\n"+
			"  Routing rows are keyed by schema_digest, so rows written now would be unreachable to the running binary -- the exact defect of 2026-09-01.\n"+
			"  Rebuild and redeploy query-api from this SDL (and rebuild THIS binary from the same commit as the Python edge), THEN re-run.\n"+
			"  See %s", local, registry.SchemaDigest, runbook)
	}

	// --- Preflight 3: does the binary register each operation, under the
	// digest the edge's catalog carries? --------------------------------
	var notRegistered, digestDivergent []string
	for _, operation := range operations {
		registered, ok := registry.DocumentDigest[operation]
		if !ok {
			notRegistered = append(notRegistered, operation)
			continue
		}
		if registered != catalog[operation] {
			digestDivergent = append(digestDivergent,
				fmt.Sprintf("%s: catalog=%s go=%s", operation, catalog[operation], registered))
		}
	}
	// `FetchRegistry`
	// (internal/goapiproof/registry.go) REFUSES a /registry response
	// that lists the same operation twice, matching the Python verb,
	// rather than silently keeping the last one. This preflight's
	// `registry.DocumentDigest` map can
	// therefore never be decided by a malformed or tampered registry
	// picking whichever duplicate the JSON decoder scanned last.
	if len(notRegistered) > 0 {
		return refuse("the running query-api does not register: %v. It serves %d operation(s); the catalog lists %d.\n"+
			"  The deployed image is the authority -- an operation it does not serve cannot be enabled into it.",
			notRegistered, len(registry.DocumentDigest), len(catalog))
	}
	if len(digestDivergent) > 0 {
		return refuse("document digest MISMATCH for %d operation(s): %v.\n"+
			"  The registered document text differs between the edge's catalog and the deployed binary; a row written with the catalog's digest would never be looked up.\n"+
			"  Regenerate the catalog (scripts/go_api/generate_operation_catalog.py) against the deployed revision, or redeploy.",
			len(digestDivergent), digestDivergent)
	}

	// The build is READ, never typed. -expect-build can only FAIL a run.
	running, err := goapiproof.FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return refuse("%v\n  the deployed query-api must identify its build at %s before any row can be pointed at it", err, goapiproof.EndpointLabelWithPort(buildInfoURL))
		}
		return refuse("%v", err)
	}
	if expectBuild != "" && expectBuild != running {
		return refuse("-expect-build %q does not match the running build %q -- the flag is a cross-check, never the source", expectBuild, running)
	}

	// The envelope's subject, read only AFTER /buildinfo answered 200 --
	// which is the deployed verifier accepting this exact token. This
	// process never verifies it itself (one validator per credential
	// class), so the order is what makes naming the subject honest rather
	// than an assertion of its own. See goapiproof.EnvelopeSubject.
	principalID, err := credential.EnvelopeSubject(ctx)
	if err != nil {
		return refuse("%v", err)
	}

	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The before-state is read inside goapiproof.Enable itself, under
	// the SAME lock and the SAME transaction as the write that replaces
	// it -- never a plain, UNLOCKED read taken here before that call even
	// starts, which under real concurrency (a third session holding the
	// row, a racing `disable` landing in between) would print a STALE
	// state that looks like nothing had changed. See
	// EnableOutcome.ModeBefore's own doc comment (routing_enable.go) for
	// the executed repro: `outcome.ModeBefore` below is always the value
	// this write ACTUALLY replaced.

	// --- Preflight 4 (inside Enable) + the write, one transaction ------
	outcomes, err := goapiproof.Enable(ctx, pool, goapiproof.EnableRequest{
		SchemaDigest:      registry.SchemaDigest,
		RunningBuild:      running,
		Operations:        operations,
		DocumentDigest:    registry.DocumentDigest,
		Mode:              mode,
		RolloutPercentage: rollout,
		RecordedBy:        common.recordedBy,
		ReviewEvidence:    common.reviewEvidence,
		// CHAOS-5505: WHO THE CREDENTIAL SAYS is acting. Distinct from
		// -recorded-by, which is what the operator typed about themselves.
		PrincipalID:         principalID,
		AcknowledgeUnproven: acknowledgeUnproven,
		DryRun:              dryRun,
	})
	if err != nil {
		if errors.Is(err, goapiproof.ErrEnableUnproven) || errors.Is(err, goapiproof.ErrEnableRequestRefused) {
			return refuse("%v", err)
		}
		return classifyWriteError(err)
	}

	var unproven int
	for _, outcome := range outcomes {
		if !outcome.Proven {
			unproven++
			// One structured line PER ROW, not one per invocation: an
			// operator (or a log search six weeks later) must be able to
			// find WHICH operations were turned on without proof, not
			// merely that some were.
			fmt.Fprintf(stderr,
				"WARNING: go_api_routing.enabled_unproven operation=%s stage_evidence=none candidate_build=%s schema_digest=%s document_digest=%s mode=%s dry_run=%t\n",
				outcome.Operation, running, registry.SchemaDigest, outcome.DocumentDigest, mode, dryRun)
		}
	}

	verb := "enabled"
	if dryRun {
		verb = "would enable"
	}
	// On success this line names
	// WHICH endpoint was actually consulted -- so a wrong-process enable
	// (an omitted URL flag silently resolving to a
	// different process) cannot look identical to a correct one except for
	// the build value. EndpointLabel is host-only, never a full URL (see
	// its own doc comment) -- it cannot leak a credential even if one
	// somehow ended up in the resolved URL.
	fmt.Fprintf(stdout, "go-api-routing: registry=%s buildinfo=%s\n",
		goapiproof.EndpointLabelWithPort(registryURL), goapiproof.EndpointLabelWithPort(buildInfoURL))
	fmt.Fprintf(stdout, "go-api-routing: schema_digest=%s candidate_build=%s mode=%s rollout=%d dry_run=%t\n",
		registry.SchemaDigest, running, mode, rollout, dryRun)
	fmt.Fprintf(stdout, "go-api-routing: %s total=%d proven=%d unproven=%d\n",
		verb, len(outcomes), len(outcomes)-unproven, unproven)
	for _, outcome := range outcomes {
		flag := ""
		if !outcome.Proven {
			flag = "  (UNPROVEN)"
		}
		fmt.Fprintf(stdout, "go-api-routing:   %-24s mode=%-8s %-40s digest=%s%s\n", outcome.Operation, outcome.Mode, outcome.CandidateBuild, outcome.DocumentDigest, flag)
		// r6 observability (1) (team-lead ruling): a structured line PER
		// ROW, mirroring disable's `go_api_routing.disabled` and
		// repoint's `go_api_routing.repointed` -- BEFORE-and-AFTER
		// mode/build, so a log search finds what a specific enable
		// REPLACED, not just that it wrote something.
		if !dryRun {
			// These values are read under the
			// SAME lock the write itself takes, inside goapiproof.Enable
			// -- see EnableOutcome.ModeBefore's own doc comment. There is
			// no "(unknown)" case: a genuine read failure there
			// aborts the whole enable rather than reaching this line at
			// all.
			modeBefore, buildBefore := "(no row)", "-"
			if outcome.HadRowBefore {
				modeBefore, buildBefore = outcome.ModeBefore, outcome.CandidateBuildBefore
			}
			fmt.Fprintf(stderr, "go_api_routing.enabled operation=%s mode_before=%s mode_after=%s build_before=%s build_after=%s schema_digest=%s document_digest=%s recorded_by=%s\n",
				outcome.Operation, modeBefore, outcome.Mode, buildBefore, outcome.CandidateBuild, registry.SchemaDigest, outcome.DocumentDigest, common.recordedBy)
		}
	}
	return nil
}

// runbook is where an operator reads the recovery procedure. Referenced
// from every refusal a digest move can cause, so the message that stops
// the command also says what to do about it.
const runbook = "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md (section: When the schema digest moves)"
