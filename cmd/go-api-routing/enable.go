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
	set.StringVar(&registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api")
	set.StringVar(&buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the candidate build written")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog -- what the Python dispatcher can map a request to")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row touched (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row touched (required)")
	set.StringVar(&mode, "mode", "", "routing mode: canary or primary. Only these two make an operation reachable, so they are the only ones an 'enable' verb offers (required)")
	set.IntVar(&rollout, "rollout", 100, "rollout_percentage written to the row. NOTE: neither plane enforces this yet -- canary means 'on for everyone, revocable'. Recorded, not obeyed")
	set.StringVar(&expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written")
	set.BoolVar(&acknowledgeUnproven, "acknowledge-unproven", false, "enable operations with no deployed-executed proof run for this build. Each such row records ACKNOWLEDGED-UNPROVEN durably and is reported UNPROVEN by `status` for as long as it is in force")
	set.BoolVar(&dryRun, "dry-run", false, "run every preflight and write NOTHING")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "per-request timeout")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
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

	// --- Preflight 1: is query-api reachable at all? -------------------
	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the running query-api's registry: %v.\n"+
			"  A measurement that did not happen is not a pass -- fix the deployment or point -registry-url at the right process.", err)
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
	// KNOWN GAP, CHAOS-5524, owner this lane, blocked on the go-api-prove
	// hotfix merging (the fix belongs in internal/goapiproof/registry.go,
	// inside that hotfix's file set).
	//
	// `FetchRegistry` accepts a /registry response that lists the SAME
	// operation twice and silently keeps the last one, where the Python
	// verb refuses outright. That map is what this preflight compares
	// against, so a malformed or tampered registry decides which document
	// digest a routing row is written with. Found by codex r1 (F6).
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
			return refuse("%v\n  the deployed query-api must identify its build at %s before any row can be pointed at it", err, buildInfoURL)
		}
		return err
	}
	if expectBuild != "" && expectBuild != running {
		return refuse("-expect-build %q does not match the running build %q -- the flag is a cross-check, never the source", expectBuild, running)
	}

	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	// --- Preflight 4 (inside Enable) + the write, one transaction ------
	outcomes, err := goapiproof.Enable(ctx, pool, goapiproof.EnableRequest{
		SchemaDigest:        registry.SchemaDigest,
		RunningBuild:        running,
		Operations:          operations,
		DocumentDigest:      registry.DocumentDigest,
		Mode:                mode,
		RolloutPercentage:   rollout,
		RecordedBy:          common.recordedBy,
		ReviewEvidence:      common.reviewEvidence,
		AcknowledgeUnproven: acknowledgeUnproven,
		DryRun:              dryRun,
	})
	if err != nil {
		if errors.Is(err, goapiproof.ErrEnableUnproven) || errors.Is(err, goapiproof.ErrEnableRequestRefused) {
			return refuse("%v", err)
		}
		return err
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
				"WARNING: go_api_routing.enabled_unproven operation=%s stage_evidence=none candidate_build=%s schema_digest=%s mode=%s dry_run=%t\n",
				outcome.Operation, running, registry.SchemaDigest, mode, dryRun)
		}
	}

	verb := "enabled"
	if dryRun {
		verb = "would enable"
	}
	fmt.Fprintf(stdout, "go-api-routing: schema_digest=%s candidate_build=%s mode=%s rollout=%d dry_run=%t\n",
		registry.SchemaDigest, running, mode, rollout, dryRun)
	fmt.Fprintf(stdout, "go-api-routing: %s total=%d proven=%d unproven=%d\n",
		verb, len(outcomes), len(outcomes)-unproven, unproven)
	for _, outcome := range outcomes {
		flag := ""
		if !outcome.Proven {
			flag = "  (UNPROVEN)"
		}
		fmt.Fprintf(stdout, "go-api-routing:   %-24s mode=%-8s %s%s\n", outcome.Operation, outcome.Mode, outcome.CandidateBuild, flag)
	}
	return nil
}

// runbook is where an operator reads the recovery procedure. Referenced
// from every refusal a digest move can cause, so the message that stops
// the command also says what to do about it.
const runbook = "docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md (section: When the schema digest moves)"
