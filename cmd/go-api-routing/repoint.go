package main

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
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func runRepoint(argv []string) error {
	set := newVerbFlagSet("repoint")
	var common commonFlags
	var registryURL, buildInfoURL, expectBuild string
	var dryRun bool
	set.StringVar(&registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api -- the only authority on which schema digest is live")
	set.StringVar(&buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build every row is pointed at")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row touched (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every row touched (required)")
	set.StringVar(&expectBuild, "expect-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written")
	set.BoolVar(&dryRun, "dry-run", false, "report what would change and write NOTHING")
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

	// Every one of these is a state the OPERATOR resolves -- an
	// unreachable process, a build the process cannot name, a filter that
	// names nothing -- so each exits 2, not 1. `enable` already routed
	// them through refuse(); this verb returned some of them raw, which
	// is the same class as r1 F2 one file over.
	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the running query-api's registry: %v.\n"+
			"  A measurement that did not happen is not a pass -- fix the deployment or point -registry-url at the right process.", err)
	}
	// r5 P1 (reproduced): same shape as enable's identical preflight --
	// `FetchRegistry` itself no longer refuses on an empty registry
	// (status needs the schema digest it still reports), so the write
	// verb refuses here instead.
	if len(registry.DocumentDigest) == 0 {
		return refuse("%s registers no operations -- there is nothing to prove", goapiproof.EndpointLabel(registryURL))
	}
	running, err := goapiproof.FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return refuse("%v\n  the deployed query-api must identify its build at %s before any row can be pointed at it", err, buildInfoURL)
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

	outcomes, err := goapiproof.Repoint(ctx, pool, goapiproof.RepointRequest{
		SchemaDigest:   registry.SchemaDigest,
		RunningBuild:   running,
		ExpectBuild:    expectBuild,
		Operations:     operations,
		RecordedBy:     common.recordedBy,
		ReviewEvidence: common.reviewEvidence,
		DryRun:         dryRun,
	})
	if err != nil {
		return refuse("%v", err)
	}

	summary := goapiproof.Summarize(outcomes)
	verb := "repointed"
	if dryRun {
		verb = "would repoint"
	}
	// Every counter prints, including the zeros: "no row needed changing"
	// and "nobody looked" must not read alike.
	fmt.Fprintf(stdout, "go-api-routing: schema_digest=%s running_build=%s dry_run=%t\n", registry.SchemaDigest, running, dryRun)
	fmt.Fprintf(stdout, "go-api-routing: %s total=%d changed=%d unchanged=%d\n", verb, summary.Total, summary.Changed, summary.Unchanged)
	for _, outcome := range outcomes {
		state := "unchanged"
		if outcome.Changed {
			state = "repointed"
		}
		fmt.Fprintf(stdout, "go-api-routing:   %-24s mode=%-8s %s  %s -> %s\n",
			outcome.Operation, outcome.ModeAfter, state, outcome.BuildFrom, outcome.BuildTo)
	}
	return nil
}
