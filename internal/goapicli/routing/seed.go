package routing

// The `seed` verb (CHAOS-7165): create the FIRST decision -- shadow only -- for an MCP class root that has none.
//
// Nothing is typed but operation names. The schema digest comes from the running query-api's /registry (which must
// agree with this binary's SDL), the build from /buildinfo. It needs Postgres, /registry (unauthenticated) and
// /buildinfo (envelope minted in process) -- not the internal listener and not the proof allowlist. A shadow decision
// alone routes no client anywhere; shadow->canary is still `enable`, behind its per-root receipt.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func runSeed(argv []string) error {
	set := newVerbFlagSet("seed")
	var common commonFlags
	var registryURL, buildInfoURL string
	var dryRun bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	set.StringVar(&buildInfoURL, "buildinfo-url", "", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build written (falls back to "+queryAPIURLEnvVar+"+\"/buildinfo\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_class_decision")
	set.StringVar(&common.operations, "operations", "", classOperationsUsage)
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on every row (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, recorded on every row (required)")
	set.BoolVar(&dryRun, "dry-run", false, "run every preflight and write NOTHING")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds EACH HTTP request, the Postgres dial, and EACH database statement")
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
	scope, err := requireClassScope("seed", common.operations)
	if err != nil {
		return err
	}
	if err := requireClassRootsServed(scope.Operations); err != nil {
		return err
	}

	ctx := context.Background()
	client := httpClient(common.timeout)
	registryURL, buildInfoURL, err = resolveQueryAPIEndpoints(registryURL, buildInfoURL)
	if err != nil {
		return err
	}
	if registryURL, err = sanitizeEndpointURL("-registry-url", registryURL); err != nil {
		return err
	}
	if buildInfoURL, err = sanitizeEndpointURL("-buildinfo-url", buildInfoURL); err != nil {
		return err
	}

	registry, err := goapiproof.FetchRegistry(ctx, client, registryURL)
	if err != nil {
		return refuse("cannot read the running query-api's registry: %v.\n  A measurement that did not happen is not a pass.", err)
	}
	if len(registry.DocumentDigest) == 0 {
		return refuse("%s registers no operations -- there is nothing to seed", goapiproof.EndpointLabel(registryURL))
	}
	if local := localSchemaDigest(); registry.SchemaDigest != local {
		return refuse("schema digest MISMATCH between planes.\n  this binary's embedded SDL: %s\n  running query-api (go)    : %s\n  Run seed from the tools image of the SAME cut. See %s",
			local, registry.SchemaDigest, runbook)
	}
	running, err := goapiproof.FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return refuse("%v\n  the deployed query-api must identify its build at %s", err, goapiproof.EndpointLabelWithPort(buildInfoURL))
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

	outcomes, err := goapiproof.Seed(ctx, pool, goapiproof.SeedRequest{
		SchemaDigest:   registry.SchemaDigest,
		RunningBuild:   running,
		Operations:     scope.Operations,
		DocumentDigest: scope.Digests,
		RecordedBy:     common.recordedBy,
		ReviewEvidence: common.reviewEvidence,
		PrincipalID:    principalID,
		DryRun:         dryRun,
	})
	// A refusal reports every decision (nothing was written). Any OTHER error
	// (audit failure, commit failure) prints no per-operation success line at
	// all: those outcomes were never committed.
	if err != nil && !errors.Is(err, goapiproof.ErrSeedRefused) {
		if errors.Is(err, goapiproof.ErrSeedRequestRefused) {
			return refuse("%v", err)
		}
		return classifyWriteError(err)
	}
	fmt.Fprintf(stdout, "go-api-routing: seed schema_digest=%s candidate_build=%s dry_run=%t\n", registry.SchemaDigest, running, dryRun)
	for _, o := range outcomes {
		fmt.Fprintf(stdout, "go-api-routing:   %-28s %-16s digest=%s %s\n", o.Operation, o.Action, o.DocumentDigest, o.Reason)
		if o.Action == goapiproof.SeedActionCreated {
			fmt.Fprintf(stderr, "go_api_routing.seeded operation=%s owner=go mode_after=shadow rollout=0 build_after=%s schema_digest=%s document_digest=%s recorded_by=%q audit_action=enable correlation_id=%s\n",
				o.Operation, running, registry.SchemaDigest, o.DocumentDigest, common.recordedBy, o.CorrelationID)
		}
	}
	if err != nil {
		return refuse("%v", err)
	}
	return nil
}
