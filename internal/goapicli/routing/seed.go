package routing

// The `seed` verb (CHAOS-7165): create the FIRST routing row -- shadow only --
// for an operation that has none at any schema digest.
//
// Nothing is typed but operation names. The schema digest and document
// digests come from the running query-api's /registry (which must agree with
// this binary's SDL and the edge catalog), the build from /buildinfo. It
// needs Postgres, /registry (unauthenticated) and /buildinfo (envelope minted
// in process) -- not the internal listener and not the proof allowlist. A
// shadow row alone routes no client anywhere; shadow->canary is still
// `enable`, behind its receipt or reviewed named limit.
//
// CHAOS-8517: for a CATALOG operation the first row is no longer neutral. An
// operation with no row at any schema digest is served by query-api; its
// shadow row holds it dark until `enable`. The verb names every such
// operation on stderr, in a dry run too.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func runSeed(argv []string) error {
	set := newVerbFlagSet("seed")
	var common commonFlags
	var registryURL, buildInfoURL string
	var allUnrouted, dryRun bool
	set.StringVar(&registryURL, "registry-url", "", "GET /registry on the DEPLOYED query-api (falls back to "+queryAPIURLEnvVar+"+\"/registry\")")
	set.StringVar(&buildInfoURL, "buildinfo-url", "", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build written (falls back to "+queryAPIURLEnvVar+"+\"/buildinfo\")")
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "", "comma-separated operation names to seed (or use -all-unrouted)")
	set.BoolVar(&allUnrouted, "all-unrouted", false, "seed every registered operation that has no row at ANY schema digest")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog")
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
	if allUnrouted == (strings.TrimSpace(common.operations) != "") {
		return refuse("give exactly one of -operations <names> or -all-unrouted")
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
	scope, isClass, err := resolveClassScope(common.operations)
	if err != nil {
		return err
	}
	var catalog map[string]string
	var named []string
	if isClass {
		// MCP class rows (CHAOS-7214): no catalog, no registered document. The
		// class digest and the root-field checks stand in for them.
		if err := requireClassRootsServed(scope.Operations); err != nil {
			return err
		}
		named = scope.Operations
	} else {
		catalog, err = goapiproof.LoadOperationCatalog(common.catalogPath)
		if err != nil {
			return refuse("%v -- refusing to seed anything on a catalog this process cannot read", err)
		}
		if !allUnrouted {
			if named, err = goapiproof.ResolveOperations(common.operations, catalog); err != nil {
				return refuse("%v", err)
			}
		}
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
		return refuse("schema digest MISMATCH between planes.\n  this binary's embedded SDL: %s\n  running query-api (go)    : %s\n  Rows are keyed by schema_digest; a row written now would be unreachable. Run seed from the tools image of the SAME cut. See %s",
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

	operations := named
	if allUnrouted {
		routed, err := goapiproof.RoutedOperations(ctx, pool)
		if err != nil {
			return classifyWriteError(err)
		}
		// The whole-picture preflight: registry vs catalog vs every row at the
		// running schema digest, not only the operations about to be seeded.
		rowsAtRunning, err := goapiproof.RunningDigestRows(ctx, pool, registry.SchemaDigest)
		if err != nil {
			return classifyWriteError(err)
		}
		if disagreements := goapiproof.AllUnroutedDisagreements(registry.DocumentDigest, catalog, rowsAtRunning); len(disagreements) > 0 {
			return refuse("the registry, the catalog and the routing rows disagree on %d point(s): %v.\n  -all-unrouted writes nothing around a disagreement; see `dho goapi routing status`.", len(disagreements), disagreements)
		}
		operations = goapiproof.UnroutedOperations(registry.DocumentDigest, routed)
		if len(operations) == 0 {
			fmt.Fprintf(stdout, "go-api-routing: seed schema_digest=%s candidate_build=%s nothing unrouted\n", registry.SchemaDigest, running)
			return nil
		}
	}

	// Registered + catalog-agreeing, for every operation, before ANY write.
	// (Class rows have neither; their digest map is the class's own.)
	documentDigests := registry.DocumentDigest
	if isClass {
		documentDigests = scope.Digests
	}
	var notRegistered, divergent []string
	for _, operation := range operations {
		if isClass {
			break
		}
		registered, ok := registry.DocumentDigest[operation]
		switch {
		case !ok:
			notRegistered = append(notRegistered, operation)
		case registered != catalog[operation]:
			divergent = append(divergent, fmt.Sprintf("%s: catalog=%q go=%s", operation, catalog[operation], registered))
		}
	}
	if len(notRegistered) > 0 {
		return refuse("the running query-api does not register: %v", notRegistered)
	}
	if len(divergent) > 0 {
		return refuse("document digest MISMATCH or operation outside the catalog for %d operation(s): %v", len(divergent), divergent)
	}

	outcomes, err := goapiproof.Seed(ctx, pool, goapiproof.SeedRequest{
		SchemaDigest:   registry.SchemaDigest,
		RunningBuild:   running,
		Operations:     operations,
		DocumentDigest: documentDigests,
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
	if note := seedHoldsDarkNote(outcomes, isClass); note != "" {
		fmt.Fprintln(stderr, note)
	}
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

// seedHoldsDarkNote is what `seed` says when it writes (or, in a dry run or after a refusal, would
// write) the first row of a catalog operation: that operation was served with no row, and its shadow
// row holds it dark until `enable` admits it (CHAOS-8517). Empty for MCP class rows, whose roots are
// dark with no row, and when no first row is involved.
func seedHoldsDarkNote(outcomes []goapiproof.SeedOutcome, isClass bool) string {
	if isClass {
		return ""
	}
	// `seed` creates a row only for an operation with no row at ANY schema digest (it refuses one that
	// has a row only at an older digest), which is exactly the state the catalog rule serves.
	var written, planned []string
	for _, outcome := range outcomes {
		switch outcome.Action {
		case goapiproof.SeedActionCreated:
			written = append(written, outcome.Operation)
		case goapiproof.SeedActionWouldCreate:
			planned = append(planned, outcome.Operation)
		}
	}
	switch {
	case len(written) > 0:
		return fmt.Sprintf("go-api-routing: NOTE: %d catalog operation(s) had no routing row and were SERVED by query-api's catalog rule; each now has a shadow row and is NOT served until `dho goapi routing enable` admits it: %s",
			len(written), strings.Join(written, ","))
	case len(planned) > 0:
		return fmt.Sprintf("go-api-routing: NOTE: %d catalog operation(s) have no routing row and are SERVED by query-api's catalog rule; a shadow row would hold each dark until `dho goapi routing enable` admits it: %s",
			len(planned), strings.Join(planned, ","))
	}
	return ""
}
