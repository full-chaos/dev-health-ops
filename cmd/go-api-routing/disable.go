package main

// The `disable` verb: the rollback half of the rollout.
//
// It contacts NOTHING. No /registry, no /buildinfo, no credential: it
// must work when the planes disagree and when the deployed process is
// down, which is exactly when it is needed. The live schema digest comes
// from this binary's own embedded SDL -- the same value the Python edge
// computes and the same one `enable` proved equal to the running
// process's before any row was written.
//
// It never writes current_candidate_build. -candidate-build is a GUARD
// ("refuse if someone repointed this row since I looked"), documented
// "Never written -- disable changes mode only" in go_api_cli.py. That
// asymmetry with `enable` is the Python contract, kept deliberately.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func runDisable(argv []string) error {
	set := newVerbFlagSet("disable")
	var common commonFlags
	var mode, candidateBuild string
	var apply bool
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_routing_state")
	set.StringVar(&common.operations, "operations", "all-registered", "comma-separated operation names, or 'all-registered' (default)")
	set.StringVar(&common.catalogPath, "catalog", goapiproof.DefaultCatalogPath, "the edge's registered-document catalog")
	set.StringVar(&mode, "mode", "", "python = the documented safe default (same as no row); disabled = same reachability but records a deliberate decision; shadow = the client still gets Python's response (required)")
	set.StringVar(&candidateBuild, "candidate-build", "", "optional GUARD: refuse if a row points at a different build than this, i.e. somebody repointed it since you looked. NEVER written -- disable changes mode only")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this. Required with -apply")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY. Required with -apply; recorded durably on each row")
	set.BoolVar(&apply, "apply", false, "write the changes. Without it, prints what would change and exits 0")
	// No -registry-url or -buildinfo-url: this verb makes no HTTP call at
	// all, and a flag that is accepted and ignored tells an operator this
	// command does something it does not. -timeout IS honoured, because
	// it bounds the Postgres dial -- the one thing this verb waits on.
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "how long to wait for Postgres to answer. This verb makes no HTTP call; the timeout bounds the database connection only")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if mode == "" {
		return refuse("-mode is required and must be one of %v", goapiproof.DisableModes)
	}
	if err := common.requirePostgres(); err != nil {
		return err
	}
	if apply {
		if err := common.requireProvenance(); err != nil {
			return err
		}
	}

	catalog, err := goapiproof.LoadOperationCatalog(common.catalogPath)
	if err != nil {
		return refuse("%v", err)
	}
	operations, err := goapiproof.ResolveOperations(common.operations, catalog)
	if err != nil {
		return refuse("%v", err)
	}

	ctx := context.Background()
	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	local := localSchemaDigest()
	changes, err := goapiproof.Disable(ctx, pool, goapiproof.DisableRequest{
		SchemaDigest:           local,
		Operations:             operations,
		DocumentDigest:         catalog,
		NewMode:                mode,
		ExpectedCandidateBuild: candidateBuild,
		RecordedBy:             common.recordedBy,
		ReviewEvidence:         common.reviewEvidence,
		Apply:                  apply,
	})
	if err != nil {
		if errors.Is(err, goapiproof.ErrDisableGuardMismatch) || errors.Is(err, goapiproof.ErrDisableRefusesEnablingMode) || errors.Is(err, goapiproof.ErrDisableStaleDigestOnly) {
			return refuse("%v", err)
		}
		return classifyWriteError(err)
	}

	summary := goapiproof.SummarizeDisable(changes)
	fmt.Fprintf(stdout, "schema_digest %s\n", local)
	fmt.Fprintf(stdout, "%-24s %-10s -> %-10s CANDIDATE BUILD\n", "OPERATION", "FROM", "TO")
	for _, change := range changes {
		current := change.CurrentMode
		if current == "" {
			current = "(no row)"
		}
		build := change.CandidateBuild
		if build == "" {
			build = "-"
		}
		suffix := ""
		if change.IsNoop() {
			suffix = "   [no change]"
		}
		fmt.Fprintf(stdout, "%-24s %-10s -> %-10s %s%s\n", change.Operation, current, change.NewMode, build, suffix)
		// A `primary` operation is the one Go is fully serving; saying so
		// out loud is cheap and the operator may not have realised.
		if change.CurrentMode == "primary" && change.NewMode == "disabled" {
			fmt.Fprintln(stdout, "    NOTE: this removes Go entirely for this operation -- Python serves it from the next request.")
		}
		// r6 F2 (reproduced): this binary computes SchemaDigest from its
		// OWN embedded SDL, and (unlike the Python verb, which runs
		// inside the deployed edge image) is built from an operator
		// checkout by design -- so a stale checkout silently no-ops here
		// while leaving a real row, at the digest the deployed process
		// actually uses, completely untouched. `status`'s census already
		// has this fact; naming it here is the fix.
		if len(change.StaleSchemaDigests) > 0 {
			fmt.Fprintf(stdout, "    !! %d row(s) for this operation exist at OTHER schema digest(s) this checkout does not match: %v -- if you expected THOSE rows to change, this binary's embedded SDL is stale; see `status` or rebuild from the deployed revision\n",
				len(change.StaleSchemaDigests), change.StaleSchemaDigests)
		}
	}

	if !apply {
		fmt.Fprintf(stdout, "\nDRY RUN: %d row(s) would change, %d unchanged (%d of those have no row at this digest). Re-run with -apply -recorded-by <who> -review-evidence '<why>' to write.\n",
			summary.Actionable, summary.Total-summary.Actionable, summary.NoRow)
		return nil
	}

	for _, change := range changes {
		if !change.Applied {
			continue
		}
		// One structured line per row, so a log search finds the specific
		// operation and not merely that "something was disabled".
		fmt.Fprintf(stderr, "go_api_routing.disabled operation=%s from=%s to=%s schema_digest=%s recorded_by=%s\n",
			change.Operation, change.CurrentMode, change.NewMode, local, common.recordedBy)
	}
	fmt.Fprintf(stdout, "\napplied: %d row(s) now mode=%s\n", summary.Applied, mode)
	// r6 T1 (reproduced): the `summary.Applied != expected` refusal that
	// used to live here described a race ("a row was repointed between
	// the read and the write") the r2 R2-08 `FOR UPDATE` plan read
	// already makes impossible -- see goapiproof.Disable's own write
	// loop, which now marks every row Applied unconditionally on a
	// successful write for exactly this reason. Dead code removed rather
	// than kept as unreachable dressing around a stale comment.
	return nil
}
