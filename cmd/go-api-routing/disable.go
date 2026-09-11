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
	"flag"
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
	// `-candidate-build ""` -- e.g. `-candidate-build
	// "$SEEN"` in a script where $SEEN happened to be unset -- must never
	// read the SAME as never passing the flag at all: `""` is exactly
	// what `ExpectedCandidateBuild == ""` already means as "no guard", so
	// an explicitly empty value would otherwise apply the write
	// completely unguarded. Python refuses the identical command
	// (`REFUSED: ...row points at candidate build ..., not the  you
	// named`, exit 2). `set.Visit` only walks flags actually
	// PASSED, so this tells "typed empty" from "never typed" -- the flag
	// package itself cannot.
	var candidateBuildPassedEmpty bool
	set.Visit(func(f *flag.Flag) {
		if f.Name == "candidate-build" && f.Value.String() == "" {
			candidateBuildPassedEmpty = true
		}
	})
	if candidateBuildPassedEmpty {
		return refuse("-candidate-build was passed but empty -- an empty value is silently the SAME as no guard at all, which this refuses rather than applies unguarded. Omit the flag entirely to skip the guard on purpose, or pass a real build sha")
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
		if errors.Is(err, goapiproof.ErrDisableGuardMismatch) || errors.Is(err, goapiproof.ErrDisableRefusesEnablingMode) {
			return refuse("%v", err)
		}
		return classifyWriteError(err)
	}

	summary := goapiproof.SummarizeDisable(changes)
	fmt.Fprintf(stdout, "schema_digest %s\n", local)
	fmt.Fprintf(stdout, "%-24s %-10s -> %-10s CANDIDATE BUILD                           DOCUMENT DIGEST\n", "OPERATION", "FROM", "TO")
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
		// document_digest is the row's identity alongside operation and
		// schema_digest: two rows for the same operation, differing only
		// here, must print as two distinguishable lines, not two copies of
		// the same one.
		fmt.Fprintf(stdout, "%-24s %-10s -> %-10s %-40s %s%s\n", change.Operation, current, change.NewMode, build, change.DocumentDigest, suffix)
		// A `primary` operation is the one Go is fully serving; saying so
		// out loud is cheap and the operator may not have realised.
		if change.CurrentMode == "primary" && change.NewMode == "disabled" {
			fmt.Fprintln(stdout, "    NOTE: this removes Go entirely for this operation -- Python serves it from the next request.")
		}
		// This binary computes SchemaDigest from its
		// OWN embedded SDL, and (unlike the Python verb, which runs
		// inside the deployed edge image) is built from an operator
		// checkout by design -- so a stale checkout would silently no-op
		// while leaving a real row, at the digest the deployed process
		// actually uses, completely untouched, unless named here.
		// `status`'s census already has this fact.
		if len(change.StaleSchemaDigests) > 0 {
			// `StaleSchemaDigests` is a deduplicated
			// list of DIGESTS, not a row count -- two rows for this
			// operation at ONE stale digest still print as a single
			// entry here, so labelling that count "row(s)" understates
			// how many rows actually exist there. Say what is actually
			// being counted.
			fmt.Fprintf(stdout, "    !! %d digest(s) OTHER than this checkout's live one carry a row for this operation: %v -- if you expected THOSE rows to change, this binary's embedded SDL is stale; see `status` or rebuild from the deployed revision\n",
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
		fmt.Fprintf(stderr, "go_api_routing.disabled operation=%s from=%s to=%s schema_digest=%s document_digest=%s recorded_by=%s\n",
			change.Operation, change.CurrentMode, change.NewMode, local, change.DocumentDigest, common.recordedBy)
	}
	fmt.Fprintf(stdout, "\napplied: %d row(s) now mode=%s\n", summary.Applied, mode)
	// No `summary.Applied != expected` refusal here: the race it would
	// describe ("a row was repointed between the read and the write") is
	// made impossible by the `FOR UPDATE` plan read -- see
	// goapiproof.Disable's own write loop, which marks every row Applied
	// unconditionally on a successful write for exactly this reason.
	return nil
}
