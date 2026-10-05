package routing

// The `disable` verb: the rollback half of the rollout, for MCP class roots.
//
// It contacts NOTHING. No /registry, no /buildinfo, no credential: it must work when the planes disagree and when
// the deployed process is down, which is exactly when it is needed.
//
// It never writes current_candidate_build. -candidate-build is a GUARD ("refuse if someone repointed this
// decision since I looked"); changing the build is `repoint`'s job.

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
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_class_decision")
	set.StringVar(&common.operations, "operations", "", classOperationsUsage)
	set.StringVar(&mode, "mode", "", "python = not served, the documented safe default; disabled = same reachability but records a deliberate decision; shadow = the client still gets Python's response (required). Each holds a catalog operation dark: an operation with NO row at any schema digest is served, and this verb never inserts one")
	set.StringVar(&candidateBuild, "candidate-build", "", "optional GUARD: refuse if a row points at a different build than this, i.e. somebody repointed it since you looked. NEVER written -- disable changes mode only")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this. Required with -apply")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY. Required with -apply; recorded durably on each row")
	set.BoolVar(&apply, "apply", false, "write the changes. Without it, prints what would change and exits 0")
	// No -registry-url or -buildinfo-url: this verb makes no HTTP call at
	// all, and a flag that is accepted and ignored tells an operator this
	// command does something it does not. -timeout IS honoured, because
	// it bounds the Postgres dial -- the one thing this verb waits on.
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "how long to wait for Postgres to answer. This verb makes no HTTP call; the timeout bounds the Postgres dial and EACH database statement (server-side statement_timeout/lock_timeout), never the run as a whole")
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
		switch {
		case f.Name == "candidate-build" && f.Value.String() == "":
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

	scope, err := requireClassScope("disable", common.operations)
	if err != nil {
		return err
	}
	// No root-in-SDL check here on purpose: disabling a root the SDL no longer has is exactly the cleanup a
	// removed root needs.
	catalog, operations := scope.Digests, scope.Operations

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
		fmt.Fprintf(stderr, "go_api_routing.disabled operation=%s from=%s to=%s schema_digest=%s document_digest=%s recorded_by=%q\n",
			change.Operation, change.CurrentMode, change.NewMode, local, change.DocumentDigest, common.recordedBy)
	}
	fmt.Fprintf(stdout, "\napplied: %d row(s) now mode=%s\n", summary.Applied, mode)
	return nil
}
