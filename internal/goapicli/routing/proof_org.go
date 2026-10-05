package routing

// The `proof-org` verb: CHAOS-7096's own allowlist management, independent
// of every other verb's class-decision machinery -- no schema digest,
// no candidate build, no mode. add/remove/list on go_api_proof_orgs, each
// write audited to go_api_proof_org_audits (goapiproof.AddProofOrg/
// RemoveProofOrg do both in one transaction).

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

const proofOrgUsage = `dho goapi routing proof-org <verb> [flags]

verbs:
  add      allowlist an org for /query/proof-write (-org, -recorded-by, -review-evidence)
  remove   remove an org from the allowlist (-org, -recorded-by, -review-evidence)
  list     print the whole allowlist; no flags beyond -postgres-uri/-timeout

The allowlist is EMPTY on every deployment until this verb adds an org --
/query/proof-write refuses every org until then. See go_api_proof_orgs
(alembic 0145) and internal/queryapi/server/query_route.go's
newDocumentDispatchHandler doc comment for the request-time check this
table backs.`

func runProofOrg(argv []string) error {
	// Checked BEFORE splitVerb: splitVerb's "-h"/"--help" check (built for
	// the TOP-LEVEL command's "no verb given" fallback) does not recognize
	// bare "-help" -- Go's own flag package treats -h/-help/--help/--h all
	// as help requests, and this sub-dispatcher must match that, not
	// splitVerb's narrower top-level-only set (which would otherwise send
	// an unrecognized "-help" through the "no verb given, defaulting to
	// repoint" fallback -- nonsensical here, there is no sub-verb default).
	if len(argv) > 0 && (argv[0] == "-h" || argv[0] == "-help" || argv[0] == "--help") {
		return runProofOrgHelp()
	}
	verb, rest := splitVerb(argv)
	switch verb {
	case "add":
		return helpAsSuccess(runProofOrgAdd(rest))
	case "remove":
		return helpAsSuccess(runProofOrgRemove(rest))
	case "list":
		return helpAsSuccess(runProofOrgList(rest))
	case "help":
		return runProofOrgHelp()
	default:
		return refuse("unknown proof-org sub-verb %q\n\n%s", verb, proofOrgUsage)
	}
}

// runProofOrgHelp prints proofOrgUsage's own verbs: section for a human,
// then falls through to a REAL flag.FlagSet's `-h` (the shared add/remove
// flags) so `proof-org -h`/`-help`/`--help` behaves exactly like every
// other top-level verb's help flag -- real usage text on verbFlagOutput,
// errHelpRequested -> helpAsSuccess -> exit 0. Matters structurally, not
// cosmetically: this command's own `everyVerb` test harness drives every
// verb the usage text names, generically, and expects that shape from all
// of them, proof-org included.
func runProofOrgHelp() error {
	fmt.Fprintln(stdout, proofOrgUsage)
	set := newVerbFlagSet("proof-org")
	var common commonFlags
	var orgID string
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_proof_orgs")
	set.StringVar(&orgID, "org", "", "the org id (required by add/remove)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this (required by add/remove)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY (required by add/remove)")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds the Postgres dial and EACH database statement")
	return parseVerbFlags(set, []string{"-h"})
}

func runProofOrgAdd(argv []string) error {
	set := newVerbFlagSet("proof-org add")
	var common commonFlags
	var orgID string
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_proof_orgs")
	set.StringVar(&orgID, "org", "", "the org id to allowlist (required)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on the row and its audit entry (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words (required)")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds the Postgres dial and EACH database statement")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if orgID == "" {
		return refuse("-org is required")
	}
	if err := refuseControlChars("-org", orgID); err != nil {
		return err
	}
	if err := common.requireProvenance(); err != nil {
		return err
	}
	if err := common.requirePostgres(); err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := goapiproof.AddProofOrg(ctx, pool, goapiproof.ProofOrgRequest{
		OrgID:          orgID,
		RecordedBy:     common.recordedBy,
		ReviewEvidence: common.reviewEvidence,
	}); err != nil {
		return classifyWriteError(err)
	}
	fmt.Fprint(stderr, proofOrgAddedEvent(orgID, common.recordedBy))
	fmt.Fprintf(stdout, "go-api-routing: %q is now allowlisted for /query/proof-write\n", orgID)
	return nil
}

func runProofOrgRemove(argv []string) error {
	set := newVerbFlagSet("proof-org remove")
	var common commonFlags
	var orgID string
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_proof_orgs")
	set.StringVar(&orgID, "org", "", "the org id to remove (required)")
	set.StringVar(&common.recordedBy, "recorded-by", "", "WHO is running this, recorded on the audit entry (required)")
	set.StringVar(&common.reviewEvidence, "review-evidence", "", "WHY, in your own words (required)")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds the Postgres dial and EACH database statement")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if orgID == "" {
		return refuse("-org is required")
	}
	if err := refuseControlChars("-org", orgID); err != nil {
		return err
	}
	if err := common.requireProvenance(); err != nil {
		return err
	}
	if err := common.requirePostgres(); err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	removed, err := goapiproof.RemoveProofOrg(ctx, pool, goapiproof.ProofOrgRequest{
		OrgID:          orgID,
		RecordedBy:     common.recordedBy,
		ReviewEvidence: common.reviewEvidence,
	})
	if err != nil {
		return classifyWriteError(err)
	}
	fmt.Fprint(stderr, proofOrgRemovedEvent(orgID, common.recordedBy, removed))
	if removed {
		fmt.Fprintf(stdout, "go-api-routing: %q removed from the /query/proof-write allowlist\n", orgID)
	} else {
		fmt.Fprintf(stdout, "go-api-routing: %q was not on the allowlist (no-op on the live row; the removal is still audited)\n", orgID)
	}
	return nil
}

func runProofOrgList(argv []string) error {
	set := newVerbFlagSet("proof-org list")
	var common commonFlags
	common.bindPostgresURI(set, "domain Postgres DSN holding go_api_proof_orgs")
	set.DurationVar(&common.timeout, "timeout", 30*time.Second, "bounds the Postgres dial and the query")
	if err := parseVerbFlags(set, argv); err != nil {
		return err
	}
	if err := common.requirePositiveTimeout(); err != nil {
		return err
	}
	if err := common.requirePostgres(); err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := connectPostgres(ctx, common.postgresURI, common.timeout)
	if err != nil {
		return err
	}
	defer pool.Close()

	rows, err := goapiproof.ListProofOrgs(ctx, pool)
	if err != nil {
		return internal("%v", err)
	}
	if len(rows) == 0 {
		fmt.Fprintln(stdout, "go-api-routing: the /query/proof-write allowlist is empty")
		return nil
	}
	fmt.Fprintf(stdout, "%-24s %-20s %s\n", "ORG_ID", "ADDED_BY", "REASON")
	for _, row := range rows {
		fmt.Fprint(stdout, proofOrgListRow(row.OrgID, row.AddedBy, row.Reason))
	}
	return nil
}

// The structured one-line events and the list table print operator-supplied
// values (-org, -recorded-by, review evidence stored as the reason). Each is
// quoted (%q) so a value carrying spaces, `key=value` text or control
// characters cannot forge a second field or a second line (CHAOS-7174).

func proofOrgAddedEvent(orgID, recordedBy string) string {
	return fmt.Sprintf("go_api_proof_orgs.added org_id=%q recorded_by=%q\n", orgID, recordedBy)
}

func proofOrgRemovedEvent(orgID, recordedBy string, existed bool) string {
	return fmt.Sprintf("go_api_proof_orgs.removed org_id=%q recorded_by=%q existed=%t\n", orgID, recordedBy, existed)
}

func proofOrgListRow(orgID, addedBy, reason string) string {
	return fmt.Sprintf("%-24q %-20q %q\n", orgID, addedBy, reason)
}
