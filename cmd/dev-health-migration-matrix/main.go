// Command dev-health-migration-matrix renders and checks the evidence
// columns of docs/go-migration-matrix.md.
//
// Two modes, and the split between them is the design:
//
//	-render   reads LIVE sources (Postgres go_api_routing_state /
//	          go_api_proof_run, the running fleet's image labels) plus the
//	          committed ledger, writes the rendered blocks into the doc AND
//	          a snapshot of what it read into
//	          contracts/migration-status/v1/last-render.json.
//
//	-check    reads ONLY committed files, re-renders, and fails if the doc
//	          disagrees or if any row claims a status its evidence cell
//	          cannot support. No database, no docker, no network -- so it
//	          runs in any CI job, on any runner, offline.
//
// The reason the snapshot exists at all: a status page whose live cells are
// re-queried at check time tells you about the fleet at CI time, not about
// the fleet the committed page describes. Committing the reading, with its
// timestamp, makes the page a dated claim a reader can audit -- and makes a
// STALE reading a CI failure rather than an invisible one.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	docRelative       = "docs/go-migration-matrix.md"
	statusRelative    = "contracts/migration-status/v1/status.json"
	renderRelative    = "contracts/migration-status/v1/last-render.json"
	nativeRelative    = "contracts/native-families/v1/native-families.json"
	digestPinRelative = "contracts/graphql/v1/schema-digest.json"

	// CHAOS-5473: the four sources scripts/gen_go_migration_matrix_docs.py
	// (deleted) used to read for the provider-sync/daily/remaining/workgraph
	// blocks. Trap #98: dev-health-migration-matrix now reads non-Go files --
	// go.yml/go-quality.yml's path filters must include all four plus
	// nativeRelative/docRelative above.
	providerMatrixRelative = "contracts/provider-matrix/v1/matrix.json"
	dailyFamiliesRelative  = "internal/jobs/metrics/daily/families.json"
	remainingFamiliesRel   = "internal/jobs/metrics/remaining/families.json"
	jobDailyPyRelative     = "src/dev_health_ops/metrics/job_daily.py"

	// The "Per REST endpoint" section's two sources. Both are already
	// covered by go.yml's path filters -- main.py by the existing
	// `src/dev_health_ops/api/**` entry, query-api's *.go files as
	// ordinary Go source.
	mainPyRelative      = "src/dev_health_ops/api/main.py"
	queryAPIDirRelative = "internal/queryapi/server"

	// catalogRelative is the registered-operation catalog the edge
	// dispatches by -- the file `dev-hops go-api routing status` reports
	// DOCUMENT_DRIFT against. Read by -check as well as -render, so a row
	// the edge cannot dispatch fails the committed page (R14) rather than
	// rendering as served. Trap #98: it is a non-Go input;
	// go.yml's `src/dev_health_ops/api/**` path filter already covers it.
	catalogRelative = "src/dev_health_ops/api/graphql/go_api_operations.json"
)

// defaultFleetContainers is the compose fleet whose image labels answer
// "what commit is running?". One per distinct image, not one per replica.
//
// queryAPIContainerName is the compose container ReadFleetRevisions reads
// query-api's OWN build identity from -- REST proof has no
// go_api_routing_state row to read a per-operation candidate build from
// (restproven.go's own package doc comment), so -render checks REST
// receipts against THIS container's label specifically, never the
// fleet-wide Revision field, which requires every container (including
// worker images unrelated to query-api) to agree.
const queryAPIContainerName = "dev-health-query-api-1"

// postgresURIEnvVar is the ONLY place the DSN may come from other than the
// -dsn flag -- named so the usage text and the flag-parsing path can name
// the variable without ever naming its value.
const postgresURIEnvVar = "POSTGRES_URI"

var defaultFleetContainers = []string{
	"dev-health-go-worker-1",
	"dev-health-go-worker-heavy-1",
	"dev-health-go-worker-ops-1",
	"dev-health-go-scheduler-1",
	"dev-health-go-reconciler-1",
	queryAPIContainerName,
	"dev-health-api-1",
}

// matrixFlags holds the pointers registerFlags binds. A struct rather than
// main's own local vars so a test can register on a private *flag.FlagSet
// instead of the process-global flag.CommandLine.
type matrixFlags struct {
	root, fleet, routing, containers, dsn *string
	render, check, printSQL               *bool
}

// registerFlags binds every flag this command accepts on set. -dsn is
// registered through secrets.BindFlag: an EMPTY default, never
// os.Getenv(postgresURIEnvVar), so `flag`'s usage text (printed on -h or
// any parse error) never carries the DSN's value. Call
// secrets.ResolveFlag(set, f.dsn, "dsn", postgresURIEnvVar) after set.Parse to apply
// the environment fallback.
func registerFlags(set *flag.FlagSet) *matrixFlags {
	f := &matrixFlags{}
	f.root = set.String("root", ".", "repository root")
	f.render = set.Bool("render", false, "read live sources and rewrite the generated blocks")
	f.check = set.Bool("check", false, "validate the committed doc against the committed sources")
	f.dsn = new(string)
	secrets.BindFlag(set, f.dsn, "dsn", postgresURIEnvVar, "Postgres DSN for the go_api_registry tables")
	f.fleet = set.String("fleet", "docker", `how to read the fleet: "docker", "none", or a path to a JSON {container: revision} file`)
	f.routing = set.String("routing", "", "path to a JSON routing snapshot, INSTEAD of -dsn (for hosts where the operator has no DSN; build it with -print-routing-sql)")
	f.containers = set.String("containers", strings.Join(defaultFleetContainers, ","), "comma-separated container names to inspect")
	f.printSQL = set.Bool("print-routing-sql", false, "print the statement ReadRoutingState runs and exit; `psql -At -f -` on it writes a -routing snapshot file as-is")
	return f
}

func main() {
	f := registerFlags(flag.CommandLine)
	flag.Parse()
	secrets.ResolveFlag(flag.CommandLine, f.dsn, "dsn", postgresURIEnvVar)

	// A single boundary, applied at every error print in this function
	// from here on, covers every error this command can print, no matter
	// which layer produced it or whether that layer remembered the DSN
	// could be inside -- construct it once, from the DSN this run
	// actually resolved (registerFlags/ResolveFlag above), and apply it
	// to each print rather than trusting every call site downstream to
	// redact its own.
	boundary := secrets.NewBoundary(*f.dsn)

	explicit := map[string]bool{}
	flag.Visit(func(fl *flag.Flag) { explicit[fl.Name] = true })
	notice, err := checkFlagCombination(explicit, *f.printSQL, *f.render, *f.check, *f.fleet, os.Getenv(postgresURIEnvVar) != "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", boundary.Redact(err))
		os.Exit(2)
	}
	if notice != "" {
		fmt.Fprintf(os.Stderr, "notice: %s\n", notice)
	}

	// Printing the statement is not a render or a check, so it is answered
	// before the -render/-check exclusivity rule: an operator asking how to
	// produce a routing snapshot has neither yet.
	if *f.printSQL {
		if err := printRoutingSQL(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", boundary.Redact(err))
			os.Exit(1)
		}
		return
	}

	if *f.render == *f.check {
		fmt.Fprintln(os.Stderr, "exactly one of -render or -check is required")
		os.Exit(2)
	}

	if *f.render {
		err = runRender(*f.root, *f.dsn, *f.routing, *f.fleet, splitCSV(*f.containers))
	} else {
		err = runCheck(*f.root)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", boundary.Redact(err))
		os.Exit(1)
	}
}

// checkFlagCombination refuses every pair of flags in which one would be
// silently ignored, and names the one pair that is honoured on purpose.
//
// Executed before this existed: `-render -dsn X -routing F` rendered from F
// and never mentioned X, and `-check -dsn X -routing /nonexistent -fleet
// /nonexistent` printed "migration matrix OK" -- three flags naming three
// sources, none read, no word said. An operator who passes a source and is
// not told it was ignored believes the page describes that source. Two
// knobs naming one resource must never resolve silently.
//
//   - -print-routing-sql prints a statement and reads nothing, so any other
//     flag with it is refused.
//   - -check reads ONLY committed files, so -dsn, -routing, -fleet and
//     -containers with it are refused.
//   - -render takes ONE routing source: -dsn and -routing together are
//     refused. -routing while $POSTGRES_URI is set (the -dsn DEFAULT) is
//     honoured -- the explicit flag wins -- and says so in a notice.
//   - -containers applies only to `-fleet docker`; with any other -fleet it
//     is refused.
func checkFlagCombination(explicit map[string]bool, printSQL, render, check bool, fleet string, envDSN bool) (string, error) {
	// The mode flags are judged by VALUE (`-check=false` is set but is not
	// -check); only the source flags are judged by presence.
	sources := []string{"dsn", "routing", "fleet", "containers", "root"}
	others := func(allowed ...string) []string {
		allow := map[string]bool{}
		for _, name := range allowed {
			allow[name] = true
		}
		var out []string
		for _, name := range sources {
			if explicit[name] && !allow[name] {
				out = append(out, "-"+name)
			}
		}
		sort.Strings(out)
		return out
	}
	switch {
	case printSQL:
		if render || check {
			return "", fmt.Errorf("-print-routing-sql prints a statement and exits; it cannot be combined with -render or -check")
		}
		if extra := others(); len(extra) > 0 {
			return "", fmt.Errorf("-print-routing-sql prints a statement and reads nothing; %s would be ignored -- pass it alone", strings.Join(extra, " "))
		}
		return "", nil
	case check:
		if extra := others("root"); len(extra) > 0 {
			return "", fmt.Errorf("-check reads only committed files; %s would be ignored -- those flags apply to -render", strings.Join(extra, " "))
		}
		return "", nil
	}
	if explicit["dsn"] && explicit["routing"] {
		return "", fmt.Errorf("-dsn and -routing both name the routing source and only one can be read -- pass one")
	}
	if explicit["containers"] && fleet != "docker" {
		return "", fmt.Errorf("-containers applies only to -fleet docker; with -fleet %q it would be ignored", fleet)
	}
	if explicit["routing"] && envDSN {
		return "reading routing rows from -routing; $POSTGRES_URI (the -dsn default) is set and is NOT read", nil
	}
	return "", nil
}

// legacyBlocks renders the four blocks CHAOS-5473 absorbed from the deleted
// scripts/gen_go_migration_matrix_docs.py, from committed sources only, plus
// the REST endpoints block's "proven" derivation.
//
// restProven is ReadRESTProof's own shape (routeswitch operation name ->
// admissible receipt id), or nil. -check passes the COMMITTED snapshot's
// own Render.RESTProven (offline, no DB); -render passes what it just read
// live. Both callers reach the SAME ApplyRESTProof call below, so the two
// modes cannot compute "proven" two different ways.
func legacyBlocks(root string, families *migrationmatrix.NativeFamilies, restProven map[string]string) (map[string]string, error) {
	pairs, err := migrationmatrix.LoadProviderMatrixPairs(filepath.Join(root, providerMatrixRelative))
	if err != nil {
		return nil, err
	}
	dailyNames, err := migrationmatrix.LoadFamilyNames(filepath.Join(root, dailyFamiliesRelative))
	if err != nil {
		return nil, err
	}
	remainingNames, err := migrationmatrix.LoadFamilyNames(filepath.Join(root, remainingFamiliesRel))
	if err != nil {
		return nil, err
	}
	finalizeCompat, err := migrationmatrix.LoadDailyFinalizeCompatFamilies(
		filepath.Join(root, jobDailyPyRelative), dailyNames, remainingNames)
	if err != nil {
		return nil, err
	}

	providerBlock, err := migrationmatrix.RenderProviderSyncBlock(pairs)
	if err != nil {
		return nil, fmt.Errorf("provider sync block: %w", err)
	}
	dailyBlock, err := migrationmatrix.RenderDailyMetricsBlock(dailyNames, families, finalizeCompat)
	if err != nil {
		return nil, fmt.Errorf("daily metrics block: %w", err)
	}
	remainingBlock, err := migrationmatrix.RenderRemainingMetricsBlock(remainingNames, families.Remaining, finalizeCompat)
	if err != nil {
		return nil, fmt.Errorf("remaining metrics block: %w", err)
	}
	workgraphBlock, err := migrationmatrix.RenderWorkgraphInvestmentBlock(families.Workgraph)
	if err != nil {
		return nil, fmt.Errorf("workgraph investment block: %w", err)
	}
	restRows, err := migrationmatrix.LoadRESTEndpoints(filepath.Join(root, mainPyRelative), filepath.Join(root, queryAPIDirRelative))
	if err != nil {
		return nil, fmt.Errorf("REST endpoints: %w", err)
	}
	restRows = migrationmatrix.ApplyRESTProof(restRows, restProven)
	restBlock := migrationmatrix.RenderRESTEndpointsBlock(restRows)
	return map[string]string{
		"provider":  providerBlock,
		"daily":     dailyBlock,
		"remaining": remainingBlock,
		"workgraph": workgraphBlock,
		"rest":      restBlock,
	}, nil
}

func runCheck(root string) error {
	ledger, err := migrationmatrix.LoadStatusLedger(filepath.Join(root, statusRelative))
	if err != nil {
		return err
	}
	families, err := migrationmatrix.LoadNativeFamilies(filepath.Join(root, nativeRelative))
	if err != nil {
		return err
	}
	snapshot, err := migrationmatrix.LoadRender(filepath.Join(root, renderRelative))
	if err != nil {
		return err
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		return err
	}

	violations := migrationmatrix.ValidateLedger(ledger, families)
	violations = append(violations, migrationmatrix.ValidateRender(snapshot)...)
	violations = append(violations, migrationmatrix.ValidateDocumentDrift(snapshot, catalog)...)

	// ops_sha's SHAPE is checked above (R7); this checks its TRUTH -- that
	// the merge-base -render recorded is still reachable from the tree
	// -check is actually running against. A shape-valid sha that a rebase
	// or a main squash left behind is exactly how a rendered artefact can
	// assert a merge base no longer in this branch's history while every
	// other rule here stays green.
	opsShaViolations, err := checkOpsShaAncestry(root, snapshot.OpsSha)
	if err != nil {
		return err
	}
	violations = append(violations, opsShaViolations...)

	if unjudged := migrationmatrix.UnjudgedLive(snapshot.Operations); unjudged > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d live routing row(s) in the committed render carry no document digest -- the snapshot predates the reader carrying the routing key -- so DOCUMENT_DRIFT / UNREGISTERED cannot be judged for them until the next -render. The page states the count.\n", unjudged)
	}

	// The digest pin is checked against the snapshot rather than the
	// database: if the SDL moved after the last render, every "live" row in
	// the committed page is describing rows that can no longer be matched.
	// That is precisely the kind of failure a stale pin causes, and it is a
	// doc-level fact this offline check can catch.
	pin, err := migrationmatrix.SchemaDigestPin(filepath.Join(root, digestPinRelative))
	if err != nil {
		return err
	}
	if pin != snapshot.SchemaDigest {
		violations = append(violations, migrationmatrix.Violation{
			Subject: "schema digest",
			Rule:    "R12-digest-moved",
			Detail: fmt.Sprintf("the SDL pin is now %s but the committed render was made at %s; "+
				"every routing row on this page is keyed by the old digest and cannot be matched by the router. "+
				"Re-render, and re-enable routing after the query-api image is rebuilt.", pin, snapshot.SchemaDigest),
		})
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath) //nolint:gosec // repo-relative path
	if err != nil {
		return fmt.Errorf("read %s: %w", docRelative, err)
	}
	doc := string(raw)

	legacy, err := legacyBlocks(root, families, snapshot.RESTProven)
	if err != nil {
		return err
	}

	blocks := []struct {
		name         string
		begin, end   string
		wantRendered string
	}{
		{"family status", migrationmatrix.FamilyBlockBegin, migrationmatrix.FamilyBlockEnd,
			migrationmatrix.RenderFamilyBlock(ledger, families)},
		{"go-api operations", migrationmatrix.OpsBlockBegin, migrationmatrix.OpsBlockEnd,
			migrationmatrix.RenderOpsBlock(snapshot, catalog)},
		{"provider sync", migrationmatrix.ProviderSyncBegin, migrationmatrix.ProviderSyncEnd, legacy["provider"]},
		{"daily metrics", migrationmatrix.DailyMetricsBegin, migrationmatrix.DailyMetricsEnd, legacy["daily"]},
		{"remaining metrics", migrationmatrix.RemainingMetricsBegin, migrationmatrix.RemainingMetricsEnd, legacy["remaining"]},
		{"workgraph investment", migrationmatrix.WorkgraphInvestmentBegin, migrationmatrix.WorkgraphInvestmentEnd, legacy["workgraph"]},
		{"REST endpoints", migrationmatrix.RESTEndpointsBegin, migrationmatrix.RESTEndpointsEnd, legacy["rest"]},
	}
	for _, block := range blocks {
		got, err := migrationmatrix.ExtractBlock(doc, block.begin, block.end)
		if err != nil {
			return fmt.Errorf("%s block: %w", block.name, err)
		}
		want := strings.Trim(block.wantRendered, "\n")
		if got != want {
			violations = append(violations, migrationmatrix.Violation{
				Subject: block.name + " block",
				Rule:    "R13-doc-drift",
				Detail: "the committed markdown does not match what the committed sources render; " +
					"run `go run ./cmd/dev-health-migration-matrix -render` (or -render -fleet none to keep the last fleet reading)",
			})
		}
	}

	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "%s has %d violation(s):\n%s",
			docRelative, len(violations), migrationmatrix.FormatViolations(violations))
		return fmt.Errorf("migration matrix contract failed")
	}
	fmt.Printf("migration matrix OK: %d families, %d routing rows, rendered %s\n",
		len(ledger.Families), len(snapshot.Operations), snapshot.RenderedAt.UTC().Format(time.RFC3339))
	return nil
}

func runRender(root, dsn, routingFile, fleetMode string, containers []string) error {
	ctx := context.Background()

	ledger, err := migrationmatrix.LoadStatusLedger(filepath.Join(root, statusRelative))
	if err != nil {
		return err
	}
	families, err := migrationmatrix.LoadNativeFamilies(filepath.Join(root, nativeRelative))
	if err != nil {
		return err
	}
	pin, err := migrationmatrix.SchemaDigestPin(filepath.Join(root, digestPinRelative))
	if err != nil {
		return err
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		return err
	}
	opsSha, renderCommit, err := renderShas(root)
	if err != nil {
		return err
	}

	previous, prevErr := migrationmatrix.LoadRender(filepath.Join(root, renderRelative))

	snapshot := &migrationmatrix.Render{
		SchemaVersion: 1,
		// Truncated to the second on purpose: the timestamp is re-parsed
		// by ci/check_migration_matrix.sh's freshness mode with `date`,
		// and BSD `date -j -f` cannot parse fractional seconds. A format
		// only one of the two supported hosts can read is a check that
		// silently stops running on the other.
		RenderedAt:   time.Now().UTC().Truncate(time.Second),
		OpsSha:       opsSha,
		RenderCommit: renderCommit,
		SchemaDigest: pin,
	}

	switch {
	case routingFile != "":
		rows, proofTotal, err := readRoutingFile(routingFile, pin)
		if err != nil {
			return err
		}
		snapshot.Operations = rows
		snapshot.ProofRunTotal = proofTotal
	case dsn != "":
		rows, proofTotal, err := migrationmatrix.ReadRoutingState(ctx, dsn, pin)
		if err != nil {
			return err
		}
		snapshot.Operations = rows
		snapshot.ProofRunTotal = proofTotal
	default:
		if prevErr != nil {
			return fmt.Errorf("no -routing file, no -dsn/$POSTGRES_URI and no previous render to carry forward: %w", prevErr)
		}
		// Carrying forward is allowed but never silent: the timestamp
		// stays the OLD one, so the freshness gate still fails on it.
		fmt.Fprintf(os.Stderr, "warning: no routing source; carrying forward the rows read at %s (their timestamp is NOT refreshed, so the freshness gate still ages them out)\n",
			previous.RenderedAt.UTC().Format(time.RFC3339))
		snapshot.Operations = previous.Operations
		snapshot.ProofRunTotal = previous.ProofRunTotal
		snapshot.RenderedAt = previous.RenderedAt
	}

	// Carry the parity tickets curated per operation forward across renders
	// -- they are a human's judgement, not a live fact, and must survive a
	// re-read of the database.
	if previous != nil {
		carried := map[string]string{}
		for _, row := range previous.Operations {
			if row.ParityTicket != "" {
				carried[row.SchemaDigest+"/"+row.Operation] = row.ParityTicket
			}
		}
		for i := range snapshot.Operations {
			key := snapshot.Operations[i].SchemaDigest + "/" + snapshot.Operations[i].Operation
			if ticket, ok := carried[key]; ok && snapshot.Operations[i].ParityTicket == "" {
				snapshot.Operations[i].ParityTicket = ticket
			}
		}
	}

	// queryAPIBuild is query-api's OWN fleet-label revision (never the
	// cross-container Revision field) -- see queryAPIContainerName's own
	// doc comment for why REST proof is checked against it specifically.
	// Empty unless the docker fleet read actually names this container
	// with a KNOWN (non-UnknownRevision) label, which is exactly the
	// condition under which a REST receipt could be usefully checked.
	var queryAPIBuild string

	switch {
	case fleetMode == "none":
		if previous != nil {
			snapshot.FleetReadAt = previous.FleetReadAt
			snapshot.FleetSource = previous.FleetSource
		} else {
			snapshot.FleetSource = "not read"
		}
	case fleetMode == "docker":
		reading, err := migrationmatrix.ReadFleetRevisions(ctx, containers)
		if err != nil {
			return fmt.Errorf("read fleet: %w", err)
		}
		snapshot.FleetReadAt = reading.ReadAt
		snapshot.FleetSource = reading.Source
		if reading.Disagreement != "" {
			fmt.Fprintf(os.Stderr, "warning: %s\n", reading.Disagreement)
		}
		applyFleet(ledger, reading)
		if label := reading.PerContainer[queryAPIContainerName]; label != "" && label != migrationmatrix.UnknownRevision {
			queryAPIBuild = label
		}
	default:
		reading, err := fleetFromFile(fleetMode)
		if err != nil {
			return err
		}
		snapshot.FleetReadAt = reading.ReadAt
		snapshot.FleetSource = reading.Source
		applyFleet(ledger, reading)
		if label := reading.PerContainer[queryAPIContainerName]; label != "" && label != migrationmatrix.UnknownRevision {
			queryAPIBuild = label
		}
	}

	// REST "proven": read ONLY when both a live DSN and a known query-api
	// build are available -- either missing means Render.RESTProven stays
	// nil, and ApplyRESTProof(rows, nil) promotes nothing (see its own doc
	// comment), so a render with no usable source for this column renders
	// every REST row exactly as LoadRESTEndpoints found it, same as before
	// this column existed.
	var restProven map[string]string
	if dsn != "" && queryAPIBuild != "" {
		restProven, err = migrationmatrix.ReadRESTProof(ctx, dsn, queryAPIBuild)
		if err != nil {
			return fmt.Errorf("read REST proof: %w", err)
		}
	}
	snapshot.RESTProven = restProven

	if err := writeJSON(filepath.Join(root, renderRelative), snapshot); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, statusRelative), ledger); err != nil {
		return err
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath) //nolint:gosec // repo-relative path
	if err != nil {
		return fmt.Errorf("read %s: %w", docRelative, err)
	}
	doc := string(raw)
	doc, err = migrationmatrix.ReplaceBlock(doc, migrationmatrix.FamilyBlockBegin, migrationmatrix.FamilyBlockEnd,
		migrationmatrix.RenderFamilyBlock(ledger, families))
	if err != nil {
		return fmt.Errorf("family block: %w", err)
	}
	doc, err = migrationmatrix.ReplaceBlock(doc, migrationmatrix.OpsBlockBegin, migrationmatrix.OpsBlockEnd,
		migrationmatrix.RenderOpsBlock(snapshot, catalog))
	if err != nil {
		return fmt.Errorf("go-api operations block: %w", err)
	}

	legacy, err := legacyBlocks(root, families, restProven)
	if err != nil {
		return err
	}
	legacyReplacements := []struct{ begin, end, name, body string }{
		{migrationmatrix.ProviderSyncBegin, migrationmatrix.ProviderSyncEnd, "provider sync", legacy["provider"]},
		{migrationmatrix.DailyMetricsBegin, migrationmatrix.DailyMetricsEnd, "daily metrics", legacy["daily"]},
		{migrationmatrix.RemainingMetricsBegin, migrationmatrix.RemainingMetricsEnd, "remaining metrics", legacy["remaining"]},
		{migrationmatrix.WorkgraphInvestmentBegin, migrationmatrix.WorkgraphInvestmentEnd, "workgraph investment", legacy["workgraph"]},
		{migrationmatrix.RESTEndpointsBegin, migrationmatrix.RESTEndpointsEnd, "REST endpoints", legacy["rest"]},
	}
	for _, r := range legacyReplacements {
		doc, err = migrationmatrix.ReplaceBlock(doc, r.begin, r.end, r.body)
		if err != nil {
			return fmt.Errorf("%s block: %w", r.name, err)
		}
	}

	if err := os.WriteFile(docPath, []byte(doc), 0o644); err != nil { //nolint:gosec // documentation file
		return fmt.Errorf("write %s: %w", docRelative, err)
	}

	violations := migrationmatrix.ValidateLedger(ledger, families)
	violations = append(violations, migrationmatrix.ValidateRender(snapshot)...)
	violations = append(violations, migrationmatrix.ValidateDocumentDrift(snapshot, catalog)...)
	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "rendered, but the result has %d violation(s):\n%s",
			len(violations), migrationmatrix.FormatViolations(violations))
		return fmt.Errorf("migration matrix contract failed after render")
	}
	fmt.Printf("rendered %s at %s (main merge-base %s, render commit %s, %d routing rows, %d proof runs)\n",
		docRelative, snapshot.RenderedAt.UTC().Format(time.RFC3339), opsSha[:12], renderCommit[:12],
		len(snapshot.Operations), snapshot.ProofRunTotal)
	return nil
}

// applyFleet writes the ONE reading into every family's deployed cell. Every
// family runs in the same fleet, so a per-family deployed sha would be a
// fiction; what varies per family is parity and proof, not the build.
func applyFleet(ledger *migrationmatrix.StatusLedger, reading *migrationmatrix.FleetReading) {
	for id, entry := range ledger.Families {
		entry.Deployed = migrationmatrix.Deployed{
			Revision: reading.Revision,
			ReadAt:   reading.ReadAt,
			Source:   reading.Source,
		}
		ledger.Families[id] = entry
	}
}

// printRoutingSQL writes the statement -print-routing-sql prints: exactly
// migrationmatrix.RoutingStateSQL(), the statement ReadRoutingState runs.
//
// The offline pipeline, which the lane's executed claim runs literally on
// PostgreSQL 16, 17 and 18:
//
//	dev-health-migration-matrix -print-routing-sql > routing.sql
//	docker exec -i <pg> psql -U <user> -d <db> -At -f - < routing.sql > routing.json
//	dev-health-migration-matrix -render -routing routing.json
//
// The statement emits the whole snapshot payload as one JSON value, so the
// file psql writes IS the -routing file, unedited: an earlier version
// printed only the row statement, whose `psql -At` output the reader
// rejected, leaving the JSON wrapper and a second count query to be
// written by hand. Credentials stay with psql -- its own environment or
// ~/.pgpass -- never in this tool's argv or the docs generator's code.
func printRoutingSQL(w io.Writer) error {
	statement, err := migrationmatrix.RoutingStateSQL()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, strings.TrimSpace(statement))
	return err
}

// readRoutingFile reads a -routing snapshot: the unedited `psql -At` output
// of -print-routing-sql. Parsing is migrationmatrix.ParseRoutingSnapshot --
// the same function ReadRoutingState uses on the same statement's value --
// so an offline snapshot cannot be read by a different rule than a live one.
func readRoutingFile(path, currentDigest string) ([]migrationmatrix.OperationRow, int, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, 0, fmt.Errorf("read routing file: %w", err)
	}
	rows, total, err := migrationmatrix.ParseRoutingSnapshot(raw, currentDigest)
	if err != nil {
		return nil, 0, fmt.Errorf("routing file %s: %w", path, err)
	}
	if len(rows) == 0 {
		return nil, 0, fmt.Errorf("routing file %s has no rows; an EMPTY go_api_routing_state is itself a finding, "+
			"but it must be recorded deliberately rather than read as a parse failure", path)
	}
	return rows, total, nil
}

func fleetFromFile(path string) (*migrationmatrix.FleetReading, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("read fleet file: %w", err)
	}
	var perContainer map[string]string
	if err := json.Unmarshal(raw, &perContainer); err != nil {
		return nil, fmt.Errorf("parse fleet file %s: %w", path, err)
	}
	if len(perContainer) == 0 {
		return nil, fmt.Errorf("fleet file %s names no containers", path)
	}
	distinct := map[string]bool{}
	for _, revision := range perContainer {
		distinct[revision] = true
	}
	reading := &migrationmatrix.FleetReading{
		PerContainer: perContainer,
		ReadAt:       time.Now().UTC(),
		Source:       "fleet file " + filepath.Base(path),
		Revision:     migrationmatrix.UnknownRevision,
	}
	if len(distinct) == 1 {
		for revision := range distinct {
			reading.Revision = revision
		}
	} else {
		reading.Disagreement = fmt.Sprintf("the fleet file names %d distinct revisions", len(distinct))
	}
	return reading, nil
}

func writeJSON(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o644); err != nil { //nolint:gosec // contract file
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// renderShas returns (merge-base with main, HEAD).
//
// The FIRST is what the freshness gate checks for ancestry, and it must be a
// commit that is already on main -- because a squash merge replaces the
// branch's own commits with a single new one, leaving the originals
// unreachable from main forever. A render that recorded its branch tip took
// main red for every subsequent PR the moment #2389 landed: the check
// correctly reported "not an ancestor" about a commit that could not
// possibly be one.
//
// The merge-base is the right anchor because it is the last main commit the
// render actually observed. It survives the squash, and it still bounds
// staleness: a render made from a branch that forked weeks ago carries a
// correspondingly old merge-base and ages out on schedule.
func renderShas(root string) (string, string, error) {
	head, err := gitOutput(root, "rev-parse", "HEAD")
	if err != nil {
		return "", "", err
	}

	// On main itself the merge-base IS HEAD, so this yields the same value;
	// on a branch it is the fork point. Both refs are tried because a clone
	// may carry a local `main` with no `origin/` remote-tracking ref.
	for _, ref := range []string{"origin/main", "main"} {
		if _, err := gitOutput(root, "rev-parse", "--verify", "--quiet", ref+"^{commit}"); err != nil {
			continue
		}
		base, err := gitOutput(root, "merge-base", ref, "HEAD")
		if err != nil {
			continue
		}
		return base, head, nil
	}

	// No main to fork from (a detached checkout of an unrelated tree, or a
	// fresh repo with no main yet). Fall back to HEAD and SAY SO -- a wrong
	// value written silently is exactly what caused the incident this
	// function exists to prevent.
	fmt.Fprintln(os.Stderr,
		"warning: neither origin/main nor main resolves here, so ops_sha falls back to HEAD.\n"+
			"         If this render is on a branch that will be SQUASH-merged, the freshness\n"+
			"         gate will fail on main afterwards -- re-render from a checkout that can\n"+
			"         see main.")
	return head, head, nil
}

// checkOpsShaAncestry re-derives the tree -check is actually running
// against and applies migrationmatrix.CheckOpsShaAncestry to the committed
// ops_sha. It shares renderShas' own git plumbing so the "a render right
// now would write" value in a failure is computed the exact same way
// -render itself would -- never a second, possibly-diverging lookup of the
// same thing.
//
// Two ways a git answer can be untrustworthy are told apart, not folded
// into one "warn and continue" branch:
//
//   - No git repository at all is a legitimate skip -- someone running the
//     tool against an exported tree with no .git has genuinely given it
//     nothing to check ancestry against, and every real -check target (in
//     CI or on a contributor's machine) is a checkout, so this path is not
//     the gate's own environment.
//   - A real git repository that cannot resolve HEAD, or a shallow clone
//     that answers "not an ancestor", is the gate's own environment
//     behaving abnormally, and that is refused rather than silently
//     skipped or silently trusted. A shallow clone in particular can give a
//     confidently WRONG negative: `.git/shallow` records a boundary commit
//     as having no parents at all, independent of whether the real history
//     is still present as objects, so a commit that genuinely IS an
//     ancestor reads as "not an ancestor" the moment anything on the path
//     between them was ever fetched shallow -- observed directly: a later,
//     unrelated `git fetch --depth=1` against an already-fully-fetched
//     commit still re-shallowed the checkout and flipped a true ancestor to
//     a false negative for every git command after it in the same job.
func checkOpsShaAncestry(root, opsSha string) ([]migrationmatrix.Violation, error) {
	if _, err := gitOutput(root, "rev-parse", "--git-dir"); err != nil {
		fmt.Fprintf(os.Stderr,
			"warning: %s has no git repository at all; ops_sha ancestry was NOT checked.\n", root)
		return nil, nil
	}

	head, err := gitOutput(root, "rev-parse", "HEAD")
	if err != nil {
		return []migrationmatrix.Violation{{
			Subject: "ops_sha",
			Rule:    "R9-ops-sha-unverifiable",
			Detail: fmt.Sprintf(
				"ops_sha's ancestry could not be checked: this is a git repository, but HEAD does not "+
					"resolve (%v). A real checkout with no usable HEAD is exactly the environment this rule "+
					"protects, so this is refused rather than silently skipped -- a detached, unborn, or "+
					"corrupt ref is the usual cause; fix the checkout and re-run.", err),
		}}, nil
	}

	// Best-effort only: renderShas' own fallback (an unresolvable
	// origin/main or main) is not fatal here -- it only weakens the
	// "a render right now would write" value in a failure message, never
	// the ancestry verdict itself, which is decided below from opsSha and
	// head alone.
	currentBase, _, err := renderShas(root)
	if err != nil {
		currentBase = "(could not be computed: " + err.Error() + ")"
	}

	if _, err := gitOutput(root, "cat-file", "-e", opsSha+"^{commit}"); err != nil {
		return []migrationmatrix.Violation{{
			Subject: "ops_sha",
			Rule:    "R9-ops-sha-unknown",
			Detail: fmt.Sprintf(
				"ops_sha %s does not resolve to a commit in this checkout (a shallow clone with no "+
					"fetch-depth: 0 is the usual CI cause; a render carried forward from an unrelated "+
					"history is the other). A render right now would write ops_sha %s. "+
					"Re-render: go run ./cmd/dev-health-migration-matrix -render -root .",
				opsSha, currentBase),
		}}, nil
	}

	isAncestor, err := gitIsAncestor(root, opsSha, head)
	if err != nil {
		return nil, fmt.Errorf("check ops_sha ancestry: %w", err)
	}

	if !isAncestor {
		if shallow, shallowErr := gitOutput(root, "rev-parse", "--is-shallow-repository"); shallowErr == nil && shallow == "true" {
			return []migrationmatrix.Violation{{
				Subject: "ops_sha",
				Rule:    "R9-ops-sha-unverifiable-shallow",
				Detail: fmt.Sprintf(
					"ops_sha %s's ancestry of HEAD %s could not be reliably determined: this checkout is a "+
						"shallow clone, and a negative merge-base result from one is not trustworthy -- git "+
						"records a shallow boundary as \"no parents beyond here\" even when the real history "+
						"is still present as objects, so a true ancestor can read as false the moment anything "+
						"on the path between them was ever fetched shallow. This is refused rather than either "+
						"accepted as a real violation or silently skipped. Fetch full history (fetch-depth: 0, "+
						"and no later step in the same checkout may re-shallow it) and re-run.",
					opsSha, head),
			}}, nil
		}
	}

	return migrationmatrix.CheckOpsShaAncestry(opsSha, head, currentBase, isAncestor), nil
}

// gitIsAncestor reports whether ancestor is an ancestor of (or equal to)
// descendant. `git merge-base --is-ancestor` documents exit code 1 as the
// real "no" -- distinguished here from every other failure (an unknown ref,
// a corrupt repo, ...), which is returned as an error instead of folding
// into a false "no" that would read as a clean, ordinary staleness.
func gitIsAncestor(root, ancestor, descendant string) (bool, error) {
	cmd := exec.Command("git", "-C", root, "merge-base", "--is-ancestor", ancestor, descendant)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return false, nil
		}
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return false, fmt.Errorf("git merge-base --is-ancestor %s %s: %w: %s", ancestor, descendant, err, msg)
		}
		return false, fmt.Errorf("git merge-base --is-ancestor %s %s: %w", ancestor, descendant, err)
	}
	return true, nil
}

func gitOutput(root string, args ...string) (string, error) {
	full := append([]string{"-C", root}, args...)
	out, err := exec.Command("git", full...).Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w%s", strings.Join(args, " "), err, gitStderr(err))
	}
	return strings.TrimSpace(string(out)), nil
}

// gitStderr appends a command's captured stderr to an error message, when
// there is one to show. (*exec.ExitError).Error() is just "exit status N" --
// the diagnostic text git actually printed (e.g. "detected dubious
// ownership", "not a git repository") lives in the Stderr field instead,
// and dropping it is exactly what turned a two-minute diagnosis into a
// raw-CI-log excavation the first time this mattered.
func gitStderr(err error) string {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if msg := strings.TrimSpace(string(exitErr.Stderr)); msg != "" {
			return ": " + msg
		}
	}
	return ""
}

func splitCSV(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
