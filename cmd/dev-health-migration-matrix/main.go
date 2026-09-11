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
	"context"
	"encoding/json"
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

	// catalogRelative is the registered-operation catalog the edge
	// dispatches by -- the file `dev-hops go-api routing status` reports
	// DOCUMENT_DRIFT against. Read by -check as well as -render, so a row
	// the edge cannot dispatch fails the committed page (R14) rather than
	// rendering as served (opus r6, P2-2). Trap #98: it is a non-Go input;
	// go.yml's `src/dev_health_ops/api/**` path filter already covers it.
	catalogRelative = "src/dev_health_ops/api/graphql/go_api_operations.json"
)

// defaultFleetContainers is the compose fleet whose image labels answer
// "what commit is running?". One per distinct image, not one per replica.
var defaultFleetContainers = []string{
	"dev-health-go-worker-1",
	"dev-health-go-worker-heavy-1",
	"dev-health-go-worker-ops-1",
	"dev-health-go-scheduler-1",
	"dev-health-go-reconciler-1",
	"dev-health-query-api-1",
	"dev-health-api-1",
}

func main() {
	var (
		root       = flag.String("root", ".", "repository root")
		render     = flag.Bool("render", false, "read live sources and rewrite the generated blocks")
		check      = flag.Bool("check", false, "validate the committed doc against the committed sources")
		dsn        = flag.String("dsn", os.Getenv("POSTGRES_URI"), "Postgres DSN for the go_api_registry tables (default $POSTGRES_URI)")
		fleet      = flag.String("fleet", "docker", `how to read the fleet: "docker", "none", or a path to a JSON {container: revision} file`)
		routing    = flag.String("routing", "", "path to a JSON routing snapshot, INSTEAD of -dsn (for hosts where the operator has no DSN; build it with -print-routing-sql)")
		containers = flag.String("containers", strings.Join(defaultFleetContainers, ","), "comma-separated container names to inspect")
		printSQL   = flag.Bool("print-routing-sql", false, "print the statement ReadRoutingState runs and exit; `psql -At -f -` on it writes a -routing snapshot file as-is")
	)
	flag.Parse()

	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	notice, err := checkFlagCombination(explicit, *printSQL, *render, *check, *fleet, os.Getenv("POSTGRES_URI") != "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", err)
		os.Exit(2)
	}
	if notice != "" {
		fmt.Fprintf(os.Stderr, "notice: %s\n", notice)
	}

	// Printing the statement is not a render or a check, so it is answered
	// before the -render/-check exclusivity rule: an operator asking how to
	// produce a routing snapshot has neither yet.
	if *printSQL {
		if err := printRoutingSQL(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if *render == *check {
		fmt.Fprintln(os.Stderr, "exactly one of -render or -check is required")
		os.Exit(2)
	}

	if *render {
		err = runRender(*root, *dsn, *routing, *fleet, splitCSV(*containers))
	} else {
		err = runCheck(*root)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "dev-health-migration-matrix: %v\n", err)
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
// scripts/gen_go_migration_matrix_docs.py, from committed sources only.
func legacyBlocks(root string, families *migrationmatrix.NativeFamilies) (map[string]string, error) {
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
	return map[string]string{
		"provider":  providerBlock,
		"daily":     dailyBlock,
		"remaining": remainingBlock,
		"workgraph": workgraphBlock,
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
	if unjudged := migrationmatrix.UnjudgedLive(snapshot.Operations); unjudged > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d live routing row(s) in the committed render carry no document digest -- the snapshot predates the reader carrying the routing key -- so DOCUMENT_DRIFT cannot be judged for them until the next -render. The page states the count.\n", unjudged)
	}

	// The digest pin is checked against the snapshot rather than the
	// database: if the SDL moved after the last render, every "live" row in
	// the committed page is describing rows that can no longer be matched.
	// That is precisely the 2026-09-01 failure, and it is a doc-level fact
	// this offline check can catch.
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

	legacy, err := legacyBlocks(root, families)
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
	default:
		reading, err := fleetFromFile(fleetMode)
		if err != nil {
			return err
		}
		snapshot.FleetReadAt = reading.ReadAt
		snapshot.FleetSource = reading.Source
		applyFleet(ledger, reading)
	}

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

	legacy, err := legacyBlocks(root, families)
	if err != nil {
		return err
	}
	legacyReplacements := []struct{ begin, end, name, body string }{
		{migrationmatrix.ProviderSyncBegin, migrationmatrix.ProviderSyncEnd, "provider sync", legacy["provider"]},
		{migrationmatrix.DailyMetricsBegin, migrationmatrix.DailyMetricsEnd, "daily metrics", legacy["daily"]},
		{migrationmatrix.RemainingMetricsBegin, migrationmatrix.RemainingMetricsEnd, "remaining metrics", legacy["remaining"]},
		{migrationmatrix.WorkgraphInvestmentBegin, migrationmatrix.WorkgraphInvestmentEnd, "workgraph investment", legacy["workgraph"]},
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
// file psql writes IS the -routing file, unedited (opus r6, P3-3: the r5
// version printed only the row statement, whose `psql -At` output the
// reader rejected, leaving the JSON wrapper and a second count query to be
// written by hand). Credentials stay with psql -- its own environment or
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

func gitOutput(root string, args ...string) (string, error) {
	full := append([]string{"-C", root}, args...)
	out, err := exec.Command("git", full...).Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
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
