// Command go-api-prove is CHAOS-5425's `prove` verb: it executes every
// registered Go-API operation against the DEPLOYED query-api through the
// real edge, compares both planes' complete observable responses under
// CHAOS-4381's parity rules, and records an immutable
// `deployed_executed` receipt per operation in `go_api_proof_run`.
//
// Why this is a standalone Go binary and not a `dev-hops go-api routing
// prove` subcommand beside enable/disable/status (team-lead ruling R49,
// 2026-09-09): the Go cutover's standing rule is that no new Python
// compute is written (chris: "move to go, do not straddle"), and a Python
// shim that shells out to this binary would be new Python on the critical
// path of a proof. The existing Python verbs stay exactly as they are.
//
// It ships as a binary run by hand (`go run ./cmd/go-api-prove`, or a
// built binary) against a deployment; this change adds no image and no
// compose service.
//
// The document digest comes from internal/goapidigest -- the same
// function query_route.go and registrydump reach through
// cmd/query-api/internal/digest, which forwards there. Re-implementing it
// here would be exactly the two-copies drift CHAOS-4696 closed.
//
// What this command will NOT do:
//
//   - It never writes or mutates a go_api_routing_state row. Enablement is
//     a rollout decision; this only records evidence a rollout decision can
//     later read.
//   - It never names a candidate build from a flag. The build identity
//     comes from the RUNNING process (GET /buildinfo) or the run refuses --
//     a receipt built from a hand-typed sha proves that somebody typed a
//     sha (see ErrNoBuildIdentity). --candidate-build is a cross-check
//     only: it can FAIL a run, never supply the value written.
//   - It never executes a shadow-mode operation through the product edge.
//     Those go to /query/proof, the measurement-only route, and their
//     receipts record measurement_route='proof'.
//   - It never prints a credential. The bearer token is read from the
//     environment by NAME and is never echoed, logged, or folded into a
//     request identity.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// bearerEnvVar names the environment variable carrying the edge bearer
// token. The VALUE never appears in output; only this NAME does.
const bearerEnvVar = "GO_API_PROVE_BEARER"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "go-api-prove: %v\n", err)
		os.Exit(1)
	}
}

type flags struct {
	registryURL    string
	buildInfoURL   string
	edgeURL        string
	candidateBuild string
	proofURL       string
	documentsPath  string
	postgresURI    string
	orgID          string
	artifactDir    string
	recordedBy     string
	reviewEvidence string
	principalKind  string
	audience       string
	keyID          string
	dryRun         bool
	timeout        time.Duration
	window         goapiproof.Window
	reportPath     string
}

func parseFlags() (flags, error) {
	defaults := goapiproof.DefaultWindow()
	var f flags
	flag.StringVar(&f.registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api -- the only authority on which operations and document digests it serves")
	flag.StringVar(&f.buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build identity every receipt names")
	flag.StringVar(&f.candidateBuild, "candidate-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written (team-lead ruling R51)")
	flag.StringVar(&f.edgeURL, "edge-url", "http://localhost:8000/graphql", "the real product GraphQL edge; both the canary/primary candidate leg and every baseline leg go through it")
	flag.StringVar(&f.proofURL, "proof-url", "", "measurement-only route able to execute a SHADOW-mode operation on the deployed Go build; empty means shadow operations are refused by name rather than skipped")
	flag.StringVar(&f.documentsPath, "documents", "", "path to `registrydump -file cmd/query-api/query_route.go` JSON output (required)")
	flag.StringVar(&f.postgresURI, "postgres-uri", os.Getenv("POSTGRES_URI"), "domain Postgres DSN holding go_api_routing_state / go_api_proof_run")
	flag.StringVar(&f.orgID, "org", "", "org id every request is made for (required)")
	flag.StringVar(&f.artifactDir, "artifact-dir", "", "directory for content-addressed response bodies; receipts store a reference, never an inlined body (required)")
	flag.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on every receipt (required)")
	flag.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every receipt (required)")
	flag.StringVar(&f.principalKind, "principal-kind", "stored_account", "auth-context SHAPE recorded in request_identity; never a credential")
	flag.StringVar(&f.audience, "audience", "query-api", "envelope audience, part of the auth-context shape")
	flag.StringVar(&f.keyID, "key-id", "", "envelope signing key id (kid) -- a public identifier, and the value that silently broke routing three times on 2026-09-07")
	flag.BoolVar(&f.dryRun, "dry-run", false, "execute and compare, but write NO receipts")
	flag.DurationVar(&f.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.StringVar(&f.reportPath, "report", "", "write the full JSON report here in addition to stdout")
	flag.StringVar(&f.window.SinceUTC, "since-utc", defaults.SinceUTC, "request window start (RFC3339)")
	flag.StringVar(&f.window.UntilUTC, "until-utc", defaults.UntilUTC, "request window end (RFC3339)")
	flag.StringVar(&f.window.SinceDate, "since-date", defaults.SinceDate, "request window start (YYYY-MM-DD)")
	flag.StringVar(&f.window.UntilDate, "until-date", defaults.UntilDate, "request window end (YYYY-MM-DD)")
	flag.StringVar(&f.window.WeekStart, "week-start", defaults.WeekStart, "operatingReview week start (YYYY-MM-DD)")
	flag.Parse()

	missing := map[string]string{
		"-documents": f.documentsPath, "-org": f.orgID, "-artifact-dir": f.artifactDir,
		"-recorded-by": f.recordedBy, "-review-evidence": f.reviewEvidence,
	}
	if !f.dryRun {
		missing["-postgres-uri (or POSTGRES_URI)"] = f.postgresURI
	}
	var absent []string
	for name, value := range missing {
		if value == "" {
			absent = append(absent, name)
		}
	}
	sort.Strings(absent)
	if len(absent) > 0 {
		return f, fmt.Errorf("required flags are missing: %v", absent)
	}
	return f, f.window.Validate()
}

func run() error {
	f, err := parseFlags()
	if err != nil {
		return err
	}

	bearer := os.Getenv(bearerEnvVar)
	if bearer == "" {
		return fmt.Errorf("no bearer token: set %s (the VALUE is never printed by this command)", bearerEnvVar)
	}

	ctx := context.Background()
	client := &http.Client{Timeout: f.timeout}

	authHeaders := map[string]string{"Authorization": "Bearer " + bearer}

	registry, err := goapiproof.FetchRegistry(ctx, client, f.registryURL)
	if err != nil {
		return err
	}

	// The build identity is fetched BEFORE anything is measured, and a
	// failure here stops the run rather than degrading into an
	// operator-supplied name: the receipt is the whole deliverable, and
	// one naming an unverifiable build is worse than none (CHAOS-5425
	// acceptance, 2026-09-08: "Do not construct a receipt from a digest or
	// an arbitrary build name").
	registry.BuildIdentity, err = goapiproof.FetchBuildIdentity(ctx, client, f.buildInfoURL, authHeaders)
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return fmt.Errorf("%w\n  the deployed query-api must identify its build at %s before any receipt can be written", err, f.buildInfoURL)
		}
		return err
	}

	documents, err := goapiproof.LoadDocuments(f.documentsPath)
	if err != nil {
		return err
	}
	if err := goapiproof.VerifyDocuments(documents, registry, goapidigest.Document); err != nil {
		return err
	}

	registered := make([]string, 0, len(registry.DocumentDigest))
	for operation := range registry.DocumentDigest {
		registered = append(registered, operation)
	}
	if err := goapiproof.AssertCoverage(registered); err != nil {
		return err
	}

	artifacts, err := goapiproof.NewArtifactStore(f.artifactDir)
	if err != nil {
		return err
	}

	var pool *pgxpool.Pool
	if !f.dryRun {
		pool, err = pgxpool.New(ctx, f.postgresURI)
		if err != nil {
			return fmt.Errorf("connect to Postgres: %w", err)
		}
		defer pool.Close()
	}

	routing, err := readRoutingState(ctx, pool, registry)
	if err != nil {
		return err
	}
	if err := goapiproof.VerifyCandidateBuild(registry.BuildIdentity, f.candidateBuild, routing); err != nil {
		return err
	}

	runner := &goapiproof.Runner{
		Client:    client,
		Documents: documents,
		Registry:  registry,
		Routing:   routing,
		Artifacts: artifacts,
		Config: goapiproof.Config{
			OrgID:         f.orgID,
			Window:        f.window,
			PythonEdgeURL: f.edgeURL,
			GoProofURL:    f.proofURL,
			Headers:       authHeaders,
			Auth: goapiproof.AuthContext{
				PrincipalKind: f.principalKind,
				Audience:      f.audience,
				KeyID:         f.keyID,
			},
			RecordedBy:     f.recordedBy,
			ReviewEvidence: f.reviewEvidence,
			Timeout:        f.timeout,
		},
	}

	var db goapiproof.Querier
	if pool != nil {
		db = pool
	}
	outcomes, summary, runErr := runner.Run(ctx, db)

	// The report is written and printed BEFORE the run error is returned:
	// a failed run's evidence is exactly what an operator needs, and a
	// command that swallows its own output on failure is the "report the
	// problem and return" trap D15/R4 names.
	if err := emitReport(f, registry, outcomes, summary); err != nil {
		return err
	}
	return runErr
}

// readRoutingState reads each registered operation's current mode from
// go_api_routing_state at the LIVE schema digest.
//
// Read at the live digest only, never "the newest row for this
// operation": rows keyed to a superseded digest are dead by construction
// (PostgresSwitch looks up by the digest the running binary computes),
// and treating one as current is precisely the six-day outage CHAOS-5416
// records.
func readRoutingState(ctx context.Context, pool *pgxpool.Pool, registry goapiproof.RegistryView) (map[string]goapiproof.RoutingRow, error) {
	routing := map[string]goapiproof.RoutingRow{}
	if pool == nil {
		// --dry-run with no database: every operation is reported as
		// unrouted and refused BY NAME. It never silently assumes canary.
		return routing, nil
	}

	rows, err := pool.Query(ctx,
		`SELECT selected_operation, document_digest, mode, current_candidate_build
		   FROM go_api_routing_state
		  WHERE schema_digest = $1`,
		registry.SchemaDigest)
	if err != nil {
		return nil, fmt.Errorf("read go_api_routing_state: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var operation, documentDigest, mode, candidateBuild string
		if err := rows.Scan(&operation, &documentDigest, &mode, &candidateBuild); err != nil {
			return nil, fmt.Errorf("scan go_api_routing_state row: %w", err)
		}
		// A row whose document digest is not the one the running process
		// registers for that operation is dead the same way a stale schema
		// digest is dead -- do not adopt its mode.
		if registry.DocumentDigest[operation] != documentDigest {
			continue
		}
		routing[operation] = goapiproof.RoutingRow{Mode: mode, CandidateBuild: candidateBuild}
	}
	return routing, rows.Err()
}

type report struct {
	SchemaDigest   string               `json:"schema_digest"`
	CandidateBuild string               `json:"candidate_build"`
	Stage          string               `json:"stage"`
	OrgID          string               `json:"org_id"`
	Window         goapiproof.Window    `json:"window"`
	Summary        goapiproof.Summary   `json:"summary"`
	Outcomes       []goapiproof.Outcome `json:"outcomes"`
}

// emitReport prints the explicit-zero telemetry block and the full JSON.
//
// Every counter is printed on every run, including the zeros. "prove
// measured nothing" and "prove measured everything and found nothing
// wrong" are different facts, and the shape of the output must never let
// them look alike.
func emitReport(f flags, registry goapiproof.RegistryView, outcomes []goapiproof.Outcome, summary goapiproof.Summary) error {
	fmt.Printf("go-api-prove: schema_digest=%s candidate_build=%s stage=%s org=%s\n",
		registry.SchemaDigest, registry.BuildIdentity, goapiproof.Stage, f.orgID)
	fmt.Printf("go-api-prove: attempted=%d executed=%d refused=%d receipts_written=%d\n",
		summary.Attempted, summary.Executed, summary.Refused, summary.ReceiptsWritten)
	proofURL := f.proofURL
	if proofURL == "" {
		proofURL = "(none: shadow-mode operations cannot be measured in this deployment)"
	}
	fmt.Printf("go-api-prove: edge=%s proof_route=%s\n", f.edgeURL, proofURL)
	for _, state := range sortedKeys(summary.ByTerminalState) {
		fmt.Printf("go-api-prove:   terminal_state %s = %d\n", state, summary.ByTerminalState[state])
	}
	for _, reason := range sortedKeys(summary.ByRefusalReason) {
		fmt.Printf("go-api-prove:   refused %s = %d\n", reason, summary.ByRefusalReason[reason])
	}
	for _, outcome := range outcomes {
		if outcome.Executed {
			fmt.Printf("go-api-prove:   %-22s mode=%-8s route=%-5s %s (%d findings, %d outside a declared baseline defect %v)\n",
				outcome.Operation, outcome.Mode, outcome.Route, outcome.TerminalState,
				len(outcome.Findings), outcome.DifferencesOutsideBaselineDefect, outcome.BaselineDefects)
			continue
		}
		fmt.Printf("go-api-prove:   %-22s mode=%-8s REFUSED %s: %s\n",
			outcome.Operation, outcome.Mode, outcome.RefusalReason, outcome.RefusalDetail)
	}

	if f.reportPath == "" {
		return nil
	}
	encoded, err := json.MarshalIndent(report{
		SchemaDigest:   registry.SchemaDigest,
		CandidateBuild: registry.BuildIdentity,
		Stage:          goapiproof.Stage,
		OrgID:          f.orgID,
		Window:         f.window,
		Summary:        summary,
		Outcomes:       outcomes,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(f.reportPath, encoded, 0o640); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	fmt.Printf("go-api-prove: report written to %s\n", f.reportPath)
	return nil
}

func sortedKeys(counts map[string]int) []string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
