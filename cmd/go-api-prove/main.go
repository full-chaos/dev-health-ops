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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// The two credential environment variables. VALUES never appear in
// output; only these NAMES do.
//
// There are two because the two planes accept different credential KINDS,
// measured on the deployed stack (JOB 4, 2026-09-09): an access token gets
// HTTP 200 on the Python edge and 401 on /buildinfo; an effective-principal
// envelope gets the reverse. GO_API_PROVE_BEARER keeps its old name and
// its old meaning -- the EDGE token -- so an existing invocation does not
// silently change which plane it authenticates.
const (
	edgeBearerEnvVar  = "GO_API_PROVE_BEARER"
	proofBearerEnvVar = "GO_API_PROVE_PROOF_BEARER"
)

// proofCredentialFreshness is how long a minted envelope is reused before
// a fresh one is requested.
//
// ENVELOPE_DEFAULT_TTL_SECONDS is 60 (principal_envelope.py:96). 25
// seconds leaves 35 for the request to reach the server and be verified,
// so no request is ever sent carrying a value already near expiry -- which
// is how JOB 4's attempt B failed, at the CLOSING /buildinfo, after every
// measurement had already been taken.
const proofCredentialFreshness = 25 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "go-api-prove: %v\n", err)
		os.Exit(1)
	}
}

type flags struct {
	registryURL     string
	buildInfoURL    string
	edgeURL         string
	candidateBuild  string
	proofURL        string
	documentsPath   string
	postgresURI     string
	orgID           string
	artifactDir     string
	recordedBy      string
	reviewEvidence  string
	principalKind   string
	audience        string
	keyID           string
	proofBearerExec string
	dryRun          bool
	timeout         time.Duration
	window          goapiproof.Window
	reportPath      string
}

func parseFlags() (flags, error) {
	defaults := goapiproof.DefaultWindow()
	var f flags
	flag.StringVar(&f.registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api -- the only authority on which operations and document digests it serves")
	flag.StringVar(&f.buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build identity every receipt names")
	flag.StringVar(&f.candidateBuild, "candidate-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written (team-lead ruling R51)")
	flag.StringVar(&f.edgeURL, "edge-url", "http://localhost:8000/graphql", "the real product GraphQL edge; both the canary/primary candidate leg and every baseline leg go through it")
	flag.StringVar(&f.proofURL, "proof-url", "", "measurement-only route able to execute a SHADOW-mode operation on the deployed Go build; empty means shadow operations are refused by name rather than skipped")
	flag.StringVar(&f.proofBearerExec, "proof-bearer-exec", "", "JSON argv of a helper printing a FRESH effective-principal envelope on stdout, e.g. '[\"/opt/job5/mint-envelope.sh\",\"--org\",\"70d529e0\"]'. Re-run as the previous envelope ages out; required when the envelope's TTL is shorter than the run (it is: 60s). argv, not a shell string, so nothing is interpolated into a shell. NOTE: argv IS visible in the process table -- a secret passed as an ARGUMENT here is readable by any user on the box, so the helper must read its own credential rather than be handed one (CHAOS-5511 tracks passing it through an inherited file descriptor). The helper's stdout and stderr are NEVER reported by this command")
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

	// Refused before anything is measured: the note is stored inside a
	// JSON provenance object on EVERY receipt the run writes, so an
	// over-long one is a configuration error, not a late surprise.
	if err := goapiproof.ValidateOperatorEvidence(f.reviewEvidence); err != nil {
		return err
	}

	// Refused at parse time, before any request is built. A rebuilt label
	// keeps a credential out of this program's OWN messages; only refusal
	// keeps it off the command line, where the process table and every log
	// that records an invocation can already read it.
	if err := validateEndpointFlags(f); err != nil {
		return err
	}

	ctx := context.Background()
	client := &http.Client{Timeout: f.timeout}

	// The opening and closing /buildinfo reads get the SAME per-request
	// deadline the measured legs get (r1 P2). They used to take
	// context.Background(), so `-timeout` bounded every request except the
	// two that bracket the run -- including the minting helper invoked
	// before the first measurement, where a hang blocks the whole proof
	// with nothing to show for it.
	boundedCtx := func() (context.Context, context.CancelFunc) {
		if f.timeout <= 0 {
			return context.WithCancel(ctx)
		}
		return context.WithTimeout(ctx, f.timeout)
	}

	edgeCredential, proofCredential, err := credentials(f)
	if err != nil {
		return err
	}

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
	buildCtx, cancelBuild := boundedCtx()
	registry.BuildIdentity, err = goapiproof.FetchBuildIdentity(buildCtx, client, f.buildInfoURL, proofCredential)
	cancelBuild()
	if err != nil {
		if errors.Is(err, goapiproof.ErrNoBuildIdentity) {
			return fmt.Errorf("%w\n  the deployed query-api must identify its build at %s before any receipt can be written", err, goapiproof.EndpointLabel(f.buildInfoURL))
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

	routing, err := readRoutingState(ctx, pool, registry, f.candidateBuild)
	if err != nil {
		return err
	}
	// A routing row naming a build the running process is not is a
	// REFUSAL again (r1 P1). The demotion assumed /buildinfo identifies
	// the replica that served the MEASURED request; with multiple
	// query-api replicas and an edge that drops the per-request build
	// header, it does not. Re-pointing the rows after a deploy is an
	// operator step, and the refusal below says so.
	runner := &goapiproof.Runner{
		Client:    client,
		Documents: documents,
		Registry:  registry,
		Routing:   routing,
		Artifacts: artifacts,
		Config: goapiproof.Config{
			OrgID:           f.orgID,
			Window:          f.window,
			PythonEdgeURL:   f.edgeURL,
			GoProofURL:      f.proofURL,
			EdgeCredential:  edgeCredential,
			ProofCredential: proofCredential,
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

	outcomes, summary, runErr := runner.Run(ctx)

	// NOTHING has been written yet, and nothing will be until the build is
	// shown to have held for the whole run (round 2's F3). Receipts used to
	// commit as each operation finished, so a build that moved mid-run left
	// already-committed match receipts behind, enablement-eligible,
	// describing a build that was not serving for all of it.
	observedAt := time.Now().UTC()
	stableCtx, cancelStable := boundedCtx()
	stabilityErr := goapiproof.VerifyBuildStable(stableCtx, client, f.buildInfoURL, proofCredential, registry.BuildIdentity)
	cancelStable()

	var receipts []goapiproof.Receipt
	var receiptErr error
	switch {
	case stabilityErr != nil:
		// No match receipt may be written -- but writing nothing would make
		// the run invisible, indistinguishable from one that never ran.
		receipts, receiptErr = runner.RefusalReceipts(observedAt, stabilityErr.Error())
	default:
		receipts, receiptErr = runner.ReceiptsFor(observedAt)
	}
	if receiptErr != nil {
		return receiptErr
	}

	var writeErr error
	if !f.dryRun && pool != nil {
		written, err := goapiproof.WriteReceipts(ctx, pool, receipts)
		summary.ReceiptsWritten = len(written)
		for i := range outcomes {
			outcomes[i].ReceiptWritten = written[outcomes[i].Operation]
		}
		// Held, not returned: the report below is exactly the evidence
		// somebody needs to see when a write fails halfway, and returning
		// here would drop it -- "report the problem and return" is the trap
		// that loses the failure signal.
		writeErr = err
	}
	for _, err := range []error{stabilityErr, writeErr} {
		if err == nil {
			continue
		}
		if runErr == nil {
			runErr = err
		} else {
			runErr = fmt.Errorf("%w; additionally: %v", runErr, err)
		}
	}

	// The report is written and printed BEFORE the run error is returned:
	// a failed run's evidence is exactly what an operator needs, and a
	// command that swallows its own output on failure is the "report the
	// problem and return" trap D15/R4 names.
	if err := emitReport(f, registry, outcomes, summary, proofCredential); err != nil {
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
// readRoutingState reads the rows AND verifies them against the running
// build, returning both or neither.
//
// r8 killed the previous shape by replacing the CALLER's `return err` with
// `_ = err`: the check was a separate statement in run(), so it could be
// ignored, and nothing failed. Folding it in means a caller cannot obtain
// the rows without the check having run.
//
// It does NOT make the mutation unwritable -- an earlier version of this
// comment claimed that, and the opus round disproved it with the exact
// mutation the comment named: `if err := VerifyCandidateBuild(...); err
// != nil { _ = err }` compiles here just as well as it did one level up.
// What changed is that the guard now lives with the data it guards, and
// it is under test: TestReadRoutingStateRefusesAndFilters drives this
// function against a fake Querier.
//
// A comment asserting a guarantee the code does not have is worse than no
// comment, so this one now says what is true.
// routingRowSource is the narrow slice of pgx readRoutingState needs, so
// a test can drive it without a database. Extracted for exactly that
// reason: the function had zero tests, and both of its guards survived
// removal.
type routingRowSource interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// isNilSource reports whether the source is absent, including the
// typed-nil-in-an-interface case the --dry-run path produces.
func isNilSource(source routingRowSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return value.IsNil()
	}
	return false
}

func readRoutingState(ctx context.Context, pool routingRowSource, registry goapiproof.RegistryView, expectedBuild string) (map[string]goapiproof.RoutingRow, error) {
	routing := map[string]goapiproof.RoutingRow{}
	// A typed-nil *pgxpool.Pool in an interface is not == nil, and
	// reflect.IsNil panics on a non-pointer kind, so both are handled.
	if isNilSource(pool) {
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// The rows and the verdict on them come back together, or neither
	// does. See this function's doc comment.
	if err := goapiproof.VerifyCandidateBuild(registry.BuildIdentity, expectedBuild, routing); err != nil {
		return nil, err
	}
	return routing, nil
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
func emitReport(f flags, registry goapiproof.RegistryView, outcomes []goapiproof.Outcome, summary goapiproof.Summary, proofCredential *goapiproof.Credential) error {
	fmt.Printf("go-api-prove: schema_digest=%s candidate_build=%s stage=%s org=%s\n",
		registry.SchemaDigest, registry.BuildIdentity, goapiproof.Stage, f.orgID)
	// admitted is printed alongside the others, including when it is zero:
	// it is the count that says whether anything got past the preconditions
	// at all, and "nothing was admissible" reads nothing like "everything
	// matched" once it is on the line.
	// stale_routing_rows is printed even at zero, like every other counter
	// here: "no row was stale" and "nobody looked" must not read alike.
	fmt.Printf("go-api-prove: attempted=%d admitted=%d executed=%d refused=%d receipts_written=%d stale_routing_rows=%d\n",
		summary.Attempted, summary.Admitted, summary.Executed, summary.Refused, summary.ReceiptsWritten, summary.StaleRoutingRows)
	proofURL := f.proofURL
	if proofURL == "" {
		proofURL = "(none: shadow-mode operations cannot be measured in this deployment)"
	}
	// Rebuilt labels even on the HAPPY path: this line is copied into
	// tickets and pasted into chat, which is exactly how a credential
	// outlives the terminal it was typed in. r3 found this one, and it is
	// printed on every successful run rather than only on failure.
	fmt.Printf("go-api-prove: edge=%s proof_route=%s\n", goapiproof.EndpointLabel(f.edgeURL), labelledProofURL(proofURL))
	// Report what this run ESTABLISHED, counted from the outcomes, rather
	// than restating what each route usually provides. r4 found both of
	// these lines missing: an earlier edit of mine reverted the computed
	// version back to a hardcoded sentence and dropped the mint count
	// entirely, and nothing failed, because no test read this output. Both
	// are now pinned by TestTheReportCarriesTheCountersItComputes.
	byBinding := map[string]int{}
	for _, outcome := range outcomes {
		if outcome.Admitted {
			byBinding[outcome.EdgeBuildBinding]++
		}
	}
	if len(byBinding) == 0 {
		fmt.Println("go-api-prove:   build binding: none (no operation passed admission)")
	}
	for _, binding := range sortedKeys(byBinding) {
		fmt.Printf("go-api-prove:   build binding %s = %d\n", binding, byBinding[binding])
	}
	// A COUNT, never a value. A fifteen-operation run that minted once is
	// a run that will fail at the closing /buildinfo, and without this
	// line that is invisible until it does.
	fmt.Printf("go-api-prove:   envelope mints = %d\n", proofCredential.Mints())
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

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// credentials builds the two credential sources, one per plane.
//
// The edge token is static: an access token outlives any run. The proof
// credential is normally MINTED, because the effective-principal envelope
// it needs lives 60 seconds and a fifteen-operation run does not fit in
// that. Go cannot mint one itself -- issue_effective_principal_envelope
// needs the envelope signing private key and a database-authenticated
// user, and nothing exposes it to an external caller -- so the minting
// stays outside this process behind an operator-supplied command.
//
// A static proof bearer is still accepted, because it is the right shape
// for a one-operation run and for a test, but it is exactly what failed
// on the real stack; the refusal below says so rather than letting a
// second operator rediscover it at the closing /buildinfo.
func credentials(f flags) (edge, proof *goapiproof.Credential, err error) {
	edgeBearer := os.Getenv(edgeBearerEnvVar)
	if edgeBearer == "" {
		return nil, nil, fmt.Errorf("no edge credential: set %s to the ACCESS TOKEN the Python edge accepts (the VALUE is never printed by this command)", edgeBearerEnvVar)
	}
	edge = goapiproof.StaticCredential("Authorization", "edge access token", "Bearer "+edgeBearer)

	switch {
	case f.proofBearerExec != "":
		var argv []string
		if err := json.Unmarshal([]byte(f.proofBearerExec), &argv); err != nil {
			return nil, nil, fmt.Errorf("-proof-bearer-exec must be a JSON array of strings, e.g. [\"/path/to/helper\",\"--org\",\"ORG\"]: %w", err)
		}
		if len(argv) == 0 {
			return nil, nil, errors.New("-proof-bearer-exec is an empty argv")
		}
		proof = goapiproof.MintedCredential("Authorization", "effective-principal envelope", proofCredentialFreshness,
			func(ctx context.Context) (string, error) {
				return mintBearer(ctx, argv)
			}).WithShapeValidator(goapiproof.ValidateEnvelopeShape)
	case os.Getenv(proofBearerEnvVar) != "":
		// Validated HERE, at construction, so a malformed static envelope
		// fails as a configuration error before anything is measured
		// rather than as a 401 fifteen operations later (r1 P2). The
		// message never echoes the value.
		static := "Bearer " + os.Getenv(proofBearerEnvVar)
		if err := goapiproof.ValidateEnvelopeShape(static); err != nil {
			return nil, nil, fmt.Errorf("%s is not a well-formed effective-principal envelope (%w). Its VALUE is not printed; check that you exported the envelope and not the edge access token", proofBearerEnvVar, err)
		}
		proof = goapiproof.StaticCredential("Authorization", "effective-principal envelope", static)
	default:
		return nil, nil, fmt.Errorf(
			"no proof-plane credential: /buildinfo and %s check the effective-principal ENVELOPE, which the edge access token cannot satisfy (measured: access token -> 401 on /buildinfo, envelope -> 401 on the edge).\n"+
				"  Set -proof-bearer-exec to a JSON argv printing a fresh envelope -- preferred, because ENVELOPE_DEFAULT_TTL_SECONDS is 60 and a full run outlives that --\n"+
				"  or %s for a short run.", "/query/proof", proofBearerEnvVar)
	}
	return edge, proof, nil
}

// Bounds on the minting helper. All three exist because the helper is
// operator-supplied and its output becomes an Authorization header.
const (
	// mintStdoutLimit caps what is read. A helper that streams megabytes
	// (a log, a core dump, /dev/urandom) must not be buffered whole just
	// to be rejected as the wrong shape. Exceeding it is a REFUSAL, never
	// a truncation: r2 built a helper emitting exactly 8192 JWT-shaped
	// bytes followed by noise, and the truncated prefix passed the shape
	// check and was installed as an Authorization header. A silently
	// truncated credential fails remotely as another 401 that reads like a
	// rejected one.
	mintStdoutLimit = 8 << 10
	// mintTimeout bounds one invocation. It is separate from the run
	// deadline so a hung helper fails as a hung helper, with its own
	// message, rather than as an unexplained slow run.
	mintTimeout = 20 * time.Second
)

// mintBearer runs the operator's helper and returns its stdout.
//
// Three properties, each from an executed r1 finding or its root cause:
//
//  1. NOTHING from the helper reaches the error. Not its stderr, not its
//     argv, not its output. r1 proved the previous version printed a
//     secret written to stderr straight into the operator-facing error --
//     the helper's own diagnostics are not separable from its credential,
//     so the only safe amount to quote is none. The error carries a fixed
//     message and the exit code, which is what an operator needs to go
//     look at their own helper's logs.
//
//  2. argv, never `sh -c`. A shell string puts the whole command line in
//     the process table, so a credential written inline in that string is
//     readable by every user on the box, and it invites shell injection
//     through anything interpolated into it. The helper is now a path plus
//     arguments, executed directly.
//
//  3. Bounded and killable. Output is capped, the invocation has its own
//     timeout, and the helper runs in its own process group so a timeout
//     kills the children it spawned rather than orphaning them -- a
//     `docker compose exec` helper is a process tree, not a process.
func mintBearer(ctx context.Context, argv []string) (string, error) {
	if len(argv) == 0 {
		return "", errors.New("no minting helper configured")
	}
	ctx, cancel := context.WithTimeout(ctx, mintTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	// Own process group, so Cancel below reaches the whole tree.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid = the process GROUP.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	// WaitDelay bounds the wait on the OUTPUT PIPE, which Cancel does not
	// reach. r4: a helper whose parent exits while a CHILD inherited
	// stdout leaves the write end open, and cmd.Run() blocks reading it
	// long after the group has been signalled.
	cmd.WaitDelay = time.Second

	var stdout bytes.Buffer
	bounded := &limitedWriter{w: &stdout, remaining: mintStdoutLimit}
	cmd.Stdout = bounded
	// stderr is DISCARDED rather than captured. Captured, it would sit in
	// memory waiting for somebody to decide it was safe to print, and r1
	// showed how that decision goes.
	cmd.Stderr = io.Discard

	err := cmd.Run()
	// r6: WaitDelay bounds the WAIT, not the descendants. The parent can
	// exit, mintBearer can return, and a grandchild the helper spawned is
	// still running -- observed `State: S (sleeping)` after the deadline
	// error was already returned. So the group is killed unconditionally
	// and REAPED here, whatever Run reported: a helper is a process tree,
	// and returning while part of it lives is reporting a termination that
	// did not happen.
	killHelperGroup(cmd)
	// Overflow is checked FIRST. r3 found the specific message never
	// fired: a helper that overruns the limit also makes cmd.Run() return
	// an error, so the run-failure branch classified it as "could not be
	// run" and the operator was told the wrong thing about a case we
	// deliberately detect.
	if bounded.overflowed {
		return "", fmt.Errorf("the envelope minting helper printed more than %d bytes on stdout; refusing rather than using a truncated value, which would fail remotely as an ordinary 401 (its output is deliberately not reported here -- check the helper's own logs)", mintStdoutLimit)
	}
	if ctxErr := ctx.Err(); errors.Is(ctxErr, context.DeadlineExceeded) {
		return "", fmt.Errorf("the envelope minting helper did not finish within %s and was killed (its output is deliberately not reported here -- check the helper's own logs)", mintTimeout)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return "", fmt.Errorf("the envelope minting helper exited %d (its output is deliberately not reported here -- check the helper's own logs)", exit.ExitCode())
		}
		return "", errors.New("the envelope minting helper could not be run (its output is deliberately not reported here -- check the helper's own logs)")
	}

	minted := strings.TrimSpace(stdout.String())
	if minted == "" {
		return "", errors.New("the envelope minting helper printed nothing on stdout")
	}
	if strings.HasPrefix(minted, "Bearer ") {
		return minted, nil
	}
	return "Bearer " + minted, nil
}

// limitedWriter stops storing past its limit and RECORDS that it did.
//
// It keeps accepting writes rather than returning an error, so the helper
// is not killed by a broken pipe mid-sentence and the caller decides what
// an overflow means -- and the caller refuses. Recording the overflow is
// the part r2 found missing: dropping the excess silently left a truncated
// prefix looking like a whole credential.
type limitedWriter struct {
	w          io.Writer
	remaining  int
	overflowed bool
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > l.remaining {
		l.overflowed = true
		p = p[:max(l.remaining, 0)]
	}
	if len(p) == 0 {
		return len(p), nil
	}
	n, err := l.w.Write(p)
	l.remaining -= n
	// Report the full length: the caller wrote it, we chose to drop it.
	return len(p), err
}

// labelledProofURL renders a proof URL safely, passing through the
// placeholder text used when no proof route is configured.
func labelledProofURL(proofURL string) string {
	if !strings.HasPrefix(proofURL, "http") {
		return proofURL
	}
	return goapiproof.EndpointLabel(proofURL)
}

// killHelperGroup signals the helper's whole process group and waits for
// it to actually be gone.
//
// Called on EVERY path, success included: a helper that spawned a
// background child and exited 0 has still left that child holding
// whatever it inherited. The kill is best-effort -- an already-dead group
// gives ESRCH, which is the outcome we want -- and the wait is bounded, so
// a process this program cannot kill (a different owner, an unkillable
// state) delays it by helperReapTimeout and no longer.
func killHelperGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pgid := cmd.Process.Pid
	_ = syscall.Kill(-pgid, syscall.SIGKILL)

	deadline := time.Now().Add(helperReapTimeout)
	for time.Now().Before(deadline) {
		// Signal 0 probes for existence without delivering anything.
		if err := syscall.Kill(-pgid, 0); err != nil {
			return // the group is gone
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// helperReapTimeout bounds the wait for the helper's group to die. Short:
// this runs after the helper has already been SIGKILLed, and anything
// still alive after it is not going to be killed by waiting longer.
const helperReapTimeout = 2 * time.Second

// validateEndpointFlags refuses any endpoint flag this package cannot
// fully account for.
//
// A function rather than an inline loop so a test can call it. r7 killed
// the inline version by bypassing the check, and nothing failed --
// `run()` has no test at all, so a guard inside it is a guard nobody
// holds. This is the smallest seam that makes the check testable without
// a harness for the whole command.
func validateEndpointFlags(f flags) error {
	for _, flagged := range []struct{ name, value string }{
		{"-registry-url", f.registryURL},
		{"-buildinfo-url", f.buildInfoURL},
		{"-edge-url", f.edgeURL},
		{"-proof-url", f.proofURL},
	} {
		if flagged.value == "" {
			continue
		}
		if err := goapiproof.RefuseCredentialsInURL(flagged.name, flagged.value); err != nil {
			return err
		}
	}
	return nil
}
