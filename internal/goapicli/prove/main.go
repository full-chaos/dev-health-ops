// Command go-api-prove is CHAOS-5425's `prove` verb: it executes every
// registered Go-API operation against the DEPLOYED query-api through the
// real edge, compares both planes' complete observable responses under
// CHAOS-4381's parity rules, and records an immutable
// `deployed_executed` receipt per operation in `go_api_proof_run`.
//
// Why this is a standalone Go binary and not a `dev-hops go-api routing
// prove` subcommand beside enable/disable/status: the Go cutover's standing
// rule is that no new Python
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
// internal/queryapi/digest, which forwards there. Re-implementing it
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
package prove

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// The proof-plane credential environment variable. VALUES never appear in
// output; only this NAME does.
//
// The two planes accept different credential KINDS, measured on the
// deployed stack (JOB 4): an access token gets HTTP 200 on the Python edge
// and 401 on /buildinfo; an effective-principal envelope gets the reverse.
// The edge leg has no env var: minting in process (mint-edge-token) is its
// only source, a fresh access token every run.
const proofBearerEnvVar = "GO_API_PROVE_PROOF_BEARER"

// postgresURIEnvVar is the ONLY place the DSN may come from other than the
// -postgres-uri flag -- named so the usage text and the flag-parsing path
// can name the variable without ever naming its value.
const postgresURIEnvVar = "POSTGRES_URI"

// proofCredentialFreshness is how long a minted envelope is reused before
// a fresh one is requested.
//
// ENVELOPE_DEFAULT_TTL_SECONDS is 60 (principal_envelope.py:96). 25
// seconds leaves 35 for the request to reach the server and be verified,
// so no request is ever sent carrying a value already near expiry -- which
// is how JOB 4's attempt B failed, at the CLOSING /buildinfo, after every
// measurement had already been taken.
const proofCredentialFreshness = 25 * time.Second

// edgeCredentialFreshness is how long a minted edge access token is reused
// before mint-edge-token is re-invoked. Its default TTL is ten minutes;
// four leaves six for the request to arrive, and it keeps a long run on
// short-lived tokens instead of one token as long as the run.
const edgeCredentialFreshness = 4 * time.Minute

// Command is the `goapi prove` verb of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "prove",
		Kind:    cli.Verb,
		Summary: "execute every registered Go-API GraphQL operation against the deployed edge and record a receipt",
		Run: func(_ context.Context, env cli.Env) int {
			err := run(env.Args)
			if err != nil && !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintf(env.Stderr, "go-api-prove: %v\n", err)
			}
			return cli.ExitForVerbError(err)
		},
	}
}

type flags struct {
	registryURL          string
	buildInfoURL         string
	edgeURL              string
	candidateBuild       string
	allowProverBuildSkew bool
	proofURL             string
	documentsPath        string
	postgresURI          string
	orgID                string
	artifactDir          string
	recordedBy           string
	reviewEvidence       string
	principalKind        string
	audience             string
	keyID                string
	dryRun               bool
	timeout              time.Duration
	window               goapiproof.Window
	reportPath           string
	instanceIDs          instanceIDFlag
}

// instanceIDFlag collects repeated -instance-id operation=value pairs into
// Config.InstanceIDs. A flag rather than a table edit: the table
// (operationSpecs' `pr` entry) says the id has to come from the RUN, and a
// flag is the run's own input, never a value this checkout invents.
type instanceIDFlag map[string]string

func (m instanceIDFlag) String() string {
	// flag calls String on the zero value while building its usage text,
	// before Set has ever run -- must not panic on a nil map.
	pairs := make([]string, 0, len(m))
	for operation, id := range m {
		pairs = append(pairs, operation+"="+id)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (m instanceIDFlag) Set(value string) error {
	operation, id, ok := strings.Cut(value, "=")
	if !ok || operation == "" || id == "" {
		return fmt.Errorf("-instance-id must be operation=value (e.g. pr=<a real stored pr id>), got %q", value)
	}
	m[operation] = id
	return nil
}

// flagOutput is where a fresh flag set's usage/error text goes. A package
// variable, in the same shape goapicli/routing uses for stdout/stderr, so a
// test can capture parseFlags' own usage output (e.g. to assert it never
// leaks POSTGRES_URI) without reaching into the process-global flag.CommandLine.
var flagOutput io.Writer = os.Stderr //nolint:gochecknoglobals // see routing's stdout/stderr for the same seam

// registerFlags builds a fresh flag set with every flag this command
// defines, without parsing anything. A test that only needs to know the
// flag set's SHAPE (e.g. that a documented invocation names real flags)
// calls this directly instead of driving a full parseFlags/run.
func registerFlags() (*flag.FlagSet, *flags) {
	defaults := goapiproof.DefaultWindow()
	f := &flags{}
	fs := flag.NewFlagSet("go-api-prove", flag.ContinueOnError)
	fs.SetOutput(flagOutput)
	fs.StringVar(&f.registryURL, "registry-url", "http://localhost:8090/registry", "GET /registry on the DEPLOYED query-api -- the only authority on which operations and document digests it serves")
	fs.StringVar(&f.buildInfoURL, "buildinfo-url", "http://localhost:8090/buildinfo", "GET /buildinfo on the DEPLOYED query-api -- the ONLY source of the build identity every receipt names")
	fs.BoolVar(&f.allowProverBuildSkew, proverBuildSkewFlag[1:], false, "measure even when this binary was not built from the candidate build's commit (or carries no commit at all): its declarations and parity rules are then another commit's, and the report records the skew as prover_build_skew_allowed")
	fs.StringVar(&f.candidateBuild, "candidate-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written (team-lead ruling R51)")
	fs.StringVar(&f.edgeURL, "edge-url", "http://localhost:8000/graphql", "the real product GraphQL edge; both the canary/primary candidate leg and every baseline leg go through it")
	fs.StringVar(&f.proofURL, "proof-url", "", "measurement-only route able to execute a SHADOW-mode operation on the deployed Go build; empty means shadow operations are refused by name rather than skipped")
	fs.StringVar(&f.documentsPath, "documents", "", "path to `registrydump -file internal/queryapi/server/query_route.go` JSON output (required)")
	secrets.BindFlag(fs, &f.postgresURI, "postgres-uri", postgresURIEnvVar, "domain Postgres DSN holding go_api_routing_state / go_api_proof_run")
	fs.StringVar(&f.orgID, "org", "", "org id every request is made for (required)")
	fs.StringVar(&f.artifactDir, "artifact-dir", "", "directory for content-addressed response bodies; receipts store a reference, never an inlined body (required)")
	fs.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on every receipt (required)")
	fs.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every receipt (required)")
	fs.StringVar(&f.principalKind, "principal-kind", "stored_account", "auth-context SHAPE recorded in request_identity; never a credential")
	fs.StringVar(&f.audience, "audience", "query-api", "envelope audience, part of the auth-context shape")
	fs.StringVar(&f.keyID, "key-id", "", "envelope signing key id (kid) -- a public identifier, and the value that silently broke routing three times on 2026-09-07")
	fs.BoolVar(&f.dryRun, "dry-run", false, "execute and compare, but write NO receipts")
	fs.DurationVar(&f.timeout, "timeout", 60*time.Second, "per-request timeout")
	fs.StringVar(&f.reportPath, "report", "", "write the full JSON report here in addition to stdout")
	f.instanceIDs = instanceIDFlag{}
	fs.Var(&f.instanceIDs, "instance-id", "operation=value or operation.VARIANT=value, repeatable: a REAL row identifier for an operation (or one corpus variant) whose request needs one (e.g. the pr operation needs $id), read from this org's own data -- a flag is the run's own input, never a value this command invents. An operation needing one with no entry here is refused by name (operation_needs_an_instance_identifier), not measured with a guess")
	fs.StringVar(&f.window.SinceUTC, "since-utc", defaults.SinceUTC, "request window start (RFC3339)")
	fs.StringVar(&f.window.UntilUTC, "until-utc", defaults.UntilUTC, "request window end (RFC3339)")
	fs.StringVar(&f.window.SinceDate, "since-date", defaults.SinceDate, "request window start (YYYY-MM-DD)")
	fs.StringVar(&f.window.UntilDate, "until-date", defaults.UntilDate, "request window end (YYYY-MM-DD)")
	fs.StringVar(&f.window.WeekStart, "week-start", defaults.WeekStart, "operatingReview week start (YYYY-MM-DD)")
	return fs, f
}

func parseFlags(args []string) (flags, error) {
	fs, fp := registerFlags()
	if err := fs.Parse(args); err != nil {
		return *fp, cli.WrapFlagParseError(err)
	}
	f := *fp
	secrets.ResolveFlag(fs, &f.postgresURI, "postgres-uri", postgresURIEnvVar)

	missing := map[string]string{
		"-documents": f.documentsPath, "-org": f.orgID, "-artifact-dir": f.artifactDir,
		"-recorded-by": f.recordedBy, "-review-evidence": f.reviewEvidence,
	}
	if !f.dryRun {
		missing["-postgres-uri (or "+postgresURIEnvVar+")"] = f.postgresURI
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

func run(args []string) (err error) {
	var f flags
	// Every exit before emitReport runs still writes -report (when one
	// was given), with exit_cause refused_before_measuring and the
	// redacted error: registered first, so it runs last.
	reported := false
	// resolved is both builds once /buildinfo has named the candidate:
	// every report written after that carries them.
	var resolved *goapiproof.ProverBuild
	defer func() {
		if err == nil || reported || f.reportPath == "" {
			return
		}
		refused := report{OrgID: f.orgID, Window: f.window, Stage: goapiproof.Stage, Outcomes: []goapiproof.Outcome{}, ExitCause: exitRefusedBeforeMeasuring, ExitDetail: err.Error()}
		if resolved != nil {
			refused.ProverBuild, refused.CandidateBuild = resolved, resolved.Candidate
		}
		if writeErr := writeReportFile(f.reportPath, refused); writeErr != nil {
			err = fmt.Errorf("%w; additionally, writing the report failed: %v", err, writeErr)
		}
	}()
	// parseFlags returns what it parsed alongside a refusal, so the
	// report path and the DSN to redact are known on that exit too.
	parsed, parseErr := parseFlags(args)
	f = parsed
	if parseErr != nil {
		return secrets.NewBoundary(f.postgresURI).Redact(parseErr)
	}
	// A single boundary, applied here via defer, covers every error this
	// function returns from this point on, no matter which layer
	// produced it or whether that layer remembered the DSN could be
	// inside -- construct it once, from the DSN this run actually
	// resolved, and apply it to the named return AFTER the fact rather
	// than trusting every call site downstream to redact its own. A
	// bare `return expr` still assigns expr to `err` before this defer
	// runs, so every return in the rest of the function is covered.
	boundary := secrets.NewBoundary(f.postgresURI)
	defer func() { err = boundary.Redact(err) }()

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

	// Cancelled by SIGINT or SIGTERM (the signal an orchestrator sends
	// before it kills a pod): every leg still in flight then fails as a
	// named per-operation refusal, and the report below is still written,
	// with exit_cause stopped_by_signal.
	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()
	client := goapiproof.NewLegClient(f.timeout)

	// The opening and closing /buildinfo reads get the SAME per-request
	// deadline the measured legs get. They used to take
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
	// Both planes scope a request by the credential's org_id claim, so
	// every value either credential sends must name -org (and no
	// impersonation) -- checked on the exact value each time it is set on
	// a request.
	edgeCredential.BindOrg(f.orgID)
	proofCredential.BindOrg(f.orgID)

	registry, err := goapiproof.FetchRegistry(ctx, client, f.registryURL)
	if err != nil {
		return err
	}
	if err := refuseEmptyRegistry(registry, f.registryURL); err != nil {
		return err
	}

	// The build identity is fetched BEFORE anything is measured, and a
	// failure here stops the run rather than degrading into an
	// operator-supplied name: the receipt is the whole deliverable, and
	// one naming an unverifiable build is worse than none (CHAOS-5425
	// acceptance: "Do not construct a receipt from a digest or
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
	builds := goapiproof.NewProverBuild(version.Current("go-api-prove"), registry.BuildIdentity, f.allowProverBuildSkew)
	fmt.Printf("go-api-prove: %s\n", builds.Line())
	resolved = &builds
	if err := builds.Check(proverBuildSkewFlag); err != nil {
		return err
	}

	// The Python app's own answer, with the credential every baseline leg
	// carries, naming the org it serves that credential: refused unless it
	// is -org with no impersonation session in force. Every leg is still
	// checked for the impersonation stamp (admitPlanes).
	principalCtx, cancelPrincipal := boundedCtx()
	err = goapiproof.VerifyReferencePrincipal(principalCtx, client, originOf(f.edgeURL), edgeCredential, f.orgID)
	cancelPrincipal()
	if err != nil {
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

	var pool dbPool
	if !f.dryRun {
		pool, err = openPostgresPool(ctx, f.postgresURI)
		if err != nil {
			return secrets.RedactedConnectError("connect to Postgres", "postgres-uri", postgresURIEnvVar)
		}
		defer pool.Close()
	}

	routing, err := readRoutingState(ctx, pool, registry, f.candidateBuild)
	if err != nil {
		return err
	}
	goServed, err := goapiproof.DefaultGoServedLedger()
	if err != nil {
		return err
	}
	// A routing row naming a build the running process is not is a
	// REFUSAL again. The demotion assumed /buildinfo identifies
	// the replica that served the MEASURED request; with multiple
	// query-api replicas and an edge that drops the per-request build
	// header, it does not. Re-pointing the rows after a deploy is an
	// operator step, and the refusal below says so.
	runner := &goapiproof.Runner{
		GoServed:  goServed,
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
			Auth:            authContextFor(f),
			RecordedBy:      f.recordedBy,
			ReviewEvidence:  f.reviewEvidence,
			Timeout:         f.timeout,
			InstanceIDs:     map[string]string(f.instanceIDs),
		},
	}

	outcomes, summary, runErr := runner.Run(ctx)

	// NOTHING has been written yet, and nothing will be until the build is
	// shown to have held for the whole run. Receipts used to
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
	// Held, not returned: the outcomes measured above go into the report
	// whatever happens to their receipts.

	var writeErr error
	if !f.dryRun && pool != nil {
		written, err := goapiproof.WriteReceipts(ctx, pool, receipts)
		summary.ReceiptsWritten = len(written)
		for i := range outcomes {
			// CHAOS-5623: keyed by (operation, variant) -- an operation
			// with Variants (flowMatrix's TEAM/REPO) can share one
			// Operation across more than one Outcome, and a bare
			// written[outcomes[i].Operation] read ANY variant's successful
			// write as every sibling variant's, including one whose write
			// genuinely failed.
			outcomes[i].ReceiptWritten = written[goapiproof.ReceiptKey(outcomes[i].Operation, outcomes[i].Variant)]
		}
		// Held, not returned: the report below is exactly the evidence
		// somebody needs to see when a write fails halfway, and returning
		// here would drop it -- "report the problem and return" is the trap
		// that loses the failure signal.
		writeErr = err
	}
	for _, err := range []error{stabilityErr, receiptErr, writeErr} {
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
	exitCause := exitCompleted
	switch {
	case ctx.Err() != nil:
		exitCause = exitStoppedBySignal
		if runErr == nil {
			runErr = fmt.Errorf("run interrupted: %w", ctx.Err())
		} else {
			runErr = fmt.Errorf("run interrupted: %w; %w", ctx.Err(), runErr)
		}
	case runErr != nil:
		exitCause = exitCompletedWithRunError
	}
	reported = true
	if err := emitReport(f, registry, builds, outcomes, summary, proofCredential, edgeCredential, exitCause, runErr); err != nil {
		if runErr == nil {
			return err
		}
		return fmt.Errorf("%w; additionally, writing the report failed: %v", runErr, err)
	}
	return runErr
}

// Exit causes, one per way a run can end; each is written into the
// report on that path.
const (
	exitCompleted              = "completed"
	exitCompletedWithRunError  = "completed_with_run_error"
	exitStoppedBySignal        = "stopped_by_signal"
	exitRefusedBeforeMeasuring = "refused_before_measuring"
)

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
// A mutation killed the previous shape by replacing the CALLER's `return err` with
// `_ = err`: the check was a separate statement in run(), so it could be
// ignored, and nothing failed. Folding it in means a caller cannot obtain
// the rows without the check having run.
//
// It does NOT make the mutation unwritable -- an earlier version of this
// comment claimed that, and further testing disproved it with the exact
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

// dbPool is the Postgres surface run() needs: reading routing rows
// (routingRowSource) plus writing receipts (goapiproof.Querier) and
// closing the connection when the run ends.
//
// openPostgresPool is a package-level function var rather than a direct
// pgxpool.New call at the call site, so a test can substitute a fake pool
// and drive run() end to end without a real database. pgxpool.New itself
// never dials at construction (the first query does), so this
// substitution changes nothing about how a real invocation connects.
type dbPool interface {
	routingRowSource
	goapiproof.Querier
	Close()
}

var openPostgresPool = func(ctx context.Context, uri string) (dbPool, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, err
	}
	// pgxpool.New never dials -- the first real operation does, and
	// readRoutingState's own pool.Query would otherwise be the first
	// thing to discover a bad DSN, with pgx's connection error (which can
	// carry the DSN itself) surfacing through THAT call's %w wrap instead
	// of the redacted one right below. Ping forces the dial here, while
	// the caller is still one `if err != nil` away from
	// secrets.RedactedConnectError.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// refuseEmptyRegistry is this command's own copy of the guard
// `enable`/`repoint` each carry at their call site: FetchRegistry itself
// accepts an empty registry (status needs the schema digest that comes
// with one even when nothing is registered), so a caller with nothing to
// prove or write against refuses it locally instead. Extracted to its
// own function so a test can drive it without a live query-api.
func refuseEmptyRegistry(registry goapiproof.RegistryView, registryURL string) error {
	if len(registry.DocumentDigest) == 0 {
		return fmt.Errorf("goapiproof: %s registers no operations -- there is nothing to prove", goapiproof.EndpointLabel(registryURL))
	}
	return nil
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
	SchemaDigest   string `json:"schema_digest"`
	CandidateBuild string `json:"candidate_build"`
	// ProverBuild names the commit whose declarations and parity rules
	// the run applied beside the candidate's (its candidate_build is
	// CandidateBuild's); absent until /buildinfo named the candidate.
	*goapiproof.ProverBuild
	Stage    string               `json:"stage"`
	OrgID    string               `json:"org_id"`
	Window   goapiproof.Window    `json:"window"`
	Summary  goapiproof.Summary   `json:"summary"`
	Outcomes []goapiproof.Outcome `json:"outcomes"`
	// ExitCause names how the run ended (one of the exit* constants), so
	// a report read after the fact says whether it is a whole run, a run
	// stopped early, or one refused before it measured anything.
	ExitCause string `json:"exit_cause"`
	// ExitDetail is the run-level error behind ExitCause, redacted, when
	// there was one.
	ExitDetail string `json:"exit_detail,omitempty"`
}

// authContextFor is the auth-context shape a run records.
func authContextFor(f flags) goapiproof.AuthContext {
	return goapiproof.AuthContext{PrincipalKind: f.principalKind, Audience: f.audience, KeyID: f.keyID}
}

// writeReportFile writes one report as JSON.
func writeReportFile(path string, r report) error {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o640); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

// emitReport prints the explicit-zero telemetry block and the full JSON.
//
// Every counter is printed on every run, including the zeros. "prove
// measured nothing" and "prove measured everything and found nothing
// wrong" are different facts, and the shape of the output must never let
// them look alike.
func emitReport(f flags, registry goapiproof.RegistryView, builds goapiproof.ProverBuild, outcomes []goapiproof.Outcome, summary goapiproof.Summary, proofCredential, edgeCredential *goapiproof.Credential, exitCause string, runErr error) error {
	fmt.Printf("go-api-prove: schema_digest=%s candidate_build=%s stage=%s org=%s\n",
		registry.SchemaDigest, registry.BuildIdentity, goapiproof.Stage, f.orgID)
	fmt.Printf("go-api-prove: %s\n", builds.Line())
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
	// outlives the terminal it was typed in -- so it is
	// printed on every successful run rather than only on failure.
	fmt.Printf("go-api-prove: edge=%s proof_route=%s\n", goapiproof.EndpointLabel(f.edgeURL), labelledProofURL(proofURL))
	// Report what this run ESTABLISHED, counted from the outcomes, rather
	// than restating what each route usually provides. Both of
	// these lines went missing once: an earlier edit reverted the computed
	// version back to a hardcoded sentence and dropped the mint count
	// entirely, and nothing failed, because no test read this output. Both
	// are now pinned by TestTheReportCarriesTheCountersItComputes.
	//
	// The counted form matters beyond the missing-line bug: today every
	// edge outcome is unbound and every proof outcome is per-request, so a
	// hardcoded sentence would be RIGHT -- until #2365 deletes the Python
	// edge and edge responses start carrying the header. Printing the
	// assumption would then be printing a falsehood. Each receipt records
	// the real value in build_binding (alembic 0129, CHAOS-5484); this
	// line is the same fact for whoever is reading a terminal.
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
	// The edge credential is always minted (the only source), so this is
	// at least 1 on a successful run -- a 0 here means the edge leg was
	// never actually exercised.
	fmt.Printf("go-api-prove:   edge access token mints = %d\n", edgeCredential.Mints())
	for _, state := range sortedKeys(summary.ByTerminalState) {
		fmt.Printf("go-api-prove:   terminal_state %s = %d\n", state, summary.ByTerminalState[state])
	}
	fmt.Printf("go-api-prove:   proven_under %s = %d\n", goapiproof.ProvenUnderStochasticLeafClass, summary.ProvenUnderStochasticLeafClass)
	fmt.Printf("go-api-prove:   %s = %d\n", goapiproof.VerdictGoOnly, summary.ProvenGoOnly)
	for _, reason := range sortedKeys(summary.ByRefusalReason) {
		fmt.Printf("go-api-prove:   refused %s = %d\n", reason, summary.ByRefusalReason[reason])
	}
	for _, outcome := range outcomes {
		if outcome.Executed {
			fmt.Println(executedOutcomeLine(outcome))
			continue
		}
		fmt.Printf("go-api-prove:   %-22s mode=%-8s REFUSED %s: %s\n",
			outcome.Operation, outcome.Mode, outcome.RefusalReason, outcome.RefusalDetail)
	}

	fmt.Printf("go-api-prove: exit_cause=%s\n", exitCause)
	if f.reportPath == "" {
		return nil
	}
	r := report{
		SchemaDigest:   registry.SchemaDigest,
		CandidateBuild: registry.BuildIdentity,
		ProverBuild:    &builds,
		Stage:          goapiproof.Stage,
		OrgID:          f.orgID,
		Window:         f.window,
		Summary:        summary,
		Outcomes:       outcomes,
		ExitCause:      exitCause,
	}
	if runErr != nil {
		r.ExitDetail = secrets.NewBoundary(f.postgresURI).Redact(runErr).Error()
	}
	if err := writeReportFile(f.reportPath, r); err != nil {
		return err
	}
	fmt.Printf("go-api-prove: report written to %s\n", f.reportPath)
	return nil
}

// proverBuildSkewFlag permits a run whose prover build is not the
// candidate's.
const proverBuildSkewFlag = "-allow-prover-build-skew"

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// mintCredential mints one of the two allowlisted credentials, exactly
// like goapiproof.MintViaAllowlistedHelper -- which this package-level
// var defaults to. credentials() below calls THIS, never the function
// directly, so a test can substitute a synthetic minter for the duration
// of one run() call (see e2e_test.go's withFakeMinter), the same seam
// openPostgresPool already gives run()'s Postgres connection step.
var mintCredential = goapiproof.MintViaAllowlistedHelper //nolint:gochecknoglobals // deliberately test-substitutable, same seam as openPostgresPool

// credentials builds the two credential sources, one per plane, minted
// IN PROCESS via mintCredential -- the same mechanism `goapi rest-prove`
// already uses: mint-envelope and mint-edge-token are compiled into this
// binary, so minting calls their packages directly; nothing forks a
// subprocess to mint a credential anymore.
//
// The edge token is always minted (mint-edge-token), re-run as it ages --
// the only source for this credential; a hand-minted static token expired
// mid-day once and had to be replaced by hand.
//
// The proof credential is normally minted too (mint-envelope): the
// effective-principal envelope it needs lives 60 seconds and a
// fifteen-operation run does not fit in that. A static proof bearer
// (proofBearerEnvVar) is still accepted as a fallback, because it is the
// right shape for a one-operation run and for a test -- but it is exactly
// what failed on the real stack; the refusal below says so rather than
// letting a second operator rediscover it at the closing /buildinfo.
func credentials(f flags) (edge, proof *goapiproof.Credential, err error) {
	edge = goapiproof.MintedCredential("Authorization", "edge access token", edgeCredentialFreshness,
		func(ctx context.Context) (string, error) {
			return mintCredential(ctx, "mint-edge-token", []string{"-org", f.orgID})
		}).WithShapeValidator(goapiproof.ValidateEnvelopeShape)

	switch {
	case os.Getenv(proofBearerEnvVar) != "":
		// Validated HERE, at construction, so a malformed static envelope
		// fails as a configuration error before anything is measured
		// rather than as a 401 fifteen operations later. The
		// message never echoes the value.
		static := "Bearer " + os.Getenv(proofBearerEnvVar)
		if err := goapiproof.ValidateEnvelopeShape(static); err != nil {
			return nil, nil, fmt.Errorf("%s is not a well-formed effective-principal envelope (%w). Its VALUE is not printed; check that you exported the envelope and not the edge access token", proofBearerEnvVar, err)
		}
		proof = goapiproof.StaticCredential("Authorization", "effective-principal envelope", static)
	default:
		proof = goapiproof.MintedCredential("Authorization", "effective-principal envelope", proofCredentialFreshness,
			func(ctx context.Context) (string, error) {
				return mintCredential(ctx, "mint-envelope", []string{"-org", f.orgID})
			}).WithShapeValidator(goapiproof.ValidateEnvelopeShape)
	}
	return edge, proof, nil
}

// labelledProofURL renders a proof URL safely, passing through the
// placeholder text used when no proof route is configured.
func labelledProofURL(proofURL string) string {
	if !strings.HasPrefix(proofURL, "http") {
		return proofURL
	}
	return goapiproof.EndpointLabel(proofURL)
}

// validateEndpointFlags refuses any endpoint flag this package cannot
// fully account for.
//
// A function rather than an inline loop so a test can call it. A mutation
// killed the inline version by bypassing the check, and nothing failed --
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
		if err := goapiproof.ValidateBaseURL(flagged.name, flagged.value); err != nil {
			return err
		}
	}
	return nil
}

// originOf is raw's scheme and host: the Python app serves its own
// ReferencePrincipalPath beside the GraphQL route -edge-url names.
// validateEndpointFlags has already refused a URL it cannot account for.
func originOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String()
}

// executedOutcomeLine is the terminal line for one executed measurement. It
// names WHAT the citation covered and what it did not, by shape:
// "one value differed" and "Go returned no rows" never print alike.
func executedOutcomeLine(outcome goapiproof.Outcome) string {
	verdict := outcome.TerminalState
	switch outcome.ProvenUnder {
	case "":
	case goapiproof.ProvenUnderGoOnly:
		// Never printed as a two-plane word: no baseline answered.
		verdict = goapiproof.VerdictGoOnly + " (no two-plane baseline)"
	case goapiproof.ProvenUnderGoOnlyUnproven:
		// The ledger names no two-plane match for this operation.
		verdict = goapiproof.VerdictGoOnlyUnproven + " (named limit, no two-plane match)"
	default:
		verdict += " PROVEN_UNDER=" + outcome.ProvenUnder
	}
	return fmt.Sprintf("go-api-prove:   %-22s mode=%-8s route=%-5s %s (%d findings, %d outside a declared baseline defect %v) %s",
		outcome.Operation, outcome.Mode, outcome.Route, verdict,
		len(outcome.Findings), outcome.DifferencesOutsideBaselineDefect, outcome.BaselineDefects,
		goapiproof.FormatShapeCounts(outcome.CoveredByShape, outcome.OutsideByShape))
}
