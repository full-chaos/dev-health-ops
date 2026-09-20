// Command go-api-rest-prove is the REST sibling of go-api-prove: it
// executes every registered REST route's committed corpus
// (goapiproof.KnownRESTOperations) against a DEPLOYED query-api build AND
// the Python api service, IN CLUSTER, with no edge process and no ingress
// in the path -- both services are called by their own direct address, so
// which plane answered is known by construction (which URL was called),
// never sniffed from a header. It compares status codes and (where the
// corpus declares RESTBodyModeJSON) the two decoded JSON bodies under the
// same signed parity rules the GraphQL prover uses, and records an
// immutable `deployed_executed` receipt in go_api_rest_proof_run per
// request, exactly like go-api-prove's own GraphQL receipts land in
// go_api_proof_run -- see goapiproof/restreceipt.go for the dedicated
// table and the shared admission rule (goapiproof.EnablementProofClause)
// both tables are judged by.
//
// Operator command of record: run from the go-api-tools image,
// in-cluster, with query-api-url and python-api-url pointed at each
// service's own ClusterIP/Service address (never a host or an ingress
// hostname). The two bearer helpers mint DIFFERENT credential CLASSES:
// -candidate-bearer-exec always names mint-envelope (the
// effective-principal envelope query-api has always checked) and
// -baseline-bearer-exec always names mint-edge-token (the edge access
// token the Python api service checks), never the other way round or the
// same helper twice:
//
//	go-api-rest-prove \
//	  -query-api-url http://<query-api-service>:8090 \
//	  -python-api-url http://<api-service>:8000 \
//	  -candidate-bearer-exec '["mint-envelope","-org","<org>"]' \
//	  -baseline-bearer-exec  '["mint-edge-token","-org","<org>"]' \
//	  -artifact-dir "$ARTIFACT_DIR" \
//	  -org <org> -recorded-by <operator> -review-evidence "<why>" \
//	  -postgres-uri "$POSTGRES_URI"
//
// query-api's REST routes now ALSO accept the edge access token
// directly (credential.go's own doc comment predates this and is now
// only half the story) -- ingress path-splitting forwards that
// credential, never a minted envelope, to a real user's request. Every
// run of this command already proves that: -baseline-bearer-exec's own
// value is, per request, additionally sent straight to query-api as a
// THIRD leg (proveEdgeCredentialOnCandidate, run() below) -- no NEW flag
// of its own: it reuses -baseline-bearer-exec (the SAME credential the
// baseline leg already mints, just pointed at the other service) and
// -artifact-dir (already required above, for the ordinary legs' own
// bodies). Its own outcome line is suffixed
// "(edge-credential-on-candidate)"; a status or build-header mismatch
// there means query-api rejected the credential real traffic actually
// carries, which is exactly the regression this whole binary exists to
// catch before it reaches a real user.
//
// No host or secret is named above by design: an operator fills in the
// service addresses and org from the deployment's own operator record.
// No -query-api-src either: the corpus's coverage check runs against
// goapiproof.MountedRESTPaths, a checked-in snapshot compiled into this
// binary, never against a live query-api source tree -- the tools image
// this command actually ships in carries no Go source at all (see that
// flag's own doc string and MountedRESTPaths' own doc comment).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

func main() {
	// ContinueOnError: a flag the flag package cannot parse is returned,
	// not an exit inside flag.Parse, so it reaches execute like every other
	// refusal and the run's accounting is still printed and written.
	flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)
	if err := execute(parseFlags()); err != nil {
		log.Fatalf("go-api-rest-prove: %v", err)
	}
}

// execute is the command's one entry after flag parsing. A refused
// configuration (flag syntax, a missing or invalid flag) ends here with
// the same accounting as every other exit: every planned request named in
// not_run on stdout and, when -report was parsed, in a freshly written
// report, with exit_cause refused_before_measuring.
func execute(f flags, parseErr error) error {
	if errors.Is(parseErr, flag.ErrHelp) {
		return nil
	}
	if parseErr != nil {
		err := secrets.NewBoundary(f.postgresURI).Redact(parseErr)
		if writeErr := writeStoppedBeforeMeasuringReport(f, nil, nil, err); writeErr != nil {
			return fmt.Errorf("%w; additionally, writing the report failed: %v", err, writeErr)
		}
		return err
	}
	return run(f)
}

// defaultRunDeadline is -run-deadline's default.
const defaultRunDeadline = 30 * time.Minute

// runContext is the run's context: cancelled by SIGINT or SIGTERM (the
// signal an orchestrator sends before it kills a pod), so the loop stops
// between requests and the report is still written, and bounded by
// deadline (-run-deadline).
func runContext(deadline time.Duration) (context.Context, context.CancelFunc) {
	sigCtx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(sigCtx, deadline)
	return ctx, func() {
		cancel()
		stopSignal()
	}
}

// postgresURIEnvVar is the ONLY place the DSN may come from other than the
// -postgres-uri flag, and it is named rather than inlined so the usage
// text and the flag-parsing path can name the variable without ever
// naming its value.
const postgresURIEnvVar = "POSTGRES_URI"

type flags struct {
	queryAPIURL    string
	pythonAPIURL   string
	buildInfoURL   string
	candidateBuild string
	queryAPISrc    string

	allowProverBuildSkew bool

	candidateBearerExec string
	baselineBearerExec  string

	// pythonForwarderOff is the operator's attestation that the Python
	// app's forwarding switch for every PythonForwarder endpoint is off
	// for this run, so a 200 baseline there is Python's own answer.
	// Default false: such a 200 baseline is refused by name. Recorded in
	// every receipt it affects.
	pythonForwarderOff bool

	postgresURI    string
	org            string
	recordedBy     string
	reviewEvidence string
	principalKind  string
	audience       string
	keyID          string
	artifactDir    string

	dryRun  bool
	timeout time.Duration
	// runDeadline bounds the whole run (-run-deadline).
	runDeadline time.Duration
	reportPath  string
	// gapDelay is -gap-reread-delay: how long the delayed re-read waits
	// (0 disables the stage); gapBudget is -gap-reread-budget: the most
	// delay one run spends in total. gapReread is the run's shared budget
	// state, built by runMeasurement; nil disables the stage.
	gapDelay  time.Duration
	gapBudget time.Duration
	gapReread *gapRereadState

	// binds is every -bind NAME=VALUE the run invocation supplied, keyed
	// by NAME -- an id for a corpus request whose IDBindings names a
	// goapiproof.IsOperatorSuppliedIDProducer producer, resolved from the
	// run invocation itself rather than an earlier request's own
	// Produces (restcorpus.go's own restOperatorSuppliedProducers doc
	// comment). Never populated from anything but this flag: no
	// production literal lives in this binary's own source.
	binds map[string]string
}

func parseFlags() (flags, error) {
	var f flags
	f.binds = map[string]string{}
	flag.Func("bind", "an operator-supplied id binding for a corpus request whose IDBindings names a goapiproof.IsOperatorSuppliedIDProducer producer, as NAME=VALUE. Repeatable, once per name. NAME must match a producer the corpus actually declares operator-suppliable (an unmatched NAME is simply never consumed); VALUE is never validated beyond being non-empty, since its shape is the corpus request's own concern. Rejected immediately, before any request is planned or sent, when the argument carries no \"=\" or either side of it is empty", func(raw string) error {
		name, value, ok := strings.Cut(raw, "=")
		if !ok {
			return fmt.Errorf("-bind %q: want NAME=VALUE", raw)
		}
		if name == "" {
			return fmt.Errorf("-bind %q: empty NAME", raw)
		}
		if value == "" {
			return fmt.Errorf("-bind %q: empty VALUE", raw)
		}
		f.binds[name] = value
		return nil
	})
	flag.StringVar(&f.queryAPIURL, "query-api-url", "http://localhost:8090", "query-api's OWN in-cluster address -- the candidate leg. Never an edge or ingress URL: every request this tool sends goes DIRECTLY to this service")
	flag.StringVar(&f.pythonAPIURL, "python-api-url", "", "the Python api service's OWN in-cluster address -- the baseline leg (required). Never an edge or ingress URL, for the same reason as -query-api-url")
	flag.BoolVar(&f.pythonForwarderOff, "python-forwarder-off", false, "attest that the Python app's forwarding switch for every endpoint it can forward to query-api (RESTEndpointSpec.PythonForwarder: POST /api/v1/investment/explain) is OFF for this whole run, so a 200 baseline there is Python's own answer and is compared. The Python app relays query-api's answer without any header that marks it, so nothing on the response can show which plane computed it: without this flag such a 200 baseline is refused by name, and with it every receipt for such an endpoint records the attestation")
	flag.StringVar(&f.buildInfoURL, "buildinfo-url", "", "GET /buildinfo on query-api -- the ONLY source of the build identity every receipt names. Defaults to -query-api-url + \"/buildinfo\"")
	flag.BoolVar(&f.allowProverBuildSkew, proverBuildSkewFlag[1:], false, "measure even when this binary was not built from the candidate build's commit (or carries no commit at all): its declarations, shapes and corpus are then another commit's, and the report records the skew as prover_build_skew_allowed")
	flag.StringVar(&f.candidateBuild, "candidate-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written -- the value written always comes from /buildinfo, matching go-api-prove's own -candidate-build flag")
	flag.StringVar(&f.queryAPISrc, "query-api-src", "", "OPTIONAL dev-only override: path to a REAL query-api source checkout, read LIVE to confirm this corpus's paths match what the mux actually mounts (migrationmatrix.LoadQueryAPIMuxRoutes). Empty (the default) uses goapiproof.MountedRESTPaths, the checked-in snapshot this binary ships with -- the operator tools image carries no Go source tree at all, so that is the ONLY option available there. Set this only when running from a real repo checkout, to catch drift immediately instead of waiting for TestMountedRESTPathsMatchesTheRealQueryAPIMux's own CI run")
	flag.StringVar(&f.candidateBearerExec, "candidate-bearer-exec", "", "JSON array whose first element is an ALLOWLISTED HELPER NAME (\"mint-envelope\" or \"mint-edge-token\", never a path -- see goapiproof.MintViaAllowlistedHelper) printing a FRESH bearer credential for query-api on stdout, e.g. [\"mint-envelope\",\"-org\",\"<org>\"]. Re-run as the credential ages. The helper reads any secret it needs from ITS OWN environment -- never from an argument here. The remaining elements are the helper's own argv, never a shell string: nothing is interpolated into a shell. The helper's stdout and stderr are NEVER reported by this command")
	flag.StringVar(&f.baselineBearerExec, "baseline-bearer-exec", "", "JSON array, same allowlisted-helper-name-plus-argv shape as -candidate-bearer-exec, printing a FRESH bearer credential for the Python api service on stdout -- see credential.go's own doc comment for why one credential kind cannot be assumed to reach both planes")
	secrets.BindFlag(flag.CommandLine, &f.postgresURI, "postgres-uri", postgresURIEnvVar, "domain Postgres DSN holding go_api_proof_run. Required unless -dry-run")
	flag.StringVar(&f.org, "org", "", "org id every request is made for (required)")
	flag.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on every receipt (required)")
	flag.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every receipt (required)")
	flag.StringVar(&f.principalKind, "principal-kind", "stored_account", "auth-context SHAPE recorded in request_identity; never a credential")
	flag.StringVar(&f.audience, "audience", "query-api", "envelope audience, part of the auth-context shape")
	flag.StringVar(&f.keyID, "key-id", "", "envelope signing key id (kid) -- a public identifier")
	flag.StringVar(&f.artifactDir, "artifact-dir", "", "directory for content-addressed leg bodies and finding lists; go_api_rest_proof_run's baseline/candidate_response_ref name the two legs, and review_evidence names the finding-list ref -- never an inlined body (required), matching go-api-prove's own -artifact-dir")
	flag.BoolVar(&f.dryRun, "dry-run", false, "execute and compare, but write NO receipts")
	flag.DurationVar(&f.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.DurationVar(&f.runDeadline, "run-deadline", defaultRunDeadline, "how long the whole run may take; at the deadline the run stops between requests, names a leg in flight as cut by the run deadline, names every request it did not reach in the report's not_run, writes partial_cause run_deadline and exits non-zero. Must be greater than zero")
	flag.DurationVar(&f.gapDelay, "gap-reread-delay", goapiproof.GapRereadDefaultDelay, "how long the delayed re-read waits before it reads both planes again for a case whose difference the bracketed re-read left outside (or could not take up): it must outlast the 4-9 s materializer gap in which the Python investment scope gate fails open (CHAOS-5975; worst recorded ~12 s). 0 disables the stage; above 60s is refused")
	flag.DurationVar(&f.gapBudget, "gap-reread-budget", goapiproof.GapRereadDefaultBudget, "the most delay one run spends in delayed re-reads in total; a case that does not fit stands as the bracketed re-read left it")
	flag.StringVar(&f.reportPath, "report", "", "write the full JSON report here in addition to stdout")
	// flag.CommandLine.Parse, not the package-level flag.Parse (which
	// discards Parse's own returned error): -bind's own Func callback
	// above is the first flag in this command that can fail AT PARSE
	// TIME rather than only in the post-parse required-flag check below,
	// and that error must reach the caller -- under the default
	// ExitOnError handling this changes nothing (Parse still never
	// returns on an error, since Set already called os.Exit), but under
	// a test's ContinueOnError flag.CommandLine (resetFlagsForTest) it is
	// the only way a malformed -bind is ever observed at all, rather than
	// silently discarded.
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		return f, err
	}
	secrets.ResolveFlag(flag.CommandLine, &f.postgresURI, "postgres-uri", postgresURIEnvVar)

	if f.buildInfoURL == "" {
		f.buildInfoURL = strings.TrimRight(f.queryAPIURL, "/") + "/buildinfo"
	}
	var missing []string
	if f.pythonAPIURL == "" {
		missing = append(missing, "-python-api-url")
	}
	if f.candidateBearerExec == "" {
		missing = append(missing, "-candidate-bearer-exec")
	}
	if f.baselineBearerExec == "" {
		missing = append(missing, "-baseline-bearer-exec")
	}
	if f.org == "" {
		missing = append(missing, "-org")
	}
	if f.recordedBy == "" {
		missing = append(missing, "-recorded-by")
	}
	if f.reviewEvidence == "" {
		missing = append(missing, "-review-evidence")
	}
	if f.artifactDir == "" {
		missing = append(missing, "-artifact-dir")
	}
	if !f.dryRun && f.postgresURI == "" {
		missing = append(missing, "-postgres-uri (or -dry-run)")
	}
	if len(missing) > 0 {
		return f, fmt.Errorf("missing required flag(s): %s", strings.Join(missing, ", "))
	}
	if f.runDeadline <= 0 {
		return f, fmt.Errorf("-run-deadline must be greater than zero, got %s", f.runDeadline)
	}
	if err := validateGapRereadFlags(f.gapDelay, f.gapBudget); err != nil {
		return f, err
	}
	// Every base URL a leg or a /buildinfo read is built from: a fragment,
	// a query or userinfo would make the request sent differ from the one
	// named, so each is refused here, before anything is sent.
	for _, base := range []struct{ name, value string }{
		{"-query-api-url", f.queryAPIURL},
		{"-python-api-url", f.pythonAPIURL},
		{"-buildinfo-url", f.buildInfoURL},
	} {
		if err := goapiproof.ValidateBaseURL(base.name, base.value); err != nil {
			return f, err
		}
	}
	return f, nil
}

// prepareLegs does everything run() checks before the first corpus
// request, in order: it binds both credentials to -org, reads the build
// the receipts will name from /buildinfo, and asks the Python app, with
// the baseline credential, which org it serves that credential. Any
// refusal stops the run before a leg is sent.
func prepareLegs(ctx context.Context, client *goapiproof.LegClient, f flags, candidateCredential, baselineCredential *goapiproof.Credential, prover version.Info) (*goapiproof.ProverBuild, error) {
	// Both planes scope a request by the credential's org_id claim, so
	// every value either credential sends must name -org (and no
	// impersonation) -- checked on the exact value, each time it is set
	// on a request. The baseline credential also backs the
	// edge-credential leg on the candidate.
	candidateCredential.BindOrg(f.org)
	baselineCredential.BindOrg(f.org)

	// candidateCredential -- the SAME credential every corpus request's
	// own candidate leg uses below (doREST's own candidateCredential
	// argument in proveOneRESTRequest) -- never a second, separately
	// minted credential: /buildinfo is authenticated with the same
	// bearer-envelope verifier the REST routes themselves use
	// (buildinfo_route.go's own doc comment), so there is no "proof-
	// plane" credential distinct from what this binary already mints for
	// every other request in the run.
	//
	// Bounded by the run's own -timeout, same as every measured leg --
	// the leg client carries no Timeout of its own, so this read would
	// otherwise wait on ctx's 30-minute run-level bound alone.
	buildInfoCtx, cancelBuildInfo := readContext(ctx, f.timeout)
	namedBuild, err := goapiproof.FetchBuildIdentity(buildInfoCtx, client, f.buildInfoURL, candidateCredential)
	cancelBuildInfo()
	if err != nil {
		return nil, fmt.Errorf("read the candidate build from /buildinfo: %w", err)
	}
	// From here every return carries both builds, refusal or not, for
	// the report the stopped run writes.
	builds := goapiproof.NewProverBuild(prover, namedBuild, f.allowProverBuildSkew)
	fmt.Println(builds.Line())
	if f.candidateBuild != "" && f.candidateBuild != namedBuild {
		return &builds, fmt.Errorf("-candidate-build %q does not match the running build %q reported by %s", f.candidateBuild, namedBuild, f.buildInfoURL)
	}
	if err := builds.Check(proverBuildSkewFlag); err != nil {
		return &builds, err
	}

	// The Python app's own answer, with the baseline credential, naming
	// the org it serves that credential -- refused unless it is -org with
	// no impersonation session in force. Every baseline leg is still
	// checked for the impersonation stamp (RESTAdmit).
	principalCtx, cancelPrincipal := readContext(ctx, f.timeout)
	err = goapiproof.VerifyReferencePrincipal(principalCtx, client, f.pythonAPIURL, baselineCredential, f.org)
	cancelPrincipal()
	if err != nil {
		return &builds, err
	}
	return &builds, nil
}

// parseHelperArgv decodes a -*-bearer-exec flag's JSON array. argv[0] is
// an ALLOWLISTED HELPER NAME (goapiproof.MintViaAllowlistedHelper's own
// switch, never a path -- see that function's doc comment for why a
// runtime path can never satisfy go.lang.security.audit.dangerous-exec-
// command no matter how it is validated first, confirmed empirically
// against this repo's own Semgrep config). argv[1:] are the helper's own
// arguments, passed through unchanged.
func parseHelperArgv(flagName, raw string) ([]string, error) {
	var argv []string
	if err := json.Unmarshal([]byte(raw), &argv); err != nil {
		return nil, fmt.Errorf("%s: not a JSON array of strings: %w", flagName, err)
	}
	if len(argv) == 0 {
		return nil, fmt.Errorf("%s: empty argv", flagName)
	}
	return argv, nil
}

func buildCredential(header, kind, flagName, rawArgv string) (*goapiproof.Credential, error) {
	argv, err := parseHelperArgv(flagName, rawArgv)
	if err != nil {
		return nil, err
	}
	helperName, args := argv[0], argv[1:]
	// Same shape check cmd/go-api-prove applies to BOTH of its own
	// credentials (edge and proof): a helper that printed a usage line,
	// an error or a shell prompt instead of a token is caught here, as a
	// clearly-labelled "not a well-formed bearer" error, rather than
	// surfacing 200 requests later as an indistinguishable 401.
	return goapiproof.MintedCredential(header, kind, 25*time.Second, func(ctx context.Context) (string, error) {
		return goapiproof.MintViaAllowlistedHelper(ctx, helperName, args)
	}).WithShapeValidator(goapiproof.ValidateEnvelopeShape), nil
}

// /buildinfo is read via goapiproof.FetchBuildIdentity -- the same
// function cmd/go-api-prove uses for its own -buildinfo-url read, rather
// than a second, ad hoc decoder in this binary. That function refuses a
// redirect (NoRedirectClient), guards the commit string's UTF-8, and,
// load-bearing for an operator debugging a 401 here, distinguishes "the
// endpoint is unreachable" from "this credential was rejected" and names
// WHICH credential kind (candidate bearer) was rejected and why
// (EndpointLabel + credential.Kind(), never the credential's value) --
// see run()'s own call below.

// doREST sends one leg of one corpus request and returns its observation.
//
// A transport-level failure -- the leg never answered at all, whether it
// timed out or the connection failed some other way -- is returned as a
// goapiproof.TransportFailure, never a bare wrapped error: RESTAdmit
// exists to judge a response that arrived, never the absence of one, so
// the ABSENCE is reported in a shape a caller can classify by name (which
// leg, which failure class) and turn into a named, per-case refusal
// instead of having to string-match an error message. See
// proveOneRESTRequest/proveEdgeCredentialOnCandidate for where that
// classification happens -- doREST itself does not know which leg
// (baseline or candidate) it was called for, so it cannot name that part
// of the refusal itself.
//
// timeout bounds THIS call alone, by wrapping ctx -- never client's own
// Timeout, which go-api-rest-prove's own client leaves unset for exactly
// this reason: a fixed client-level timeout is a hard ceiling under Go's
// http.Client (it re-derives its own internal deadline from Timeout
// regardless of what the passed context already carries), so a per-
// request timeout LONGER than some other request's own budget would be
// silently capped back down to it. Zero means "no deadline beyond ctx's
// own", matching context.WithTimeout's own zero-is-unused convention.
//
// credential is nil for a PublicNoAuth route (goapiproof.RESTEndpointSpec.
// PublicNoAuth) -- the request is then sent with NO Authorization header
// on this leg at all, matching meta.go's own "Auth: PUBLIC" contract:
// measuring meta WITH a bearer token would exercise a path real anonymous
// traffic never takes, and a route that happens to also accept an
// unrelated valid token is not proof of the public code path.
func doREST(ctx context.Context, client *goapiproof.LegClient, baseURL, method, path string, query url.Values, body any, credential *goapiproof.Credential, timeout time.Duration) (goapiproof.RESTLeg, error) {
	target := strings.TrimRight(baseURL, "/") + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return goapiproof.RESTLeg{}, fmt.Errorf("encode request body: %w", err)
		}
		bodyReader = bytes.NewReader(encoded)
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, method, target, bodyReader)
	if err != nil {
		return goapiproof.RESTLeg{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if credential != nil {
		if err := credential.Apply(ctx, req); err != nil {
			return goapiproof.RESTLeg{}, err
		}
	}

	legResponse, err := client.Do(req)
	if err != nil {
		return goapiproof.RESTLeg{}, goapiproof.NewTransportFailure(target, err)
	}
	resp := legResponse.Response
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return goapiproof.RESTLeg{}, goapiproof.NewTransportFailure(target, err)
	}
	return goapiproof.RESTLeg{
		StatusCode:    resp.StatusCode,
		Body:          raw,
		Build:         resp.Header.Get("x-dev-health-build"),
		Server:        resp.Header.Get("Server"),
		Impersonating: goapiproof.ServedUnderImpersonation(resp.Header),
		WireAttempts:  legResponse.WireAttempts,
	}, nil
}

// resolveRESTTimeout is the per-request timeout a leg actually gets: the
// corpus entry's OWN declared Timeout when it set one -- a slow but
// legitimate leg can declare its own longer budget -- else the run's
// -timeout default.
func resolveRESTTimeout(perRequest, runDefault time.Duration) time.Duration {
	if perRequest > 0 {
		return perRequest
	}
	return runDefault
}

// unresolvedIDBindingDetail builds the RESTRefusalIDBindingUnresolved
// Detail for one or more Producer names ResolveRESTIDBindings could not
// resolve, naming EACH name's own cause: an operator-supplied producer
// (goapiproof.IsOperatorSuppliedIDProducer) reads as an operator
// omission -- the fix is a flag on THIS run's own invocation -- while any
// other name reads as a corpus-ordering fact, exactly as before. A run
// record built from this text names what an operator did or did not
// supply, never leaving a reader to guess whether a missing id was this
// run's own omission or a genuine corpus defect.
func unresolvedIDBindingDetail(unresolved []string) string {
	var operatorNames, producedNames []string
	for _, name := range unresolved {
		if goapiproof.IsOperatorSuppliedIDProducer(name) {
			operatorNames = append(operatorNames, name)
		} else {
			producedNames = append(producedNames, name)
		}
	}
	var parts []string
	if len(producedNames) > 0 {
		parts = append(parts, fmt.Sprintf("no earlier request in this run produced: %v", producedNames))
	}
	for _, name := range operatorNames {
		parts = append(parts, fmt.Sprintf("supply -bind %s=…", name))
	}
	return strings.Join(parts, "; ")
}

// legTransportOutcome turns a leg's OWN transport failure into the
// per-case refusal outcome this ticket exists to produce, naming the
// FAILING LEG and the FAILURE CLASS rather than folding it into an
// existing reason. ok is false when err is not itself a transport
// failure -- something other than "the leg never answered" went wrong,
// which is not this function's claim to make safe, and the caller keeps
// treating it as a fatal error.
func legTransportOutcome(runCtx context.Context, operation, requestName, leg string, boundIDs map[string]string, err error) (outcome, bool) {
	var failure goapiproof.TransportFailure
	if !errors.As(err, &failure) {
		return outcome{}, false
	}
	timedOut := failure.Class == goapiproof.TransportTimeout
	// The run's own context is read first: a leg whose request ended
	// because the run did (its deadline, or a signal) says nothing about
	// the plane, and is named for what cut it, never as a leg timeout.
	runEnded := runCtx.Err()
	var reason string
	switch {
	case errors.Is(runEnded, context.DeadlineExceeded) && leg == "candidate":
		reason = goapiproof.RESTRefusalCandidateLegCutByRunDeadline
	case errors.Is(runEnded, context.DeadlineExceeded):
		reason = goapiproof.RESTRefusalBaselineLegCutByRunDeadline
	case runEnded != nil && leg == "candidate":
		reason = goapiproof.RESTRefusalCandidateLegCutBySignal
	case runEnded != nil:
		reason = goapiproof.RESTRefusalBaselineLegCutBySignal
	case leg == "candidate" && timedOut:
		reason = goapiproof.RESTRefusalCandidateLegTimedOut
	case leg == "candidate":
		reason = goapiproof.RESTRefusalCandidateLegTransportError
	case leg == "baseline" && timedOut:
		reason = goapiproof.RESTRefusalBaselineLegTimedOut
	default:
		reason = goapiproof.RESTRefusalBaselineLegTransportError
	}
	return outcome{
		Operation: operation, Request: requestName,
		Admitted: false,
		Refusal:  reason,
		Detail:   err.Error(),
		BoundIDs: boundIDs,
	}, true
}

// isLegTransportRefusal reports whether reason is one of the four leg-
// transport refusals legTransportOutcome names -- used after a run
// completes to decide whether the process must exit non-zero even though
// every case that hit this class was recorded as an ordinary refusal, not
// a tool error.
func isLegTransportRefusal(reason string) bool {
	switch reason {
	case goapiproof.RESTRefusalCandidateLegTimedOut,
		goapiproof.RESTRefusalCandidateLegTransportError,
		goapiproof.RESTRefusalBaselineLegTimedOut,
		goapiproof.RESTRefusalBaselineLegTransportError:
		return true
	}
	return false
}

// legFailuresIn names, one line each, every outcome in outcomes whose
// Refusal is one of the four leg-transport reasons -- run()'s own,
// isolated answer to "did this run measure a leg that never answered",
// kept as its own function so that question is testable without driving
// the whole CLI (credentials, Postgres, a live query-api and python-api)
// through run() itself.
func legFailuresIn(outcomes []outcome) []string {
	var failures []string
	for _, out := range outcomes {
		if isLegTransportRefusal(out.Refusal) {
			failures = append(failures, fmt.Sprintf("%s/%s: %s -- %s", out.Operation, out.Request, out.Refusal, out.Detail))
		}
	}
	sort.Strings(failures)
	return failures
}

// plannedRequest is one (operation, request) pair a run intends to
// attempt, computed before any request is sent -- see run()'s own use of
// it and notRunKeys below.
type plannedRequest struct {
	operation string
	request   goapiproof.RESTRequest
	spec      goapiproof.RESTEndpointSpec
}

// planRESTRequests is every request a run sends, in run order, built in
// full BEFORE any request is sent, so a run that stops partway -- or
// before its first request -- can still say by NAME what it never got
// to. Its error is SpecForREST's, unreachable once ValidateRESTCorpus and
// ValidateRESTIDBindingOrder have passed, kept as a named failure rather
// than a panic on the day that stops being true.
func planRESTRequests() ([]plannedRequest, error) {
	var planned []plannedRequest
	for _, operation := range goapiproof.RESTRunOrder() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			return planned, fmt.Errorf("%s: %w", operation, err)
		}
		for _, request := range spec.Requests {
			planned = append(planned, plannedRequest{operation: operation, request: request, spec: spec})
		}
	}
	return planned, nil
}

// notRunKeys names, by "operation/request" key (and, for a request whose
// spec is not PublicNoAuth, its own "... (edge-credential-on-candidate)"
// sibling key too), every entry in planned that attemptedKeys does not
// mark true -- kept as its own function, isolated from run()'s HTTP and
// credential plumbing, so "what did a partial run never reach" is
// checkable directly against a hand-built plan.
func notRunKeys(planned []plannedRequest, attemptedKeys map[string]bool) []string {
	var notRun []string
	for _, p := range planned {
		key := p.operation + "/" + p.request.Name
		if !attemptedKeys[key] {
			notRun = append(notRun, key)
		}
		if !p.spec.PublicNoAuth {
			edgeKey := key + " (edge-credential-on-candidate)"
			if !attemptedKeys[edgeKey] {
				notRun = append(notRun, edgeKey)
			}
		}
	}
	sort.Strings(notRun)
	return notRun
}

// restRequestVariables is what RequestIdentity digests for one corpus
// request -- the method, query and body, the same "identity is the actual
// request shape, never the operation name" rule identity.go's own
// RequestIdentity doc comment states for GraphQL variables.
func restRequestVariables(method string, query url.Values, body any) map[string]any {
	return map[string]any{"method": method, "query": query, "body": body}
}

// restReviewEvidence is what go_api_rest_proof_run.review_evidence
// actually carries -- a JSON envelope, not the operator's raw prose,
// mirroring go-api-prove's own ReceiptProvenance (internal/goapiproof/
// run.go's own doc comment: "a JSON OBJECT rather than prose ... a reader
// cannot tell which half a machine wrote" was the earlier, rejected
// design there too). go_api_rest_proof_run (alembic 0134) carries no
// findings_ref column of its own -- this ticket does not migrate the
// table -- so the finding-list artifact ref this run wrote rides inside
// this already-durable Text column instead of nowhere at all.
type restReviewEvidence struct {
	// Operator is -review-evidence's own text, verbatim, in its own key --
	// never modified or appended to, for the identical reason
	// ReceiptProvenance.Operator's own doc comment gives.
	Operator string `json:"operator,omitempty"`
	// FindingsRef is the -artifact-dir reference to this comparison's
	// decoded finding list (goapiproof.Result.Findings), set only when
	// this request's BodyMode decoded and compared both legs.
	FindingsRef string `json:"findings_ref,omitempty"`
	// PythonForwarderOffAttested is set on a receipt for a
	// PythonForwarder endpoint compared under -python-forwarder-off: the
	// baseline is taken as Python's own answer on the operator's word,
	// not on anything the response showed.
	PythonForwarderOffAttested bool `json:"python_forwarder_off_attested,omitempty"`
	// AdmittedBaselineRef and AdmittedCandidateRef name the response pair a
	// re-read admission (write-skew or delayed gap re-read) stands on. The
	// receipt's own BaselineResponseRef/CandidateResponseRef keep the FIRST
	// comparison (the mismatch the citation excuses); when a re-read admits
	// the case, the baseline that changed and the candidate it was matched
	// to are recorded here, so the durable receipt identifies both pairs.
	// Empty when no re-read admitted the case.
	AdmittedBaselineRef  string `json:"admitted_baseline_ref,omitempty"`
	AdmittedCandidateRef string `json:"admitted_candidate_ref,omitempty"`
}

// encodeRESTReviewEvidence renders restReviewEvidence for one receipt.
// Falls back to the raw operator string on a marshal error (both fields
// are plain strings, so this cannot actually fail) rather than writing
// nothing -- same fallback ReceiptProvenance's own reviewEvidence method
// uses, and the same reasoning: the operator's own words are worth more
// than a dropped column.
func encodeRESTReviewEvidence(operator, findingsRef string, forwarderOffAttested bool, admittedBaselineRef, admittedCandidateRef string) string {
	encoded, err := json.Marshal(restReviewEvidence{
		Operator: operator, FindingsRef: findingsRef, PythonForwarderOffAttested: forwarderOffAttested,
		AdmittedBaselineRef: admittedBaselineRef, AdmittedCandidateRef: admittedCandidateRef,
	})
	if err != nil {
		return operator
	}
	return string(encoded)
}

// outcome is one corpus request's result line.
type outcome struct {
	Operation string `json:"operation"`
	Request   string `json:"request"`
	Admitted  bool   `json:"admitted"`
	Refusal   string `json:"refusal,omitempty"`
	Detail    string `json:"detail,omitempty"`

	TerminalState                    string   `json:"terminal_state,omitempty"`
	DifferencesOutsideBaselineDefect int      `json:"differences_outside_baseline_defect,omitempty"`
	BaselineDefectsMatched           []string `json:"baseline_defect,omitempty"`

	ReceiptID string `json:"receipt_id,omitempty"`

	// Attempts names every LOSING candidate a bounded-candidate binding
	// (goapiproof.RESTIDBinding.Candidates > 0) tried on THIS request
	// before either the winning candidate or exhaustion -- see
	// resolveIteratingRequest's own doc comment. Empty for every request
	// with no bounded-candidate binding, the overwhelming majority: this
	// is a deliberate ONE outcome row per corpus request, even when
	// finding its winner (or exhausting its bound) took several tries --
	// attempted/admitted bookkeeping and the JSON report's one-row-per-
	// request shape stay exactly as they were before this mechanism
	// existed.
	Attempts []attemptRecord `json:"attempts,omitempty"`

	// WriteSkew records the bracketed re-read (goapiproof.ClassifyWriteSkew)
	// when the first comparison left only value leaves outside every
	// declaration: the baseline was read a second time after the
	// candidate, and each outside leaf carries both baseline reads and the
	// candidate value. Nil for every case that needed no re-read.
	WriteSkew *writeSkewRecord `json:"write_skew,omitempty"`

	// GapReread records the delayed re-read (goapiproof.ClassifyGapReread)
	// for a case the bracketed re-read left outside or could not take up.
	// Nil for every case that did not reach it.
	GapReread *gapRereadRecord `json:"gap_reread,omitempty"`

	// ObservedAt and CandidateObservedAt are when the first baseline and
	// first candidate reads of this case returned (UTC), to line a case up
	// with a write the data plane made.
	ObservedAt          time.Time `json:"observed_at,omitzero"`
	CandidateObservedAt time.Time `json:"candidate_observed_at,omitzero"`

	// BaselineResponseRef/CandidateResponseRef mirror the identically
	// named RESTReceipt fields (goapiproof.RESTReceipt's own doc
	// comment) -- the content-addressed -artifact-dir reference to each
	// leg's raw response body. Set whenever -artifact-dir stored a body,
	// which is BEFORE admission runs (proveOneRESTRequest stores both
	// legs first) -- so a REFUSED request's own outcome line still shows
	// where its bodies landed, not only an admitted one's.
	BaselineResponseRef  string `json:"baseline_response_ref,omitempty"`
	CandidateResponseRef string `json:"candidate_response_ref,omitempty"`

	// BaselineWireAttempts/CandidateWireAttempts are the wire attempts
	// the transport made for each leg (goapiproof.LegResponse): 1 for a
	// leg sent once, more when the standard library resent it on its own
	// (a failed reused connection, an HTTP/2 stream replay). Recorded, not
	// prevented; zero when the leg was never sent.
	BaselineWireAttempts  int `json:"baseline_wire_attempts,omitempty"`
	CandidateWireAttempts int `json:"candidate_wire_attempts,omitempty"`

	// FindingsRef is the -artifact-dir reference to this comparison's
	// decoded finding list (goapiproof.Result.Findings), set only when
	// BodyMode decoded and compared both legs. go_api_rest_proof_run
	// (alembic 0134) carries no findings_ref column -- this ticket does
	// not migrate the table -- so the same ref is also folded into the
	// written receipt's own review_evidence (see encodeRESTReviewEvidence),
	// which DOES persist to the row; this field exists so the JSON report
	// and a refused/dry-run outcome (neither of which reaches a receipt
	// at all) can still show it.
	FindingsRef string `json:"findings_ref,omitempty"`

	// BoundIDs names every id (goapiproof.RESTIDBinding.Producer -> the
	// resolved value) this request's Query/Path were bound to before
	// either leg was sent -- see goapiproof.ResolveRESTIDBindings and
	// RESTReceipt.BoundIDs' own doc comment for why ids, unlike a bearer
	// token, are safe to print here. Empty for every request with no
	// IDBindings, the overwhelming majority.
	BoundIDs map[string]string `json:"bound_ids,omitempty"`

	// vacuityErrors names every stale/unused declaration this comparison
	// found -- see resultVacuityErrors. Non-empty here fails the whole run
	// (see run's own closing check), same discipline compare.go's Result
	// doc comments state for every one of these fields.
	vacuityErrors []string

	// producedIDs names every id (goapiproof.RESTIDProducer.Name -> the
	// extracted value) this request's OWN baseline response yielded for a
	// LATER request's IDBindings -- see goapiproof.ExtractRESTID. Never
	// serialised: it is run()'s own bookkeeping between one request and
	// the next, not a fact about this request worth reporting on its own
	// outcome line (a later consumer's BoundIDs already reports the same
	// value where it matters).
	producedIDs map[string]string

	// producedCandidateIDs names, for every producer this request's OWN
	// baseline (or, for a StatusOnly+Produces request, candidate)
	// response yielded, EVERY non-empty candidate id it found, in array
	// order -- goapiproof.ExtractRESTIDCandidates' full result, not only
	// producedIDs' first-element value. Never serialised, same bookkeeping
	// discipline as producedIDs; the run loop merges this into its own
	// producedCandidates map so a LATER request's own bounded-candidate
	// binding (goapiproof.RESTIDBinding.Candidates) has a pool to draw
	// from. Populated for every producer regardless of whether anything
	// downstream ever uses more than the first candidate -- computing the
	// full list costs nothing ExtractRESTID was not already paying to
	// walk the same body.
	producedCandidateIDs map[string][]string

	// DeclarationFiring reports, one entry per BaselineDefect this
	// request declares, that ticket's own firing history read back from
	// go_api_rest_proof_run -- see goapiproof.FiringHistory's own doc
	// comment. Empty when this request declares no BaselineDefects, or
	// when no receipt was written for it this run (a refused or
	// dry-run request extends no history to read back against).
	DeclarationFiring []goapiproof.FiringHistory `json:"declaration_firing,omitempty"`
}

// attemptRecord is one losing candidate a bounded-candidate binding
// (goapiproof.RESTIDBinding.Candidates > 0) tried before either the
// winning candidate or exhaustion -- see resolveIteratingRequest's own
// doc comment. Attached to the FINAL outcome for that request (the
// winner's, or an exhaustion refusal's), never emitted as its own
// outcome/receipt.
type attemptRecord struct {
	CandidateID string `json:"candidate_id"`
	Refusal     string `json:"refusal"`
	Detail      string `json:"detail,omitempty"`
}

func attemptsSuffix(attempts []attemptRecord) string {
	if len(attempts) == 0 {
		return ""
	}
	ids := make([]string, len(attempts))
	for i, a := range attempts {
		ids[i] = a.CandidateID
	}
	return fmt.Sprintf(" attempts=%v", ids)
}

// writeSkewRecord is one case's bracketed re-read: its verdict, the
// outside leaves with the first baseline value, the candidate value and
// the second baseline value, and where the second baseline body landed in
// -artifact-dir.
type writeSkewRecord struct {
	Verdict                   goapiproof.WriteSkewVerdict `json:"verdict"`
	Leaves                    []goapiproof.WriteSkewLeaf  `json:"leaves,omitempty"`
	Detail                    string                      `json:"detail,omitempty"`
	SecondBaselineResponseRef string                      `json:"second_baseline_response_ref,omitempty"`
	// SecondBaselineWireAttempts is the second read's wire attempts
	// (goapiproof.LegResponse), recorded like every leg's.
	SecondBaselineWireAttempts int `json:"second_baseline_wire_attempts,omitempty"`
	// SecondBaselineAt is when the second baseline read returned.
	SecondBaselineAt time.Time `json:"second_baseline_at,omitzero"`
	// referenceUnmoved is the decision's ReferenceUnmoved (never serialised).
	referenceUnmoved bool
}

// writeSkewSuffix names a re-read's verdict on the stdout line, so a
// skew-admitted case never reads like an ordinary cited mismatch.
func writeSkewSuffix(record *writeSkewRecord) string {
	if record == nil {
		return ""
	}
	return fmt.Sprintf(" write_skew=%s", record.Verdict)
}

// gapRereadSuffix names the delayed re-read's outcome on the stdout line.
func gapRereadSuffix(record *gapRereadRecord) string {
	if record == nil {
		return ""
	}
	return fmt.Sprintf(" gap_reread=%s", record.Outcome)
}

func (o outcome) line() string {
	if !o.Admitted {
		return fmt.Sprintf("%s/%s: REFUSED %s -- %s%s%s", o.Operation, o.Request, o.Refusal, o.Detail, attemptsSuffix(o.Attempts), writeSkewSuffix(o.WriteSkew)+gapRereadSuffix(o.GapReread))
	}
	boundSuffix := ""
	if len(o.BoundIDs) > 0 {
		boundSuffix = fmt.Sprintf(" bound=%v", o.BoundIDs)
	}
	firingSuffix := ""
	if len(o.DeclarationFiring) > 0 {
		firingSuffix = " " + formatDeclarationFiring(o.DeclarationFiring)
	}
	return fmt.Sprintf("%s/%s: %s outside=%d baseline_defect=%v receipt=%s%s%s%s%s",
		o.Operation, o.Request, o.TerminalState, o.DifferencesOutsideBaselineDefect, o.BaselineDefectsMatched, o.ReceiptID, boundSuffix, firingSuffix, attemptsSuffix(o.Attempts), writeSkewSuffix(o.WriteSkew)+gapRereadSuffix(o.GapReread))
}

// formatDeclarationFiring renders one request's own declared defects'
// firing history for the stdout line -- the JSON report
// (outcome.DeclarationFiring) carries the same facts for a reader who
// wants the exact numbers rather than a one-line summary. NEVER_FIRED is
// printed as a plain word in this line, exactly like every other
// terminal-state word already printed here: nothing about its presence
// changes attempted/admitted/match/mismatch/refused counts, and nothing
// here changes run()'s own exit status.
//
// Every declaration always prints its own "window: N of FiringWindow
// known runs", flagged or not: a reader seeing no NEVER_FIRED word must
// be able to tell an incomplete window (still filling, healthy or not)
// apart from a complete, clean one -- an incomplete window that prints
// nothing would reproduce, in miniature, the exact silence-reads-as-
// health gap this whole report exists to close.
func formatDeclarationFiring(histories []goapiproof.FiringHistory) string {
	parts := make([]string, len(histories))
	for i, h := range histories {
		state := ""
		if h.NeverFired {
			state = " NEVER_FIRED"
		}
		lastFired := h.LastFiredBuild
		if lastFired == "" {
			lastFired = "-"
		}
		parts[i] = fmt.Sprintf("%s(live=%d fired=%d last_fired=%s window: %d of %d known runs%s)",
			h.Ticket, h.RunsLive, h.RunsFired, lastFired, h.WindowKnownRuns, goapiproof.FiringWindow, state)
	}
	return "declarations[" + strings.Join(parts, " ") + "]"
}

// resultVacuityErrors names every declaration in result that matched
// nothing -- a citation, exclusion or Tier-B entry that excuses nothing
// is either stale or misspelled -- from the SAME shared definition the
// GraphQL prover refuses on, goapiproof.Result.Acceptance (compare.go).
// It reports only the SOFT half (Hard == false): a soft refusal means the
// declaration matched nothing else in an otherwise fully-executed,
// honest comparison, so this run() (see its own closing check) may fail
// the whole batch on it rather than refusing the one request immediately.
// A HARD refusal (Acceptance's own doc comment: TerminalState may read
// match despite something never actually being checked) is never soft --
// proveOneRESTRequest refuses that request outright, with no receipt,
// before resultVacuityErrors ever runs, so a Hard entry can never reach
// here in the first place; the filter stays as a second, defensive line
// so this function is correct standing alone too.
func resultVacuityErrors(result goapiproof.Result) []string {
	var errs []string
	for _, refusal := range result.Acceptance() {
		if refusal.Hard {
			continue
		}
		errs = append(errs, refusal.Detail)
	}
	return errs
}

func run(f flags) (err error) {
	// A run that stops before its request loop still writes -report: it
	// names every planned request in not_run and says why in
	// partial_cause. Registered first, so it runs last, on the redacted
	// error; from the loop on, runMeasurement writes the report itself.
	// runEnded is the run context's own Err() as the run exits, read
	// before this function's own cancel() runs (which would otherwise
	// read as a signal).
	var runEnded error
	var resolved *goapiproof.ProverBuild
	measuring := false
	defer func() {
		if err == nil || measuring {
			return
		}
		if writeErr := writeStoppedBeforeMeasuringReport(f, resolved, runEnded, err); writeErr != nil {
			err = fmt.Errorf("%w; additionally, writing the report failed: %v", err, writeErr)
		}
	}()

	// A single boundary, applied here via defer, covers every error this
	// function returns, no matter which layer produced it or whether
	// that layer remembered the DSN could be inside -- construct it
	// once, from the DSN this run actually resolved, and apply it to
	// the named return AFTER the fact rather than trusting every call
	// site downstream to redact its own. A bare `return expr` still
	// assigns expr to `err` before this defer runs, so every return in
	// the rest of the function is covered.
	boundary := secrets.NewBoundary(f.postgresURI)
	defer func() { err = boundary.Redact(err) }()

	// signal.NotifyContext gives an operator's Ctrl-C (or a SIGTERM from
	// the surrounding orchestration) a chance to stop the run between
	// requests rather than kill the process outright -- see the ctx.Err()
	// checks in the request loop below. Cheap: this changes nothing for a
	// run nobody interrupts.
	ctx, cancel := runContext(f.runDeadline)
	defer func() {
		runEnded = ctx.Err()
		cancel()
	}()

	if f.pythonForwarderOff {
		fmt.Println("python_forwarder_off=attested: a 200 baseline on an endpoint the Python app can forward is compared as Python's own answer, and each such receipt records the attestation")
	}

	// Checked FIRST, ahead of every credential/network step below: both
	// need no network and (in the default, -query-api-src-unset case) no
	// filesystem either, so a corpus/coverage problem is refused
	// immediately rather than after a wasted round-trip -- and, not
	// incidentally, this ordering is what lets a smoke run from a
	// directory with no source tree at all prove the coverage check on
	// its own, before anything network-dependent has a chance to fail
	// first for an unrelated reason. Nothing has been attempted yet at
	// any of these steps, so there is no report to write if one refuses.
	var mountedPaths []string
	if f.queryAPISrc != "" {
		// OPTIONAL dev-only override: read a REAL query-api checkout live,
		// to catch drift immediately instead of waiting for
		// TestMountedRESTPathsMatchesTheRealQueryAPIMux's own CI run. Off
		// by default -- see that flag's own doc string for why the
		// runtime image cannot use this path at all.
		mounted, err := migrationmatrix.LoadQueryAPIMuxRoutes(f.queryAPISrc)
		if err != nil {
			return fmt.Errorf("read query-api's mounted REST routes from %s: %w", f.queryAPISrc, err)
		}
		mountedPaths = make([]string, 0, len(mounted))
		for _, route := range mounted {
			mountedPaths = append(mountedPaths, route.Path)
		}
	} else {
		mountedPaths = goapiproof.MountedRESTPaths()
	}
	if err := goapiproof.AssertRESTPathCoverage(mountedPaths); err != nil {
		return err
	}
	if err := goapiproof.ValidateRESTCorpus(); err != nil {
		return err
	}

	// Built before any credential or network step for the same reason the
	// coverage/corpus checks above are: a bad -artifact-dir is a
	// configuration error, refused before this run spends a single
	// request. Every leg body and finding list this run produces is
	// stored here (see proveOneRESTRequest) -- go-api-rest-prove's own
	// sibling to go-api-prove's identical -artifact-dir contract
	// (cmd/go-api-prove/main.go), so a mismatch this run finds can be
	// read back after the fact instead of existing only in this
	// process's stdout.
	artifacts, err := goapiproof.NewArtifactStore(f.artifactDir)
	if err != nil {
		return err
	}

	candidateCredential, err := buildCredential("Authorization", "candidate bearer", "-candidate-bearer-exec", f.candidateBearerExec)
	if err != nil {
		return err
	}
	baselineCredential, err := buildCredential("Authorization", "baseline bearer", "-baseline-bearer-exec", f.baselineBearerExec)
	if err != nil {
		return err
	}

	// No client-level Timeout: Go's http.Client re-derives its own
	// internal deadline from a fixed Timeout regardless of what a
	// request's own context already carries, so a client-level ceiling
	// here would silently cap every per-request context.WithTimeout doREST
	// builds back down to it -- defeating RESTRequest.Timeout's own point
	// (a slow but legitimate entry declaring a LONGER budget than the
	// run's default). Every request this run sends is bounded instead by
	// its own context deadline -- see doREST and resolveRESTTimeout.
	client := goapiproof.NewLegClient(0)

	resolved, err = prepareLegs(ctx, client, f, candidateCredential, baselineCredential, version.Current("go-api-rest-prove"))
	if err != nil {
		return err
	}

	var pgPool receiptWriter
	if f.postgresURI != "" {
		pool, err := newPGXPool(ctx, f.postgresURI)
		if err != nil {
			return err
		}
		defer pool.Close()
		pgPool = pool
	}

	measuring = true
	return runMeasurement(ctx, client, f, candidateCredential, baselineCredential, *resolved, pgPool, artifacts)
}

// readContext bounds one opening read by -timeout; zero or less means no
// per-read deadline, as on every request leg (doREST).
func readContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// proverBuildSkewFlag permits a run whose prover build is not the
// candidate's.
const proverBuildSkewFlag = "-allow-prover-build-skew"

// writeStoppedBeforeMeasuringReport writes the report of a run that
// stopped before its first request: every planned request in not_run,
// and partial_cause from the run context's own state (run_deadline,
// signal), else refused_before_measuring. runEnded is the run context's
// own Err() as the run exited.
func writeStoppedBeforeMeasuringReport(f flags, builds *goapiproof.ProverBuild, runEnded, stopErr error) error {
	planned, _ := planRESTRequests()
	notRun := notRunKeys(planned, nil)
	cause := stopCause(runEnded, nil)
	exitCause := exitCauseFromPartial(cause)
	if cause == "" {
		cause = partialCauseRefusedBeforeMeasuring
		exitCause = exitRefusedBeforeMeasuring
	}
	// Printed whether or not -report was given: stdout carries the same
	// accounting the report file does.
	fmt.Printf("partial run: %d request(s) never attempted: %v\npartial_cause=%s\nexit_cause=%s\n", len(notRun), notRun, cause, exitCause)
	if f.reportPath == "" {
		return nil
	}
	return writeJSONReport(f.reportPath, jsonReport{
		ProverBuild:  builds,
		Outcomes:     []outcome{},
		NotRun:       notRun,
		PartialCause: cause,
		PartialError: stopErr.Error(),
		ExitCause:    exitCause,
		RunDeadline:  f.runDeadline.String(),
	})
}

// exitCauseFromPartial maps a run-level stop (run deadline, signal) to its
// exit cause; empty for anything else.
func exitCauseFromPartial(partialCause string) string {
	switch partialCause {
	case partialCauseRunDeadline:
		return exitStoppedByRunDeadline
	case partialCauseSignal:
		return exitStoppedBySignal
	}
	return ""
}

// runMeasurement is run()'s own request loop, report write and exit-code
// decision, factored out as its own seam: run() itself cannot be driven
// in a test without a real minted credential (buildCredential execs a
// FIXED path, /usr/local/bin/mint-envelope or /usr/local/bin/mint-edge-
// token -- see mintexec.go's own doc comment for why that path can never
// be a caller-supplied value) and, outside -dry-run, a real Postgres.
// Every dependency runMeasurement itself needs is already a value or an
// interface a test can fake -- staticCredentialForTest() (used
// throughout this file's own tests already), an httptest server pair,
// and (for -dry-run) a nil receiptWriter -- so the exact loop/report/
// exit-code logic this ticket rewrote is reachable from a test without
// reaching into run()'s own CLI wiring at all.
// resolvedAttempt is what resolveSingleShotRequest and
// resolveIteratingRequest both return: the request loop's own
// post-processing (outcomes append, produced/producedCandidates merge,
// admitted counting, the edge-credential leg) runs identically over
// either one, never caring which binding shape produced it.
type resolvedAttempt struct {
	out     outcome
	spec    goapiproof.RESTEndpointSpec
	request goapiproof.RESTRequest
	// legsSent is false when this request was refused before either leg
	// was ever sent -- an unresolved binding (single-shot) or an
	// exhausted bounded-candidate search -- in which case spec/request
	// are the UNRESOLVED originals and the edge-credential leg must be
	// skipped (there is nothing resolved to send it against).
	legsSent bool
}

// findIteratingBinding returns request's own bounded-candidate binding
// (goapiproof.RESTIDBinding.Candidates > 0) and true, or the zero value
// and false when none is declared. ValidateRESTCorpus enforces at most
// one such binding per request, so the first match is the only one there
// can ever be.
func findIteratingBinding(bindings []goapiproof.RESTIDBinding) (goapiproof.RESTIDBinding, bool) {
	for _, b := range bindings {
		if b.Candidates > 0 {
			return b, true
		}
	}
	return goapiproof.RESTIDBinding{}, false
}

// resolveSingleShotRequest is run()'s own pre-iteration request
// resolution, unchanged: it takes every IDBindings.Producer's single
// first-extracted id from produced and, if every binding resolves, runs
// proveOneRESTRequest exactly once. This is still the ONLY path a request
// with no bounded-candidate binding ever takes -- byte-for-byte the same
// behaviour this file always had.
func resolveSingleShotRequest(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	produced map[string]string,
	candidateCredential, baselineCredential *goapiproof.Credential,
	namedBuild string,
	auth goapiproof.AuthContext,
	observedAt time.Time,
	writer receiptWriter,
	artifacts *goapiproof.ArtifactStore,
) (resolvedAttempt, error) {
	resolvedSpec := spec
	resolvedRequest := request
	var boundIDs map[string]string
	if len(request.IDBindings) > 0 {
		resolvedPath, resolvedQuery, resolvedBody, unresolved := goapiproof.ResolveRESTIDBindings(spec.Path, request, produced)
		if len(unresolved) > 0 {
			// An entry whose id does not resolve is refused by name and
			// counts as unproven, never a tool failure -- neither leg is
			// ever called.
			out := outcome{
				Operation: operation, Request: request.Name,
				Admitted: false,
				Refusal:  goapiproof.RESTRefusalIDBindingUnresolved,
				Detail:   unresolvedIDBindingDetail(unresolved),
			}
			return resolvedAttempt{out: out, spec: resolvedSpec, request: resolvedRequest, legsSent: false}, nil
		}
		resolvedSpec.Path = resolvedPath
		resolvedRequest.Query = resolvedQuery
		resolvedRequest.Body = resolvedBody
		boundIDs = make(map[string]string, len(request.IDBindings))
		for _, binding := range request.IDBindings {
			// Already confirmed present above (unresolved was empty):
			// the same value ResolveRESTIDBindings just wrote into
			// resolvedPath/resolvedQuery.
			boundIDs[binding.Producer] = produced[binding.Producer]
		}
	}

	out, err := proveOneRESTRequest(ctx, client, f, operation, resolvedSpec, resolvedRequest, candidateCredential, baselineCredential, namedBuild, auth, observedAt, writer, artifacts, f.dryRun, boundIDs)
	if err != nil {
		return resolvedAttempt{}, err
	}
	return resolvedAttempt{out: out, spec: resolvedSpec, request: resolvedRequest, legsSent: true}, nil
}

// resolveIteratingRequest is the bounded-candidate-iteration sibling of
// resolveSingleShotRequest: instead of taking iterating's own Producer's
// single first-extracted id, it tries up to iterating.Candidates of that
// producer's own candidate pool (producedCandidates), in order, actually
// sending BOTH legs for each -- exactly proveOneRESTRequest's own cost,
// paid once per candidate -- until one candidate's own request.Produces
// all resolve (that candidate wins) or the bound is exhausted. Only a
// candidate refused for having no data (candidateHasNoData) moves the
// search on; any other refusal ends it and is reported as this request's
// own outcome.
//
// A losing attempt writes no receipt: proveOneRESTRequest's own
// Produces-unresolved branch returns a refused outcome before ever
// reaching WriteReceipt (see that function's own doc comment), so only
// the eventual winner -- or nothing, on exhaustion -- is ever persisted.
// Every losing attempt is recorded on the WINNING (or exhausted)
// outcome's own Attempts field, never as a separate outcome/receipt of
// its own, so attempted/admitted bookkeeping and the JSON report's
// one-row-per-request shape are unaffected by how many candidates it
// took.
//
// On a win, iterating.ExposeAs (required non-empty by ValidateRESTCorpus
// whenever Candidates > 0) is added to the winning outcome's own
// producedIDs, mapped to the WINNING candidate's id -- not anything
// extracted from a response body -- so the request loop's ordinary
// produced-merge step (identical for both this and the single-shot path)
// makes it available to a later request's own IDBindings exactly like an
// ordinary producer would. On exhaustion, ExposeAs is never set in
// produced: a sibling bound to it refuses separately, through the
// EXISTING RESTRefusalIDBindingUnresolved path resolveSingleShotRequest
// above already implements -- no special-casing needed for that cascade.
func resolveIteratingRequest(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	iterating goapiproof.RESTIDBinding,
	produced map[string]string,
	producedCandidates map[string][]string,
	candidateCredential, baselineCredential *goapiproof.Credential,
	namedBuild string,
	auth goapiproof.AuthContext,
	observedAt time.Time,
	writer receiptWriter,
	artifacts *goapiproof.ArtifactStore,
) (resolvedAttempt, error) {
	pool := producedCandidates[iterating.Producer]
	bound := iterating.Candidates
	if bound > len(pool) {
		bound = len(pool)
	}

	var attempts []attemptRecord
	for i := 0; i < bound; i++ {
		candidateID := pool[i]
		attemptProduced := make(map[string]string, len(produced)+1)
		for k, v := range produced {
			attemptProduced[k] = v
		}
		attemptProduced[iterating.Producer] = candidateID

		resolvedPath, resolvedQuery, resolvedBody, unresolved := goapiproof.ResolveRESTIDBindings(spec.Path, request, attemptProduced)
		if len(unresolved) > 0 {
			// A SIBLING binding on this same request (never the
			// iterating one -- candidateID always resolves it) has not
			// itself produced an id. Every candidate would fail
			// identically, but recording it per-candidate keeps this
			// function's own "every attempt recorded" contract uniform
			// rather than special-casing an early exit.
			attempts = append(attempts, attemptRecord{
				CandidateID: candidateID,
				Refusal:     goapiproof.RESTRefusalIDBindingUnresolved,
				Detail:      unresolvedIDBindingDetail(unresolved),
			})
			continue
		}
		resolvedSpec := spec
		resolvedSpec.Path = resolvedPath
		resolvedRequest := request
		resolvedRequest.Query = resolvedQuery
		resolvedRequest.Body = resolvedBody
		boundIDs := make(map[string]string, len(request.IDBindings))
		for _, binding := range request.IDBindings {
			boundIDs[binding.Producer] = attemptProduced[binding.Producer]
		}

		out, err := proveOneRESTRequest(ctx, client, f, operation, resolvedSpec, resolvedRequest, candidateCredential, baselineCredential, namedBuild, auth, observedAt, writer, artifacts, f.dryRun, boundIDs)
		if err != nil {
			return resolvedAttempt{}, err
		}
		if !out.Admitted {
			if !candidateHasNoData(out.Refusal) {
				// A failure of a plane (a status, transport or structural
				// refusal) ends the search on this candidate: trying the
				// next one could only replace this real failure with a
				// later match. Nothing is exposed, so siblings bound to
				// ExposeAs refuse by name.
				out.Attempts = attempts
				return resolvedAttempt{out: out, spec: resolvedSpec, request: resolvedRequest, legsSent: true}, nil
			}
			attempts = append(attempts, attemptRecord{CandidateID: candidateID, Refusal: out.Refusal, Detail: out.Detail})
			continue
		}

		// This candidate's own request.Produces all resolved -- the win
		// condition.
		if iterating.ExposeAs != "" {
			if out.producedIDs == nil {
				out.producedIDs = map[string]string{}
			}
			out.producedIDs[iterating.ExposeAs] = candidateID
		}
		out.Attempts = attempts
		return resolvedAttempt{out: out, spec: resolvedSpec, request: resolvedRequest, legsSent: true}, nil
	}

	// Exhausted: no candidate's own Produces resolved (or the pool was
	// empty/shorter than the bound to begin with). Refused by name,
	// naming every candidate tried.
	tried := make([]string, len(attempts))
	for i, a := range attempts {
		tried[i] = a.CandidateID
	}
	out := outcome{
		Operation: operation, Request: request.Name,
		Admitted: false,
		Refusal:  goapiproof.RESTRefusalCandidateIterationExhausted,
		Detail:   fmt.Sprintf("tried %d candidate(s) %v, none produced this request's own declared ids", len(attempts), tried),
		Attempts: attempts,
	}
	return resolvedAttempt{out: out, spec: spec, request: request, legsSent: false}, nil
}

// declaredListsEmpty reports whether every declared Produces list is an
// empty array in body.
func declaredListsEmpty(body any, produces []goapiproof.RESTIDProducer) bool {
	for _, producer := range produces {
		if !goapiproof.RESTIDListIsEmpty(body, producer) {
			return false
		}
	}
	return true
}

// bracketedReread reads the baseline a second time, admits that leg the
// same way the first was admitted (status, JSON body, alongside the one
// candidate leg), applies the same dedup-key injection, and classifies
// the case. refused is non-nil when the case must be refused: the second
// read failed in transport or admission, or an outside leaf moved to a
// third value.
func bracketedReread(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	baselineCredential *goapiproof.Credential,
	timeout time.Duration,
	namedBuild string,
	candidateLeg goapiproof.RESTLeg,
	admission goapiproof.RESTAdmission,
	first goapiproof.Result,
	artifacts *goapiproof.ArtifactStore,
) (*writeSkewRecord, *outcome, goapiproof.Snapshot, goapiproof.Result, error) {
	record := &writeSkewRecord{Verdict: goapiproof.WriteSkewRefused}
	var none goapiproof.Snapshot
	secondLeg, err := doREST(ctx, client, f.pythonAPIURL, spec.Method, spec.Path, request.Query, request.Body, baselineCredential, timeout)
	if err != nil {
		transport, ok := legTransportOutcome(ctx, "", request.Name, "baseline", nil, err)
		if !ok {
			return nil, nil, none, goapiproof.Result{}, fmt.Errorf("second baseline leg: %w", err)
		}
		record.Detail = "second baseline read: " + transport.Detail
		return record, &outcome{Refusal: transport.Refusal, Detail: record.Detail}, none, goapiproof.Result{}, nil
	}
	if artifacts != nil {
		if record.SecondBaselineResponseRef, err = artifacts.Put(secondLeg.Body); err != nil {
			return nil, nil, none, goapiproof.Result{}, fmt.Errorf("store second baseline leg artifact: %w", err)
		}
	}
	record.SecondBaselineAt = time.Now().UTC()
	record.SecondBaselineWireAttempts = secondLeg.WireAttempts
	// The second read is admitted exactly as the first was: the same
	// plane identity (server, build header, impersonation stamp) and the
	// same forwarder rule, so a re-read can never be a different plane's
	// answer.
	secondAdmission := goapiproof.RESTAdmit(goapiproof.RESTAdmissionInput{
		NamedBuild:          namedBuild,
		WantCandidateStatus: request.WantCandidateStatus,
		WantBaselineStatus:  request.WantBaselineStatus,
		PythonForwarder:     spec.PythonForwarder && !f.pythonForwarderOff,
		Candidate:           candidateLeg,
		Baseline:            secondLeg,
	}, true)
	if !secondAdmission.Admitted {
		record.Detail = "second baseline read: " + secondAdmission.Detail
		return record, &outcome{Refusal: secondAdmission.Reason, Detail: record.Detail}, none, goapiproof.Result{}, nil
	}
	second := secondAdmission.BaselineSnap
	second.Data = goapiproof.InjectRESTDedupKeys(second.Data, request.DedupListPath, request.DedupKeyFields)
	decision := goapiproof.ClassifyWriteSkew(first, admission.BaselineSnap, admission.CandidateSnap, second, request.Parity)
	record.Verdict, record.Leaves, record.Detail = decision.Verdict, decision.Leaves, decision.Detail
	record.referenceUnmoved = decision.ReferenceUnmoved
	if decision.Verdict == goapiproof.WriteSkewRefused {
		return record, &outcome{Refusal: decision.Refusal, Detail: decision.Detail}, none, goapiproof.Result{}, nil
	}
	return record, nil, second, decision.Second, nil
}

// candidateHasNoData reports whether a refused attempt of a bounded-
// candidate search refused because the candidate has no data to compare
// -- both legs the same empty answer with every declared list empty, a clean match with the declared
// list an empty array on both legs, or (status-only) the declared list an
// empty array on the candidate leg -- which moves the search to the next
// candidate. Every other refusal is a failure of a plane and ends it.
func candidateHasNoData(refusal string) bool {
	switch refusal {
	case goapiproof.RefusalVacuousEmptyLegs,
		goapiproof.RESTRefusalNoLegProducedTheDeclaredID,
		goapiproof.RESTRefusalCandidateProducerUnresolved:
		return true
	}
	return false
}

func runMeasurement(ctx context.Context, client *goapiproof.LegClient, f flags, candidateCredential, baselineCredential *goapiproof.Credential, builds goapiproof.ProverBuild, pgPool receiptWriter, artifacts *goapiproof.ArtifactStore) error {
	namedBuild := builds.Candidate
	if f.gapDelay > 0 {
		f.gapReread = newGapRereadState(f.gapDelay, f.gapBudget)
		fmt.Printf("gap_reread_delay=%s gap_reread_budget=%s\n", f.gapDelay, f.gapBudget)
	}
	// Printed before the first request, so a run cut by the deadline
	// shows on its own log what it was given.
	fmt.Printf("run_deadline=%s\n", f.runDeadline)
	auth := goapiproof.AuthContext{PrincipalKind: f.principalKind, Audience: f.audience, KeyID: f.keyID}
	observedAt := time.Now().UTC()

	// planned is built in full BEFORE any request is sent, so a run that
	// stops partway can still say by NAME what it never got to, not
	// merely how many it skipped. specErr holds SpecForREST's own error
	// when planning itself could not proceed -- unreachable in practice
	// once ValidateRESTCorpus and ValidateRESTIDBindingOrder have already
	// passed above (every RESTRunOrder entry is checked against
	// restEndpointSpecs there), kept as a named, reported failure rather
	// than a panic on the day that stops being true.
	planned, specErr := planRESTRequests()

	var outcomes []outcome
	attempted, admitted, matched, mismatched := 0, 0, 0, 0
	attemptedKeys := map[string]bool{}

	// produced accumulates every id an earlier request's OWN Produces
	// declaration yielded from its baseline (Python) response, keyed by
	// producer name -- RESTRunOrder (not KnownRESTOperations' alphabetical
	// order) guarantees a producer's operation is always visited before
	// any operation that binds one of its ids; see restcorpus.go's own
	// restRunOrder doc comment for why alphabetical order cannot make
	// that guarantee. Seeded from f.binds before the loop starts: an
	// operator-supplied id (goapiproof.IsOperatorSuppliedIDProducer) is,
	// from ResolveRESTIDBindings' own point of view, already produced --
	// it just never came from a request in this run. A shallow copy, not
	// f.binds itself, since this map also accumulates real Produces
	// entries below as the loop runs.
	produced := make(map[string]string, len(f.binds))
	for name, value := range f.binds {
		produced[name] = value
	}

	// producedCandidates accumulates, per producer name, EVERY non-empty
	// candidate id an earlier request's own Produces declaration yielded
	// (goapiproof.ExtractRESTIDCandidates' full result), not only the
	// first -- the pool a LATER request's own bounded-candidate binding
	// (goapiproof.RESTIDBinding.Candidates) draws from. Merged the same
	// way, and at the same point, as produced above.
	producedCandidates := map[string][]string{}

	// runErr is the run-level failure -- distinct from a per-case
	// refusal, which is never fatal -- that stops the loop early. Once
	// set, the loop breaks and falls straight through to the summary
	// line and report write below: EVERY exit from this function, early
	// or not, reaches that write, which is the fix for the empty-report/
	// no-summary failure mode this ticket exists to close. This file used
	// to reach both only after the whole loop returned with no error at
	// all, so ANY error that stopped the loop early -- not only a leg
	// timeout -- silently dropped the run's own evidence with it.
	runErr := specErr

requestLoop:
	for _, p := range planned {
		if runErr != nil {
			break requestLoop
		}
		if ctx.Err() != nil {
			runErr = fmt.Errorf("run interrupted: %w", ctx.Err())
			break requestLoop
		}
		operation, spec, request := p.operation, p.spec, p.request
		attempted++
		attemptedKeys[operation+"/"+request.Name] = true

		var attempt resolvedAttempt
		var err error
		if iterating, ok := findIteratingBinding(request.IDBindings); ok {
			attempt, err = resolveIteratingRequest(ctx, client, f, operation, spec, request, iterating, produced, producedCandidates, candidateCredential, baselineCredential, namedBuild, auth, observedAt, pgPool, artifacts)
		} else {
			attempt, err = resolveSingleShotRequest(ctx, client, f, operation, spec, request, produced, candidateCredential, baselineCredential, namedBuild, auth, observedAt, pgPool, artifacts)
		}
		if err != nil {
			runErr = fmt.Errorf("%s/%s: %w", operation, request.Name, err)
			// Not measured: named in not_run, never counted as attempted
			// with no outcome of its own.
			attempted--
			delete(attemptedKeys, operation+"/"+request.Name)
			break requestLoop
		}
		outcomes = append(outcomes, attempt.out)
		fmt.Println(attempt.out.line())
		for name, id := range attempt.out.producedIDs {
			produced[name] = id
		}
		for name, candidates := range attempt.out.producedCandidateIDs {
			producedCandidates[name] = candidates
		}
		if attempt.out.Admitted {
			admitted++
			switch attempt.out.TerminalState {
			case goapiproof.TerminalStateMatch:
				matched++
			case goapiproof.TerminalStateMismatch:
				mismatched++
			}
		}

		if !attempt.legsSent {
			// Refused before either leg was ever sent -- an unresolved
			// binding (single-shot) or an exhausted bounded-candidate
			// search (see resolveSingleShotRequest/resolveIteratingRequest's
			// own doc comments). Its own edge-credential-on-candidate
			// sibling is marked attempted here too, for the identical
			// reason the pre-iteration code already stated: this is a
			// deliberate, named skip of BOTH legs for this one request,
			// never the run stopping before it reached them -- notRunKeys
			// must not read this the same way it reads a run that broke
			// off early.
			if !spec.PublicNoAuth {
				// Named in its own outcome, never left in neither list: the
				// edge-credential leg is not sent, for its case's own reason.
				edgeName := request.Name + " (edge-credential-on-candidate)"
				attemptedKeys[operation+"/"+edgeName] = true
				skipped := outcome{
					Operation: operation, Request: edgeName,
					Admitted: false,
					Refusal:  attempt.out.Refusal,
					Detail:   "not sent: this case was refused before either of its legs was sent",
					BoundIDs: attempt.out.BoundIDs,
				}
				outcomes = append(outcomes, skipped)
				fmt.Println(skipped.line())
			}
			continue
		}

		// A real user's browser never carries an effective-principal
		// envelope -- it carries the edge access token
		// baselineCredential already holds for this request's
		// baseline (Python) leg above. This third leg sends that SAME
		// credential straight to query-api, proving THE CANDIDATE
		// accepts the credential real traffic actually carries, not
		// only the envelope the ordinary candidate leg above already
		// exercises. Skipped for a PublicNoAuth route: meta.go's own
		// "Auth: PUBLIC" contract sends no Authorization header on
		// either leg, so there is no edge credential to re-send here.
		if !attempt.spec.PublicNoAuth {
			// The run may have ended during this case's own legs: its
			// edge-credential leg is then never sent, and is named in
			// not_run like every other request the run did not reach.
			if ctx.Err() != nil {
				runErr = fmt.Errorf("run interrupted: %w", ctx.Err())
				break requestLoop
			}
			attempted++
			attemptedKeys[operation+"/"+request.Name+" (edge-credential-on-candidate)"] = true
			edgeOut, err := proveEdgeCredentialOnCandidate(ctx, client, f, operation, attempt.spec, attempt.request, baselineCredential, namedBuild, artifacts)
			if err != nil {
				runErr = fmt.Errorf("%s/%s (edge credential on candidate): %w", operation, request.Name, err)
				attempted--
				delete(attemptedKeys, operation+"/"+request.Name+" (edge-credential-on-candidate)")
				break requestLoop
			}
			outcomes = append(outcomes, edgeOut)
			fmt.Println(edgeOut.line())
			if edgeOut.Admitted {
				admitted++
			}
		}
	}

	// notRun names every planned request this run never reached, by
	// NAME, so a partial run says exactly what it is missing rather than
	// leaving a reader to infer it from attempted < len(planned). Empty
	// whenever the loop reached the end of planned with no runErr.
	notRun := notRunKeys(planned, attemptedKeys)

	// The summary line and the JSON report are written HERE, before
	// runErr (or any later check below) is ever returned -- the same
	// "report first, error second" discipline cmd/go-api-prove's own
	// run() already follows for the GraphQL sibling of this command (see
	// that file's own comment on emitReport). Nothing below this point
	// may skip either write.
	// partialCause says WHY a run stopped short, read from the run's own
	// context -- never from an error chain, since a single request's own
	// timeout also wraps context.DeadlineExceeded. A run the deadline or
	// a signal ended always exits non-zero, even when it had reached its
	// last case: a leg in flight was cut, not measured.
	// The exit cause is decided before anything is printed or written,
	// from the run context's own state, and decided AGAIN after the report
	// is written: a deadline or signal that lands during the write ends
	// the run too, and the report and the exit say so.
	legFailures := legFailuresIn(outcomes)
	var vacuityErrs []string
	for _, out := range outcomes {
		for _, e := range out.vacuityErrors {
			vacuityErrs = append(vacuityErrs, fmt.Sprintf("%s/%s: %s", out.Operation, out.Request, e))
		}
	}
	sort.Strings(vacuityErrs)
	if len(notRun) > 0 {
		fmt.Printf("partial run: %d request(s) never attempted: %v\n", len(notRun), notRun)
	}
	fmt.Printf("attempted=%d admitted=%d match=%d mismatch=%d refused=%d\n",
		attempted, admitted, matched, mismatched, attempted-admitted)
	// Every skew-admitted case is named on its own line; this count per
	// route (operation) makes a route admitted by skew run after run a
	// visible pattern rather than a clean run.
	skewAdmitted := skewAdmittedByOperation(outcomes)
	for _, operation := range sortedStringKeys(skewAdmitted) {
		fmt.Printf("skew_admitted %s=%d\n", operation, skewAdmitted[operation])
	}
	gapAdmitted := gapAdmittedByOperation(outcomes)
	for _, operation := range sortedStringKeys(gapAdmitted) {
		fmt.Printf("gap_admitted %s=%d\n", operation, gapAdmitted[operation])
	}

	runEnded := ctx.Err()
	report, finalErr := finalRunReport(f, outcomes, notRun, runEnded, runErr, legFailures, vacuityErrs)
	report.ProverBuild = &builds
	finalErr = writeFinalReport(f, report, finalErr)
	if late := ctx.Err(); late != nil && runEnded == nil {
		report, finalErr = finalRunReport(f, outcomes, notRun, late, runErr, legFailures, vacuityErrs)
		report.ProverBuild = &builds
		fmt.Println("the run ended while its report was being written")
		finalErr = writeFinalReport(f, report, finalErr)
	}
	return finalErr
}

// skewAdmittedByOperation counts the skew-admitted cases per operation;
// nil when there are none.
func skewAdmittedByOperation(outcomes []outcome) map[string]int {
	var counts map[string]int
	for _, out := range outcomes {
		if out.WriteSkew == nil || out.WriteSkew.Verdict != goapiproof.WriteSkewAdmitted {
			continue
		}
		if counts == nil {
			counts = map[string]int{}
		}
		counts[out.Operation] = counts[out.Operation] + 1
	}
	return counts
}

// gapAdmittedByOperation counts the delayed-re-read admissions per
// operation; nil when there are none.
func gapAdmittedByOperation(outcomes []outcome) map[string]int {
	var counts map[string]int
	for _, out := range outcomes {
		if out.GapReread == nil || out.GapReread.Outcome != goapiproof.GapRereadAdmitted {
			continue
		}
		if counts == nil {
			counts = map[string]int{}
		}
		counts[out.Operation]++
	}
	return counts
}

// sortedStringKeys returns m's keys in order.
func sortedStringKeys(m map[string]int) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// finalRunReport decides how a run that reached the end of its request
// loop ended -- partial_cause, exit_cause and the error it exits with --
// from runEnded (the run context's own Err()), the loop's own error, the
// legs that never answered and the declarations that excused nothing.
func finalRunReport(f flags, outcomes []outcome, notRun []string, runEnded, runErr error, legFailures, vacuityErrs []string) (jsonReport, error) {
	partialCause := stopCause(runEnded, runErr)
	if runErr == nil && runEnded != nil {
		runErr = fmt.Errorf("run interrupted: %w", runEnded)
	}
	exitCause, finalErr := exitCauseFor(runEnded, runErr, legFailures, vacuityErrs)
	report := jsonReport{Outcomes: outcomes, NotRun: notRun, PartialCause: partialCause, PartialError: partialError(f, runErr), ExitCause: exitCause, RunDeadline: f.runDeadline.String(), SkewAdmittedByOperation: skewAdmittedByOperation(outcomes), GapAdmittedByOperation: gapAdmittedByOperation(outcomes)}
	return report, finalErr
}

// writeFinalReport prints the run's causes on stdout and writes the
// report when -report was given, returning the run's exit error (joined
// with a write failure when there is one).
func writeFinalReport(f flags, report jsonReport, finalErr error) error {
	if report.PartialCause != "" {
		fmt.Printf("partial_cause=%s\n", report.PartialCause)
	}
	fmt.Printf("exit_cause=%s\n", report.ExitCause)
	if f.reportPath == "" {
		return finalErr
	}
	if writeErr := writeJSONReport(f.reportPath, report); writeErr != nil {
		if finalErr == nil {
			return writeErr
		}
		return fmt.Errorf("%w; additionally, writing the report failed: %v", finalErr, writeErr)
	}
	return finalErr
}

// exitCauseFor decides the exit cause of a run that reached the end of
// its request loop, and the error it exits with. runEnded is the run
// context's own Err(): the run deadline or a signal is read from the
// context that carries them, never inferred from an error chain (a
// single request's own timeout also wraps context.DeadlineExceeded).
// Then a tool error, then a leg that never answered, then a declaration
// that excused nothing.
func exitCauseFor(runEnded, runErr error, legFailures, vacuityErrs []string) (string, error) {
	switch {
	case errors.Is(runEnded, context.DeadlineExceeded):
		return exitStoppedByRunDeadline, runErr
	case runEnded != nil:
		return exitStoppedBySignal, runErr
	case runErr != nil:
		return exitAbortedByToolError, runErr
	case len(legFailures) > 0:
		// A leg that never answered is not a tool error (it is already
		// recorded as its own named refusal, per case), but a run
		// containing one still exits non-zero: the instrument did not
		// complete cleanly, and an operator must not have to read every
		// line of output to notice.
		return exitCompletedWithLegsThatNeverAnswered, fmt.Errorf("this run measured a leg that never answered (fix the deployment, or for a legitimately slow baseline declare a longer RESTRequest.Timeout):\n%s", strings.Join(legFailures, "\n"))
	case len(vacuityErrs) > 0:
		return exitCompletedWithVacuousDeclarations, fmt.Errorf("this run found declarations that excuse nothing (fix or remove them):\n%s", strings.Join(vacuityErrs, "\n"))
	}
	return exitCompleted, nil
}

// jsonReport is -report's own top-level shape: not a bare array of
// outcomes any more, so a partial run can say it is partial IN the file
// an operator reads back, not only on stdout.
type jsonReport struct {
	// ProverBuild names the commit whose declarations, shapes and corpus
	// the run applied, beside the candidate build it measured; absent
	// only when the run stopped before /buildinfo named a candidate.
	*goapiproof.ProverBuild
	Outcomes []outcome `json:"outcomes"`
	// Partial is true whenever NotRun is non-empty -- named separately
	// rather than left for a reader to infer from an empty NotRun slice,
	// the same discipline Summary.Attempted's own doc comment states for
	// explicit zeroes elsewhere in this package: a fact worth knowing must
	// never depend on a reader noticing an absence.
	Partial bool `json:"partial"`
	// NotRun names, by "operation/request" key, every planned request
	// this run never reached -- empty on a run that completed its whole
	// plan, whatever its outcomes.
	NotRun []string `json:"not_run,omitempty"`
	// PartialCause names why the run stopped short of its plan, or cut a
	// leg in flight: one of the partialCause* constants. Empty when the
	// run reached the end of its plan with the run itself still live.
	PartialCause string `json:"partial_cause,omitempty"`
	// PartialError is the redacted run-level error behind PartialCause,
	// when there was one.
	PartialError string `json:"partial_error,omitempty"`
	// SkewAdmittedByOperation counts, per route (operation), the cases
	// admitted by a bracketed re-read (outcome.write_skew verdict
	// skew_admitted). Absent when there were none.
	SkewAdmittedByOperation map[string]int `json:"skew_admitted_by_operation,omitempty"`
	// GapAdmittedByOperation counts, per route, the cases admitted by the
	// delayed re-read (outcome.gap_reread outcome gap_admitted), so the
	// record shows how often the materializer-gap class fired. Absent when
	// there were none.
	GapAdmittedByOperation map[string]int `json:"gap_admitted_by_operation,omitempty"`
	// ExitCause names how the run ended, one of the exit* constants: a
	// whole run, a run stopped early (and by what), or one refused before
	// it measured anything.
	ExitCause string `json:"exit_cause"`
	// RunDeadline is this run's -run-deadline.
	RunDeadline string `json:"run_deadline"`
}

// Exit causes, one per way the command can end; each is written into the
// report and printed on that path.
const (
	exitCompleted                          = "completed"
	exitCompletedWithLegsThatNeverAnswered = "completed_with_legs_that_never_answered"
	exitCompletedWithVacuousDeclarations   = "completed_with_declarations_that_excuse_nothing"
	exitStoppedBySignal                    = "stopped_by_signal"
	exitStoppedByRunDeadline               = "stopped_by_run_deadline"
	exitAbortedByToolError                 = "aborted_by_tool_error"
	exitRefusedBeforeMeasuring             = "refused_before_measuring"
)

// Partial causes, read from the run's own context first.
const (
	partialCauseRunDeadline = "run_deadline"
	partialCauseSignal      = "signal"
	partialCauseToolError   = "tool_error"
	// partialCauseRefusedBeforeMeasuring: the run stopped before its first
	// request for a reason of its own (configuration, a credential, the
	// build read), with the run itself still live.
	partialCauseRefusedBeforeMeasuring = "refused_before_measuring"
)

// stopCause decides why a run stopped short. runEnded is the run
// context's own Err(): the run deadline and a signal are read from the
// context that carries them. Otherwise a run-level error that stopped the
// loop is a tool error; a run that neither ended nor erred has none.
func stopCause(runEnded, runErr error) string {
	switch {
	case errors.Is(runEnded, context.DeadlineExceeded):
		return partialCauseRunDeadline
	case runEnded != nil:
		return partialCauseSignal
	case runErr != nil:
		return partialCauseToolError
	}
	return ""
}

// partialError is runErr's redacted text, or empty.
func partialError(f flags, runErr error) string {
	if runErr == nil {
		return ""
	}
	return secrets.NewBoundary(f.postgresURI).Redact(runErr).Error()
}

func writeJSONReport(path string, report jsonReport) error {
	report.Partial = len(report.NotRun) > 0
	if report.Outcomes == nil {
		report.Outcomes = []outcome{}
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write report to %s: %w", path, err)
	}
	return nil
}

// proveOneRESTRequest runs one corpus request end to end: both legs,
// admission, comparison (when the corpus asks for one) and the receipt.
func proveOneRESTRequest(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	candidateCredential, baselineCredential *goapiproof.Credential,
	namedBuild string,
	auth goapiproof.AuthContext,
	observedAt time.Time,
	writer receiptWriter,
	artifacts *goapiproof.ArtifactStore,
	dryRun bool,
	boundIDs map[string]string,
) (outcome, error) {
	for _, binding := range request.IDBindings {
		if binding.PathParam == "" {
			continue
		}
		if id := boundIDs[binding.Producer]; !goapiproof.IsPathLiteralID(id) {
			// Neither leg is sent: the server's path router would decode
			// or split this id, so both planes would answer for an id
			// other than the one the receipt names.
			return outcome{
				Operation: operation, Request: request.Name,
				Admitted: false,
				Refusal:  goapiproof.RESTRefusalBoundIDNotAPathLiteral,
				Detail:   fmt.Sprintf("the id bound to path parameter %q by %q is not a path literal", binding.PathParam, binding.Producer),
				BoundIDs: boundIDs,
			}, nil
		}
	}
	if spec.PublicNoAuth {
		candidateCredential, baselineCredential = nil, nil
	}
	timeout := resolveRESTTimeout(request.Timeout, f.timeout)
	// The baseline (reference) leg is read FIRST and the candidate second.
	// The bracketed re-read (bracketedReread) depends on this order: it
	// re-reads the baseline after the candidate, so a write landing
	// between the legs shows as a change on the reference plane itself.
	baselineLeg, err := doREST(ctx, client, f.pythonAPIURL, spec.Method, spec.Path, request.Query, request.Body, baselineCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(ctx, operation, request.Name, "baseline", boundIDs, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("baseline leg: %w", err)
	}
	baselineObservedAt := time.Now().UTC()
	candidateLeg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, candidateCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(ctx, operation, request.Name, "candidate", boundIDs, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("candidate leg: %w", err)
	}
	candidateObservedAt := time.Now().UTC()

	// Stored BEFORE admission runs, and unconditionally: a refused
	// request's own bodies are exactly what a reader needs to see WHY it
	// was refused, and the whole point of this ticket is that a mismatch
	// used to leave no evidence behind once this process exited. Mirrors
	// go-api-prove's own Runner.observe (internal/goapiproof/run.go),
	// which stores every leg it fetches the same way, at the same layer,
	// regardless of what Admit later decides. artifacts is nil only in a
	// test that does not care about refs -- main() always builds one
	// (run()'s own -artifact-dir, required).
	var candidateRef, baselineRef string
	if artifacts != nil {
		if candidateRef, err = artifacts.Put(candidateLeg.Body); err != nil {
			return outcome{}, fmt.Errorf("store candidate leg artifact: %w", err)
		}
		if baselineRef, err = artifacts.Put(baselineLeg.Body); err != nil {
			return outcome{}, fmt.Errorf("store baseline leg artifact: %w", err)
		}
	}

	decodeBody := request.BodyMode == goapiproof.RESTBodyModeJSON
	admission := goapiproof.RESTAdmit(goapiproof.RESTAdmissionInput{
		NamedBuild:          namedBuild,
		WantCandidateStatus: request.WantCandidateStatus,
		WantBaselineStatus:  request.WantBaselineStatus,
		PythonForwarder:     spec.PythonForwarder && !f.pythonForwarderOff,
		Candidate:           candidateLeg,
		Baseline:            baselineLeg,
	}, decodeBody)

	out := outcome{
		Operation: operation, Request: request.Name,
		Admitted: admission.Admitted, Refusal: admission.Reason, Detail: admission.Detail,
		BoundIDs:              boundIDs,
		BaselineResponseRef:   baselineRef,
		CandidateResponseRef:  candidateRef,
		BaselineWireAttempts:  baselineLeg.WireAttempts,
		CandidateWireAttempts: candidateLeg.WireAttempts,
		ObservedAt:            baselineObservedAt,
		CandidateObservedAt:   candidateObservedAt,
	}
	if !admission.Admitted {
		return out, nil
	}

	variables := restRequestVariables(spec.Method, request.Query, request.Body)
	identity, err := goapiproof.RequestIdentity(f.org, auth, variables)
	if err != nil {
		return outcome{}, err
	}

	var (
		terminalState  = goapiproof.TerminalStateMatch
		differences    int
		matchedDefects []string
		vacuity        []string
		findingsRef    string
		// admittedBaselineRef/admittedCandidateRef: the pair a re-read
		// admission stands on (see restReviewEvidence).
		admittedBaselineRef, admittedCandidateRef string
	)
	if decodeBody {
		baselineData := goapiproof.InjectRESTDedupKeys(admission.BaselineSnap.Data, request.DedupListPath, request.DedupKeyFields)
		candidateData := goapiproof.InjectRESTDedupKeys(admission.CandidateSnap.Data, request.DedupListPath, request.DedupKeyFields)
		admission.BaselineSnap.Data = baselineData
		admission.CandidateSnap.Data = candidateData

		// Extract every id this request Produces from the BASELINE
		// (Python) leg -- see restidbind.go's own doc comment for why
		// that plane, specifically. Populated unconditionally once the
		// body has decoded, regardless of what Compare finds below: a
		// producer's real id is still usable by a later consumer even
		// when THIS entry's own comparison mismatches or is refused
		// structurally just afterward.
		if len(request.Produces) > 0 {
			out.producedIDs = make(map[string]string, len(request.Produces))
			out.producedCandidateIDs = make(map[string][]string, len(request.Produces))
			for _, producer := range request.Produces {
				candidates := goapiproof.ExtractRESTIDCandidates(admission.BaselineSnap.Data, producer)
				out.producedCandidateIDs[producer.Name] = candidates
				if len(candidates) > 0 {
					out.producedIDs[producer.Name] = candidates[0]
				}
			}
		}

		result := goapiproof.Compare(admission.BaselineSnap, admission.CandidateSnap, request.Parity)

		// The finding list itself, not just the two bodies it was derived
		// from: without it, a mismatch receipt names a verdict and a
		// count with no way to see afterward WHICH paths differed or how.
		// Stored whenever Compare ran at all, match or mismatch or
		// structural refusal alike, so "nothing differed" is as much on
		// record as a real divergence is. go_api_rest_proof_run carries
		// no findings_ref column of its own -- see out.FindingsRef's own
		// doc comment for where the ref actually lands.
		if artifacts != nil {
			encodedFindings, marshalErr := json.Marshal(result.Findings)
			if marshalErr != nil {
				return outcome{}, fmt.Errorf("encode finding list: %w", marshalErr)
			}
			if findingsRef, err = artifacts.Put(encodedFindings); err != nil {
				return outcome{}, fmt.Errorf("store finding list artifact: %w", err)
			}
		}

		if result.StructuralRefusal != "" {
			// Mirrors goapiproof/run.go's own GraphQL runner: Compare
			// returns immediately on a structural refusal (see
			// structuralAgreementFailure/vacuousEmptyLegs), leaving
			// TerminalState at its zero value -- writing that value into
			// a receipt would either fail the database's own CHECK
			// constraint outright or, worse, silently pass a value
			// nothing else in this vocabulary means. run.go's own
			// refuse() catches this ahead of every declared-relaxation
			// guard for the identical reason stated there: a comparison
			// with zero non-null leaves on BOTH legs under a declared
			// BaselineDefect (vacuousEmptyLegs) is not "the citation
			// matched nothing, go delete the ticket" -- it is "there was
			// nothing here to test the citation against", e.g. a window
			// with genuinely zero PRs/issues returned by either plane.
			// This binary does not yet port go-api-prove's own
			// refusal-receipt recording (documented scope reduction,
			// this PR's own RISK-NOTES), so -- exactly like an admission
			// refusal above -- this reports the refusal and writes no
			// receipt, rather than crashing the whole run on an invalid
			// terminal_state.
			out.Admitted = false
			out.Refusal = result.StructuralRefusal
			out.Detail = result.StructuralDetail
			out.FindingsRef = findingsRef
			// A vacuous refusal is always the same empty answer on both
			// legs (Compare judges vacuity only on equal decoded legs).
			// For a bounded candidate search it is "no data" only when
			// every declared list is also an empty array; any other
			// shape of the declared list is a failure and ends the search
			// (candidateHasNoData), the same as after a clean match.
			if _, iterating := findIteratingBinding(request.IDBindings); iterating && result.StructuralRefusal == goapiproof.RefusalVacuousEmptyLegs && !declaredListsEmpty(admission.BaselineSnap.Data, request.Produces) {
				out.Refusal = goapiproof.RESTRefusalDeclaredIDListUnrecognised
				out.Detail = "both legs are the same empty answer without the declared list as an empty array"
			}
			return out, nil
		}

		// The HARD half of goapiproof.Result.Acceptance -- see that
		// method's own doc comment and AcceptanceRefusal.Hard. Mirrors
		// run.go's own GraphQL runner: refused immediately, with NO
		// receipt written, the same way the StructuralRefusal block just
		// above does, because a Hard refusal means TerminalState here
		// could read match despite this comparison not actually checking
		// what its own declaration promises -- an undeclared numeric leaf
		// compared under the unverified Tier-A default is the corpus-
		// completeness gap this exists to close; a route with one stays
		// invisible to every reader of go_api_rest_proof_run until it is
		// declared, one way or the other, in restcorpus.go.
		for _, refusal := range result.Acceptance() {
			if !refusal.Hard {
				continue
			}
			out.Admitted = false
			out.Refusal = refusal.Code
			out.Detail = refusal.Detail
			out.FindingsRef = findingsRef
			return out, nil
		}

		terminalState = result.TerminalState
		differences = result.DifferencesOutsideBaselineDefect
		matchedDefects = result.BaselineDefectsMatched
		vacuity = resultVacuityErrors(result)

		// Bracketed re-read: the baseline was read FIRST, so a
		// materializer write landing between the two legs leaves the
		// baseline on the older generation. Only when every outside
		// difference is a value leaf (goapiproof.WriteSkewRereadNeeded)
		// is the baseline read a second time, now after the candidate;
		// see goapiproof.ClassifyWriteSkew for what that second read may
		// and may not admit.
		iterationGateState := terminalState
		if goapiproof.WriteSkewRereadNeeded(result) {
			record, refused, secondSnap, secondResult, err := bracketedReread(ctx, client, f, spec, request, baselineCredential, timeout, namedBuild, candidateLeg, admission, result, artifacts)
			if err != nil {
				return outcome{}, err
			}
			out.WriteSkew = record
			if refused != nil {
				out.Admitted = false
				out.Refusal = refused.Refusal
				out.Detail = refused.Detail
				out.FindingsRef = findingsRef
				return out, nil
			}
			if record.Verdict == goapiproof.WriteSkewAdmitted {
				// The case now stands on the second comparison (B2 against
				// C1): the receipt keeps the first comparison's mismatch
				// with nothing outside and the write-skew citation, and
				// every later gate reads B2 -- the declared ids are
				// produced from B2, and the bounded-search gate below
				// judges B2/C1's own terminal state exactly as the normal
				// path judges a clean comparison.
				iterationGateState = secondResult.TerminalState
				differences = 0
				matchedDefects = append(append([]string(nil), secondResult.BaselineDefectsMatched...), goapiproof.WriteSkewCitation)
				admission.BaselineSnap = secondSnap
				admittedBaselineRef, admittedCandidateRef = record.SecondBaselineResponseRef, candidateRef
				if len(request.Produces) > 0 {
					out.producedIDs = make(map[string]string, len(request.Produces))
					out.producedCandidateIDs = make(map[string][]string, len(request.Produces))
					for _, producer := range request.Produces {
						candidates := goapiproof.ExtractRESTIDCandidates(secondSnap.Data, producer)
						out.producedCandidateIDs[producer.Name] = candidates
						if len(candidates) > 0 {
							out.producedIDs[producer.Name] = candidates[0]
						}
					}
				}
			}
		}

		// Delayed re-read (goapiproof.ClassifyGapReread): a case still outside
		// after the bracketed re-read (reference unmoved) or never eligible
		// for it (presence/type findings) waits out the materializer gap and
		// reads both planes again. It only ever turns such a case into an
		// admission on a moved reference and a stable candidate.
		if f.gapReread != nil {
			var bracket *goapiproof.WriteSkewDecision
			if out.WriteSkew != nil {
				bracket = &goapiproof.WriteSkewDecision{Verdict: out.WriteSkew.Verdict, ReferenceUnmoved: out.WriteSkew.referenceUnmoved}
			}
			if goapiproof.GapRereadEligible(result, bracket) {
				record, thirdSnap, decision := f.gapReread.run(ctx, client, f, spec, request, baselineCredential, candidateCredential, timeout, namedBuild, admission, result, artifacts)
				out.GapReread = record
				if record.Outcome == goapiproof.GapRereadAdmitted {
					iterationGateState = decision.Second.TerminalState
					differences = 0
					matchedDefects = append(append([]string(nil), decision.Second.BaselineDefectsMatched...), goapiproof.GapRereadCitation)
					admission.BaselineSnap = thirdSnap
					admittedBaselineRef, admittedCandidateRef = record.BaselineResponseRef, record.CandidateResponseRef
					if len(request.Produces) > 0 {
						out.producedIDs = make(map[string]string, len(request.Produces))
						out.producedCandidateIDs = make(map[string][]string, len(request.Produces))
						for _, producer := range request.Produces {
							candidates := goapiproof.ExtractRESTIDCandidates(thirdSnap.Data, producer)
							out.producedCandidateIDs[producer.Name] = candidates
							if len(candidates) > 0 {
								out.producedIDs[producer.Name] = candidates[0]
							}
						}
					}
				}
			}
		}

		// Compare first, classify after. A bounded-candidate attempt is
		// "no data" -- it loses, writes no receipt, and the search moves
		// to the next candidate (candidateHasNoData) -- ONLY when its
		// comparison is clean (a match: nothing differs, admitted or
		// outside) AND every declared id it lacks has its declared list as
		// an empty array on BOTH legs. Any difference, structural
		// refusal or admission failure never reaches this point as "no
		// data": it is the attempt's own result and ends the search. A
		// clean match whose bodies lack a declared id in any other shape
		// (the list missing, null, not an array, or elements without the
		// id) refuses as a failure and ends the search as well.
		if _, iterating := findIteratingBinding(request.IDBindings); iterating && iterationGateState == goapiproof.TerminalStateMatch {
			var empty, unrecognised []string
			for _, producer := range request.Produces {
				if _, ok := out.producedIDs[producer.Name]; ok {
					continue
				}
				if goapiproof.RESTIDListIsEmpty(admission.BaselineSnap.Data, producer) && goapiproof.RESTIDListIsEmpty(admission.CandidateSnap.Data, producer) {
					empty = append(empty, producer.Name)
					continue
				}
				unrecognised = append(unrecognised, producer.Name)
			}
			if len(unrecognised) > 0 {
				out.Admitted = false
				out.Refusal = goapiproof.RESTRefusalDeclaredIDListUnrecognised
				out.Detail = fmt.Sprintf("both legs matched without the declared id, and not as an empty list: %v", unrecognised)
				out.FindingsRef = findingsRef
				return out, nil
			}
			if len(empty) > 0 {
				out.Admitted = false
				out.Refusal = goapiproof.RESTRefusalNoLegProducedTheDeclaredID
				out.Detail = fmt.Sprintf("both legs matched with the declared list empty: %v", empty)
				out.FindingsRef = findingsRef
				return out, nil
			}
		}
	} else if len(request.Produces) > 0 {
		// A StatusOnly request may still Produce ids, but only in the one
		// shape ValidateRESTCorpus admits (goapiproof/restcorpus.go): the
		// two Want statuses differ and the CANDIDATE's own want is 200 --
		// this route's baseline is declared failing in production. An id
		// is a request parameter, not evidence compared between planes,
		// so it is honest to read it from whichever leg actually answers
		// with a body: here that is the CANDIDATE, not the BASELINE the
		// decodeBody branch above reads for every other request. Decoded
		// through the same production decoder (DecodeRESTSnapshot) real
		// evidence uses, never hand-built.
		//
		// A declared producer this branch cannot resolve refuses THIS
		// request by name -- a body that does not decode as
		// goapiproof.RESTRefusalCandidateBodyUndecodable (a failure of
		// the candidate plane), a decoded body that yields no value at a
		// declared entry's path as
		// goapiproof.RESTRefusalCandidateProducerUnresolved (no data) --
		// rather than leaving out.producedIDs silently short: a check that can never
		// fail a run proves nothing, and a later consumer's own
		// IDBindings would otherwise be refused by name
		// (rest_request_id_binding_unresolved) with no visible reason on
		// the request that actually failed to produce it.
		candidateSnap, decodeErr := goapiproof.DecodeRESTSnapshot(candidateLeg.Body)
		if decodeErr != nil {
			out.Admitted = false
			out.Refusal = goapiproof.RESTRefusalCandidateBodyUndecodable
			out.Detail = fmt.Sprintf("the candidate body did not decode: %s", decodeErr)
			return out, nil
		}
		if candidateSnap.TrailingBytes {
			out.Admitted = false
			out.Refusal = goapiproof.RESTRefusalTrailingBytes
			out.Detail = "the candidate body carried bytes after its JSON value"
			return out, nil
		}
		out.producedIDs = make(map[string]string, len(request.Produces))
		out.producedCandidateIDs = make(map[string][]string, len(request.Produces))
		var unresolved, unrecognised []string
		for _, producer := range request.Produces {
			candidates := goapiproof.ExtractRESTIDCandidates(candidateSnap.Data, producer)
			out.producedCandidateIDs[producer.Name] = candidates
			if len(candidates) == 0 {
				if goapiproof.RESTIDListIsEmpty(candidateSnap.Data, producer) {
					unresolved = append(unresolved, producer.Name)
				} else {
					unrecognised = append(unrecognised, producer.Name)
				}
				continue
			}
			out.producedIDs[producer.Name] = candidates[0]
		}
		// Only an empty declared list is "no data"
		// (RESTRefusalCandidateProducerUnresolved); any other shape
		// without the id is a failure of the candidate plane.
		if len(unrecognised) > 0 {
			out.Admitted = false
			out.producedIDs = nil
			out.Refusal = goapiproof.RESTRefusalDeclaredIDListUnrecognised
			out.Detail = fmt.Sprintf("the candidate body did not carry the declared id, and not as an empty list: %v", unrecognised)
			return out, nil
		}
		if len(unresolved) > 0 {
			out.Admitted = false
			out.Refusal = goapiproof.RESTRefusalCandidateProducerUnresolved
			out.Detail = fmt.Sprintf("the candidate body did not yield: %v", unresolved)
			return out, nil
		}
	}

	out.TerminalState = terminalState
	out.DifferencesOutsideBaselineDefect = differences
	out.BaselineDefectsMatched = matchedDefects
	out.vacuityErrors = vacuity
	out.FindingsRef = findingsRef

	if dryRun || writer == nil {
		return out, nil
	}

	// declaredTickets is every BaselineDefect THIS request's own corpus
	// entry declares right now, always a non-nil slice (possibly empty)
	// so RESTReceipt.DeclaredDefects writes a KNOWN, real array rather
	// than SQL NULL -- see that field's own doc comment. Built
	// unconditionally, not only when decodeBody: a StatusOnly request
	// with no Parity set simply writes a known-empty array, which is
	// the truth (nothing was, or could have been, checked for it).
	declaredTickets := make([]string, len(request.Parity.BaselineDefects))
	for i, defect := range request.Parity.BaselineDefects {
		declaredTickets[i] = defect.Ticket
	}

	// MeasurementRoute/BuildBinding are always RouteProof/EdgeBuildPresent:
	// RESTAdmit refuses admission outright when the candidate leg's build
	// header is absent or mismatched (RESTRefusalBuildUnbound), so a
	// receipt is only ever built from an observation that already
	// satisfies that binding -- see goapiproof.RESTReceipt's own doc
	// comment.
	receipt := goapiproof.RESTReceipt{
		Method:                           spec.Method,
		Path:                             spec.Path,
		CandidateBuild:                   namedBuild,
		RequestIdentity:                  identity,
		Stage:                            goapiproof.EnablementProofStage,
		TerminalState:                    terminalState,
		OrgID:                            f.org,
		ReviewEvidence:                   encodeRESTReviewEvidence(f.reviewEvidence, findingsRef, spec.PythonForwarder && f.pythonForwarderOff, admittedBaselineRef, admittedCandidateRef),
		RecordedBy:                       f.recordedBy,
		ObservedAt:                       observedAt,
		MeasurementRoute:                 goapiproof.RouteProof,
		BaselineDefects:                  matchedDefects,
		DeclaredDefects:                  declaredTickets,
		DifferencesOutsideBaselineDefect: differences,
		BuildBinding:                     goapiproof.EdgeBuildPresent,
		BoundIDs:                         boundIDs,
		BaselineResponseRef:              baselineRef,
		CandidateResponseRef:             candidateRef,
	}
	id, err := writer.WriteReceipt(ctx, receipt)
	if err != nil {
		return outcome{}, fmt.Errorf("write receipt: %w", err)
	}
	out.ReceiptID = id.String()

	// Read back every declared BaselineDefect's own firing history --
	// AFTER the write above, so this run's own just-committed receipt is
	// already part of what gets read: the report line for THIS run
	// includes THIS run. Gated on decodeBody, not just on the
	// declaration list being non-empty: a StatusOnly request's body is
	// never decoded or compared (Compare never runs, so no declaration
	// could possibly have fired this run), and reading history for one
	// anyway would report every such run as "live" against a mechanism
	// that was never actually checked.
	if decodeBody && len(declaredTickets) > 0 {
		firing, err := writer.ReadFiringHistory(ctx, spec.Method, spec.Path, identity, declaredTickets)
		if err != nil {
			return outcome{}, fmt.Errorf("read declaration firing history: %w", err)
		}
		out.DeclarationFiring = firing
	}
	return out, nil
}

// proveEdgeCredentialOnCandidate sends this request's OWN edge access
// token -- edgeCredential, the SAME credential value the baseline leg
// already sent to the Python api service in proveOneRESTRequest above --
// DIRECTLY to query-api (the candidate) -- the point measured live:
// query-api must answer the credential a real user's browser actually
// carries, not only the effective-principal envelope the ordinary
// candidate leg already exercises.
//
// A STATUS-ONLY admission check (declared-admissible status code, plus
// the same build-header binding RESTAdmit's own candidate check
// enforces), never a body Compare: the ordinary candidate leg (still
// envelope-authenticated, a few lines above in run()) already owns body
// parity against the baseline snapshot, under this request's own
// BaselineDefect/VolatileFields declarations. Running Compare a second
// time here would score ONE baseline observation against TWO
// differently-authenticated candidate observations under those SAME
// declarations -- not what they were written to excuse, and not what
// resultVacuityErrors' "did this declaration excuse anything" accounting
// expects either. No receipt is written for this leg: go_api_rest_proof_
// run's own schema is RESTReceipt's one-candidate-one-baseline shape
// (see that struct's own doc comment); recording a second, structurally
// different observation type there is a separate, larger change out of
// scope here.
//
// Its own response body IS still stored to artifacts when one is given,
// unconditionally and before the switch below -- the same
// store-before-admission discipline proveOneRESTRequest's own bodies
// follow (see its doc comment), so a refusal here leaves the SAME kind
// of evidence behind a refusal on the ordinary candidate leg does.
// artifacts is nil only in a test that does not care about the ref;
// main() always builds one (run()'s own -artifact-dir, required).
func proveEdgeCredentialOnCandidate(
	ctx context.Context,
	client *goapiproof.LegClient,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	edgeCredential *goapiproof.Credential,
	namedBuild string,
	artifacts *goapiproof.ArtifactStore,
) (outcome, error) {
	requestName := request.Name + " (edge-credential-on-candidate)"
	timeout := resolveRESTTimeout(request.Timeout, f.timeout)
	leg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, edgeCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(ctx, operation, requestName, "candidate", nil, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("candidate leg (edge credential): %w", err)
	}

	out := outcome{Operation: operation, Request: requestName, CandidateWireAttempts: leg.WireAttempts}
	if artifacts != nil {
		ref, refErr := artifacts.Put(leg.Body)
		if refErr != nil {
			return outcome{}, fmt.Errorf("store candidate (edge credential) leg artifact: %w", refErr)
		}
		out.CandidateResponseRef = ref
	}
	switch {
	case leg.StatusCode != request.WantCandidateStatus:
		out.Refusal = goapiproof.RESTRefusalUnexpectedStatus
		out.Detail = fmt.Sprintf("candidate (edge credential) answered HTTP %d, this request declared %d admissible", leg.StatusCode, request.WantCandidateStatus)
	case leg.Build == "":
		out.Refusal = goapiproof.RESTRefusalBuildUnbound
		out.Detail = "the candidate response (edge credential) carried no x-dev-health-build header"
	case leg.Build != namedBuild:
		out.Refusal = goapiproof.RESTRefusalBuildUnbound
		out.Detail = fmt.Sprintf("the process that served this request (edge credential) reports build %q, but /buildinfo named %q", leg.Build, namedBuild)
	default:
		out.Admitted = true
		out.TerminalState = "edge_credential_admitted"
	}
	return out, nil
}

// receiptWriter is the subset of *pgxpool.Pool this command needs, so a
// test can supply a fake instead of a real Postgres. ReadFiringHistory
// sits beside WriteReceipt rather than behind a second interface: both
// are read/write halves of the SAME go_api_rest_proof_run table, and a
// caller that can write a receipt to it always also needs to read its
// own declarations' history back.
type receiptWriter interface {
	WriteReceipt(ctx context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error)
	ReadFiringHistory(ctx context.Context, method, path, requestIdentity string, tickets []string) ([]goapiproof.FiringHistory, error)
}

type pgxReceiptWriter struct{ pool *pgxpool.Pool }

func (w *pgxReceiptWriter) WriteReceipt(ctx context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error) {
	return goapiproof.WriteRESTAtomic(ctx, w.pool, receipt)
}

func (w *pgxReceiptWriter) ReadFiringHistory(ctx context.Context, method, path, requestIdentity string, tickets []string) ([]goapiproof.FiringHistory, error) {
	return goapiproof.ReadRESTFiringHistory(ctx, w.pool, method, path, requestIdentity, tickets)
}

func (w *pgxReceiptWriter) Close() { w.pool.Close() }

// receiptWriterCloser is what newPGXPool returns: receiptWriter plus the
// Close the caller's own defer needs. A var of this (interface) type,
// not a direct pgxpool.New call at the call site, so a test can
// substitute a fake writer and drive run() end to end without a real
// database -- the same reason openPostgresPool is a var in
// cmd/go-api-prove.
type receiptWriterCloser interface {
	receiptWriter
	Close()
}

var newPGXPool = func(ctx context.Context, dsn string) (receiptWriterCloser, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, secrets.RedactedConnectError("connect to postgres", "postgres-uri", postgresURIEnvVar)
	}
	// pgxpool.New never dials -- the first real operation does, and
	// WriteReceipt's own goapiproof.WriteRESTAtomic (Begin, then the
	// insert) would otherwise be the first thing to discover a bad DSN,
	// with pgx's connection error (which can carry the DSN itself)
	// surfacing through THAT call's own %w wrap. Ping forces the dial
	// here, still behind the redacted error above.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, secrets.RedactedConnectError("connect to postgres", "postgres-uri", postgresURIEnvVar)
	}
	return &pgxReceiptWriter{pool: pool}, nil
}
