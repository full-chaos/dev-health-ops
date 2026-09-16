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
// hostname), and both bearer helpers minting a fresh envelope from
// inside the pod:
//
//	go-api-rest-prove \
//	  -query-api-url http://<query-api-service>:8090 \
//	  -python-api-url http://<api-service>:8000 \
//	  -candidate-bearer-exec '["mint-envelope","-org","<org>"]' \
//	  -baseline-bearer-exec  '["mint-envelope","-org","<org>"]' \
//	  -org <org> -recorded-by <operator> -review-evidence "<why>" \
//	  -postgres-uri "$POSTGRES_URI"
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
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

func main() {
	f, err := parseFlags()
	if err != nil {
		log.Fatalf("go-api-rest-prove: %v", err)
	}
	if err := run(f); err != nil {
		log.Fatalf("go-api-rest-prove: %v", err)
	}
}

type flags struct {
	queryAPIURL    string
	pythonAPIURL   string
	buildInfoURL   string
	candidateBuild string
	queryAPISrc    string

	candidateBearerExec string
	baselineBearerExec  string

	postgresURI    string
	org            string
	recordedBy     string
	reviewEvidence string
	principalKind  string
	audience       string
	keyID          string

	dryRun     bool
	timeout    time.Duration
	reportPath string
}

func parseFlags() (flags, error) {
	var f flags
	flag.StringVar(&f.queryAPIURL, "query-api-url", "http://localhost:8090", "query-api's OWN in-cluster address -- the candidate leg. Never an edge or ingress URL: every request this tool sends goes DIRECTLY to this service")
	flag.StringVar(&f.pythonAPIURL, "python-api-url", "", "the Python api service's OWN in-cluster address -- the baseline leg (required). Never an edge or ingress URL, for the same reason as -query-api-url")
	flag.StringVar(&f.buildInfoURL, "buildinfo-url", "", "GET /buildinfo on query-api -- the ONLY source of the build identity every receipt names. Defaults to -query-api-url + \"/buildinfo\"")
	flag.StringVar(&f.candidateBuild, "candidate-build", "", "optional CROSS-CHECK: fail if the running build is not this sha. Never the source of the value written -- the value written always comes from /buildinfo, matching go-api-prove's own -candidate-build flag")
	flag.StringVar(&f.queryAPISrc, "query-api-src", "", "OPTIONAL dev-only override: path to a REAL query-api source checkout, read LIVE to confirm this corpus's paths match what the mux actually mounts (migrationmatrix.LoadQueryAPIMuxRoutes). Empty (the default) uses goapiproof.MountedRESTPaths, the checked-in snapshot this binary ships with -- the operator tools image carries no Go source tree at all, so that is the ONLY option available there. Set this only when running from a real repo checkout, to catch drift immediately instead of waiting for TestMountedRESTPathsMatchesTheRealQueryAPIMux's own CI run")
	flag.StringVar(&f.candidateBearerExec, "candidate-bearer-exec", "", "JSON array whose first element is an ALLOWLISTED HELPER NAME (\"mint-envelope\" or \"mint-edge-token\", never a path -- see goapiproof.MintViaAllowlistedHelper) printing a FRESH bearer credential for query-api on stdout, e.g. [\"mint-envelope\",\"-org\",\"<org>\"]. Re-run as the credential ages. The helper reads any secret it needs from ITS OWN environment -- never from an argument here. The remaining elements are the helper's own argv, never a shell string: nothing is interpolated into a shell. The helper's stdout and stderr are NEVER reported by this command")
	flag.StringVar(&f.baselineBearerExec, "baseline-bearer-exec", "", "JSON array, same allowlisted-helper-name-plus-argv shape as -candidate-bearer-exec, printing a FRESH bearer credential for the Python api service on stdout -- see credential.go's own doc comment for why one credential kind cannot be assumed to reach both planes")
	flag.StringVar(&f.postgresURI, "postgres-uri", os.Getenv("POSTGRES_URI"), "domain Postgres DSN holding go_api_proof_run. Required unless -dry-run")
	flag.StringVar(&f.org, "org", "", "org id every request is made for (required)")
	flag.StringVar(&f.recordedBy, "recorded-by", "", "WHO is running this, recorded on every receipt (required)")
	flag.StringVar(&f.reviewEvidence, "review-evidence", "", "WHY, in your own words, recorded on every receipt (required)")
	flag.StringVar(&f.principalKind, "principal-kind", "stored_account", "auth-context SHAPE recorded in request_identity; never a credential")
	flag.StringVar(&f.audience, "audience", "query-api", "envelope audience, part of the auth-context shape")
	flag.StringVar(&f.keyID, "key-id", "", "envelope signing key id (kid) -- a public identifier")
	flag.BoolVar(&f.dryRun, "dry-run", false, "execute and compare, but write NO receipts")
	flag.DurationVar(&f.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.StringVar(&f.reportPath, "report", "", "write the full JSON report here in addition to stdout")
	flag.Parse()

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
	if !f.dryRun && f.postgresURI == "" {
		missing = append(missing, "-postgres-uri (or -dry-run)")
	}
	if len(missing) > 0 {
		return flags{}, fmt.Errorf("missing required flag(s): %s", strings.Join(missing, ", "))
	}
	return f, nil
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
// A transport-level failure (the leg never answered at all) is a hard
// error, not a refusal: RESTAdmit exists to judge a response that arrived,
// never the absence of one.
//
// credential is nil for a PublicNoAuth route (goapiproof.RESTEndpointSpec.
// PublicNoAuth) -- the request is then sent with NO Authorization header
// on this leg at all, matching meta.go's own "Auth: PUBLIC" contract:
// measuring meta WITH a bearer token would exercise a path real anonymous
// traffic never takes, and a route that happens to also accept an
// unrelated valid token is not proof of the public code path.
func doREST(ctx context.Context, client *http.Client, baseURL, method, path string, query url.Values, body any, credential *goapiproof.Credential) (goapiproof.RESTLeg, error) {
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

	resp, err := client.Do(req)
	if err != nil {
		return goapiproof.RESTLeg{}, fmt.Errorf("%s %s: %w", method, target, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return goapiproof.RESTLeg{}, fmt.Errorf("%s %s: read body: %w", method, target, err)
	}
	return goapiproof.RESTLeg{StatusCode: resp.StatusCode, Body: raw, Build: resp.Header.Get("x-dev-health-build")}, nil
}

// restRequestVariables is what RequestIdentity digests for one corpus
// request -- the method, query and body, the same "identity is the actual
// request shape, never the operation name" rule identity.go's own
// RequestIdentity doc comment states for GraphQL variables.
func restRequestVariables(method string, query url.Values, body any) map[string]any {
	return map[string]any{"method": method, "query": query, "body": body}
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
}

func (o outcome) line() string {
	if !o.Admitted {
		return fmt.Sprintf("%s/%s: REFUSED %s -- %s", o.Operation, o.Request, o.Refusal, o.Detail)
	}
	boundSuffix := ""
	if len(o.BoundIDs) > 0 {
		boundSuffix = fmt.Sprintf(" bound=%v", o.BoundIDs)
	}
	return fmt.Sprintf("%s/%s: %s outside=%d baseline_defect=%v receipt=%s%s",
		o.Operation, o.Request, o.TerminalState, o.DifferencesOutsideBaselineDefect, o.BaselineDefectsMatched, o.ReceiptID, boundSuffix)
}

// resultVacuityErrors names every declaration in result that matched
// nothing or was refused -- a citation, exclusion, Tier-B entry or
// order-insensitive-list declaration that excuses nothing is either stale
// or misspelled, and compare.go's own Result field comments state the
// caller fails the run on each of these, uniformly.
func resultVacuityErrors(result goapiproof.Result) []string {
	var errs []string
	if len(result.UnusedExclusions) > 0 {
		errs = append(errs, fmt.Sprintf("unused VolatileFields: %v", result.UnusedExclusions))
	}
	if len(result.UnusedTierB) > 0 {
		errs = append(errs, fmt.Sprintf("unused FloatTierB: %v", result.UnusedTierB))
	}
	if len(result.StaleBaselineDefects) > 0 {
		errs = append(errs, fmt.Sprintf("stale BaselineDefects: %v", result.StaleBaselineDefects))
	}
	if len(result.UnusedOrderInsensitiveLists) > 0 {
		errs = append(errs, fmt.Sprintf("unused OrderInsensitiveLists: %v", result.UnusedOrderInsensitiveLists))
	}
	if len(result.OrderInsensitiveListRefusals) > 0 {
		errs = append(errs, fmt.Sprintf("OrderInsensitiveList refusals: %v", result.OrderInsensitiveListRefusals))
	}
	return errs
}

func run(f flags) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Checked FIRST, ahead of every credential/network step below: both
	// need no network and (in the default, -query-api-src-unset case) no
	// filesystem either, so a corpus/coverage problem is refused
	// immediately rather than after a wasted round-trip -- and, not
	// incidentally, this ordering is what lets a smoke run from a
	// directory with no source tree at all prove the coverage check on
	// its own, before anything network-dependent has a chance to fail
	// first for an unrelated reason.
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

	candidateCredential, err := buildCredential("Authorization", "candidate bearer", "-candidate-bearer-exec", f.candidateBearerExec)
	if err != nil {
		return err
	}
	baselineCredential, err := buildCredential("Authorization", "baseline bearer", "-baseline-bearer-exec", f.baselineBearerExec)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: f.timeout}

	// candidateCredential -- the SAME credential every corpus request's
	// own candidate leg uses below (doREST's own candidateCredential
	// argument in proveOneRESTRequest) -- never a second, separately
	// minted credential: /buildinfo is authenticated with the same
	// bearer-envelope verifier the REST routes themselves use
	// (buildinfo_route.go's own doc comment), so there is no "proof-
	// plane" credential distinct from what this binary already mints for
	// every other request in the run.
	namedBuild, err := goapiproof.FetchBuildIdentity(ctx, client, f.buildInfoURL, candidateCredential)
	if err != nil {
		return fmt.Errorf("read the candidate build from /buildinfo: %w", err)
	}
	if f.candidateBuild != "" && f.candidateBuild != namedBuild {
		return fmt.Errorf("-candidate-build %q does not match the running build %q reported by %s", f.candidateBuild, namedBuild, f.buildInfoURL)
	}

	var pgPool receiptWriter
	if f.postgresURI != "" {
		pool, err := newPGXPool(ctx, f.postgresURI)
		if err != nil {
			return fmt.Errorf("connect to postgres: %w", err)
		}
		defer pool.Close()
		pgPool = pool
	}

	auth := goapiproof.AuthContext{PrincipalKind: f.principalKind, Audience: f.audience, KeyID: f.keyID}
	observedAt := time.Now().UTC()

	var outcomes []outcome
	attempted, admitted, matched, mismatched := 0, 0, 0, 0

	// produced accumulates every id an earlier request's OWN Produces
	// declaration yielded from its baseline (Python) response, keyed by
	// producer name -- RESTRunOrder (not KnownRESTOperations' alphabetical
	// order) guarantees a producer's operation is always visited before
	// any operation that binds one of its ids; see restcorpus.go's own
	// restRunOrder doc comment for why alphabetical order cannot make
	// that guarantee.
	produced := map[string]string{}

	for _, operation := range goapiproof.RESTRunOrder() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			return err
		}
		for _, request := range spec.Requests {
			attempted++

			resolvedSpec := spec
			resolvedRequest := request
			var boundIDs map[string]string
			if len(request.IDBindings) > 0 {
				resolvedPath, resolvedQuery, unresolved := goapiproof.ResolveRESTIDBindings(spec.Path, request, produced)
				if len(unresolved) > 0 {
					// An entry whose id does not resolve is refused by
					// name and counts as unproven, never a tool failure
					// -- neither leg is ever called.
					out := outcome{
						Operation: operation, Request: request.Name,
						Admitted: false,
						Refusal:  goapiproof.RESTRefusalIDBindingUnresolved,
						Detail:   fmt.Sprintf("no earlier request in this run produced: %v", unresolved),
					}
					outcomes = append(outcomes, out)
					fmt.Println(out.line())
					continue
				}
				resolvedSpec.Path = resolvedPath
				resolvedRequest.Query = resolvedQuery
				boundIDs = make(map[string]string, len(request.IDBindings))
				for _, binding := range request.IDBindings {
					// Already confirmed present above (unresolved was
					// empty): the same value ResolveRESTIDBindings just
					// wrote into resolvedPath/resolvedQuery.
					boundIDs[binding.Producer] = produced[binding.Producer]
				}
			}

			out, err := proveOneRESTRequest(ctx, client, f, operation, resolvedSpec, resolvedRequest, candidateCredential, baselineCredential, namedBuild, auth, observedAt, pgPool, f.dryRun, boundIDs)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", operation, request.Name, err)
			}
			outcomes = append(outcomes, out)
			fmt.Println(out.line())
			for name, id := range out.producedIDs {
				produced[name] = id
			}
			if out.Admitted {
				admitted++
				switch out.TerminalState {
				case goapiproof.TerminalStateMatch:
					matched++
				case goapiproof.TerminalStateMismatch:
					mismatched++
				}
			}
		}
	}

	fmt.Printf("attempted=%d admitted=%d match=%d mismatch=%d refused=%d\n",
		attempted, admitted, matched, mismatched, attempted-admitted)

	if f.reportPath != "" {
		if err := writeJSONReport(f.reportPath, outcomes); err != nil {
			return err
		}
	}

	var vacuityErrs []string
	for _, out := range outcomes {
		for _, e := range out.vacuityErrors {
			vacuityErrs = append(vacuityErrs, fmt.Sprintf("%s/%s: %s", out.Operation, out.Request, e))
		}
	}
	if len(vacuityErrs) > 0 {
		sort.Strings(vacuityErrs)
		return fmt.Errorf("this run found declarations that excuse nothing (fix or remove them):\n%s", strings.Join(vacuityErrs, "\n"))
	}
	return nil
}

func writeJSONReport(path string, outcomes []outcome) error {
	encoded, err := json.MarshalIndent(outcomes, "", "  ")
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
	client *http.Client,
	f flags,
	operation string,
	spec goapiproof.RESTEndpointSpec,
	request goapiproof.RESTRequest,
	candidateCredential, baselineCredential *goapiproof.Credential,
	namedBuild string,
	auth goapiproof.AuthContext,
	observedAt time.Time,
	writer receiptWriter,
	dryRun bool,
	boundIDs map[string]string,
) (outcome, error) {
	if spec.PublicNoAuth {
		candidateCredential, baselineCredential = nil, nil
	}
	candidateLeg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, candidateCredential)
	if err != nil {
		return outcome{}, fmt.Errorf("candidate leg: %w", err)
	}
	baselineLeg, err := doREST(ctx, client, f.pythonAPIURL, spec.Method, spec.Path, request.Query, request.Body, baselineCredential)
	if err != nil {
		return outcome{}, fmt.Errorf("baseline leg: %w", err)
	}

	decodeBody := request.BodyMode == goapiproof.RESTBodyModeJSON
	admission := goapiproof.RESTAdmit(goapiproof.RESTAdmissionInput{
		NamedBuild:          namedBuild,
		WantCandidateStatus: request.WantCandidateStatus,
		WantBaselineStatus:  request.WantBaselineStatus,
		Candidate:           candidateLeg,
		Baseline:            baselineLeg,
	}, decodeBody)

	out := outcome{Operation: operation, Request: request.Name, Admitted: admission.Admitted, Refusal: admission.Reason, Detail: admission.Detail, BoundIDs: boundIDs}
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
			for _, producer := range request.Produces {
				if id, ok := goapiproof.ExtractRESTID(admission.BaselineSnap.Data, producer); ok {
					out.producedIDs[producer.Name] = id
				}
			}
		}

		result := goapiproof.Compare(admission.BaselineSnap, admission.CandidateSnap, request.Parity)
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
			return out, nil
		}
		terminalState = result.TerminalState
		differences = result.DifferencesOutsideBaselineDefect
		matchedDefects = result.BaselineDefectsMatched
		vacuity = resultVacuityErrors(result)
	}

	out.TerminalState = terminalState
	out.DifferencesOutsideBaselineDefect = differences
	out.BaselineDefectsMatched = matchedDefects
	out.vacuityErrors = vacuity

	if dryRun || writer == nil {
		return out, nil
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
		ReviewEvidence:                   f.reviewEvidence,
		RecordedBy:                       f.recordedBy,
		ObservedAt:                       observedAt,
		MeasurementRoute:                 goapiproof.RouteProof,
		BaselineDefects:                  matchedDefects,
		DifferencesOutsideBaselineDefect: differences,
		BuildBinding:                     goapiproof.EdgeBuildPresent,
		BoundIDs:                         boundIDs,
	}
	id, err := writer.WriteReceipt(ctx, receipt)
	if err != nil {
		return outcome{}, fmt.Errorf("write receipt: %w", err)
	}
	out.ReceiptID = id.String()
	return out, nil
}

// receiptWriter is the subset of *pgxpool.Pool this command needs, so a
// test can supply a fake instead of a real Postgres.
type receiptWriter interface {
	WriteReceipt(ctx context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error)
}

type pgxReceiptWriter struct{ pool *pgxpool.Pool }

func (w *pgxReceiptWriter) WriteReceipt(ctx context.Context, receipt goapiproof.RESTReceipt) (uuid.UUID, error) {
	return goapiproof.WriteRESTAtomic(ctx, w.pool, receipt)
}

func (w *pgxReceiptWriter) Close() { w.pool.Close() }

func newPGXPool(ctx context.Context, dsn string) (*pgxReceiptWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &pgxReceiptWriter{pool: pool}, nil
}
