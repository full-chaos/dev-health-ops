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
	artifactDir    string

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
	flag.StringVar(&f.artifactDir, "artifact-dir", "", "directory for content-addressed leg bodies and finding lists; go_api_rest_proof_run's baseline/candidate_response_ref name the two legs, and review_evidence names the finding-list ref -- never an inlined body (required), matching go-api-prove's own -artifact-dir")
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
	if f.artifactDir == "" {
		missing = append(missing, "-artifact-dir")
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
func doREST(ctx context.Context, client *http.Client, baseURL, method, path string, query url.Values, body any, credential *goapiproof.Credential, timeout time.Duration) (goapiproof.RESTLeg, error) {
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

	resp, err := client.Do(req)
	if err != nil {
		return goapiproof.RESTLeg{}, goapiproof.NewTransportFailure(target, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return goapiproof.RESTLeg{}, goapiproof.NewTransportFailure(target, err)
	}
	return goapiproof.RESTLeg{StatusCode: resp.StatusCode, Body: raw, Build: resp.Header.Get("x-dev-health-build")}, nil
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

// legTransportOutcome turns a leg's OWN transport failure into the
// per-case refusal outcome this ticket exists to produce, naming the
// FAILING LEG and the FAILURE CLASS rather than folding it into an
// existing reason. ok is false when err is not itself a transport
// failure -- something other than "the leg never answered" went wrong,
// which is not this function's claim to make safe, and the caller keeps
// treating it as a fatal error.
func legTransportOutcome(operation, requestName, leg string, boundIDs map[string]string, err error) (outcome, bool) {
	var failure goapiproof.TransportFailure
	if !errors.As(err, &failure) {
		return outcome{}, false
	}
	timedOut := failure.Class == goapiproof.TransportTimeout
	var reason string
	switch {
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
}

// encodeRESTReviewEvidence renders restReviewEvidence for one receipt.
// Falls back to the raw operator string on a marshal error (both fields
// are plain strings, so this cannot actually fail) rather than writing
// nothing -- same fallback ReceiptProvenance's own reviewEvidence method
// uses, and the same reasoning: the operator's own words are worth more
// than a dropped column.
func encodeRESTReviewEvidence(operator, findingsRef string) string {
	encoded, err := json.Marshal(restReviewEvidence{Operator: operator, FindingsRef: findingsRef})
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

	// BaselineResponseRef/CandidateResponseRef mirror the identically
	// named RESTReceipt fields (goapiproof.RESTReceipt's own doc
	// comment) -- the content-addressed -artifact-dir reference to each
	// leg's raw response body. Set whenever -artifact-dir stored a body,
	// which is BEFORE admission runs (proveOneRESTRequest stores both
	// legs first) -- so a REFUSED request's own outcome line still shows
	// where its bodies landed, not only an admitted one's.
	BaselineResponseRef  string `json:"baseline_response_ref,omitempty"`
	CandidateResponseRef string `json:"candidate_response_ref,omitempty"`

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

	// DeclarationFiring reports, one entry per BaselineDefect this
	// request declares, that ticket's own firing history read back from
	// go_api_rest_proof_run -- see goapiproof.FiringHistory's own doc
	// comment. Empty when this request declares no BaselineDefects, or
	// when no receipt was written for it this run (a refused or
	// dry-run request extends no history to read back against).
	DeclarationFiring []goapiproof.FiringHistory `json:"declaration_firing,omitempty"`
}

func (o outcome) line() string {
	if !o.Admitted {
		return fmt.Sprintf("%s/%s: REFUSED %s -- %s", o.Operation, o.Request, o.Refusal, o.Detail)
	}
	boundSuffix := ""
	if len(o.BoundIDs) > 0 {
		boundSuffix = fmt.Sprintf(" bound=%v", o.BoundIDs)
	}
	firingSuffix := ""
	if len(o.DeclarationFiring) > 0 {
		firingSuffix = " " + formatDeclarationFiring(o.DeclarationFiring)
	}
	return fmt.Sprintf("%s/%s: %s outside=%d baseline_defect=%v receipt=%s%s%s",
		o.Operation, o.Request, o.TerminalState, o.DifferencesOutsideBaselineDefect, o.BaselineDefectsMatched, o.ReceiptID, boundSuffix, firingSuffix)
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
	if len(result.LiveBaselineDefectsUnexplained) > 0 {
		errs = append(errs, fmt.Sprintf("live but unexplained BaselineDefects: %v", result.LiveBaselineDefectsUnexplained))
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
	// signal.NotifyContext gives an operator's Ctrl-C (or a SIGTERM from
	// the surrounding orchestration) a chance to stop the run between
	// requests rather than kill the process outright -- see the ctx.Err()
	// checks in the request loop below. Cheap: this changes nothing for a
	// run nobody interrupts.
	sigCtx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopSignal()
	ctx, cancel := context.WithTimeout(sigCtx, 30*time.Minute)
	defer cancel()

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
	client := &http.Client{}

	// candidateCredential -- the SAME credential every corpus request's
	// own candidate leg uses below (doREST's own candidateCredential
	// argument in proveOneRESTRequest) -- never a second, separately
	// minted credential: /buildinfo is authenticated with the same
	// bearer-envelope verifier the REST routes themselves use
	// (buildinfo_route.go's own doc comment), so there is no "proof-
	// plane" credential distinct from what this binary already mints for
	// every other request in the run.
	//
	// Bounded by the run's own -timeout default, same as every measured
	// leg -- client carries no Timeout of its own any more (above), so
	// this read would otherwise wait on ctx's 30-minute run-level bound
	// alone.
	buildInfoCtx, cancelBuildInfo := context.WithTimeout(ctx, f.timeout)
	namedBuild, err := goapiproof.FetchBuildIdentity(buildInfoCtx, client, f.buildInfoURL, candidateCredential)
	cancelBuildInfo()
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

	return runMeasurement(ctx, client, f, candidateCredential, baselineCredential, namedBuild, pgPool, artifacts)
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
func runMeasurement(ctx context.Context, client *http.Client, f flags, candidateCredential, baselineCredential *goapiproof.Credential, namedBuild string, pgPool receiptWriter, artifacts *goapiproof.ArtifactStore) error {
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
	var planned []plannedRequest
	var specErr error
	for _, operation := range goapiproof.RESTRunOrder() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			specErr = fmt.Errorf("%s: %w", operation, err)
			break
		}
		for _, request := range spec.Requests {
			planned = append(planned, plannedRequest{operation: operation, request: request, spec: spec})
		}
	}

	var outcomes []outcome
	attempted, admitted, matched, mismatched := 0, 0, 0, 0
	attemptedKeys := map[string]bool{}

	// produced accumulates every id an earlier request's OWN Produces
	// declaration yielded from its baseline (Python) response, keyed by
	// producer name -- RESTRunOrder (not KnownRESTOperations' alphabetical
	// order) guarantees a producer's operation is always visited before
	// any operation that binds one of its ids; see restcorpus.go's own
	// restRunOrder doc comment for why alphabetical order cannot make
	// that guarantee.
	produced := map[string]string{}

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

		resolvedSpec := spec
		resolvedRequest := request
		var boundIDs map[string]string
		if len(request.IDBindings) > 0 {
			resolvedPath, resolvedQuery, resolvedBody, unresolved := goapiproof.ResolveRESTIDBindings(spec.Path, request, produced)
			if len(unresolved) > 0 {
				// An entry whose id does not resolve is refused by
				// name and counts as unproven, never a tool failure
				// -- neither leg is ever called. Its own edge-
				// credential-on-candidate sibling is marked attempted
				// here too, for the identical reason: this is a
				// deliberate, named skip of BOTH legs for this one
				// request, never the run stopping before it reached
				// them -- notRunKeys must not read this the same way
				// it reads a run that broke off early.
				out := outcome{
					Operation: operation, Request: request.Name,
					Admitted: false,
					Refusal:  goapiproof.RESTRefusalIDBindingUnresolved,
					Detail:   fmt.Sprintf("no earlier request in this run produced: %v", unresolved),
				}
				outcomes = append(outcomes, out)
				fmt.Println(out.line())
				if !spec.PublicNoAuth {
					attemptedKeys[operation+"/"+request.Name+" (edge-credential-on-candidate)"] = true
				}
				continue
			}
			resolvedSpec.Path = resolvedPath
			resolvedRequest.Query = resolvedQuery
			resolvedRequest.Body = resolvedBody
			boundIDs = make(map[string]string, len(request.IDBindings))
			for _, binding := range request.IDBindings {
				// Already confirmed present above (unresolved was
				// empty): the same value ResolveRESTIDBindings just
				// wrote into resolvedPath/resolvedQuery.
				boundIDs[binding.Producer] = produced[binding.Producer]
			}
		}

		out, err := proveOneRESTRequest(ctx, client, f, operation, resolvedSpec, resolvedRequest, candidateCredential, baselineCredential, namedBuild, auth, observedAt, pgPool, artifacts, f.dryRun, boundIDs)
		if err != nil {
			runErr = fmt.Errorf("%s/%s: %w", operation, request.Name, err)
			break requestLoop
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
		if !resolvedSpec.PublicNoAuth {
			attempted++
			attemptedKeys[operation+"/"+request.Name+" (edge-credential-on-candidate)"] = true
			edgeOut, err := proveEdgeCredentialOnCandidate(ctx, client, f, operation, resolvedSpec, resolvedRequest, baselineCredential, namedBuild, artifacts)
			if err != nil {
				runErr = fmt.Errorf("%s/%s (edge credential on candidate): %w", operation, request.Name, err)
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
	if len(notRun) > 0 {
		fmt.Printf("partial run: %d request(s) never attempted: %v\n", len(notRun), notRun)
	}
	fmt.Printf("attempted=%d admitted=%d match=%d mismatch=%d refused=%d\n",
		attempted, admitted, matched, mismatched, attempted-admitted)

	if f.reportPath != "" {
		if writeErr := writeJSONReport(f.reportPath, outcomes, notRun); writeErr != nil {
			if runErr == nil {
				runErr = writeErr
			} else {
				runErr = fmt.Errorf("%w; additionally, writing the report failed: %v", runErr, writeErr)
			}
		}
	}

	if runErr != nil {
		return runErr
	}

	// A leg that never answered is not a tool error (it is already
	// recorded above as its own named refusal, per case), but a run
	// containing one must still exit non-zero: it is evidence the
	// instrument itself did not complete cleanly, distinct from an
	// ordinary admission refusal, and an operator must not have to read
	// every line of output to notice one happened.
	if legFailures := legFailuresIn(outcomes); len(legFailures) > 0 {
		return fmt.Errorf("this run measured a leg that never answered (fix the deployment, or for a legitimately slow baseline declare a longer RESTRequest.Timeout):\n%s", strings.Join(legFailures, "\n"))
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

// jsonReport is -report's own top-level shape: not a bare array of
// outcomes any more, so a partial run can say it is partial IN the file
// an operator reads back, not only on stdout.
type jsonReport struct {
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
}

func writeJSONReport(path string, outcomes []outcome, notRun []string) error {
	encoded, err := json.MarshalIndent(jsonReport{Outcomes: outcomes, Partial: len(notRun) > 0, NotRun: notRun}, "", "  ")
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
	artifacts *goapiproof.ArtifactStore,
	dryRun bool,
	boundIDs map[string]string,
) (outcome, error) {
	if spec.PublicNoAuth {
		candidateCredential, baselineCredential = nil, nil
	}
	timeout := resolveRESTTimeout(request.Timeout, f.timeout)
	candidateLeg, err := doREST(ctx, client, f.queryAPIURL, spec.Method, spec.Path, request.Query, request.Body, candidateCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(operation, request.Name, "candidate", boundIDs, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("candidate leg: %w", err)
	}
	baselineLeg, err := doREST(ctx, client, f.pythonAPIURL, spec.Method, spec.Path, request.Query, request.Body, baselineCredential, timeout)
	if err != nil {
		if out, ok := legTransportOutcome(operation, request.Name, "baseline", boundIDs, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("baseline leg: %w", err)
	}

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
		Candidate:           candidateLeg,
		Baseline:            baselineLeg,
	}, decodeBody)

	out := outcome{
		Operation: operation, Request: request.Name,
		Admitted: admission.Admitted, Refusal: admission.Reason, Detail: admission.Detail,
		BoundIDs:             boundIDs,
		BaselineResponseRef:  baselineRef,
		CandidateResponseRef: candidateRef,
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
			return out, nil
		}
		terminalState = result.TerminalState
		differences = result.DifferencesOutsideBaselineDefect
		matchedDefects = result.BaselineDefectsMatched
		vacuity = resultVacuityErrors(result)
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
		// A declared producer this branch cannot resolve -- a decode
		// failure, or a body that yields no value at a declared entry's
		// path -- refuses THIS request by name
		// (goapiproof.RESTRefusalCandidateProducerUnresolved) rather than
		// leaving out.producedIDs silently short: a check that can never
		// fail a run proves nothing, and a later consumer's own
		// IDBindings would otherwise be refused by name
		// (rest_request_id_binding_unresolved) with no visible reason on
		// the request that actually failed to produce it.
		candidateSnap, decodeErr := goapiproof.DecodeRESTSnapshot(candidateLeg.Body)
		if decodeErr != nil {
			out.Admitted = false
			out.Refusal = goapiproof.RESTRefusalCandidateProducerUnresolved
			out.Detail = fmt.Sprintf("the candidate body did not decode: %s", decodeErr)
			return out, nil
		}
		out.producedIDs = make(map[string]string, len(request.Produces))
		var unresolved []string
		for _, producer := range request.Produces {
			id, ok := goapiproof.ExtractRESTID(candidateSnap.Data, producer)
			if !ok {
				unresolved = append(unresolved, producer.Name)
				continue
			}
			out.producedIDs[producer.Name] = id
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
		ReviewEvidence:                   encodeRESTReviewEvidence(f.reviewEvidence, findingsRef),
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
	client *http.Client,
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
		if out, ok := legTransportOutcome(operation, requestName, "candidate", nil, err); ok {
			return out, nil
		}
		return outcome{}, fmt.Errorf("candidate leg (edge credential): %w", err)
	}

	out := outcome{Operation: operation, Request: requestName}
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

func newPGXPool(ctx context.Context, dsn string) (*pgxReceiptWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &pgxReceiptWriter{pool: pool}, nil
}
