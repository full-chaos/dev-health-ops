package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
)

// ErrNoBuildIdentity reports that the running query-api did not tell us
// which BUILD it is.
//
// This is a hard refusal, not a fallback to an operator-supplied name.
// go_api_proof_run's key includes candidate_build, and a receipt is
// evidence for exactly one build -- so a receipt built from a
// hand-typed sha proves that somebody typed a sha, not that that build
// served the request. `enable`'s --candidate-build flag is documented as
// "the ops commit sha the running query-api image was built from", by
// CONVENTION, unverified; the fifteen live rows carry
// b18e56fa79cfe20ce0f75df148144b832d92be36 with nothing having checked
// it. That is the gap this refusal closes.
var ErrNoBuildIdentity = errors.New("goapiproof: the running query-api reports no build identity, so no receipt can name the build that served these requests")

// registryBody is GET /registry's response. Deliberately just the schema
// digest and the operation map -- the build identity lives at
// /buildinfo, see FetchBuildIdentity.
type registryBody struct {
	SchemaDigest string `json:"schema_digest"`
	Operations   []struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	} `json:"operations"`
}

// buildInfoBody is GET /buildinfo's response.
type buildInfoBody struct {
	Commit    string `json:"commit"`
	Version   string `json:"version"`
	BuildTime string `json:"build_time"`
	Modified  bool   `json:"modified"`
}

// EndpointLabel is a safe name for an endpoint, built from parsed parts
// only. NOTHING in this package ever prints a raw URL.
//
// The obvious implementation is url.Redacted(), and it is wrong. Measured:
//
//	url.Parse("alice:supersecret@host/registry")
//	  scheme="alice" opaque="supersecret@host/registry" user=<nil>
//	  Redacted() => "alice:supersecret@host/registry"
//
// With no "//" the parser reads the USERNAME as the scheme, the rest
// becomes opaque, User is nil, and Redacted() has nothing to redact -- so
// it hands back the password in full. A reviewer trying a
// credential-carrying URL without "//" is exactly who finds that.
//
// So this does not redact. It REBUILDS from scheme and hostname and never
// touches the input string. A string that cannot be parsed cannot be
// reasoned about safely; one that can be parsed does not need redacting,
// because its safe parts can simply be re-emitted. The scheme is
// allowlisted to http/https for the same reason: on the no-"//" form the
// scheme IS the username, so emitting it unchecked would be the leak.
//
// Ported from go_api_cli.py's _endpoint_label, which reached this shape
// after five review rounds -- four leaks from matching the credential's
// character class, then one more from deleting a known literal, which
// still leaked on the no-"//" form. Credit to lane-routing-verbs for the
// pointer; Go's parser has the same hole for the same reason.
//
// The port is deliberately omitted: it is one more thing that can be
// malformed in the code whose whole job is not to fail interestingly.
func EndpointLabel(raw string) string {
	parsed, err := safeEndpoint(raw)
	if err != nil {
		return "(unparseable endpoint)"
	}
	return parsed.Scheme + "://" + parsed.Hostname()
}

// safeEndpoint is the ONE predicate that decides whether a URL can be
// described, and every caller in this package goes through it -- the
// refusal at the flag boundary, the label in an error, and the transport
// stripper below. r4 found the previous shape's failure: the label had
// been taught about the no-"//" form and the REFUSAL had not, because they
// were separate checks that happened to agree until one was fixed.
//
// The predicate is that the URL is fully ACCOUNTED FOR, not that a
// credential was found in the place we know to look:
//
//   - scheme allowlisted to http/https, because on the no-"//" form the
//     scheme is the username;
//   - Opaque empty. `http:SECRET@host/registry` has scheme "http", which
//     passes an allowlist, and no User at all, which passes a userinfo
//     check -- the credential is in the opaque part. That exact string
//     defeated the r3 guard;
//   - User nil, the ordinary embedded-credential form;
//   - Host non-empty, so there is something safe left to name.
//
// Anything else is refused rather than described. A string this function
// cannot fully account for is one whose safe parts cannot be identified,
// and a guard that guesses at that is the guard we have now replaced
// twice.
func safeEndpoint(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("not a valid URL")
	}
	switch {
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		return nil, errors.New("scheme is not http or https")
	case parsed.Opaque != "":
		return nil, errors.New("URL has an opaque body, so it was written without //")
	case parsed.User != nil:
		return nil, ErrCredentialInURL
	case parsed.Hostname() == "":
		return nil, errors.New("URL has no host")
	}
	return parsed, nil
}

// ErrCredentialInURL is returned for a URL carrying embedded credentials.
var ErrCredentialInURL = errors.New("goapiproof: URL carries embedded credentials")

// SanitizeError is the package's ERROR BOUNDARY: every error that leaves
// goapiproof for an operator goes through it.
//
// Three rounds of per-site stripping failed, each in a place the previous
// fix had not imagined. r3 wrapped the URL in the message; r4 found the
// opaque form, where url.Error has no userinfo to redact; r5 found a
// credential inside a REDIRECT's Location header -- `failed to parse
// Location header "http://host/SECRET/%zz"` -- reached through a nested
// wrap that the top-level unwrap never saw, plus unsanitised
// request-construction errors in three functions.
//
// The pattern is the finding. A stripper that unwraps one known error type
// at one known site is a blacklist, and every round found the entry it did
// not have. This walks the WHOLE chain and then scrubs the rendered text,
// so a URL is removed wherever it came from and whatever wrapped it --
// including from a library that has not been written yet.
func SanitizeError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New(scrubURLs(unwrapOperation(err)))
}

// unwrapOperation renders the whole chain, expanding a *url.Error into its
// operation, its URL and its cause rather than using its own formatting.
//
// The URL is deliberately KEPT here and handed to scrubURLs, which rebuilds
// it from its safe parts or replaces it wholesale. Dropping it would be
// safe and unhelpful: an operator staring at "Get: connection refused"
// cannot tell which of four endpoints refused. url.Error's OWN formatting
// is what must not be trusted -- it redacts userinfo and nothing else, so
// it is a no-op on every other shape a credential takes.
func unwrapOperation(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Op + " " + urlErr.URL + ": " + unwrapOperation(urlErr.Err)
	}
	return err.Error()
}

// The sanitizer's two matchers.
//
// urlToken catches anything with a scheme -- `http://host/x`, and the
// no-"//" form `http:SECRET@host/x` that defeated two earlier fixes.
//
// quotedToken catches what r6 found the first one missing: a RELATIVE
// URL. A redirect's Location header is routinely a path, and Go quotes it
// into the error verbatim -- `failed to parse Location header
// "/REVIEW_SECRET/%zz"`. There is no scheme to key on, so the match is on
// the quoting instead.
var (
	urlToken    = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:[^\s"']+`)
	quotedToken = regexp.MustCompile(`"[^"]*"`)
)

// scrubURLs removes every URL-shaped thing from a rendered error.
//
// A rebuilt endpoint keeps its SCHEME AND HOST ONLY. The path is dropped,
// which is a correction to the first version of this function: r6 showed a
// credential living in the path of an otherwise valid URL, so re-emitting
// the path re-emits the secret. Host and scheme are enough to tell an
// operator which endpoint failed, which is the whole job.
func scrubURLs(message string) string {
	// Quoted first: Go's own errors quote the offending value, and a
	// quoted relative path has no scheme for urlToken to find.
	message = quotedToken.ReplaceAllStringFunc(message, func(quoted string) string {
		inner := quoted[1 : len(quoted)-1]
		if !looksLikeALocation(inner) {
			return quoted
		}
		if rebuilt, ok := rebuildEndpoint(inner); ok {
			return `"` + rebuilt + `"`
		}
		return `"(redacted url)"`
	})
	return urlToken.ReplaceAllStringFunc(message, func(token string) string {
		trimmed := strings.TrimRight(token, `.,;:)]}"'`)
		suffix := token[len(trimmed):]
		if rebuilt, ok := rebuildEndpoint(trimmed); ok {
			return rebuilt + suffix
		}
		return "(redacted url)" + suffix
	})
}

// looksLikeALocation reports whether a quoted string might be a URL or a
// path. Deliberately generous: over-scrubbing an error message costs
// legibility, under-scrubbing costs a credential, and this file exists
// because that trade was made the other way three times.
func looksLikeALocation(value string) bool {
	return strings.HasPrefix(value, "/") ||
		strings.Contains(value, "://") ||
		urlToken.MatchString(value)
}

// rebuildEndpoint returns scheme://host for a URL this package can fully
// account for. Everything else is unrebuildable, including every relative
// path -- a path alone has no safe part to keep.
func rebuildEndpoint(raw string) (string, bool) {
	parsed, err := safeEndpoint(raw)
	if err != nil {
		return "", false
	}
	return parsed.Scheme + "://" + parsed.Host, true
}

// credential is present -- checking User there would wave it through.
func RefuseCredentialsInURL(flagName, raw string) error {
	if _, err := safeEndpoint(raw); err != nil {
		if errors.Is(err, ErrCredentialInURL) {
			return fmt.Errorf("%w: %s carries userinfo (its value is not printed here). An embedded credential reaches the process table and every log that records a command line -- pass it through %s or %s instead",
				ErrCredentialInURL, flagName, edgeBearerEnvVarName, proofBearerEnvVarName)
		}
		return fmt.Errorf("goapiproof: %s is refused (%s). Its value is not printed here. This guard requires a URL it can fully account for -- an http/https scheme, no opaque body, no userinfo, and a host -- because a string it cannot parse into safe parts is one whose credential it cannot locate either",
			flagName, err)
	}
	return nil
}

// Named here rather than imported from the command, so this package's
// message does not depend on which binary is calling it.
const (
	edgeBearerEnvVarName  = "GO_API_PROVE_BEARER"
	proofBearerEnvVarName = "GO_API_PROVE_PROOF_BEARER"
)

// FetchRegistry asks the RUNNING process what it serves.
//
// Never a checkout, never a checked-in mirror: the six-day outage
// CHAOS-5416 records happened because rows were seeded at a digest no
// running binary computed, and every surface that could have said so was
// reading the same stale source as the thing that was wrong.
func FetchRegistry(ctx context.Context, client *http.Client, registryURL string) (RegistryView, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, registryURL, nil)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: build registry request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: read %s: %w", EndpointLabel(registryURL), SanitizeError(err))
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return RegistryView{}, fmt.Errorf("goapiproof: %s answered HTTP %d", EndpointLabel(registryURL), response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: read registry body: %w", err)
	}

	var parsed registryBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: decode registry body: %w", err)
	}
	if parsed.SchemaDigest == "" {
		return RegistryView{}, fmt.Errorf("goapiproof: %s reported an empty schema digest", EndpointLabel(registryURL))
	}
	if len(parsed.Operations) == 0 {
		return RegistryView{}, fmt.Errorf("goapiproof: %s registers no operations -- there is nothing to prove", EndpointLabel(registryURL))
	}

	view := RegistryView{
		SchemaDigest:   parsed.SchemaDigest,
		DocumentDigest: make(map[string]string, len(parsed.Operations)),
	}
	for _, operation := range parsed.Operations {
		view.DocumentDigest[operation.Operation] = operation.DocumentDigest
	}

	return view, nil
}

// FetchBuildIdentity asks the RUNNING process which build it is, via the
// authenticated GET /buildinfo route.
//
// Every failure mode is a REFUSAL, never a fallback to an
// operator-supplied name:
//
//   - an empty commit, or "unknown" (internal/platform/version's default
//     for a build with no -ldflags and no VCS stamp): the process cannot
//     say what it is, so no receipt can say it either.
//   - a MODIFIED tree: the commit does not describe what was compiled, so
//     naming it on a receipt would be a false claim, not an approximate
//     one.
//   - a 404: the deployment predates /buildinfo. Also a refusal -- an old
//     build that cannot identify itself is exactly the case this check
//     exists for.
func FetchBuildIdentity(ctx context.Context, client *http.Client, buildInfoURL string, credential *Credential) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, buildInfoURL, nil)
	if err != nil {
		return "", fmt.Errorf("goapiproof: build buildinfo request: %w", err)
	}
	if err := credential.Apply(ctx, request); err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("goapiproof: read %s: %w", EndpointLabel(buildInfoURL), SanitizeError(err))
	}
	defer func() { _ = response.Body.Close() }()

	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", fmt.Errorf("%w: %s answered 404, so this deployment predates the /buildinfo route", ErrNoBuildIdentity, EndpointLabel(buildInfoURL))
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", fmt.Errorf("goapiproof: %s rejected the %s credential (HTTP %d) -- /buildinfo checks the effective-principal envelope, not the edge access token", EndpointLabel(buildInfoURL), credential.Kind(), response.StatusCode)
	default:
		return "", fmt.Errorf("goapiproof: %s answered HTTP %d", EndpointLabel(buildInfoURL), response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("goapiproof: read buildinfo body: %w", err)
	}
	var parsed buildInfoBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("goapiproof: decode buildinfo body: %w", err)
	}

	commit := strings.TrimSpace(parsed.Commit)
	switch {
	case commit == "":
		return "", ErrNoBuildIdentity
	case commit == "unknown":
		return "", fmt.Errorf("%w: it reports commit=%q, internal/platform/version's default for a build with no -ldflags and no VCS stamp", ErrNoBuildIdentity, commit)
	case parsed.Modified:
		return "", fmt.Errorf("%w: it reports a MODIFIED working tree, so its commit does not identify the source it was built from", ErrNoBuildIdentity)
	}
	return commit, nil
}

// VerifyBuildStable re-reads /buildinfo AFTER a run and refuses when the
// serving build moved underneath it.
//
// This bounds the residual codex r1's F5 named. Per-request build binding
// is available only on the proof route, which stamps the serving build on
// its own responses; the EDGE route cannot provide it, because the Python
// dispatcher's _forward_to_go builds a new Response carrying only content,
// status and media_type and drops every header query-api set. Closing that
// half needs the edge to forward the header -- a Python change this lane is
// barred from making, and one that belongs to the dispatcher's owner.
//
// So an edge-route receipt's build claim rests on: the routing rows and
// /buildinfo agreeing before the run (VerifyCandidateBuild), and the same
// build still answering after it (this). What that leaves uncovered is
// narrow and nameable: two replicas serving DIFFERENT builds simultaneously
// for the whole run. That is the staggered-rollout state the fleet's own
// lockstep rule already forbids -- migrate and every go-* image rebuild
// together, never staggered -- so the residual is a deployment invariant,
// not an unexamined hole. It is stated on the report rather than assumed.
func VerifyBuildStable(ctx context.Context, client *http.Client, buildInfoURL string, credential *Credential, before string) error {
	after, err := FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		return fmt.Errorf("goapiproof: re-reading the build identity after the run failed, so the receipts cannot be shown to name the build that served them: %w", err)
	}
	if after != before {
		return fmt.Errorf("goapiproof: the serving build moved DURING the run (%s -> %s) -- every receipt this run wrote names a build that was not serving for all of it", before, after)
	}
	return nil
}

// VerifyCandidateBuild cross-checks the build the process reports against
// what the routing rows point at.
//
// The direction matters. The RUNNING process is the authority on which
// build served the request; the row's current_candidate_build is a
// claim an operator typed. When they disagree, the rows are describing a
// build that is not the one answering requests -- the same class of
// silent mismatch as the stale schema digest in CHAOS-5416, and a receipt
// written across it would attribute evidence to the wrong build.
//
// An operator-supplied --candidate-build is checked here too, as a
// cross-check ONLY. It never becomes the value written (team-lead ruling
// R51, 2026-09-09).
func VerifyCandidateBuild(running string, expected string, routing map[string]RoutingRow) error {
	if expected != "" && expected != running {
		return fmt.Errorf("goapiproof: --candidate-build %q does not match the running build %q -- the flag is a cross-check, never the source", expected, running)
	}
	stale := StaleRoutingRows(running, routing)
	if len(stale) == 0 {
		return nil
	}
	disagreeing := make([]string, 0, len(stale))
	for _, operation := range sortedOperations(stale) {
		disagreeing = append(disagreeing, fmt.Sprintf("%s points at %s", operation, stale[operation]))
	}
	// The remedy names the MODE-PRESERVING verb on purpose. `routing
	// enable --candidate-build` re-points a row, but its --mode accepts
	// canary|primary only, so pointing a SHADOW row at the running build
	// with it also flips that row to canary -- a routing change nobody
	// asked for, produced by following a message whose only job is to say
	// how to clear this block safely. CHAOS-5486's `go-api-routing
	// repoint` preserves the mode and reads the running build from
	// /buildinfo rather than taking it on trust from an operator.
	return fmt.Errorf("goapiproof: routing rows point at a build the running process is not (running=%s): %s.\n  Re-point the row to the running build with `go-api-routing repoint` (mode preserved); `routing enable` would also change the mode. This is a REFUSAL, not a warning: see StaleRoutingRows for the replica argument that makes it one",
		running, strings.Join(disagreeing, "; "))
}

// StaleRoutingRows reports which routing rows name a build the running
// process is not, as operation -> the build the row names.
//
// This was briefly DEMOTED to a recorded fact, on the argument that the
// comparison could not affect a receipt: reachability is decided by mode
// rather than by current_candidate_build (postgres_switch.go:71-78), the
// Python edge never reads the column, and every receipt names the identity
// read from /buildinfo, so a stale row could not make a receipt say the
// wrong thing.
//
// That argument was WRONG, and r1 broke it with an executed test. It
// assumed /buildinfo identifies the process that served the MEASURED
// request. It does not. query-api runs multiple replicas; a /buildinfo
// read can be answered by replica A while the measured /graphql request is
// served by replica B, and the Python edge's _forward_to_go rebuilds the
// response with only content, status and media_type -- dropping the
// per-request build header that would have told them apart. So during a
// rolling deploy a receipt can name build A while the measurement came
// from build B, and that receipt satisfies the four-column enablement
// lookup exactly.
//
// The routing row's build is the one remaining cross-check that the fleet
// is on ONE build, so it is a refusal again. Re-pointing the rows after a
// deploy is an operator step (R67), not something the prover may assume
// away. This function stays as the helper that finds them, so both the
// refusal message and the receipt provenance name the same rows.
func StaleRoutingRows(running string, routing map[string]RoutingRow) map[string]string {
	stale := map[string]string{}
	for operation, row := range routing {
		if row.CandidateBuild != "" && row.CandidateBuild != running {
			stale[operation] = row.CandidateBuild
		}
	}
	return stale
}

func sortedOperations(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// registrydumpDocument is one element of `registrydump -file
// cmd/query-api/query_route.go`'s JSON output.
type registrydumpDocument struct {
	Operation string `json:"operation"`
	Document  string `json:"document"`
	Digest    string `json:"digest"`
}

// LoadDocuments reads registrydump's JSON output and returns operation ->
// registered document TEXT.
//
// Why a file produced by another tool rather than re-parsing
// query_route.go here: registrydump already does that enumeration, and
// its AST walk carries four separately-reviewed guards against silent
// under-enumeration (grouped consts, post-literal map writes, a duplicate
// digestByOperation identifier, orphaned consts). A second implementation
// of that walk in this command would be a second thing to keep correct,
// and the one that drifted would under-report documents while looking
// fine -- which is the exact failure class registrydump exists to stop.
//
// The staleness risk a file introduces is closed by VerifyDocuments: the
// text must digest to what the RUNNING process registers, or the run
// refuses. A stale documents file cannot produce a receipt.
func LoadDocuments(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path to their own registrydump output
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read documents file: %w", err)
	}
	var documents []registrydumpDocument
	if err := json.Unmarshal(raw, &documents); err != nil {
		return nil, fmt.Errorf("goapiproof: decode documents file (expected `registrydump -file ...` output): %w", err)
	}
	if len(documents) == 0 {
		return nil, fmt.Errorf("goapiproof: documents file %s is empty", path)
	}

	byOperation := make(map[string]string, len(documents))
	for _, document := range documents {
		if document.Document == "" {
			return nil, fmt.Errorf("goapiproof: documents file carries no text for %q", document.Operation)
		}
		byOperation[document.Operation] = document.Document
	}
	return byOperation, nil
}

// VerifyDocuments proves the local document text is byte-identical to
// what the running process registers, by comparing digests it computes
// over that text against the digests the process reports.
//
// digestFn is passed in so this package does not depend on
// cmd/query-api/internal/digest (which Go's internal rule puts out of
// reach here); the caller hands in that exact function, so there is still
// only ONE digest implementation in play.
func VerifyDocuments(documents map[string]string, registry RegistryView, digestFn func(string) string) error {
	var drifted []string
	for operation, registered := range registry.DocumentDigest {
		text, ok := documents[operation]
		if !ok {
			// Not fatal here: the runner refuses this operation by name
			// (RefusalDocumentDigestDrift) so one missing document does not
			// abort the whole run and lose the other fourteen receipts.
			continue
		}
		if got := digestFn(text); got != registered {
			drifted = append(drifted, fmt.Sprintf("%s: local text digests to %s, the running process registers %s", operation, got, registered))
		}
	}
	if len(drifted) > 0 {
		return fmt.Errorf("goapiproof: document drift between this checkout and the running query-api -- rebuild registrydump's output against the deployed sha: %s", strings.Join(drifted, "; "))
	}
	return nil
}
