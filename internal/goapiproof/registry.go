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
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
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

// registryOperation is one element of /registry's `operations` array,
// decoded by EXACT key -- see exactStringField for why.
type registryOperation struct {
	Operation      string
	DocumentDigest string
}

// r3 P1 (reproduced): a plain struct-tagged `json.Unmarshal` of
// /registry's or /buildinfo's body is vulnerable to JSON KEY-CASE
// SHADOWING. encoding/json processes an object's keys in ENCOUNTER
// ORDER, and for a field whose JSON tag is an exact lowercase name (every
// field on both these bodies), a later differently-cased key still wins
// over an earlier exact one -- it does NOT reliably prefer the exact
// match across the whole object the way the package docs are often read
// to imply. Measured directly:
//
//	{"modified":true,"MODIFIED":false}  -> Modified == false
//	{"MODIFIED":false,"modified":true}  -> Modified == true
//
// last key wins, case notwithstanding. That is a live guard bypass at
// /buildinfo: a response naming the build MODIFIED under the correct key
// and un-modified under a shadow key defeats FetchBuildIdentity's refusal
// (ErrNoBuildIdentity is meant to fire on any modified build). The same
// shape at /registry's `operation`/`document_digest` fields can make a
// registry entry parse as one operation to this reader and another to
// Python's exact-subscript reader, the identical "Go accepts what Python
// would not" class the catalog-file and duplicate-operation fixes above
// already close for their own inputs.
//
// exactStringField/exactBoolField close it by reading from a
// map[string]json.RawMessage: a Go map's keys are exact-string equal, so
// "MODIFIED" and "modified" are two DISTINCT entries in that map and a
// lookup by one spelling can never observe the other -- the same
// agreement Python's dict subscript already has with itself. Both bodies
// below are now decoded this way instead of via a struct tag.
func exactStringField(raw map[string]json.RawMessage, key string) (value string, present bool, err error) {
	rawValue, present := raw[key]
	if !present {
		return "", false, nil
	}
	if err := json.Unmarshal(rawValue, &value); err != nil {
		return "", true, fmt.Errorf("%q is not a JSON string (%s)", key, rawValue)
	}
	return value, true, nil
}

func exactBoolField(raw map[string]json.RawMessage, key string) (value bool, present bool, err error) {
	rawValue, present := raw[key]
	if !present {
		return false, false, nil
	}
	if err := json.Unmarshal(rawValue, &value); err != nil {
		return false, true, fmt.Errorf("%q is not a JSON boolean (%s)", key, rawValue)
	}
	return value, true, nil
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

// TransportFailure describes a transport error WITHOUT its message.
//
// Four rounds tried to make an error message safe to print: wrap the URL,
// strip the URL, scrub the URL, invert the matcher. Each fixed the shape
// it was shown and the next round supplied another -- an opaque form, a
// redirect Location, a filename-relative Location, a query-only one. The
// text is attacker-shaped and there is no finite list of ways a secret can
// appear in it.
//
// So the text is DROPPED, not sanitised. What reaches an operator is a
// fixed message, the endpoint label this package rebuilt from parts it can
// account for, and a CLASS. That is everything the message was ever needed
// for -- which endpoint, and what kind of failure -- and it has no channel
// through which an arbitrary string can travel.
type TransportFailure struct {
	// Endpoint is the rebuilt scheme://host label, never the raw URL.
	Endpoint string
	// Class is one of the constants below.
	Class string
}

// Transport failure classes. Deliberately coarse: an operator needs to
// know whether to look at the network, the deployment or the request, and
// a finer taxonomy would be another place for a detail to leak.
const (
	TransportTimeout  = "timeout"
	TransportRefused  = "connection_refused"
	TransportRedirect = "redirect"
	TransportParse    = "malformed_response"
	TransportFailed   = "request_failed"
)

func (f TransportFailure) Error() string {
	return fmt.Sprintf("goapiproof: %s failed (%s). The underlying error is deliberately not reported: it can contain the request URL, and a URL can carry a credential in its userinfo, path, query or fragment", f.Endpoint, f.Class)
}

// classifyTransport maps an error to a class WITHOUT reading its message
// for anything but the redirect sentinel this package raises itself.
func classifyTransport(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded), os.IsTimeout(err):
		return TransportTimeout
	case errors.Is(err, errRedirectRefused):
		return TransportRedirect
	case errors.Is(err, syscall.ECONNREFUSED):
		return TransportRefused
	}
	return TransportFailed
}

// errRedirectRefused is returned by every client this package builds.
//
// The registry, /buildinfo and the measured routes are DIRECT endpoints:
// nothing legitimate redirects. Following one means fetching a URL the
// operator did not supply and this package never validated, and every
// redirect finding in this file's history arrived through the Location
// header. Refusing is both safer and more honest than sanitising what a
// redirect produces.
var errRedirectRefused = errors.New("goapiproof: refusing to follow a redirect")

// NoRedirectClient returns a client that refuses redirects.
func NoRedirectClient(base *http.Client) *http.Client {
	client := &http.Client{}
	if base != nil {
		*client = *base
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errRedirectRefused
	}
	return client
}

// transportError builds the only error this package emits for a failed
// request.
func transportError(rawURL string, err error) error {
	return TransportFailure{Endpoint: EndpointLabel(rawURL), Class: classifyTransport(err)}
}

// RefuseCredentialsInURL rejects an endpoint flag this package cannot
// fully account for.
//
// EndpointLabel keeps a credential out of THIS program's messages; only
// refusal keeps it off the command line, where the process table, the
// shell history and every log that records an invocation can already read
// it. So both: refuse at the boundary, and never print a raw URL even
// then, because a URL can reach an error from somewhere the boundary does
// not cover.
//
// It shares ONE predicate with the label, safeEndpoint, so the two cannot
// drift: a URL that may be accepted and a URL that may be named are the
// same question asked twice. The no-"//" form is refused as unparseable
// rather than inspected for userinfo, because that is exactly the form
// where User is nil while a credential is present -- checking User there
// would wave it through.
//
// (The head of this comment was lost in an earlier edit that removed a
// duplicated declaration, leaving it starting mid-sentence. Restored.)
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
	// Redirects are REFUSED: /registry and /buildinfo are direct
	// endpoints, so a redirect means fetching something nobody validated.
	client = NoRedirectClient(client)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, registryURL, nil)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: build registry request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return RegistryView{}, transportError(registryURL, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return RegistryView{}, fmt.Errorf("goapiproof: %s answered HTTP %d", EndpointLabel(registryURL), response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: read registry body: %w", err)
	}
	// Sibling of the catalog-file fix (routing_catalog.go's UTF-8 gate,
	// r2 P1): `go_api_cli.py` reads THIS SAME `/registry` endpoint, and
	// any Python HTTP client decodes response bytes as UTF-8 before
	// `json.loads` ever runs -- invalid UTF-8 anywhere in the body fails
	// there. encoding/json has no such requirement, so without this check
	// a malformed or tampered registry response one byte away from being
	// unreadable to the Python verb would still parse here, and this
	// binary would go on to write a row from it.
	if !utf8.Valid(body) {
		return RegistryView{}, fmt.Errorf("goapiproof: %s response is not valid UTF-8 -- the Python verb's HTTP client decodes response bytes as UTF-8 before parsing and would refuse the whole body on one bad byte anywhere in it", EndpointLabel(registryURL))
	}

	var rawTop map[string]json.RawMessage
	if err := json.Unmarshal(body, &rawTop); err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: decode registry body: %w", err)
	}
	schemaDigest, _, err := exactStringField(rawTop, "schema_digest")
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: %s registry body: %w", EndpointLabel(registryURL), err)
	}
	if schemaDigest == "" {
		return RegistryView{}, fmt.Errorf("goapiproof: %s reported an empty schema digest", EndpointLabel(registryURL))
	}
	rawOperationsValue, present := rawTop["operations"]
	var rawOperations []map[string]json.RawMessage
	if present {
		if err := json.Unmarshal(rawOperationsValue, &rawOperations); err != nil {
			return RegistryView{}, fmt.Errorf("goapiproof: %s operations field is malformed: %w", EndpointLabel(registryURL), err)
		}
	}
	if len(rawOperations) == 0 {
		return RegistryView{}, fmt.Errorf("goapiproof: %s registers no operations -- there is nothing to prove", EndpointLabel(registryURL))
	}
	operations := make([]registryOperation, 0, len(rawOperations))
	for index, rawOp := range rawOperations {
		operationName, _, err := exactStringField(rawOp, "operation")
		if err != nil {
			return RegistryView{}, fmt.Errorf("goapiproof: %s operations[%d]: %w", EndpointLabel(registryURL), index, err)
		}
		documentDigest, _, err := exactStringField(rawOp, "document_digest")
		if err != nil {
			return RegistryView{}, fmt.Errorf("goapiproof: %s operations[%d]: %w", EndpointLabel(registryURL), index, err)
		}
		// r3 P1 (reproduced): a `{}` or `null` entry (rawOp is an empty or
		// nil map either way) decoded to an empty-string operation/digest
		// with no error at all -- exactStringField correctly reports the
		// key as ABSENT rather than malformed, but nothing upstream of it
		// used to check for absence, so a plainly incomplete entry passed
		// straight through to the duplicate check and the view. Refuse it
		// by name, the same shape the catalog file already refuses.
		if operationName == "" || documentDigest == "" {
			return RegistryView{}, fmt.Errorf("goapiproof: %s operations[%d] carries an empty or missing operation or document_digest", EndpointLabel(registryURL), index)
		}
		operations = append(operations, registryOperation{Operation: operationName, DocumentDigest: documentDigest})
	}

	// r2 P1 (reproduced, CHAOS-5524 folded in per team-lead ruling): this
	// map used to be a last-write-wins CONVERSION -- two `/registry`
	// entries naming the same operation under different document digests
	// collapsed silently, and whichever happened to come last decided
	// which digest a routing row got written with. The Python verb refuses
	// outright (`GoPlaneUnavailable ... lists operation 'X' more than
	// once`); this now matches, BEFORE the map is built, so a malformed or
	// tampered registry can never decide anything by ordering.
	seen := make(map[string]bool, len(operations))
	var duplicates []string
	for _, operation := range operations {
		if seen[operation.Operation] {
			duplicates = append(duplicates, operation.Operation)
			continue
		}
		seen[operation.Operation] = true
	}
	if len(duplicates) > 0 {
		sort.Strings(duplicates)
		duplicates = dedupeSorted(duplicates)
		return RegistryView{}, fmt.Errorf("goapiproof: %s lists operation(s) more than once: %v -- a malformed or tampered registry must not decide which document digest a routing row is written with",
			EndpointLabel(registryURL), duplicates)
	}

	view := RegistryView{
		SchemaDigest:   schemaDigest,
		DocumentDigest: make(map[string]string, len(operations)),
	}
	for _, operation := range operations {
		view.DocumentDigest[operation.Operation] = operation.DocumentDigest
	}

	return view, nil
}

// dedupeSorted collapses adjacent equal strings in an already-sorted
// slice, so a name repeated more than twice is named once in a refusal
// rather than once per repeat.
func dedupeSorted(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if i == 0 || sorted[i-1] != s {
			out = append(out, s)
		}
	}
	return out
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
	// Redirects are REFUSED: /registry and /buildinfo are direct
	// endpoints, so a redirect means fetching something nobody validated.
	client = NoRedirectClient(client)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, buildInfoURL, nil)
	if err != nil {
		return "", fmt.Errorf("goapiproof: build buildinfo request: %w", err)
	}
	if err := credential.Apply(ctx, request); err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", transportError(buildInfoURL, err)
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
	// r3 P1 (reproduced): without this check, a `commit` value carrying
	// invalid UTF-8 (a single bad byte is enough) decodes silently -- Go's
	// JSON string scanner substitutes U+FFFD (the replacement character)
	// for an invalid byte rather than erroring, so the corrupted string
	// goes on to be written as current_candidate_build, a 4-column
	// foreign-key value every routing row and receipt is keyed against.
	// Executed: a commit byte 0xFF produced current_candidate_build='<20>'
	// (U+FFFD) written durably. Refusing the whole body up front is the
	// same shape as the catalog-file and /registry UTF-8 gates above.
	if !utf8.Valid(body) {
		return "", fmt.Errorf("goapiproof: %s response is not valid UTF-8 -- a build identity this corrupted cannot be written as a foreign key value", EndpointLabel(buildInfoURL))
	}
	var rawTop map[string]json.RawMessage
	if err := json.Unmarshal(body, &rawTop); err != nil {
		return "", fmt.Errorf("goapiproof: decode buildinfo body: %w", err)
	}
	rawCommit, _, err := exactStringField(rawTop, "commit")
	if err != nil {
		return "", fmt.Errorf("goapiproof: %s buildinfo body: %w", EndpointLabel(buildInfoURL), err)
	}
	modified, _, err := exactBoolField(rawTop, "modified")
	if err != nil {
		return "", fmt.Errorf("goapiproof: %s buildinfo body: %w", EndpointLabel(buildInfoURL), err)
	}

	commit := strings.TrimSpace(rawCommit)
	switch {
	case commit == "":
		return "", ErrNoBuildIdentity
	case commit == "unknown":
		return "", fmt.Errorf("%w: it reports commit=%q, internal/platform/version's default for a build with no -ldflags and no VCS stamp", ErrNoBuildIdentity, commit)
	case modified:
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
	// r3 P1 (reproduced, package-wide UTF-8 sweep): the same silent
	// U+FFFD substitution the catalog/registry/buildinfo gates close
	// elsewhere in this package applies here too -- an invalid byte in
	// the operation name, the document text or the digest field would
	// otherwise decode without error into a corrupted value this
	// function hands back as if it were the real registered text.
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("goapiproof: documents file %s is not valid UTF-8", path)
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
