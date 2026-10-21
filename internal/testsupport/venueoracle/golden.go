package venueoracle

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

// Golden replays the Python plane's frozen answers for a venue oracle. The
// Python api is gone: its answers were EXECUTED once, on the last
// Python-bearing build, and frozen in golden files. They are never rewritten
// from Go's own output, and no Python runs or can run again. The test keeps its
// shape (the same seed, the same requests, the same Diff and row comparisons);
// the Python side comes from the golden file, whose SHA-256 must equal the
// digest the test pins, so an edit by hand fails.
//
// The lifecycle is fixed: OpenGolden, Python (any number of calls), Diff,
// Finish. A call out of order fails the test with an error naming it; every
// answer the golden hands out must be compared by Diff or declared with
// Consumed; every frozen row comparison is consumed once; no answer and no row
// comparison is left over.
//
// Threat model: this harness defends against accidental drift by honest
// authors (a changed request, reused rows, a skipped comparison); a hostile
// author of golden inputs or a hostile committer is OUT OF SCOPE, defended by
// human PR review.
//
// A frozen test compares no Python response, so it writes the Go-only proof
// (WriteGoOnlyProof) naming the build the truth was executed on.
//
// The recording run and this run were different processes with different
// random ids, so a test that uses a golden seeds deterministic values
// (StableUUID) wherever an id reaches a compared response or row.
type GoldenSpec struct {
	// Path is the golden file, relative to the test's package directory.
	Path string
	// PythonBuild is the 40-hex commit the Python answers were executed on:
	// the last build that still carried the Python route bodies.
	PythonBuild string
	// SHA256 is the digest of the golden file the test pins.
	SHA256 string
	// Scrub turns a token field that is not a JWT (an opaque key, a session
	// id) into a typed placeholder. It runs on every compared body and header
	// value after the JWT projection, and must be deterministic and
	// idempotent. A golden stores no token value.
	Scrub func(text string) string
	// CredentialConstants are the credential values the test sends on purpose, by exact text (CHAOS-7898). The
	// harness replaces each by a digest placeholder wherever it stands; the golden's header pins their digests
	// (see golden_declared.go). No pattern: only the values the test declares are touched.
	CredentialConstants []string
	// KeyScrub turns a value in a REQUEST that the Python plane issued earlier
	// and the test sends back (a mailed link token) into a placeholder, for the
	// request's key only. A JWT in a request is projected without it. Like
	// Scrub it must be deterministic and idempotent, and it is not applied to
	// the authorization header.
	KeyScrub func(text string) string
}

// Golden is an opened GoldenSpec.
type Golden struct {
	spec   GoldenSpec
	loaded goldenFile
	// served counts the frozen answers handed out so far: a test may ask for
	// the Python plane's answers in several calls (one per batch of requests),
	// and the frozen file holds them in the order they were asked.
	served int
	// slots binds each answer handed out (slot n, from 1) to the request it
	// answers and records what became of it: compared by Diff or inspected by the
	// test (Consumed), each exactly once.
	slots []answerSlot
	// use is what RunTests reads after the package's tests: this golden was
	// opened by a test, and whether it reached Finish.
	use *goldenUse
	// rowsUsed records which frozen row comparisons the test asked for: a
	// snapshot nothing consumed is a comparison that no longer happens.
	rowsUsed    map[string]bool
	rowsFetched map[string]bool
	// retired counts, per ruling, the answers and row comparisons the test
	// retired (golden_retired.go).
	retired map[string]retiredCount
	state   goldenState
}

// answerSlot is one answer a Golden handed out.
type answerSlot struct {
	request  string
	compared bool
	consumed bool
}

// goldenState is where a Golden is in its lifecycle. The order is fixed:
// Open, then Python answers fetched (any number of calls), then Diff, then
// Finish; a call out of order fails the test with an error that names it.
type goldenState int

const (
	stateOpen goldenState = iota
	statePython
	stateDiffed
	stateFinished
)

func (s goldenState) String() string {
	return [...]string{"opened", "answers fetched", "compared by Diff", "finished"}[s]
}

type goldenFile struct {
	Header   goldenHeader          `json:"header"`
	Requests []goldenRequest       `json:"requests"`
	Rows     map[string]goldenRows `json:"rows,omitempty"`
}

// goldenHeader is the part of a golden's header a replay checks. A golden
// file holds more (the recipe, the recording's environment key, the record
// verb's stamp); those described the recording and are not read.
type goldenHeader struct {
	Test        string `json:"test"`
	PythonBuild string `json:"python_build"`
	// ProducerDigest is the digest of the Python source the answers were
	// executed from: every file under src of the pinned checkout, by git blob id.
	ProducerDigest string `json:"producer_digest"`
	// DeclaredConstants is the sorted full sha256 of each GoldenSpec.CredentialConstants value, under a name no
	// secret scanner reads as a key: a frozen run refuses a test that declares other constants.
	DeclaredConstants []string `json:"declared_constants_sha256,omitempty"`
	// Blanked lists, by pattern, the leaves the recording replaced by a
	// placeholder, with how many: the paths the golden does not hold by value
	// (a token projected to its claims, a Volatile header, a generated id or a
	// run time). A golden recorded before it has none, and is then not checked
	// against it.
	Blanked map[string]int `json:"blanked"`
}

type goldenRequest struct {
	Name       string `json:"name"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	BodySHA256 string `json:"body_sha256"`
	// HeadersSHA256 is the digest of the request headers the Python plane was
	// sent (see headersDigest), so a caller or content type that drifted is
	// refused instead of being served the answer of another.
	HeadersSHA256 string            `json:"request_headers_sha256"`
	Status        int               `json:"status"`
	Headers       map[string]string `json:"headers"`
	Body          string            `json:"body"`
}

type goldenRows struct {
	Rows string `json:"rows"`
}

var buildPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// OpenGolden opens spec for the running test. It never returns a golden the
// test could not trust: the file must exist, name the pinned build and the
// test, and hash to the pinned digest.
func OpenGolden(t *testing.T, spec GoldenSpec) *Golden {
	t.Helper()
	// Noted before anything can refuse the golden: RunTests asks, after the
	// package's tests, which goldens their tests opened and finished.
	use := noteOpened(t, spec.Path)
	g, err := openGolden(spec, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	g.use = use
	return g
}

func openGolden(spec GoldenSpec, test string) (*Golden, error) {
	if spec.Path == "" || !buildPattern.MatchString(spec.PythonBuild) {
		return nil, fmt.Errorf("venueoracle: a GoldenSpec needs a path and the 40-hex Python build the answers were executed on: %+v", spec)
	}
	if err := declaredConstantsErr(spec.CredentialConstants); err != nil {
		return nil, fmt.Errorf("golden %s: %w", spec.Path, err)
	}
	g := &Golden{spec: spec, rowsUsed: map[string]bool{}, rowsFetched: map[string]bool{}}
	raw, err := os.ReadFile(spec.Path)
	if err != nil {
		return nil, fmt.Errorf("golden %s is missing (%v): a golden is executed, never authored", spec.Path, err)
	}
	if spec.SHA256 == "" {
		return nil, fmt.Errorf("golden %s: the test pins no digest", spec.Path)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != spec.SHA256 {
		return nil, fmt.Errorf("golden %s changed without its digest: sha256 %s, the test pins %s; a golden is executed, never edited", spec.Path, got, spec.SHA256)
	}
	if err := json.Unmarshal(raw, &g.loaded); err != nil {
		return nil, fmt.Errorf("golden %s: %w", spec.Path, err)
	}
	if g.loaded.Header.Test != test {
		return nil, fmt.Errorf("golden %s was recorded for %q, not for %q; a golden belongs to one test", spec.Path, g.loaded.Header.Test, test)
	}
	if g.loaded.Header.PythonBuild != spec.PythonBuild {
		return nil, fmt.Errorf("golden %s was executed on build %s, the test names %s", spec.Path, g.loaded.Header.PythonBuild, spec.PythonBuild)
	}
	if err := declaredHeaderErr(spec.Path, g.loaded.Header.DeclaredConstants, spec.CredentialConstants); err != nil {
		return nil, err
	}
	if len(g.loaded.Header.ProducerDigest) != 64 {
		return nil, fmt.Errorf("golden %s names no producer digest: it was not recorded by this harness", spec.Path)
	}
	return g, nil
}

// keyOf is the key of request as this golden holds it: the key of the request
// after the golden's own projection (tokens to their claims, the spec's Scrub),
// so a request that carries a token or id the Python plane issued earlier has
// the same key in every recording and in the replay, whose request is built
// from the projected answers. A request the projection leaves unchanged has
// the key requestKey gives it, byte for byte.
func (g *Golden) keyOf(request Request) goldenRequest {
	return requestKey(g.projectRequest(request))
}

// projectKeyText is a text as the golden holds it in a request key: its tokens
// projected and the spec's KeyScrub applied (not Scrub: an id the test writes in
// a request on purpose is part of the request, and stays in its key). It is idempotent, so the request of a
// recording (real token) and of its replay (projected token) key alike.
func (g *Golden) projectKeyText(text string) string {
	text = g.DigestDeclared(text)
	text = ProjectTokens(text)
	if g.spec.KeyScrub != nil {
		text = g.spec.KeyScrub(text)
	}
	return text
}

// projectRequest is request with its path, its body (decoded from base64, and
// encoded again only when the projection changed it) and its header values but
// the authorization one (headersDigest reduces a bearer token to its claims)
// projected.
func (g *Golden) projectRequest(request Request) Request {
	if request.Method == programMethod {
		// A program request (ProgramRequest) is not projected, for two reasons. Cost: its
		// stdin is a corpus (235 MB in pyidna) and projecting it on every key took minutes
		// under -race. Safety: nothing of it can reach a golden through the key -- the key
		// holds only the sha256 of the body (requestKey, BodySHA256; goldenRequest has no
		// field for the body itself), and a program path is digests already. A corpus may
		// hold credential-SHAPED fixtures on purpose (the sanitize-error-text oracle's
		// redaction inputs), so a guard that forbade them would refuse a valid test.
		// Determinism: a corpus holding a per-run value changes its digest between runs, and
		// the two-run check and the frozen replay both refuse that by name.
		return request
	}
	out := request
	out.Path = g.projectKeyText(request.Path)
	if request.Body != nil {
		if raw, err := base64.StdEncoding.DecodeString(*request.Body); err == nil && utf8.Valid(raw) {
			if projected := g.projectKeyText(string(raw)); projected != string(raw) {
				out.Body = B64(projected)
			}
		}
	}
	if len(request.Headers) > 0 {
		out.Headers = make(map[string]string, len(request.Headers))
		for name, value := range request.Headers {
			if strings.ToLower(name) != "authorization" {
				value = g.projectKeyText(value)
			}
			out.Headers[name] = value
		}
	}
	return out
}

func requestKey(request Request) goldenRequest {
	sum := sha256.Sum256([]byte(""))
	if request.Body != nil {
		sum = sha256.Sum256([]byte(*request.Body))
	}
	return goldenRequest{Name: request.Name, Method: request.Method, Path: request.Path, BodySHA256: hex.EncodeToString(sum[:]), HeadersSHA256: headersDigest(request.Headers)}
}

// volatileClaims are the claims of a bearer token that differ between two
// processes minting the same caller's token.
var volatileClaims = []string{"iat", "exp", "nbf", "jti"}

// headersDigest is a digest of the request headers that identifies the same
// request in another process: names are lower-cased and sorted, values are
// hashed as sent, except a bearer token, which is a different string in every
// process (it carries its issue time), so it is reduced to its claims minus the
// volatile ones. A different caller, role or content type is a different digest.
func headersDigest(headers map[string]string) string {
	names := make([]string, 0, len(headers))
	values := map[string]string{}
	for name, value := range headers {
		lower := strings.ToLower(name)
		names = append(names, lower)
		values[lower] = value
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		value := values[name]
		if name == "authorization" {
			value = bearerIdentity(value)
		}
		fmt.Fprintf(hash, "%d:%s=%d:%s\n", len(name), name, len(value), value)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// bearerIdentity reduces "Bearer <jwt>" to the token's header and claims
// without the volatile ones, in a canonical order; any other value is returned
// as is. The signature is NOT part of the identity: the venue mints a token per
// process, so its signature differs between a recording and a replay by
// construction. A test that must prove the Go plane refuses a tampered token
// asserts that itself (the golden freezes what Python answered for the caller
// the claims name).
func bearerIdentity(value string) string {
	token, ok := strings.CutPrefix(value, "Bearer ")
	if !ok {
		return value
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return value
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return value
	}
	var headerFields map[string]any
	if err := json.Unmarshal(header, &headerFields); err != nil {
		return value
	}
	canonicalHeader, err := json.Marshal(headerFields)
	if err != nil {
		return value
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return value
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return value
	}
	for _, name := range volatileClaims {
		delete(claims, name)
	}
	canonical, err := json.Marshal(claims)
	if err != nil {
		return value
	}
	return "Bearer header:" + string(canonicalHeader) + " claims:" + string(canonical)
}

// stepErr is an error naming the call and the state unless the golden is in a
// state the call may run from.
func (g *Golden) stepErr(call string, allowed ...goldenState) error {
	for _, state := range allowed {
		if g.state == state {
			return nil
		}
	}
	return fmt.Errorf("golden %s: %s called when the golden is %s; the order is OpenGolden, Python (any number of calls), Diff, Finish", g.spec.Path, call, g.state)
}

// step fails the test with stepErr's error.
func (g *Golden) step(t *testing.T, call string, allowed ...goldenState) {
	t.Helper()
	if err := g.stepErr(call, allowed...); err != nil {
		t.Fatal(err)
	}
}

// Python returns the Python plane's frozen answers to requests. The request
// list must be the one the file was recorded with (name, method, path, body,
// request headers), so a test whose requests drifted fails naming the
// difference instead of comparing against another test's truth. It may be
// called any number of times before Finish; every answer it returns must then
// be compared by Diff or declared inspected (Consumed).
func (g *Golden) Python(t *testing.T, v *Venue, requests []Request) []Response {
	t.Helper()
	if v != nil && !v.frozen {
		t.Fatalf("golden %s: its venue was built without it: pass the golden to venueoracle.Start (Options.Golden)", g.spec.Path)
	}
	return g.answer(t, "Python", requests)
}

// Produce returns a Python producer's frozen answers to requests, for an
// oracle whose Python side was not the api's HTTP plane: a program run over a
// corpus, a CLI verb, a function. A request names what was asked
// (ProgramRequest builds one for a program) and a response what came back:
// Status the exit code, Body the stdout, Headers any other stream. The
// lifecycle and the accounting are Python's: every answer must then be
// compared by Diff or declared inspected (Consumed). A request carries no
// headers.
func (g *Golden) Produce(t *testing.T, requests []Request) []Response {
	t.Helper()
	if err := producerRequestsErr(requests); err != nil {
		t.Fatal(err)
	}
	return g.answer(t, "Produce", requests)
}

// producerRequestsErr is an error when a producer request carries headers.
func producerRequestsErr(requests []Request) error {
	for _, request := range requests {
		if len(request.Headers) > 0 {
			return fmt.Errorf("Produce: request %q carries headers: a producer request is its name, method, path and body", request.Name)
		}
	}
	return nil
}

// programMethod is the Method of a program request (ProgramRequest).
const programMethod = "PYTHON"

// ProgramRequest is the request of one frozen run of a program: its name and
// its stdin. A program's answer is found by its name, in order, and the digest
// of its stdin must be the recorded one; the path a golden holds for it (the
// digests of the program text and of the interpreter's environment) is not
// compared: no producer can run again, so nothing can drift there.
func ProgramRequest(name string, stdin []byte) Request {
	body := base64.StdEncoding.EncodeToString(stdin)
	return Request{Name: name, Method: programMethod, Body: &body}
}

// packedPrefix marks a response body stored compressed.
const packedPrefix = "gzip+base64:"

// UnpackBody returns the bytes of a body a golden stores packed (gzip, then
// base64, behind packedPrefix); a body that is not packed, or does not unpack,
// fails the test. A frozen answer holds exactly the recorded bytes.
func UnpackBody(t *testing.T, body string) string {
	t.Helper()
	raw, err := unpackBody(body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func unpackBody(body string) (string, error) {
	encoded, ok := strings.CutPrefix(body, packedPrefix)
	if !ok {
		return "", fmt.Errorf("venueoracle: the body is not packed (no %q prefix)", packedPrefix)
	}
	packed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("venueoracle: packed body: %w", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		return "", fmt.Errorf("venueoracle: packed body: %w", err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("venueoracle: packed body: %w", err)
	}
	return string(raw), nil
}

// answer is the one path every frozen answer takes: read from the file, then
// bound to its request.
func (g *Golden) answer(t *testing.T, call string, requests []Request) []Response {
	t.Helper()
	g.step(t, call, stateOpen, statePython, stateDiffed)
	answers, err := g.frozenAnswers(requests)
	if err != nil {
		t.Fatal(err)
	}
	for index := range answers {
		g.slots = append(g.slots, answerSlot{request: g.identityOf(requests[index])})
		answers[index].slot = len(g.slots)
	}
	if g.state == stateOpen {
		g.state = statePython
	}
	return answers
}

// sameRequest reports whether the recorded key got is the key want of the
// request a test sends now. A program request is matched by name, method and
// stdin (ProgramRequest).
func sameRequest(got, want goldenRequest) bool {
	if got.Name != want.Name || got.Method != want.Method || got.BodySHA256 != want.BodySHA256 || got.HeadersSHA256 != want.HeadersSHA256 {
		return false
	}
	return want.Method == programMethod || got.Path == want.Path
}

func (g *Golden) frozenAnswers(requests []Request) ([]Response, error) {
	if g.served+len(requests) > len(g.loaded.Requests) {
		return nil, fmt.Errorf("golden %s holds %d answers, %d already served, the test asks for %d more", g.spec.Path, len(g.loaded.Requests), g.served, len(requests))
	}
	out := make([]Response, len(requests))
	for index, request := range requests {
		want := g.keyOf(request)
		got := g.loaded.Requests[g.served+index]
		if !sameRequest(got, want) {
			return nil, fmt.Errorf("golden %s request %d is %q %s %s (body %s.., headers %s..); the test sends %q %s %s (body %s.., headers %s..): a frozen answer answers the request it was recorded for",
				g.spec.Path, g.served+index, got.Name, got.Method, got.Path, short(got.BodySHA256), short(got.HeadersSHA256), want.Name, want.Method, want.Path, short(want.BodySHA256), short(want.HeadersSHA256))
		}
		out[index] = Response{Status: got.Status, Headers: got.Headers, Body: got.Body}
	}
	g.served += len(requests)
	return out, nil
}

// CompareRows compares the Python plane's frozen row state with goRows and
// returns the Python value. A difference fails the test with both values. name
// identifies the comparison in the file; each name answers one comparison. A
// frozen snapshot counts as used only here (or through InspectRows), never
// merely because it was retrieved.
func (g *Golden) CompareRows(t *testing.T, name string, goRows string) string {
	t.Helper()
	value := g.snapshot(t, "CompareRows", name)
	// Both planes' rows are compared as projected: the golden stores no token
	// and a per-run value is a typed placeholder (GoldenSpec.Scrub).
	projectedGo, err := g.project("", "rows "+name, goRows)
	if err != nil {
		t.Fatalf("golden %s: row comparison %q: %v", g.spec.Path, name, err)
	}
	if err := rowsDiffer(name, value, projectedGo); err != nil {
		t.Error(err)
	}
	g.rowsUsed[name] = true
	return value
}

// rowsDiffer is an error naming both values when the Python plane's rows and the
// Go plane's differ.
func rowsDiffer(name, python, goRows string) error {
	if python != goRows {
		return fmt.Errorf("%s differs after the requests:\n python: %s\n go:     %s", name, python, goRows)
	}
	return nil
}

// InspectRows returns the Python plane's frozen row state for a test that
// inspects it itself instead of comparing it with the Go plane's rows (for
// example against a literal). It counts as the snapshot's use.
func (g *Golden) InspectRows(t *testing.T, name string) string {
	t.Helper()
	value := g.snapshot(t, "InspectRows", name)
	g.rowsUsed[name] = true
	return value
}

func (g *Golden) snapshot(t *testing.T, call, name string) string {
	t.Helper()
	g.step(t, call, statePython, stateDiffed)
	value, err := g.frozenRows(name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func (g *Golden) frozenRows(name string) (string, error) {
	entry, ok := g.loaded.Rows[name]
	if !ok {
		return "", fmt.Errorf("golden %s holds no row comparison %q", g.spec.Path, name)
	}
	if g.rowsFetched[name] {
		return "", fmt.Errorf("golden %s: the row comparison %q was asked for twice; a frozen snapshot answers one comparison; use a distinct name per comparison", g.spec.Path, name)
	}
	g.rowsFetched[name] = true
	return entry.Rows, nil
}

func short(digest string) string {
	if len(digest) > 8 {
		return digest[:8]
	}
	return digest
}

// beforeDiff is Diff's lifecycle check.
func (g *Golden) beforeDiff(t *testing.T) {
	t.Helper()
	g.step(t, "Diff", statePython, stateDiffed)
}

// bindAnswers is Diff's binding of answers to requests: answer i must be the
// answer this golden handed out for request i (a slot bound to that request's
// key), and each answer may be compared once. An answer reused for another
// request, one out of order, or one that did not come from golden.Python is an
// error, so a divergence on the request it stood in for cannot hide.
func (g *Golden) bindAnswers(requests []Request, answers []Response) error {
	for index, request := range requests {
		slot := answers[index].slot
		if slot < 1 || slot > len(g.slots) {
			return fmt.Errorf("golden %s: the Python answer for request %d (%q) did not come from golden.Python", g.spec.Path, index, request.Name)
		}
		bound := &g.slots[slot-1]
		if bound.request != g.identityOf(request) {
			return fmt.Errorf("golden %s: request %d (%q) was given the answer that belongs to another request (%s): each answer answers the one request it was fetched for", g.spec.Path, index, request.Name, bound.request)
		}
		if bound.compared || bound.consumed {
			return fmt.Errorf("golden %s: the answer for request %d (%q) was already used once (compared or inspected): an answer stands in for one comparison", g.spec.Path, index, request.Name)
		}
		bound.compared = true
	}
	return nil
}

// afterDiff records that Diff ran.
func (g *Golden) afterDiff() { g.state = stateDiffed }

// SkipDiff advances the lifecycle from Python to Finish for a test that never
// calls venueoracle.Diff: every answer it fetched was declared inspected
// (Consumed) instead of compared with a Go answer, because nothing in the
// test re-sends the same requests to Go for a side-by-side comparison (for
// example, a named-Python-divergence check that only asserts the Python
// plane's own status codes and row count). Finish still requires every
// answer and row snapshot handed out to have been used; SkipDiff only lifts
// the "a Diff call happened" requirement, it does not relax that one.
func (g *Golden) SkipDiff(t *testing.T) {
	t.Helper()
	g.beforeDiff(t)
	g.afterDiff()
}

// Consumed declares that the test itself inspected these answers (for example a
// /metrics scrape it reads a counter from), so they are not left uncompared:
// Finish requires every answer handed out to have been either compared by Diff
// or declared here, each once.
func (g *Golden) Consumed(t *testing.T, answers ...Response) {
	t.Helper()
	if err := g.consume(answers); err != nil {
		t.Fatal(err)
	}
}

func (g *Golden) consume(answers []Response) error {
	for _, answer := range answers {
		if answer.slot < 1 || answer.slot > len(g.slots) {
			return fmt.Errorf("golden %s: Consumed was given an answer that did not come from golden.Python", g.spec.Path)
		}
		bound := &g.slots[answer.slot-1]
		if bound.compared || bound.consumed {
			return fmt.Errorf("golden %s: an answer (for %s) was declared inspected after it was already used once", g.spec.Path, bound.request)
		}
		bound.consumed = true
	}
	return nil
}

// Finish ends the test's use of the golden, in the test body. It requires
// every frozen answer and row comparison to have been used and writes the
// Go-only proof naming the build the Python truth was executed on.
func (g *Golden) Finish(t *testing.T) {
	t.Helper()
	g.step(t, "Finish", stateDiffed)
	g.state = stateFinished
	if err := g.answersCompared(); err != nil {
		t.Fatal(err)
	}
	if err := g.unusedAnswers(); err != nil {
		t.Fatal(err)
	}
	if err := g.unusedRows(); err != nil {
		t.Fatal(err)
	}
	WriteGoOnlyProof(t, "Go against the Python plane's answers executed on build "+g.spec.PythonBuild+" (frozen golden "+filepath.Base(g.spec.Path)+")"+g.retiredText())
	g.use.finish()
}

// answersCompared is an error unless every answer handed out was compared by
// Diff or declared inspected.
func (g *Golden) answersCompared() error {
	var missing []string
	for index, slot := range g.slots {
		if !slot.compared && !slot.consumed {
			missing = append(missing, fmt.Sprintf("#%d (%s)", index+1, slot.request))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("golden %s handed out %d answers, but these were neither compared by Diff nor declared inspected (Consumed): %s; an answer nobody compared is a comparison that no longer happens", g.spec.Path, len(g.slots), strings.Join(missing, ", "))
	}
	return nil
}

// requestIdentity is what makes a request the same request across processes.
func (g *Golden) identityOf(request Request) string {
	return identityOfKey(g.keyOf(request))
}

func identityOfKey(key goldenRequest) string {
	return fmt.Sprintf("%s %s %s body=%s headers=%s", key.Name, key.Method, key.Path, key.BodySHA256, key.HeadersSHA256)
}

// unusedAnswers is an error when the frozen file holds answers the test never
// asked for.
func (g *Golden) unusedAnswers() error {
	if g.served != len(g.loaded.Requests) {
		return fmt.Errorf("golden %s holds %d answers but the test asked for %d: an unused frozen answer is a comparison that no longer happens", g.spec.Path, len(g.loaded.Requests), g.served)
	}
	return nil
}

// unusedRows is an error when the frozen file holds row comparisons the test
// never asked for.
func (g *Golden) unusedRows() error {
	var unused []string
	for name := range g.loaded.Rows {
		if !g.rowsUsed[name] {
			unused = append(unused, name)
		}
	}
	if len(unused) > 0 {
		sort.Strings(unused)
		return fmt.Errorf("golden %s holds row comparisons the test never used: %s; an unused frozen snapshot is a comparison that no longer happens", g.spec.Path, strings.Join(unused, ", "))
	}
	return nil
}

// StableUUID is a deterministic UUID for name: the same name yields the same
// value in every process, which a golden's ids need (a recording run and a
// frozen run are different processes). It is a version-5 UUID under a fixed
// namespace, so it also looks like any other UUID to the code under test.
func StableUUID(name string) string {
	return uuidV5(goldenNamespace, name)
}

var goldenNamespace = [16]byte{0x64, 0x68, 0x6f, 0x2d, 0x76, 0x65, 0x6e, 0x75, 0x65, 0x2d, 0x67, 0x6f, 0x6c, 0x64, 0x65, 0x6e}

func uuidV5(namespace [16]byte, name string) string {
	hash := sha256.New()
	hash.Write(namespace[:])
	hash.Write([]byte(name))
	sum := hash.Sum(nil)
	var u [16]byte
	copy(u[:], sum[:16])
	u[6] = (u[6] & 0x0f) | 0x50
	u[8] = (u[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
