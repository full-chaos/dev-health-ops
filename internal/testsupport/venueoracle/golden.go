package venueoracle

import (
	"crypto/sha1" //nolint:gosec // git's own blob id, not a security digest
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Golden freezes the Python plane's answers for a venue oracle whose Python
// producer is retired (a route whose Python body was reduced to the Go-served
// refusal stub).
//
// A two-plane oracle proves the Go route against the Python route running
// beside it. Once the Python body is gone that comparison cannot run, but the
// truth it established must not be rewritten from Go's own output: a golden
// is the Python plane's answers EXECUTED on the last Python-bearing build,
// recorded once, then frozen. The test keeps its shape (the same seed, the
// same requests, the same Diff and row comparisons); only where the Python
// side comes from changes:
//
//   - Frozen (the default, and what CI runs): the answers come from the golden
//     file. No Python plane is served. The file's SHA-256 must equal the
//     digest the test pins, so an edit by hand fails.
//   - Recording (DHO_VENUE_GOLDEN_UPDATE=1, driven by the record verb
//     internal/testsupport/venueoracle/goldenrecord): the test runs the Python
//     plane from the checkout DHO_VENUE_GOLDEN_PYTHON_ROOT names, which must be
//     the pinned commit (GoldenSpec.PythonBuild) with every file under src equal
//     to that commit's blobs, serves the requests, and Finish writes a
//     CANDIDATE beside the golden. The verb replays the candidate in a fresh
//     process and only then promotes it and pins its digest.
//
// The lifecycle is fixed: OpenGolden, Python (any number of calls), Diff,
// Finish. A call out of order fails the test with an error naming it; every
// answer Python returned must be compared by Diff or declared with Consumed;
// every frozen row comparison is consumed once.
//
// Threat model: this harness defends against accidental drift by honest
// authors (a stale producer, a moved build, reused rows, partial writes); a
// hostile author of golden inputs or a hostile committer is OUT OF SCOPE,
// defended by human PR review. A guard exists because an honest mistake would
// otherwise pass silently, not because a determined attacker could not get
// around it.
//
// A frozen test compares no Python response, so it writes the Go-only proof
// (WriteGoOnlyProof) naming the build the truth was executed on.
//
// The recording run and the frozen run are different processes with different
// random ids, so a test that uses a golden must seed deterministic values
// (StableUUID) wherever an id reaches a compared response or row.
type GoldenSpec struct {
	// Path is the golden file, relative to the test's package directory.
	Path string
	// PythonBuild is the 40-hex commit the Python answers were executed on:
	// the last build that still carried the Python route bodies.
	PythonBuild string
	// SHA256 is the digest of the golden file the test pins. Empty is allowed
	// only while recording.
	SHA256 string
	// Recipe is one line naming how to regenerate the file; it is printed by
	// every failure that asks for a regeneration.
	Recipe string
}

// Golden is an opened GoldenSpec.
type Golden struct {
	spec      GoldenSpec
	recording bool
	loaded    goldenFile
	recorded  goldenFile
	// served counts the frozen answers handed out so far: a test may ask for
	// the Python plane's answers in several calls (one per batch of requests),
	// and the frozen file holds them in the order they were asked.
	served int
	// compared counts the answers Diff compared with the Go plane's, consumed the
	// answers the test says it inspected itself (Consumed): together they must be
	// every answer handed out, or a comparison was skipped.
	// slots binds each answer handed out (slot n, from 1) to the request it
	// answers and records what became of it: compared by Diff or inspected by the
	// test (Consumed), each exactly once.
	slots []answerSlot
	// verifiedRoot is the checkout PythonRoot verified; a recording run serves
	// Python only through a venue built on exactly that root.
	verifiedRoot string
	// rowsUsed records which frozen row comparisons the test asked for: a
	// snapshot nothing consumed is a comparison that no longer happens.
	rowsUsed    map[string]bool
	rowsFetched map[string]bool
	state       goldenState
}

// The environment variables that switch a test to recording.
const (
	goldenUpdateEnv     = "DHO_VENUE_GOLDEN_UPDATE"
	goldenPythonRootEnv = "DHO_VENUE_GOLDEN_PYTHON_ROOT"
	// goldenCandidateEnv makes a frozen run replay the candidate a recording run
	// wrote (GoldenSpec.Path + goldenCandidateSuffix) instead of the pinned file:
	// the record verb's fresh-process check before it promotes the candidate.
	goldenCandidateEnv = "DHO_VENUE_GOLDEN_CANDIDATE"
	// GoldenCandidateSuffix is appended to a golden's path for the file a
	// recording run writes. A recording run never writes the golden itself.
	GoldenCandidateSuffix = ".recording"
)

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

type goldenHeader struct {
	Test        string `json:"test"`
	PythonBuild string `json:"python_build"`
	// ProducerDigest is the digest of the Python source the answers were
	// executed from (see producerDigest): every file under src of the pinned
	// checkout, by git blob id.
	ProducerDigest string `json:"producer_digest"`
	Recipe         string `json:"recipe"`
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
// test could not trust: a frozen file must exist, name the pinned build and the
// test, and hash to the pinned digest.
//
// A recording run never writes the golden: Finish, in the test body, writes a
// candidate beside it (spec.Path + GoldenCandidateSuffix) and the record verb
// (venueoracle/goldenrecord) promotes it only after a fresh-process replay of the
// same test passes, so a cleanup that fails after Finish (registered before or
// after OpenGolden) fails the recording run and nothing lands.
func OpenGolden(t *testing.T, spec GoldenSpec) *Golden {
	t.Helper()
	recording := os.Getenv(goldenUpdateEnv) == "1"
	if !recording && os.Getenv(goldenCandidateEnv) == "1" {
		// The record verb's replay of the candidate: the pinned digest does not
		// exist yet, the candidate's own is the one to check.
		spec.Path += GoldenCandidateSuffix
		raw, err := os.ReadFile(spec.Path)
		if err != nil {
			t.Fatalf("golden candidate %s: %v", spec.Path, err)
		}
		sum := sha256.Sum256(raw)
		spec.SHA256 = hex.EncodeToString(sum[:])
	}
	g, err := openGolden(spec, t.Name(), recording)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func openGolden(spec GoldenSpec, test string, recording bool) (*Golden, error) {
	if spec.Path == "" || spec.Recipe == "" || !buildPattern.MatchString(spec.PythonBuild) {
		return nil, fmt.Errorf("venueoracle: a GoldenSpec needs a path, a recipe and the 40-hex Python build the answers were executed on: %+v", spec)
	}
	g := &Golden{spec: spec, recording: recording, rowsUsed: map[string]bool{}, rowsFetched: map[string]bool{}}
	if recording {
		g.recorded = goldenFile{Header: goldenHeader{Test: test, PythonBuild: spec.PythonBuild, Recipe: spec.Recipe}, Rows: map[string]goldenRows{}}
		return g, nil
	}
	raw, err := os.ReadFile(spec.Path)
	if err != nil {
		return nil, fmt.Errorf("golden %s is missing (%v): a golden is executed, never authored; regenerate it: %s", spec.Path, err, spec.Recipe)
	}
	if spec.SHA256 == "" {
		return nil, fmt.Errorf("golden %s: the test pins no digest; record it (%s) and pin the digest it prints", spec.Path, spec.Recipe)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != spec.SHA256 {
		return nil, fmt.Errorf("golden %s changed without its digest: sha256 %s, the test pins %s; regenerate by execution: %s", spec.Path, got, spec.SHA256, spec.Recipe)
	}
	if err := json.Unmarshal(raw, &g.loaded); err != nil {
		return nil, fmt.Errorf("golden %s: %w", spec.Path, err)
	}
	if g.loaded.Header.Test != test {
		return nil, fmt.Errorf("golden %s was recorded for %q, not for %q; a golden belongs to one test; regenerate: %s", spec.Path, g.loaded.Header.Test, test, spec.Recipe)
	}
	if g.loaded.Header.PythonBuild != spec.PythonBuild {
		return nil, fmt.Errorf("golden %s was executed on build %s, the test names %s; regenerate: %s", spec.Path, g.loaded.Header.PythonBuild, spec.PythonBuild, spec.Recipe)
	}
	if len(g.loaded.Header.ProducerDigest) != 64 {
		return nil, fmt.Errorf("golden %s names no producer digest: it was not recorded by this harness; regenerate: %s", spec.Path, spec.Recipe)
	}
	return g, nil
}

// Recording reports whether this run executes Python and writes the file.
func (g *Golden) Recording() bool { return g.recording }

// PythonRoot is the repository root the Python plane must run from. Frozen,
// it is the caller's own root (no Python is served). Recording, it is the
// pinned checkout: DHO_VENUE_GOLDEN_PYTHON_ROOT, verified to be a clean git
// worktree at exactly the pinned build.
func (g *Golden) PythonRoot(t *testing.T, root string) string {
	t.Helper()
	if !g.recording {
		return root
	}
	pinned := os.Getenv(goldenPythonRootEnv)
	if pinned == "" {
		t.Fatalf("recording needs %s: a clean worktree at %s (git worktree add --detach <dir> %s)", goldenPythonRootEnv, g.spec.PythonBuild, g.spec.PythonBuild)
	}
	digest, err := verifyPinnedCheckout(pinned, g.spec.PythonBuild)
	if err != nil {
		t.Fatalf("recording: %v", err)
	}
	g.recorded.Header.ProducerDigest = digest
	g.verifiedRoot = pinned
	return pinned
}

// verifyPinnedCheckout is an error unless dir is a git checkout at exactly
// build whose Python source is byte for byte what that commit holds, and
// returns the digest of that source. A clean status is not enough (a file
// marked assume-unchanged or skip-worktree, or an ignored startup hook, hides
// from it), so the source under src is compared file by file with the commit's
// own blobs.
func verifyPinnedCheckout(dir, build string) (string, error) {
	head, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("%s is not a git checkout: %w", dir, err)
	}
	if got := strings.TrimSpace(string(head)); got != build {
		return "", fmt.Errorf("%s is at %s, the test pins %s", dir, got, build)
	}
	status, err := exec.Command("git", "-C", dir, "status", "--porcelain", "--untracked-files=normal").Output()
	if err != nil {
		return "", fmt.Errorf("git status in %s: %w", dir, err)
	}
	if strings.TrimSpace(string(status)) != "" {
		return "", fmt.Errorf("%s has uncommitted or untracked files: a golden is executed on a clean build", dir)
	}
	return producerDigest(dir)
}

// producerDigest compares every file under dir/src (the byte-code cache aside)
// with the blob the checked-out commit holds at that path, and returns the
// SHA-256 of the sorted "path blob" list. A file the commit lacks, one whose
// content differs, or a commit file that is missing is an error.
func producerDigest(dir string) (string, error) {
	tree, err := exec.Command("git", "-C", dir, "ls-tree", "-r", "-z", "HEAD", "--", "src").Output()
	if err != nil {
		return "", fmt.Errorf("git ls-tree in %s: %w", dir, err)
	}
	committed := map[string]string{}
	for _, entry := range strings.Split(string(tree), "\x00") {
		if entry == "" {
			continue
		}
		meta, name, ok := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if !ok || len(fields) != 3 {
			return "", fmt.Errorf("git ls-tree in %s: unreadable entry %q", dir, entry)
		}
		committed[name] = fields[2]
	}
	if len(committed) == 0 {
		return "", fmt.Errorf("%s holds no Python source under src at the pinned commit", dir)
	}
	onDisk := map[string]string{}
	err = filepath.WalkDir(filepath.Join(dir, "src"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".pyc") {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			// A link's blob is its target text, not the code it resolves to (which can
			// live outside src and change unseen): the Python source holds none.
			return fmt.Errorf("%s is a symbolic link: the Python source under src of a pinned build holds none, because a link's target is not part of what the commit's blob pins", filepath.ToSlash(rel))
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		onDisk[filepath.ToSlash(rel)] = gitBlobID(content)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk %s/src: %w", dir, err)
	}
	names := make([]string, 0, len(committed))
	for name := range committed {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		got, present := onDisk[name]
		switch {
		case !present:
			return "", fmt.Errorf("%s: the pinned commit holds %s, the checkout does not", dir, name)
		case got != committed[name]:
			return "", fmt.Errorf("%s: %s differs from the pinned commit's blob (%s on disk, %s committed): a golden is executed on the pinned build only", dir, name, got, committed[name])
		}
		fmt.Fprintf(hash, "%s %s\n", name, got)
	}
	extra := make([]string, 0)
	for name := range onDisk {
		if _, ok := committed[name]; !ok {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return "", fmt.Errorf("%s holds %s, which the pinned commit does not (an untracked, ignored or generated file under src): a golden is executed on the pinned build only", dir, extra[0])
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// gitBlobID is git's SHA-1 object id of a blob with content.
func gitBlobID(content []byte) string {
	hash := sha1.New()
	fmt.Fprintf(hash, "blob %d\x00", len(content))
	hash.Write(content)
	return hex.EncodeToString(hash.Sum(nil))
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

// Python returns the Python plane's answers to requests: served for real
// while recording, read from the frozen file otherwise. The request list must
// be the one the file was recorded with (name, method, path, body, request
// headers), so a test whose requests drifted fails naming the difference
// instead of comparing against another test's truth. It may be called any
// number of times before Finish; every answer it returns must then be compared
// by Diff or declared inspected (Consumed).
func (g *Golden) Python(t *testing.T, v *Venue, requests []Request) []Response {
	t.Helper()
	g.step(t, "Python", stateOpen, statePython, stateDiffed)
	var answers []Response
	if g.recording {
		if err := g.recordingRootErr(v); err != nil {
			t.Fatal(err)
		}
		answers = v.ServePython(t, requests)
		for index, request := range requests {
			entry := requestKey(request)
			entry.Status, entry.Headers, entry.Body = answers[index].Status, answers[index].Headers, answers[index].Body
			g.recorded.Requests = append(g.recorded.Requests, entry)
		}
	} else {
		var err error
		if answers, err = g.frozenAnswers(requests); err != nil {
			t.Fatal(err)
		}
	}
	for index := range answers {
		g.slots = append(g.slots, answerSlot{request: requestIdentity(requests[index])})
		answers[index].slot = len(g.slots)
	}
	if g.state == stateOpen {
		g.state = statePython
	}
	return answers
}

func (g *Golden) frozenAnswers(requests []Request) ([]Response, error) {
	if g.served+len(requests) > len(g.loaded.Requests) {
		return nil, fmt.Errorf("golden %s holds %d answers, %d already served, the test asks for %d more; regenerate: %s", g.spec.Path, len(g.loaded.Requests), g.served, len(requests), g.spec.Recipe)
	}
	out := make([]Response, len(requests))
	for index, request := range requests {
		want := requestKey(request)
		got := g.loaded.Requests[g.served+index]
		if got.Name != want.Name || got.Method != want.Method || got.Path != want.Path || got.BodySHA256 != want.BodySHA256 || got.HeadersSHA256 != want.HeadersSHA256 {
			return nil, fmt.Errorf("golden %s request %d is %q %s %s (body %s.., headers %s..); the test sends %q %s %s (body %s.., headers %s..); regenerate: %s",
				g.spec.Path, g.served+index, got.Name, got.Method, got.Path, short(got.BodySHA256), short(got.HeadersSHA256), want.Name, want.Method, want.Path, short(want.BodySHA256), short(want.HeadersSHA256), g.spec.Recipe)
		}
		out[index] = Response{Status: got.Status, Headers: got.Headers, Body: got.Body}
	}
	g.served += len(requests)
	return out, nil
}

// CompareRows compares the Python plane's row state with goRows and returns
// the Python value. Recording, the Python value is what source computes (the
// Python plane's database after it served); frozen, it is the recorded text. A
// difference fails the test with both values. name identifies the comparison in
// the file; each name answers one comparison. A frozen snapshot counts as used
// only here (or through InspectRows), never merely because it was retrieved.
func (g *Golden) CompareRows(t *testing.T, name string, source func() string, goRows string) string {
	t.Helper()
	value := g.snapshot(t, "CompareRows", name, source)
	if err := rowsDiffer(name, value, goRows); err != nil {
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

// InspectRows returns the Python plane's row state for a test that inspects it
// itself instead of comparing it with the Go plane's rows (for example against a
// literal). It counts as the snapshot's use.
func (g *Golden) InspectRows(t *testing.T, name string, source func() string) string {
	t.Helper()
	value := g.snapshot(t, "InspectRows", name, source)
	g.rowsUsed[name] = true
	return value
}

func (g *Golden) snapshot(t *testing.T, call, name string, source func() string) string {
	t.Helper()
	g.step(t, call, statePython, stateDiffed)
	if g.recording {
		if g.rowsUsed[name] {
			t.Fatalf("golden row comparison %q is recorded twice", name)
		}
		value := source()
		g.recorded.Rows[name] = goldenRows{Rows: value}
		return value
	}
	value, err := g.frozenRows(name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func (g *Golden) frozenRows(name string) (string, error) {
	entry, ok := g.loaded.Rows[name]
	if !ok {
		return "", fmt.Errorf("golden %s holds no row comparison %q; regenerate: %s", g.spec.Path, name, g.spec.Recipe)
	}
	if g.rowsFetched[name] {
		return "", fmt.Errorf("golden %s: the row comparison %q was asked for twice; a frozen snapshot answers one comparison; use a distinct name per comparison: %s", g.spec.Path, name, g.spec.Recipe)
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
		if bound.request != requestIdentity(request) {
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

// Finish ends the test's use of the golden, in the test body. Recording, it
// writes the candidate (spec.Path + GoldenCandidateSuffix) when every check
// passed and the test has not failed; it never writes the golden itself, the
// record verb promotes the candidate after a fresh-process replay passes.
// Frozen, it requires every recorded answer and row comparison to have been
// used and writes the Go-only proof naming the build the Python truth was
// executed on.
func (g *Golden) Finish(t *testing.T) {
	t.Helper()
	g.step(t, "Finish", stateDiffed)
	g.state = stateFinished
	if err := g.answersCompared(); err != nil {
		t.Fatal(err)
	}
	if g.recording {
		digest, err := g.writeCandidate(t.Failed())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("recorded candidate %s%s (sha256 %s): the record verb promotes it after a fresh-process replay passes (go run ./internal/testsupport/venueoracle/goldenrecord)", g.spec.Path, GoldenCandidateSuffix, digest)
		return
	}
	if err := g.unusedAnswers(); err != nil {
		t.Fatal(err)
	}
	if err := g.unusedRows(); err != nil {
		t.Fatal(err)
	}
	WriteGoOnlyProof(t, "Go against the Python plane's answers executed on build "+g.spec.PythonBuild+" (frozen golden "+filepath.Base(g.spec.Path)+")")
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

// writeCandidate writes the recording as a candidate file beside the golden and
// returns its digest. It writes nothing for a failed run, an empty recording or
// an unverified producer.
func (g *Golden) writeCandidate(failed bool) (string, error) {
	if failed {
		return "", fmt.Errorf("recording %s: the run already failed, so no candidate was written (a golden is only recorded from a run that passed every check); fix the failure and record again", g.spec.Path)
	}
	if len(g.recorded.Requests) == 0 {
		return "", fmt.Errorf("recording %s: no request was served through the golden", g.spec.Path)
	}
	if len(g.recorded.Header.ProducerDigest) != 64 {
		return "", fmt.Errorf("recording %s: the Python producer was never verified (golden.PythonRoot was not called)", g.spec.Path)
	}
	raw, err := json.MarshalIndent(g.recorded, "", "  ")
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	candidate := g.spec.Path + GoldenCandidateSuffix
	if err := os.MkdirAll(filepath.Dir(candidate), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(candidate, raw, 0o644); err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// recordingRootErr is an error unless the checkout PythonRoot verified is the
// root the venue serves Python from.
func (g *Golden) recordingRootErr(v *Venue) error {
	if g.verifiedRoot == "" {
		return fmt.Errorf("recording: serve Python only from the root golden.PythonRoot returned (it verifies the checkout is clean and at the pinned build); it was never called")
	}
	if v == nil || !sameDirectory(v.Root, g.verifiedRoot) {
		venueRoot := "<no venue>"
		if v != nil {
			venueRoot = v.Root
		}
		return fmt.Errorf("recording: the venue serves Python from %s, not from the verified checkout %s: build the venue with Options.Root = golden.PythonRoot(...)", venueRoot, g.verifiedRoot)
	}
	return nil
}

// sameDirectory reports whether a and b are the same directory, symlinks resolved.
func sameDirectory(a, b string) bool {
	resolve := func(path string) string {
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			path = resolved
		}
		if absolute, err := filepath.Abs(path); err == nil {
			path = absolute
		}
		return filepath.Clean(path)
	}
	return resolve(a) == resolve(b)
}

// requestIdentity is what makes a request the same request across processes.
func requestIdentity(request Request) string {
	key := requestKey(request)
	return fmt.Sprintf("%s %s %s body=%s headers=%s", key.Name, key.Method, key.Path, key.BodySHA256, key.HeadersSHA256)
}

// unusedAnswers is an error when the frozen file holds answers the test never
// asked for.
func (g *Golden) unusedAnswers() error {
	if g.served != len(g.loaded.Requests) {
		return fmt.Errorf("golden %s holds %d answers but the test asked for %d: an unused frozen answer is a comparison that no longer happens; regenerate: %s", g.spec.Path, len(g.loaded.Requests), g.served, g.spec.Recipe)
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
		return fmt.Errorf("golden %s holds row comparisons the test never used: %s; an unused frozen snapshot is a comparison that no longer happens; regenerate: %s", g.spec.Path, strings.Join(unused, ", "), g.spec.Recipe)
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
