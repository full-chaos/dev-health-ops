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
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
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
	// PassEnv names the ambient variables the recording needs beyond the
	// recorder's fixed set (goldenrecord -pass-env): a credential the producer
	// reads and whose value cannot be written down. Names only. The list is
	// part of the golden: the recorder must pass exactly these names, the
	// header holds them, and a frozen run refuses a header that differs. A
	// variable whose value shapes an answer is not passed: the test sets it.
	PassEnv []string
	// Scrub turns a token field that is not a JWT (an opaque key, a session
	// id) into a typed placeholder. It runs on every recorded and every
	// compared body and header value after the JWT projection, on both planes,
	// and must be deterministic and idempotent. A golden stores no token value:
	// the recorder refuses a candidate that still holds a token shape.
	Scrub func(text string) string
	// KeyScrub turns a value in a REQUEST that the Python plane issued earlier
	// and the test sends back (a mailed link token) into a placeholder, for the
	// request's key only: the request sent to Python is untouched. A JWT in a
	// request is projected without it. Like Scrub it must be deterministic and
	// idempotent, and it is not applied to the authorization header.
	KeyScrub func(text string) string
	// RawSink, set, is called while RECORDING with each Python answer as the
	// Python plane gave it, before it is projected, for a test whose later
	// requests must carry what the Python plane issued earlier (a refresh token).
	// It is never called on a replay, and what it is given must stay in memory:
	// the golden holds the projected answer only.
	RawSink func(request Request, answer Response)
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
	// pythonEnv is the key of the venue's declared Python settings, once a
	// venue was built for this golden.
	pythonEnv      string
	pythonEnvBound bool
	// testSetAtStart is the variables test code had set when the venue was
	// built: a call's key holds what changed in them since (callEnvKey).
	testSetAtStart []string
	// planeNames is the names the venue's plane sets itself: a process
	// variable of such a name does not reach a Python child.
	planeNames map[string]bool
	// use is what RunTests reads after the package's tests: this golden was
	// opened by a test, and whether it reached Finish.
	use *goldenUse
	// rowsUsed records which frozen row comparisons the test asked for: a
	// snapshot nothing consumed is a comparison that no longer happens.
	rowsUsed    map[string]bool
	rowsFetched map[string]bool
	state       goldenState
	// What a recording replaced by a placeholder (see golden_blanked.go): the
	// count per pattern, and the digests of the raw values a Scrub blanked.
	blankCounts   map[string]int
	blankOrdinals map[string]int
	scrubEntries  []scrubEntry
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
	// PassedEnv is the sorted names of the ambient variables the recorder
	// passed to the recording beyond its fixed set (GoldenSpec.PassEnv).
	PassedEnv []string `json:"passed_env,omitempty"`
	// PythonEnv is the key of the Python plane's environment when the answers
	// were recorded: a frozen run under another one is refused instead of being
	// served answers the real Python api would not give under it. For
	// PythonEnvVersion 2 it is pythonEnvKey over the variables the test set and
	// pythonPlaneEnv (the whole environment of the Python child). With no
	// version it is the first key version (legacyPlaneEnv: what the harness
	// and the test declared, not what the child inherited); only the goldens
	// on the closed list legacy_python_env_goldens.txt may still hold that.
	PythonEnv        string `json:"python_env,omitempty"`
	PythonEnvVersion int    `json:"python_env_version,omitempty"`
	// Blanked lists, by pattern, the leaves the recording replaced by a
	// placeholder, with how many: the paths the golden does not hold by value
	// (a token projected to its claims, a Volatile header, a generated id or a
	// run time). A recording always writes it (an empty list is "{}"); a golden
	// recorded before it has none, and is then not checked against it.
	Blanked map[string]int `json:"blanked"`
}

// goldenPassedEnv is how the recorder tells a recording which ambient
// variables it passed by name (comma-separated).
const goldenPassedEnv = "DHO_VENUE_GOLDEN_PASSED_ENV"

// bindPythonEnv ties the golden to the environment its venue gives the Python
// plane. Recording, the key and its version go into the header. Frozen, the
// header's key must be the one this test gives now, by the header's version:
// a changed, added or removed variable is refused.
func (g *Golden) bindPythonEnv(options Options) error {
	testSet := testSetEnv()
	version := pythonEnvKeyVersion
	if !g.recording {
		version = g.loaded.Header.PythonEnvVersion
	}
	var key string
	var err error
	switch version {
	case pythonEnvKeyVersion:
		key, err = venuePythonEnvKey(testSet, options)
	case 0:
		key, err = legacyPythonEnvKey(legacyPlaneEnv(options))
	default:
		return fmt.Errorf("golden %s holds a key of its Python environment in version %d, which this harness does not know; regenerate: %s", g.spec.Path, version, g.spec.Recipe)
	}
	if err != nil {
		return err
	}
	if g.pythonEnvBound && g.pythonEnv != key {
		return fmt.Errorf("golden %s: two venues with different Python environments use one golden: its answers belong to one environment", g.spec.Path)
	}
	g.pythonEnv, g.pythonEnvBound, g.testSetAtStart, g.planeNames = key, true, testSet, planeNames(options)
	if g.recording {
		g.recorded.Header.PythonEnv, g.recorded.Header.PythonEnvVersion = key, pythonEnvKeyVersion
		return nil
	}
	recorded := g.loaded.Header.PythonEnv
	switch {
	case recorded == key:
		return nil
	case recorded == "":
		return fmt.Errorf("golden %s holds no key of its venue's Python environment, so it cannot show that its answers were given under the environment this test gives the Python plane; regenerate: %s", g.spec.Path, g.spec.Recipe)
	default:
		return fmt.Errorf("golden %s was recorded under another Python environment than this test gives the Python plane (key %s, version %d; the golden's %s): a changed, added or removed variable changes what the real Python api answers; regenerate: %s",
			g.spec.Path, keyHead(key), version, keyHead(recorded), g.spec.Recipe)
	}
}

// callEnvKey is the key of one call's own environment: what test code changed
// in the process since the venue was built, and the call's extra entries
// (PythonWithEnv; nil for a call that passes none). It is empty when the call
// runs under the venue's environment as it was built. A golden of the first
// key version keeps that version's rule: the extra entries alone.
func (g *Golden) callEnvKey(extra []string) (string, error) {
	version := pythonEnvKeyVersion
	if !g.recording {
		version = g.loaded.Header.PythonEnvVersion
	}
	if version == 0 {
		if extra == nil {
			return "", nil
		}
		return legacyPythonEnvKey(extra)
	}
	// What the test changed in the process since the venue was built, less the
	// names the plane sets itself: the plane's entry wins, so such a variable
	// does not reach the child. Both this and the extra entries are the
	// test's: keyed by name and value (perRunPythonEnv).
	var changed []string
	for _, entry := range testSetDelta(g.testSetAtStart, testSetEnv()) {
		if name, _, _ := strings.Cut(entry, "="); !g.planeNames[name] {
			changed = append(changed, entry)
		}
	}
	if extra == nil && len(changed) == 0 {
		return "", nil
	}
	return pythonEnvKey(append(fromTest(changed...), fromTest(extra...)...))
}

// pythonEnvUnboundErr is an error when the frozen golden holds the key of a
// venue's Python settings and no venue was built for it: the settings were
// never checked.
func (g *Golden) pythonEnvUnboundErr() error {
	if g.loaded.Header.PythonEnv != "" && !g.pythonEnvBound {
		return fmt.Errorf("golden %s was recorded under a venue's Python settings, but this test built no venue for it (venueoracle.Start with Options.Golden): the settings cannot be checked; regenerate: %s", g.spec.Path, g.spec.Recipe)
	}
	return nil
}

// keyHead is the head of a key, for a message.
func keyHead(key string) string {
	if key == "" {
		return "<none>"
	}
	if len(key) > 12 {
		return key[:12]
	}
	return key
}

// envNames is names sorted, without blanks and duplicates.
func envNames(names []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, name := range names {
		if name = strings.TrimSpace(name); name != "" && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// passedEnvErr is an error unless the recorder passed exactly the names the
// spec declares: a variable passed but not declared would shape the recording
// unseen, one declared but not passed would be missing from the producer.
func passedEnvErr(spec GoldenSpec, passed string) error {
	want, got := envNames(spec.PassEnv), envNames(strings.Split(passed, ","))
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("golden %s: the recorder passed the ambient variables %q (goldenrecord -pass-env), the test's GoldenSpec.PassEnv declares %q: a passed variable is part of the golden, so the two must be the same names", spec.Path, got, want)
	}
	return nil
}

type goldenRequest struct {
	Name       string `json:"name"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	BodySHA256 string `json:"body_sha256"`
	// HeadersSHA256 is the digest of the request headers the Python plane was
	// sent (see headersDigest), so a caller or content type that drifted is
	// refused instead of being served the answer of another.
	HeadersSHA256 string `json:"request_headers_sha256"`
	// CallEnv is the key of the extra environment the Python plane answered
	// this request under (Golden.PythonWithEnv: pythonEnvKey over the call's
	// entries, so a value is never stored). It is empty for an answer given
	// under the venue's own environment. A frozen run refuses a request whose
	// call passes another extra environment than the recorded one.
	CallEnv string            `json:"call_env,omitempty"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
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
	// Noted before anything can refuse the golden: RunTests asks, after the
	// package's tests, which goldens their tests opened and finished.
	use := noteOpened(t, spec.Path)
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
	g.use = use
	return g
}

func openGolden(spec GoldenSpec, test string, recording bool) (*Golden, error) {
	if spec.Path == "" || spec.Recipe == "" || !buildPattern.MatchString(spec.PythonBuild) {
		return nil, fmt.Errorf("venueoracle: a GoldenSpec needs a path, a recipe and the 40-hex Python build the answers were executed on: %+v", spec)
	}
	g := &Golden{spec: spec, recording: recording, rowsUsed: map[string]bool{}, rowsFetched: map[string]bool{}}
	if recording {
		if err := passedEnvErr(spec, os.Getenv(goldenPassedEnv)); err != nil {
			return nil, err
		}
		g.recorded = goldenFile{Header: goldenHeader{Test: test, PythonBuild: spec.PythonBuild, Recipe: spec.Recipe, PassedEnv: envNames(spec.PassEnv)}, Rows: map[string]goldenRows{}}
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
	if want := envNames(spec.PassEnv); !reflect.DeepEqual(envNames(g.loaded.Header.PassedEnv), want) {
		return nil, fmt.Errorf("golden %s was recorded with the ambient variables %q passed, the test declares %q (GoldenSpec.PassEnv); regenerate: %s", spec.Path, g.loaded.Header.PassedEnv, want, spec.Recipe)
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
	if err := bytecodeEnvErr(); err != nil {
		t.Fatal(err)
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

// producerDigest compares every file under dir/src
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
	var files []string
	err = filepath.WalkDir(filepath.Join(dir, "src"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "__pycache__" {
				return fmt.Errorf("%s holds a bytecode cache: a timestamp-validated .pyc can run code older than the source it sits beside; clear it (the record verb does: find src -name __pycache__ -prune -exec rm -rf {} +) and record with PYTHONDONTWRITEBYTECODE=1", filepath.ToSlash(rel))
			}
			return nil
		}
		if strings.HasSuffix(path, ".pyc") {
			return fmt.Errorf("%s is a compiled Python file: the source of a pinned build holds none", filepath.ToSlash(rel))
		}
		if entry.Type()&os.ModeSymlink != 0 {
			// A link's blob is its target text, not the code it resolves to (which can
			// live outside src and change unseen): the Python source holds none.
			return fmt.Errorf("%s is a symbolic link: the Python source under src of a pinned build holds none, because a link's target is not part of what the commit's blob pins", filepath.ToSlash(rel))
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk %s/src: %w", dir, err)
	}
	// git computes the blob ids itself (unfiltered, as stored), in one batch.
	hashed := exec.Command("git", "-C", dir, "hash-object", "--no-filters", "--stdin-paths")
	hashed.Stdin = strings.NewReader(strings.Join(files, "\n") + "\n")
	blobs, err := hashed.Output()
	if err != nil {
		return "", fmt.Errorf("git hash-object in %s: %w", dir, err)
	}
	ids := strings.Fields(string(blobs))
	if len(ids) != len(files) {
		return "", fmt.Errorf("git hash-object in %s hashed %d of %d files", dir, len(ids), len(files))
	}
	for index, name := range files {
		onDisk[name] = ids[index]
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

// keyOf is the key of request as this golden holds it: the key of the request
// after the golden's own projection (tokens to their claims, the spec's Scrub),
// so a request that carries a token or id the Python plane issued earlier has
// the same key in every recording and in the replay, whose request is built
// from the projected answers. A request the projection leaves unchanged has
// the key requestKey gives it, byte for byte.
func (g *Golden) keyOf(request Request) goldenRequest {
	return requestKey(g.projectRequest(request))
}

// sameKeySameAnswerErr is an error when an earlier request of this recording
// has the key of entry (the same request once its tokens and generated values
// are projected) and another answer: the replay could not tell the two apart,
// so it would serve one answer for both. Two requests that differ only in a
// projected value may fold only when the Python plane answered both alike.
func (g *Golden) sameKeySameAnswerErr(entry goldenRequest) error {
	for _, earlier := range g.recorded.Requests {
		if earlier.Name != entry.Name || earlier.Method != entry.Method || earlier.Path != entry.Path ||
			earlier.BodySHA256 != entry.BodySHA256 || earlier.HeadersSHA256 != entry.HeadersSHA256 || earlier.CallEnv != entry.CallEnv {
			continue
		}
		if earlier.Status != entry.Status || !reflect.DeepEqual(earlier.Headers, entry.Headers) || earlier.Body != entry.Body {
			return fmt.Errorf("golden %s: two requests named %q have the same key once their tokens and generated values are projected (path %s, body %s.., headers %s..), and the Python plane answered them differently: the replay could not tell them apart; give them different names or stable values", g.spec.Path, entry.Name, entry.Path, short(entry.BodySHA256), short(entry.HeadersSHA256))
		}
	}
	return nil
}

// projectKeyText is a text as the golden holds it in a request key: its tokens
// projected and the spec's KeyScrub applied (not Scrub: an id the test writes in
// a request on purpose is part of the request, and stays in its key). It is idempotent, so the request of a
// recording (real token) and of its replay (projected token) key alike.
func (g *Golden) projectKeyText(text string) string {
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

// Python returns the Python plane's answers to requests: served for real
// while recording, read from the frozen file otherwise. The request list must
// be the one the file was recorded with (name, method, path, body, request
// headers), so a test whose requests drifted fails naming the difference
// instead of comparing against another test's truth. It may be called any
// number of times before Finish; every answer it returns must then be compared
// by Diff or declared inspected (Consumed).
func (g *Golden) Python(t *testing.T, v *Venue, requests []Request) []Response {
	t.Helper()
	return g.python(t, v, "Python", nil, requests)
}

// PythonWithEnv is Python for requests the Python plane answers under another
// configuration (Venue.ServePythonWithEnv: each entry KEY=value, later entries
// winning over the venue's own), for example the same probe with a secret
// unset. The extra environment is executed only while recording, and its key
// (pythonEnvKey, so no value is stored) is kept with each of the call's
// answers: a frozen run refuses a call that passes another extra environment
// than the recorded one, and an answer recorded before a golden kept that
// key. The test still puts what distinguishes a scenario into each request's
// Name, so a refusal names the scenario.
func (g *Golden) PythonWithEnv(t *testing.T, v *Venue, extra []string, requests []Request) []Response {
	t.Helper()
	if extra == nil {
		extra = []string{}
	}
	return g.python(t, v, "PythonWithEnv", extra, requests)
}

func (g *Golden) python(t *testing.T, v *Venue, call string, extra []string, requests []Request) []Response {
	t.Helper()
	// The key of the call's own environment: none for the venue's as it was built.
	callEnv, err := g.callEnvKey(extra)
	if err != nil {
		t.Fatal(err)
	}
	return g.answer(t, call, callEnv, requests,
		func() error { return g.recordingRootErr(v) },
		func() []Response {
			if extra == nil {
				return v.ServePython(t, requests)
			}
			return v.ServePythonWithEnv(t, extra, requests)
		},
		func() error { return g.frozenVenueErr(t, v) })
}

// Produce returns a Python producer's answers to requests, for an oracle whose
// Python side is not the api's HTTP plane: a program run over a corpus, a CLI
// verb, a function. While recording, live runs the requests from root, which
// must be the checkout PythonRoot verified, and the answers are recorded;
// frozen, they are read from the file and live is never called. A request
// names what was asked (ProgramRequest builds one for a Python program) and a
// response what came back: Status the exit code, Body the stdout, Headers any
// other stream. The lifecycle and the accounting are Python's: every answer
// must then be compared by Diff or declared inspected (Consumed). A request
// carries no headers: they are keyed case-folded, as HTTP headers are, and a
// producer's environment names are case-sensitive, so the environment belongs
// in the path (ProgramRequest), where it compares exactly.
func (g *Golden) Produce(t *testing.T, root string, requests []Request, live func(root string, requests []Request) []Response) []Response {
	t.Helper()
	if err := producerRequestsErr(requests); err != nil {
		t.Fatal(err)
	}
	return g.answer(t, "Produce", "", requests,
		func() error { return g.producerRootErr(root) },
		func() []Response { return live(root, requests) },
		func() error { return liveVenueErr(t, "golden "+g.spec.Path+"'s frozen answers") })
}

// producerRequestsErr is an error when a producer request carries headers.
func producerRequestsErr(requests []Request) error {
	for _, request := range requests {
		if len(request.Headers) > 0 {
			return fmt.Errorf("Produce: request %q carries headers: a producer's environment belongs in its path (ProgramRequest), where its names compare exactly", request.Name)
		}
	}
	return nil
}

// producerRootErr is an error unless root is the checkout PythonRoot verified.
func (g *Golden) producerRootErr(root string) error {
	if g.verifiedRoot == "" {
		return fmt.Errorf("recording: run the Python producer only from the root golden.PythonRoot returned (it verifies the checkout is clean and at the pinned build); it was never called")
	}
	if !sameDirectory(root, g.verifiedRoot) {
		return fmt.Errorf("recording: the Python producer runs from %s, not from the verified checkout %s: pass the root golden.PythonRoot returned", root, g.verifiedRoot)
	}
	return nil
}

// ProgramRequest is the request of one run of a Python program: its name, the
// program text's digest (a changed program cannot replay the answers of
// another), its stdin, and the environment entries that shape its answers. The
// path holds the program's digest and the environment's (envDigest), so a
// changed program or environment is another request.
func ProgramRequest(name, program string, stdin []byte, env map[string]string) Request {
	sum := sha256.Sum256([]byte(program))
	body := base64.StdEncoding.EncodeToString(stdin)
	return Request{Name: name, Method: "PYTHON", Path: "program sha256 " + hex.EncodeToString(sum[:]) + " env sha256 " + envDigest(env), Body: &body}
}

// envDigest is a digest of environment entries with each name as written:
// environment names are case-sensitive (PYTHONHASHSEED is not pythonhashseed).
func envDigest(env map[string]string) string {
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		fmt.Fprintf(hash, "%d:%s=%d:%s\n", len(name), name, len(env[name]), env[name])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// packedPrefix marks a response body stored compressed (PackBody).
const packedPrefix = "gzip+base64:"

// PackBody is raw as a golden stores a large producer output: gzip (no name,
// no time, so the same output packs to the same text), then base64, behind
// packedPrefix. A frozen answer holds exactly the recorded bytes: UnpackBody
// returns them unchanged, and nothing is decoded on the way.
func PackBody(raw []byte) string {
	var packed bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&packed, gzip.BestCompression)
	_, _ = writer.Write(raw)
	_ = writer.Close()
	return packedPrefix + base64.StdEncoding.EncodeToString(packed.Bytes())
}

// UnpackBody returns the bytes PackBody packed; a body that is not packed, or
// does not unpack, fails the test.
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

// answer is the one path every Python answer takes: recorded from live while
// recording (after rootErr), read from the frozen file otherwise (after
// frozenErr, when there is one), then bound to its request.
func (g *Golden) answer(t *testing.T, call, callEnv string, requests []Request, rootErr func() error, live func() []Response, frozenErr func() error) []Response {
	t.Helper()
	g.step(t, call, stateOpen, statePython, stateDiffed)
	var answers []Response
	if g.recording {
		if err := rootErr(); err != nil {
			t.Fatal(err)
		}
		answers = live()
		if len(answers) != len(requests) {
			t.Fatalf("golden %s: the Python producer answered %d of %d requests", g.spec.Path, len(answers), len(requests))
		}
		if g.spec.RawSink != nil {
			for index := range answers {
				g.spec.RawSink(requests[index], clone(answers[index]))
			}
		}
		for index := range answers {
			projected, err := g.projectResponseAt(requests[index].Name, answers[index])
			if err != nil {
				t.Fatalf("golden %s: %v", g.spec.Path, err)
			}
			answers[index] = projected
		}
		for index, request := range requests {
			entry := g.keyOf(request)
			entry.CallEnv = callEnv
			entry.Status, entry.Headers, entry.Body = answers[index].Status, answers[index].Headers, answers[index].Body
			if err := g.sameKeySameAnswerErr(entry); err != nil {
				t.Fatal(err)
			}
			g.recorded.Requests = append(g.recorded.Requests, entry)
		}
	} else {
		if frozenErr != nil {
			if err := frozenErr(); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		if answers, err = g.frozenAnswers(requests, callEnv); err != nil {
			t.Fatal(err)
		}
		markFrozenTree(t, "the frozen answers of golden "+g.spec.Path)
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

// frozenVenueErr is an error when a frozen golden's answers are asked for
// through a venue built with Python: its test forgot Options.Golden, so it
// still needs the Python substrate the golden exists to retire.
func (g *Golden) frozenVenueErr(t *testing.T, v *Venue) error {
	if v != nil && !v.frozen {
		return fmt.Errorf("golden %s is frozen but its venue was built with Python: pass the golden to venueoracle.Start (Options.Golden) so a frozen run needs no Python", g.spec.Path)
	}
	if v == nil {
		// No venue named: the test may still have started a live one.
		return liveVenueErr(t, "golden "+g.spec.Path+"'s frozen answers")
	}
	return nil
}

func (g *Golden) frozenAnswers(requests []Request, callEnv string) ([]Response, error) {
	if g.served+len(requests) > len(g.loaded.Requests) {
		return nil, fmt.Errorf("golden %s holds %d answers, %d already served, the test asks for %d more; regenerate: %s", g.spec.Path, len(g.loaded.Requests), g.served, len(requests), g.spec.Recipe)
	}
	out := make([]Response, len(requests))
	for index, request := range requests {
		want := g.keyOf(request)
		got := g.loaded.Requests[g.served+index]
		if got.Name != want.Name || got.Method != want.Method || got.Path != want.Path || got.BodySHA256 != want.BodySHA256 || got.HeadersSHA256 != want.HeadersSHA256 {
			return nil, fmt.Errorf("golden %s request %d is %q %s %s (body %s.., headers %s..); the test sends %q %s %s (body %s.., headers %s..); regenerate: %s",
				g.spec.Path, g.served+index, got.Name, got.Method, got.Path, short(got.BodySHA256), short(got.HeadersSHA256), want.Name, want.Method, want.Path, short(want.BodySHA256), short(want.HeadersSHA256), g.spec.Recipe)
		}
		switch {
		case got.CallEnv == callEnv:
		case got.CallEnv == "":
			return nil, fmt.Errorf("golden %s request %d (%q) was recorded before a golden kept the key of a call's extra environment (PythonWithEnv), so it cannot show that its answer was given under the extra environment this test passes; record the golden again (for billingvenue's TestVenueOracleBillingEdge that is CHAOS-7408): %s",
				g.spec.Path, g.served+index, got.Name, g.spec.Recipe)
		default:
			return nil, fmt.Errorf("golden %s request %d (%q) was recorded under another extra environment than this call passes (key %s, the golden's %s): a changed, added or removed entry changes what the real Python api answers; regenerate: %s",
				g.spec.Path, g.served+index, got.Name, keyHead(callEnv), keyHead(got.CallEnv), g.spec.Recipe)
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
		value, err := g.project("", "rows "+name, source())
		if err != nil {
			t.Fatalf("golden %s: row comparison %q: %v", g.spec.Path, name, err)
		}
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
		g.use.finish()
		return
	}
	if err := g.unusedAnswers(); err != nil {
		t.Fatal(err)
	}
	if err := g.unusedRows(); err != nil {
		t.Fatal(err)
	}
	if err := g.pythonEnvUnboundErr(); err != nil {
		t.Fatal(err)
	}
	WriteGoOnlyProof(t, "Go against the Python plane's answers executed on build "+g.spec.PythonBuild+" (frozen golden "+filepath.Base(g.spec.Path)+")")
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
	g.recorded.Header.Blanked = g.blankedHeader()
	raw, err := json.MarshalIndent(g.recorded, "", "  ")
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	if err := tokenShapeErr(g.spec.Path, raw); err != nil {
		return "", err
	}
	candidate := g.spec.Path + GoldenCandidateSuffix
	if err := os.MkdirAll(filepath.Dir(candidate), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(candidate, raw, 0o644); err != nil {
		return "", err
	}
	if err := g.writeScrubSidecar(candidate); err != nil {
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
func (g *Golden) identityOf(request Request) string {
	return identityOfKey(g.keyOf(request))
}

func requestIdentity(request Request) string { return identityOfKey(requestKey(request)) }

func identityOfKey(key goldenRequest) string {
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

// bytecodeEnvErr is an error unless the environment stops the Python plane
// from writing a bytecode cache into the pinned checkout during the recording.
func bytecodeEnvErr() error {
	if os.Getenv("PYTHONDONTWRITEBYTECODE") != "1" {
		return fmt.Errorf("recording needs PYTHONDONTWRITEBYTECODE=1 so the Python plane leaves no bytecode cache in the pinned checkout (the record verb sets it)")
	}
	return nil
}
