package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
//   - Recording (DHO_VENUE_GOLDEN_UPDATE=1): the test runs the Python plane
//     from the checkout DHO_VENUE_GOLDEN_PYTHON_ROOT names, which must be a
//     clean git worktree whose HEAD is exactly the build the test pins
//     (GoldenSpec.PythonBuild), serves the requests, and writes the file. The
//     new digest is printed for the test to pin.
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
}

// The environment variables that switch a test to recording.
const (
	goldenUpdateEnv     = "DHO_VENUE_GOLDEN_UPDATE"
	goldenPythonRootEnv = "DHO_VENUE_GOLDEN_PYTHON_ROOT"
)

type goldenFile struct {
	Header   goldenHeader          `json:"header"`
	Requests []goldenRequest       `json:"requests"`
	Rows     map[string]goldenRows `json:"rows,omitempty"`
}

type goldenHeader struct {
	Test        string `json:"test"`
	PythonBuild string `json:"python_build"`
	Recipe      string `json:"recipe"`
}

type goldenRequest struct {
	Name       string            `json:"name"`
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	BodySHA256 string            `json:"body_sha256"`
	Status     int               `json:"status"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
}

type goldenRows struct {
	Rows string `json:"rows"`
}

var buildPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// OpenGolden opens spec for the running test. It never returns a golden the
// test could not trust: a frozen file must exist, name the pinned build, and
// hash to the pinned digest.
func OpenGolden(t *testing.T, spec GoldenSpec) *Golden {
	t.Helper()
	g, err := openGolden(spec, t.Name(), os.Getenv(goldenUpdateEnv) == "1")
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func openGolden(spec GoldenSpec, test string, recording bool) (*Golden, error) {
	if spec.Path == "" || spec.Recipe == "" || !buildPattern.MatchString(spec.PythonBuild) {
		return nil, fmt.Errorf("venueoracle: a GoldenSpec needs a path, a recipe and the 40-hex Python build the answers were executed on: %+v", spec)
	}
	g := &Golden{spec: spec, recording: recording}
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
	if g.loaded.Header.PythonBuild != spec.PythonBuild {
		return nil, fmt.Errorf("golden %s was executed on build %s, the test names %s; regenerate: %s", spec.Path, g.loaded.Header.PythonBuild, spec.PythonBuild, spec.Recipe)
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
	if err := verifyPinnedCheckout(pinned, g.spec.PythonBuild); err != nil {
		t.Fatalf("recording: %v", err)
	}
	return pinned
}

func verifyPinnedCheckout(dir, build string) error {
	head, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return fmt.Errorf("%s is not a git checkout: %w", dir, err)
	}
	if got := strings.TrimSpace(string(head)); got != build {
		return fmt.Errorf("%s is at %s, the test pins %s", dir, got, build)
	}
	status, err := exec.Command("git", "-C", dir, "status", "--porcelain", "--untracked-files=no").Output()
	if err != nil {
		return fmt.Errorf("git status in %s: %w", dir, err)
	}
	if strings.TrimSpace(string(status)) != "" {
		return fmt.Errorf("%s has uncommitted changes: a golden is executed on a clean build", dir)
	}
	return nil
}

func requestKey(request Request) goldenRequest {
	sum := sha256.Sum256([]byte(""))
	if request.Body != nil {
		sum = sha256.Sum256([]byte(*request.Body))
	}
	return goldenRequest{Name: request.Name, Method: request.Method, Path: request.Path, BodySHA256: hex.EncodeToString(sum[:])}
}

// Python returns the Python plane's answers to requests: served for real
// while recording, read from the frozen file otherwise. The request list must
// be the one the file was recorded with (name, method, path, body), so a test
// whose requests drifted fails naming the difference instead of comparing
// against another test's truth.
func (g *Golden) Python(t *testing.T, v *Venue, requests []Request) []Response {
	t.Helper()
	if g.recording {
		answers := v.ServePython(t, requests)
		for index, request := range requests {
			entry := requestKey(request)
			entry.Status, entry.Headers, entry.Body = answers[index].Status, answers[index].Headers, answers[index].Body
			g.recorded.Requests = append(g.recorded.Requests, entry)
		}
		return answers
	}
	answers, err := g.frozenAnswers(requests)
	if err != nil {
		t.Fatal(err)
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
		if got.Name != want.Name || got.Method != want.Method || got.Path != want.Path || got.BodySHA256 != want.BodySHA256 {
			return nil, fmt.Errorf("golden %s request %d is %q %s %s (body %s..); the test sends %q %s %s (body %s..); regenerate: %s",
				g.spec.Path, g.served+index, got.Name, got.Method, got.Path, got.BodySHA256[:8], want.Name, want.Method, want.Path, want.BodySHA256[:8], g.spec.Recipe)
		}
		out[index] = Response{Status: got.Status, Headers: got.Headers, Body: got.Body}
	}
	g.served += len(requests)
	return out, nil
}

// Rows is the Python plane's value of a row comparison: what source computes
// (the Python plane's database after it served) while recording, the recorded
// text otherwise. name identifies the comparison within the file.
func (g *Golden) Rows(t *testing.T, name string, source func() string) string {
	t.Helper()
	if g.recording {
		value := source()
		if _, dup := g.recorded.Rows[name]; dup {
			t.Fatalf("golden row comparison %q is recorded twice", name)
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
	return entry.Rows, nil
}

// Finish ends the test's use of the golden. Recording, it writes the file and
// prints the digest to pin, and fails the test so a recording run is never
// mistaken for a proof. Frozen, it writes the Go-only proof naming the build
// the Python truth was executed on.
func (g *Golden) Finish(t *testing.T) {
	t.Helper()
	if g.recording {
		if len(g.recorded.Requests) == 0 {
			t.Fatalf("recording %s: no request was served through the golden", g.spec.Path)
		}
		raw, err := json.MarshalIndent(g.recorded, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, '\n')
		if err := os.MkdirAll(filepath.Dir(g.spec.Path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(g.spec.Path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		t.Fatalf("recorded %s on build %s: pin SHA256 %s in the test and re-run without %s (a recording run is not a proof)",
			g.spec.Path, g.spec.PythonBuild, hex.EncodeToString(sum[:]), goldenUpdateEnv)
	}
	if err := g.unusedAnswers(); err != nil {
		t.Fatal(err)
	}
	WriteGoOnlyProof(t, "Go against the Python plane's answers executed on build "+g.spec.PythonBuild+" (frozen golden "+filepath.Base(g.spec.Path)+")")
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

// unusedAnswers is an error when the frozen file holds answers the test never
// asked for: an unused answer is a comparison that no longer happens.
func (g *Golden) unusedAnswers() error {
	if g.served != len(g.loaded.Requests) {
		return fmt.Errorf("golden %s holds %d answers but the test used %d: an unused frozen answer is a comparison that no longer happens; regenerate: %s", g.spec.Path, len(g.loaded.Requests), g.served, g.spec.Recipe)
	}
	return nil
}
