package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The frozen Python plane (CHAOS-6817).
//
// A venue oracle that drives a Python REST route loses its producer the day
// that route's Python body is deleted (the route stays mounted and answers the
// go-served refusal). The oracle is not dropped: the Python plane's answers are
// RECORDED once, by executing the last Python-bearing build, and the test
// compares Go against the recording. The recording is a rot guard, not a
// freshness check, exactly like the checked-in Alembic goldens: it is only
// rewritten by running the oracle with DHO_FROZEN_PYTHON_RECORD=1 on a tree
// whose Python bodies still exist, and it names that tree's sha.

const (
	// FrozenRecordEnv, set to 1, makes the frozen oracles run the live Python
	// plane and rewrite their goldens instead of comparing against them.
	FrozenRecordEnv = "DHO_FROZEN_PYTHON_RECORD"
	// FrozenShaEnv names the Python-bearing build a recording is made on; the
	// golden carries it.
	FrozenShaEnv = "DHO_FROZEN_PYTHON_SHA"
)

// FrozenRequestKey is what a golden pins about each request: its name and
// method (paths and bodies carry per-run values).
type FrozenRequestKey struct {
	Name   string `json:"name"`
	Method string `json:"method"`
}

// FrozenGolden is the checked-in recording.
type FrozenGolden struct {
	// RecordedAt is the sha of the Python-bearing build the recording was
	// produced on.
	RecordedAt string `json:"recorded_at_sha"`
	Recipe     string `json:"recipe"`
	// Requests are the request names, in order, whose answers Responses holds.
	Requests  []FrozenRequestKey `json:"requests"`
	Responses []Response         `json:"responses"`
	// Texts are the other Python-side measurements the oracle compared (rows
	// after writes, the Stripe calls a plane made), by name.
	Texts map[string]string `json:"texts"`
}

// Frozen is one oracle's golden, in record or replay mode.
type Frozen struct {
	t         *testing.T
	path      string
	recording bool
	golden    FrozenGolden
	names     []string
}

// OpenFrozen opens the golden at path (relative to the test's package
// directory). In record mode it starts empty and writes the file when the test
// ends without failing; otherwise it loads the file and fails the test when it
// is missing or unreadable (a measurement that did not happen must fail).
func OpenFrozen(t *testing.T, path string) *Frozen {
	t.Helper()
	frozen := &Frozen{t: t, path: path, recording: os.Getenv(FrozenRecordEnv) == "1"}
	if frozen.recording {
		sha := strings.TrimSpace(os.Getenv(FrozenShaEnv))
		if sha == "" {
			t.Fatalf("%s=1 needs %s: the sha of the Python-bearing build the recording is made on", FrozenRecordEnv, FrozenShaEnv)
		}
		frozen.golden = FrozenGolden{
			RecordedAt: sha,
			Recipe: fmt.Sprintf("On a tree whose Python route bodies still exist (the recorded sha), from the package directory: "+
				"%s=1 %s=<sha> DEV_HEALTH_LIVE_PYTHON_ORACLES=1 go test -tags=integration -count=1 -run '^%s$' . "+
				"(the venue oracle runs the live Python plane and rewrites this file).", FrozenRecordEnv, FrozenShaEnv, t.Name()),
			Texts: map[string]string{},
		}
		t.Cleanup(func() {
			if !t.Failed() {
				frozen.write()
			}
		})
		return frozen
	}
	golden, err := loadFrozen(path)
	if err != nil {
		t.Fatalf("%v\nrecord it: %s=1 %s=<sha of a Python-bearing build>", err, FrozenRecordEnv, FrozenShaEnv)
	}
	frozen.golden = golden
	return frozen
}

// loadFrozen reads and validates a golden.
func loadFrozen(path string) (FrozenGolden, error) {
	var golden FrozenGolden
	raw, err := os.ReadFile(path)
	if err != nil {
		return golden, fmt.Errorf("the frozen Python recording %s is unreadable: %w", path, err)
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		return golden, fmt.Errorf("the frozen Python recording %s is not valid: %w", path, err)
	}
	if golden.RecordedAt == "" || len(golden.Responses) == 0 || len(golden.Responses) != len(golden.Requests) {
		return golden, fmt.Errorf("the frozen Python recording %s names no build, or its requests and responses disagree", path)
	}
	return golden, nil
}

// Recording reports whether this run records (the live Python plane is used).
func (f *Frozen) Recording() bool { return f.recording }

// Responses returns the Python plane's answers to requests, in order: recorded
// from live() in record mode (bodies passed through normalize first, so the
// golden holds no per-run value), read from the golden in replay mode, where
// the request set must be the recorded one.
func (f *Frozen) Responses(requests []Request, normalize func(string) string, live func() []Response) []Response {
	f.t.Helper()
	keys := make([]FrozenRequestKey, len(requests))
	for index, request := range requests {
		keys[index] = FrozenRequestKey{Name: request.Name, Method: request.Method}
	}
	if f.recording {
		answers := live()
		if len(answers) != len(requests) {
			f.t.Fatalf("frozen: python answered %d of %d requests", len(answers), len(requests))
		}
		recorded := make([]Response, len(answers))
		for index, answer := range answers {
			recorded[index] = clone(answer)
			if normalize != nil {
				recorded[index].Body = normalize(recorded[index].Body)
			}
		}
		f.golden.Requests, f.golden.Responses = keys, recorded
		return recorded
	}
	out, err := f.golden.replay(f.path, keys)
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

// replay is the recorded answers for keys, or why the recording no longer
// matches the test.
func (g FrozenGolden) replay(path string, keys []FrozenRequestKey) ([]Response, error) {
	if len(g.Requests) != len(keys) {
		return nil, fmt.Errorf("frozen %s records %d requests, the test sends %d: re-record it on a Python-bearing build", path, len(g.Requests), len(keys))
	}
	for index, key := range keys {
		if g.Requests[index] != key {
			return nil, fmt.Errorf("frozen %s request %d is %+v, the test sends %+v: re-record it on a Python-bearing build", path, index, g.Requests[index], key)
		}
	}
	out := make([]Response, len(g.Responses))
	for index, response := range g.Responses {
		out[index] = clone(response)
	}
	return out, nil
}

// Text returns one named Python-side measurement: live() in record mode
// (stored), the recorded text in replay mode (a missing name fails).
func (f *Frozen) Text(name string, live func() string) string {
	f.t.Helper()
	if f.recording {
		text := live()
		f.golden.Texts[name] = text
		return text
	}
	text, err := f.golden.text(f.path, name)
	if err != nil {
		f.t.Fatal(err)
	}
	return text
}

// text is the recorded measurement called name, or why there is none.
func (g FrozenGolden) text(path, name string) (string, error) {
	text, ok := g.Texts[name]
	if !ok {
		return "", fmt.Errorf("frozen %s has no recorded text %q: re-record it on a Python-bearing build", path, name)
	}
	return text, nil
}

func (f *Frozen) write() {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	out, err := json.MarshalIndent(f.golden, "", " ")
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(f.path, append(out, '\n'), 0o644); err != nil {
		f.t.Fatal(err)
	}
	digest := sha256.Sum256(out)
	f.t.Logf("recorded %s (%d responses, %d texts, sha256 %s)", f.path, len(f.golden.Responses), len(f.golden.Texts), hex.EncodeToString(digest[:]))
}

// DiffRecorded is Diff against a recording: the same comparison of each Go
// answer with the matching Python answer, but it writes a Go-only proof naming
// the recording (reason) instead of a both-planes proof, so the recording never
// reads as a live parity comparison. The venue-oracles verb counts it apart.
func DiffRecorded(t *testing.T, goBase string, requests []Request, recorded []Response, options DiffOptions, reason string) string {
	t.Helper()
	receipt := diffPlanes(t, goBase, requests, recorded, options)
	WriteGoOnlyProof(t, reason)
	return receipt
}
