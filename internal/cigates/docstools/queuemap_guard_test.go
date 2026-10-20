package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// queuemapSeed copies the recorded gen-queue-mapping-ok inputs into a fresh work directory and makes it the cwd.
func queuemapSeed(t *testing.T) string {
	t.Helper()
	work := t.TempDir()
	in, err := filepath.Abs(filepath.Join("testdata", "parity", "genqm-ok", "in"))
	if err != nil {
		t.Fatal(err)
	}
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			writeTree(t, filepath.Join(work, rel), mustRead(t, path), 0o644)
		} else if err == nil && info.Mode()&os.ModeSymlink != 0 {
			rel, _ := filepath.Rel(in, path)
			copySymlink(t, path, filepath.Join(work, rel))
		}
		return nil
	})
	t.Chdir(work)
	return work
}

const queuemapDoc = "docs/contribute/architecture/go-worker-runtime.md"

func TestQueuemapGenerateIsIdempotentAndDriftCheckPasses(t *testing.T) {
	queuemapSeed(t)
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("first generate rc %d: %s", code, stderr)
	}
	first := mustRead(t, queuemapDoc)
	if code, _, stderr := runDocs("gen-queue-mapping"); code != 0 {
		t.Fatalf("second generate rc %d: %s", code, stderr)
	}
	if second := mustRead(t, queuemapDoc); second != first {
		t.Fatalf("generator not idempotent")
	}
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 0 || !strings.Contains(out, "passed") {
		t.Fatalf("clean tree: rc %d out %q", code, out)
	}
}

func TestQueuemapDriftDetectedAndRepaired(t *testing.T) {
	queuemapSeed(t)
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 1 || !strings.Contains(out, "is stale") {
		t.Fatalf("stale doc: rc %d out %q", code, out)
	}
	runDocs("gen-queue-mapping")
	// a producer change makes the published block stale again
	reg := mustRead(t, "contracts/jobs/v1/registry.json")
	writeTree(t, "contracts/jobs/v1/registry.json", strings.Replace(reg, "7200", "7201", 1), 0o644)
	if code, out, _ := runDocs("check-queue-mapping-drift"); code != 1 || !strings.Contains(out, "is stale") {
		t.Fatalf("producer drift: rc %d out %q", code, out)
	}
	runDocs("gen-queue-mapping")
	if code, _, _ := runDocs("check-queue-mapping-drift"); code != 0 {
		t.Fatalf("regenerated doc still drifts")
	}
}

func TestQueuemapRefusals(t *testing.T) {
	cases := []struct {
		name, file, from, to, want string
	}{
		{"missing markers", queuemapDoc, "BEGIN GENERATED QUEUE MAP", "BEGIN X", "markers"},
		{"unknown process", "deploy/go-workers/deployment.json", `"name": "reconciler"`, `"name": "reconciler2"`, "process set"},
		{"kind without migration row", "contracts/jobs/v1/migration-state.json", `"kind": "webhooks.w"`, `"kind": "webhooks.z"`, "kinds"},
		{"queue without curated entry", "deploy/go-workers/deployment.json", `"sync_provider"`, `"sync_other"`, "no qmCorrespondence entry"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queuemapSeed(t)
			writeTree(t, c.file, strings.Replace(mustRead(t, c.file), c.from, c.to, 1), 0o644)
			code, _, stderr := runDocs("gen-queue-mapping")
			if code != 1 || !strings.Contains(stderr, c.want) {
				t.Fatalf("generate: rc %d stderr %q", code, stderr)
			}
			code, out, _ := runDocs("check-queue-mapping-drift")
			if code != 1 || !strings.Contains(out, "ERROR:") {
				t.Fatalf("drift: rc %d out %q", code, out)
			}
		})
	}
}

// A kind in registry.json and migration-state.json that no deployment.json process serves would be left out of the
// table without a word; both verbs refuse and name it. (The old Python script passed this input.)
func TestQueuemapRefusesKindServedByNoWorker(t *testing.T) {
	queuemapSeed(t)
	runDocs("gen-queue-mapping")
	dep := mustRead(t, "deploy/go-workers/deployment.json")
	if !strings.Contains(dep, "\"reports.run\"") {
		t.Fatal("seed lacks reports.run")
	}
	writeTree(t, "deploy/go-workers/deployment.json", strings.Replace(dep, "\"reports.run\",", "", 1), 0o644)
	before := mustRead(t, queuemapDoc)
	code, _, stderr := runDocs("gen-queue-mapping")
	if code != 1 || !strings.Contains(stderr, "['reports.run']") || !strings.Contains(stderr, "no process job_kinds") {
		t.Fatalf("gen: rc %d stderr %q", code, stderr)
	}
	if mustRead(t, queuemapDoc) != before {
		t.Fatalf("refused run wrote the page")
	}
	code, out, _ := runDocs("check-queue-mapping-drift")
	if code != 1 || !strings.Contains(out, "ERROR: generator failed to render") || !strings.Contains(out, "['reports.run']") {
		t.Fatalf("drift: rc %d out %q", code, out)
	}
}

// Python read the page in text mode (universal newlines): a CRLF page is the same page, and the generator writes LF.
func TestQueuemapCRLFPage(t *testing.T) {
	queuemapSeed(t)
	runDocs("gen-queue-mapping")
	lf := mustRead(t, queuemapDoc)
	for _, nl := range []string{"\r\n", "\r"} {
		writeTree(t, queuemapDoc, strings.ReplaceAll(lf, "\n", nl), 0o644)
		if code, out, _ := runDocs("check-queue-mapping-drift"); code != 0 {
			t.Fatalf("%q page: rc %d out %q", nl, code, out)
		}
		if code, _, _ := runDocs("gen-queue-mapping"); code != 0 || mustRead(t, queuemapDoc) != lf {
			t.Fatalf("%q page: generator did not write the LF page", nl)
		}
	}
}

func TestQueuemapRefusesInvalidUTF8(t *testing.T) {
	queuemapSeed(t)
	reg := mustRead(t, "contracts/jobs/v1/registry.json")
	writeTree(t, "contracts/jobs/v1/registry.json", reg+"\xff", 0o644)
	for _, verb := range []string{"gen-queue-mapping", "check-queue-mapping-drift"} {
		code, _, stderr := runDocs(verb)
		if code != 1 || !strings.HasPrefix(stderr, "UnicodeDecodeError: ") || !strings.HasSuffix(stderr, "registry.json: invalid UTF-8\n") || strings.Count(stderr, "\n") != 1 {
			t.Fatalf("%s: rc %d stderr %q", verb, code, stderr)
		}
	}
}

// A kind a process lists in job_kinds whose registry queue is not one of that process's queues is refused, naming the
// kind, the process and the queue. (Python generated the page without the kind and the drift check passed.)
func TestQueuemapRefusesKindQueueMismatch(t *testing.T) {
	queuemapSeed(t)
	reg := mustRead(t, "contracts/jobs/v1/registry.json")
	i := strings.Index(reg, `"kind": "sync.s"`)
	if i < 0 {
		t.Fatal("seed has no sync.s")
	}
	j := i + strings.Index(reg[i:], `"queue": "sync"`)
	writeTree(t, "contracts/jobs/v1/registry.json", reg[:j]+`"queue": "metrics"`+reg[j+len(`"queue": "sync"`):], 0o644)
	for _, verb := range []string{"gen-queue-mapping", "check-queue-mapping-drift"} {
		code, out, stderr := runDocs(verb)
		all := out + stderr
		if code != 1 || !strings.Contains(all, "'sync.s'") || !strings.Contains(all, "'sync'") || !strings.Contains(all, "'metrics'") {
			t.Errorf("%s: rc %d out %q stderr %q", verb, code, out, stderr)
		}
		// Failure contract: gen prints the reason on stderr and nothing on stdout; check prints its ERROR line.
		if verb == "gen-queue-mapping" && (out != "" || stderr == "") {
			t.Errorf("gen: stdout must be empty and the reason on stderr: out %q stderr %q", out, stderr)
		}
		if verb == "check-queue-mapping-drift" && !strings.HasPrefix(out, "ERROR: generator failed to render") {
			t.Errorf("check: want the usual ERROR line on stdout, got %q", out)
		}
	}
}

// Hand-written refusals (no recorded pair: Python rendered these, or stopped on a different error; the Go refusal
// is stricter by decision, see testdata/README.md "Stricter than Python"). Each is on the coverage.c record, which is
// served and rendered, so every field is read. Python's output for each is in the README table.
func TestQueuemapRefusesNullAndNonIntegerProducerValues(t *testing.T) {
	const regBlock = "\"kind\": \"coverage.c\",\n      \"queue\": \"coverage\",\n      \"timeout_seconds\": 60,\n      \"max_attempts\": 1"
	const migBlock = "\"kind\": \"coverage.c\",\n      \"state\": \"go_default\",\n      \"route\": \"river\",\n      \"rollback_route\": \"none\""
	reg := func(kind, queue, timeout, attempts string) string {
		return "\"kind\": " + kind + ",\n      \"queue\": " + queue + ",\n      \"timeout_seconds\": " + timeout + ",\n      \"max_attempts\": " + attempts
	}
	mig := func(state, route, rollback string) string {
		return "\"kind\": \"coverage.c\",\n      \"state\": " + state + ",\n      \"route\": " + route + ",\n      \"rollback_route\": " + rollback
	}
	const registry, migration = "contracts/jobs/v1/registry.json", "contracts/jobs/v1/migration-state.json"
	k, q := `"coverage.c"`, `"coverage"`
	cases := []struct{ name, file, from, to string }{
		{"null kind", registry, regBlock, reg("null", q, "60", "1")},
		{"null queue", registry, regBlock, reg(k, "null", "60", "1")},
		{"null timeout_seconds", registry, regBlock, reg(k, q, "null", "1")},
		{"null max_attempts", registry, regBlock, reg(k, q, "60", "null")},
		{"null state", migration, migBlock, mig("null", `"river"`, `"none"`)},
		{"null route", migration, migBlock, mig(`"go_default"`, "null", `"none"`)},
		{"null rollback_route", migration, migBlock, mig(`"go_default"`, `"river"`, "null")},
		{"float timeout_seconds", registry, regBlock, reg(k, q, "30.0", "1")},
		{"float max_attempts", registry, regBlock, reg(k, q, "60", "30.0")},
		{"exponent timeout_seconds", registry, regBlock, reg(k, q, "6e1", "1")},
		{"true timeout_seconds", registry, regBlock, reg(k, q, "true", "1")},
		{"true max_attempts", registry, regBlock, reg(k, q, "60", "true")},
		{"string timeout_seconds", registry, regBlock, reg(k, q, `"30"`, "1")},
		{"string max_attempts", registry, regBlock, reg(k, q, "60", `"30"`)},
		{"number state", migration, migBlock, mig("7", `"river"`, `"none"`)},
		{"NaN timeout_seconds", registry, regBlock, reg(k, q, "NaN", "1")},
		{"NaN max_attempts", registry, regBlock, reg(k, q, "60", "NaN")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			queuemapSeed(t)
			text := mustRead(t, c.file)
			if !strings.Contains(text, c.from) {
				t.Fatal("seed lacks the coverage.c block")
			}
			writeTree(t, c.file, strings.Replace(text, c.from, c.to, 1), 0o644)
			before := mustRead(t, queuemapDoc)
			code, out, stderr := runDocs("gen-queue-mapping")
			if code != 1 || out != "" || stderr == "" {
				t.Fatalf("generate: rc %d stdout %q stderr %q", code, out, stderr)
			}
			if mustRead(t, queuemapDoc) != before {
				t.Fatalf("refused run wrote the page")
			}
			if strings.HasPrefix(c.name, "NaN") {
				if !strings.HasPrefix(stderr, "json.decoder.JSONDecodeError: ") {
					t.Fatalf("generate stderr %q", stderr)
				}
			} else if !strings.Contains(stderr, "producer job record must be") {
				t.Fatalf("generate stderr %q", stderr)
			}
			if code, _, _ := runDocs("check-queue-mapping-drift"); code != 1 {
				t.Fatalf("drift: rc %d", code)
			}
		})
	}
}

// encoding/json matches a struct field to a key without case or Unicode-fold exactness; Python's dict lookup is
// exact, so "NAME" is a KeyError on deployment.json too (the registry and migration-state keys are recorded pairs).
func TestQueuemapDeploymentKeysAreExact(t *testing.T) {
	queuemapSeed(t)
	dep := mustRead(t, "deploy/go-workers/deployment.json")
	writeTree(t, "deploy/go-workers/deployment.json", strings.Replace(dep, `"name": "heavy"`, `"NAME": "heavy"`, 1), 0o644)
	code, _, stderr := runDocs("gen-queue-mapping")
	if code != 1 || stderr != "KeyError: 'name'\n" {
		t.Fatalf("rc %d stderr %q", code, stderr)
	}
}
