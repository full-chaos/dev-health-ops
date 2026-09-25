package pushcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// The offline push verbs are compared with the real `dev-hops push` verbs over one
// corpus: every record kind's sample, single-field mutations of every sample's
// payload and envelope, malformed JSON, unreadable and oversized files, the export
// stub, and argument shapes. Per case: the exit code, stdout, and (where Python's
// text is its own, not argparse's usage text) stderr.

type pushCase struct {
	Name  string   `json:"name"`
	Args  []string `json:"args"`
	Stdin string   `json:"stdin,omitempty"`
	// File is written to <dir>/<FileName> before the run; the argument list names it
	// as {file}.
	File     *string `json:"file"`
	FileName string  `json:"fileName,omitempty"`
	// Repeat, when set, is the content of the file (or stdin, with Stdin empty)
	// as Text repeated Count times: an oversized payload is a spec, not megabytes
	// of golden.
	Repeat *repeat `json:"repeat,omitempty"`
}

type repeat struct {
	Text  string `json:"text"`
	Count int    `json:"count"`
	// ToStdin sends the repeated text on stdin instead of writing the file.
	ToStdin bool `json:"toStdin,omitempty"`
}

type pushResult struct {
	Exit   int    `json:"exit"`
	Stdout string `json:"stdout"`
	// Stderr is kept only when the text is the verb's own (an argparse usage
	// message is compared by its exit code alone).
	Stderr string `json:"stderr,omitempty"`
}

const dirToken = "<dir>"

// pythonPushProgram runs every case through the real dev-hops entry point in one
// process: stdin is {"dir": ..., "cases": [...]}; the answers are printed as JSON.
const pythonPushProgram = `
import contextlib, io, json, os, sys
from dev_health_ops import cli

job = json.loads(sys.stdin.read())
out = []
for case in job["cases"]:
    rep = case.get("repeat")
    if rep:
        text = rep["text"] * rep["count"]
        if rep.get("toStdin"):
            case["stdin"] = text
        else:
            case["file"] = text
    if case.get("file") is not None:
        with open(os.path.join(job["dir"], case["fileName"]), "wb") as handle:
            handle.write(case["file"].encode("utf-8"))
    args = ["push"] + [a.replace("{file}", os.path.join(job["dir"], case.get("fileName", ""))) for a in case["args"]]
    stdin = io.TextIOWrapper(io.BytesIO(case.get("stdin", "").encode("utf-8")), encoding="utf-8")
    stdout, stderr = io.StringIO(), io.StringIO()
    real_stdin = sys.stdin
    sys.stdin = stdin
    code = 0
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                code = cli.main(args)
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
            except BaseException as exc:
                # A crash (traceback) is not a behaviour to port: it is reported as
                # exit 70 with the exception's type and text.
                code = 70
                print(type(exc).__name__ + ": " + str(exc)[:300], file=sys.stderr)
    finally:
        sys.stdin = real_stdin
    out.append({"exit": code, "stdout": stdout.getvalue(), "stderr": stderr.getvalue()})
print("RESULT" + json.dumps(out))
`

func normalize(text, dir string) string {
	return strings.ReplaceAll(text, dir, dirToken)
}

// keepStderr says whether a Python stderr is the verb's own text.
func keepStderr(stderr string) bool {
	return stderr != "" && !strings.Contains(stderr, "usage:") && !strings.Contains(stderr, "Traceback") && !strings.Contains(stderr, "WARNING") && !strings.Contains(stderr, "warning")
}

func runGo(t *testing.T, c pushCase, dir string) pushResult {
	t.Helper()
	if c.Repeat != nil {
		text := strings.Repeat(c.Repeat.Text, c.Repeat.Count)
		if c.Repeat.ToStdin {
			c.Stdin = text
		} else {
			c.File = &text
		}
	}
	if c.File != nil {
		if err := os.WriteFile(filepath.Join(dir, c.FileName), []byte(*c.File), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	args := make([]string, len(c.Args))
	for index, arg := range c.Args {
		args[index] = strings.ReplaceAll(arg, "{file}", filepath.Join(dir, c.FileName))
	}
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: append([]string{"push"}, args...), Stdin: strings.NewReader(c.Stdin), Stdout: &stdout, Stderr: &stderr,
	})
	result := pushResult{Exit: code, Stdout: normalize(stdout.String(), dir)}
	result.Stderr = normalize(stderr.String(), dir)
	return result
}

func envelope(records ...map[string]any) map[string]any {
	list := make([]any, len(records))
	for index, record := range records {
		list[index] = record
	}
	return map[string]any{
		"schemaVersion": schemaVersion, "idempotencyKey": "k-1",
		"source":  map[string]any{"type": "customer_push", "system": "github", "instance": "acme/api"},
		"window":  map[string]any{"startedAt": "2026-06-20T00:00:00Z", "endedAt": "2026-06-26T00:00:00Z"},
		"records": list,
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// sampleRecordMap is the sample record of a kind as a plain map.
func sampleRecordMap(t *testing.T, kind string) map[string]any {
	t.Helper()
	record, err := sampleRecord(kind)
	if err != nil {
		t.Fatal(err)
	}
	text, err := dumps(record, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func clonePlain(value map[string]any) map[string]any {
	raw, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

// corpus builds the cases; the order is fixed.
func corpus(t *testing.T) []pushCase {
	t.Helper()
	var cases []pushCase
	hasFile := false
	add := func(name string, args []string, file, stdin string) {
		hasFile = strings.Contains(strings.Join(args, "\x00"), "{file}")
		c := pushCase{Name: name, Args: args, Stdin: stdin}
		if hasFile {
			c.File, c.FileName = &file, fmt.Sprintf("case-%04d.json", len(cases))
		}
		cases = append(cases, c)
	}
	kinds := []string{}
	for _, entry := range mustKinds() {
		kinds = append(kinds, entry)
	}
	for _, kind := range kinds {
		bare := strings.TrimSuffix(kind, ".v1")
		add("sample "+kind, []string{"sample", "--kind", kind}, "", "")
		add("sample "+bare, []string{"sample", "--kind", bare}, "", "")
	}
	add("sample all", []string{"sample", "--all"}, "", "")
	add("sample nothing", []string{"sample"}, "", "")
	add("sample unknown kind", []string{"sample", "--kind", "nope"}, "", "")
	add("sample both", []string{"sample", "--kind", "commit", "--all"}, "", "")
	add("sample positional", []string{"sample", "--all", "extra"}, "", "")
	add("export github", []string{"export", "github"}, "", "")
	add("export gitlab repo", []string{"export", "gitlab", "--repo", "a/b"}, "", "")
	add("export repo first", []string{"export", "--repo", "a/b", "github"}, "", "")
	add("export nothing", []string{"export"}, "", "")

	// Valid samples, validated in every argument shape.
	for _, kind := range kinds {
		body := mustJSON(t, envelope(sampleRecordMap(t, kind)))
		add("validate "+kind, []string{"validate", "{file}"}, body, "")
		add("validate json "+kind, []string{"validate", "{file}", "--json"}, body, "")
		add("validate stdin "+kind, []string{"validate", "-", "--json"}, "", body)
	}
	all := []map[string]any{}
	for _, kind := range kinds {
		all = append(all, sampleRecordMap(t, kind))
	}
	allBody := mustJSON(t, envelope(all...))
	add("validate all", []string{"validate", "{file}"}, allBody, "")
	add("validate schema flag", []string{"validate", "--schema", schemaVersion, "{file}"}, allBody, "")
	add("validate other schema", []string{"validate", "--schema", "external-ingest.v2", "{file}"}, allBody, "")
	add("validate no payload", []string{"validate"}, "", "")
	add("validate two payloads", []string{"validate", "{file}", "{file}"}, allBody, "")

	// Single-field mutations of every kind's payload.
	replacements := []any{nil, 123, "x", []any{}, "", -1, 1.5, true, "2026-13-45T00:00:00Z", map[string]any{}}
	names := []string{"null", "int", "str", "list", "empty", "negative", "float", "true", "baddate", "object"}
	for _, kind := range kinds {
		record := sampleRecordMap(t, kind)
		payload := record["payload"].(map[string]any)
		keys := make([]string, 0, len(payload))
		for key := range payload {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			mutated := clonePlain(record)
			delete(mutated["payload"].(map[string]any), key)
			add(fmt.Sprintf("%s drop %s", kind, key), []string{"validate", "{file}"}, mustJSON(t, envelope(mutated)), "")
			for index, replacement := range replacements {
				mutated := clonePlain(record)
				mutated["payload"].(map[string]any)[key] = replacement
				args := []string{"validate", "{file}"}
				if (index+len(key))%5 == 0 {
					args = append(args, "--json")
				}
				add(fmt.Sprintf("%s %s=%s", kind, key, names[index]), args, mustJSON(t, envelope(mutated)), "")
			}
		}
		extra := clonePlain(record)
		extra["payload"].(map[string]any)["unexpectedField"] = 1
		add(kind+" extra field", []string{"validate", "{file}"}, mustJSON(t, envelope(extra)), "")
	}

	// Envelope-level mutations.
	base := envelope(sampleRecordMap(t, "commit.v1"))
	for _, key := range []string{"schemaVersion", "idempotencyKey", "source", "window", "records"} {
		mutated := clonePlain(base)
		delete(mutated, key)
		add("envelope drop "+key, []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		for index, replacement := range replacements {
			mutated := clonePlain(base)
			mutated[key] = replacement
			add("envelope "+key+"="+names[index], []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		}
	}
	for _, key := range []string{"type", "system", "instance", "entityFamily", "producer", "producerVersion"} {
		for index, replacement := range replacements {
			mutated := clonePlain(base)
			mutated["source"].(map[string]any)[key] = replacement
			add("source "+key+"="+names[index], []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		}
	}
	for _, key := range []string{"startedAt", "endedAt"} {
		for _, value := range []any{nil, 5, "yesterday", "2026-06-20", "2026-06-20T00:00:00", "2026-06-26T00:00:00+02:00"} {
			mutated := clonePlain(base)
			mutated["window"].(map[string]any)[key] = value
			add(fmt.Sprintf("window %s=%v", key, value), []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		}
	}
	swapped := clonePlain(base)
	swapped["window"] = map[string]any{"startedAt": "2026-06-26T00:00:00Z", "endedAt": "2026-06-20T00:00:00Z"}
	add("window reversed", []string{"validate", "{file}"}, mustJSON(t, swapped), "")
	for _, value := range []string{"external-ingest.v2", "", "EXTERNAL-INGEST.V1"} {
		mutated := clonePlain(base)
		mutated["schemaVersion"] = value
		add("schemaVersion "+value, []string{"validate", "{file}"}, mustJSON(t, mutated), "")
	}
	topExtra := clonePlain(base)
	topExtra["surprise"] = true
	add("envelope extra key", []string{"validate", "{file}"}, mustJSON(t, topExtra), "")
	sourceExtra := clonePlain(base)
	sourceExtra["source"].(map[string]any)["surprise"] = true
	add("source extra key", []string{"validate", "{file}"}, mustJSON(t, sourceExtra), "")
	for _, kind := range []string{"", "commit", "commit.v2", "nope.v1", "COMMIT.V1"} {
		mutated := clonePlain(base)
		mutated["records"].([]any)[0].(map[string]any)["kind"] = kind
		add("record kind "+kind, []string{"validate", "{file}"}, mustJSON(t, mutated), "")
	}
	for _, key := range []string{"kind", "externalId", "payload"} {
		mutated := clonePlain(base)
		delete(mutated["records"].([]any)[0].(map[string]any), key)
		add("record drop "+key, []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		for index, replacement := range replacements {
			mutated := clonePlain(base)
			mutated["records"].([]any)[0].(map[string]any)[key] = replacement
			add("record "+key+"="+names[index], []string{"validate", "{file}"}, mustJSON(t, mutated), "")
		}
	}
	add("no records", []string{"validate", "{file}"}, mustJSON(t, envelope()), "")
	many := make([]map[string]any, 1001)
	for index := range many {
		many[index] = sampleRecordMap(t, "commit.v1")
	}
	add("1001 records", []string{"validate", "{file}"}, mustJSON(t, envelope(many...)), "")
	add("1001 records json", []string{"validate", "{file}", "--json"}, mustJSON(t, envelope(many...)), "")
	two := envelope(sampleRecordMap(t, "commit.v1"), map[string]any{"kind": "nope.v1", "externalId": "x", "payload": map[string]any{}}, sampleRecordMap(t, "team.v1"))
	add("mixed valid and unknown", []string{"validate", "{file}"}, mustJSON(t, two), "")
	add("mixed valid and unknown json", []string{"validate", "{file}", "--json"}, mustJSON(t, two), "")

	// JSON syntax and files.
	for index, text := range []string{"", " ", "{", "}", "nul", "[1,2", `{"a":1,}`, `{"a" 1}`, "[NaN]", `{"schemaVersion": NaN}`, "\ufeff{}", "{}\n{}", `"str"`, "123", "null", "[]", "true", `{"a":` + strings.Repeat("[", 50), "\n\n  {", `{"a": "\ud800"}`, `{'a': 1}`} {
		add(fmt.Sprintf("json syntax %d", index), []string{"validate", "{file}"}, text, "")
		add(fmt.Sprintf("json syntax %d json", index), []string{"validate", "{file}", "--json"}, text, "")
	}
	add("missing file", []string{"validate", "{dir}/no-such-file.json"}, "", "")
	add("directory", []string{"validate", "{dir}"}, "", "")
	add("empty stdin", []string{"validate", "-"}, "", "")
	addRepeat := func(name string, args []string, count int, toStdin bool) {
		c := pushCase{Name: name, Args: args, Repeat: &repeat{Text: " ", Count: count, ToStdin: toStdin}}
		if !toStdin {
			c.FileName = fmt.Sprintf("case-%04d.json", len(cases))
		}
		cases = append(cases, c)
	}
	addRepeat("too large", []string{"validate", "{file}"}, 10_000_001, false)
	addRepeat("too large json", []string{"validate", "{file}", "--json"}, 10_000_001, false)
	addRepeat("exactly the limit", []string{"validate", "{file}"}, 10_000_000, false)
	addRepeat("too large stdin", []string{"validate", "-"}, 10_000_001, true)
	return cases
}

func mustKinds() []string { return recordKinds() }

const pushGolden = "testdata/push_golden.json"

// pushGoldenSHA256 pins testdata/push_golden.json (R24): what the real
// `dev-hops push validate|sample|export` printed for every case of the corpus. The
// producer is deleted with the Python CLI, so this is a rot guard: the file is
// only rewritten by TestPushMatchesThePythonProducer with DHO_PUSH_GOLDEN_UPDATE=1,
// then this digest is updated.
const pushGoldenSHA256 = "388516853de1f54215080c80de533afc2ba9f59cd99b61aacb36ebb85e1c4784"

func TestPushGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(pushGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != pushGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", pushGolden, got, pushGoldenSHA256)
	}
}

// namedDivergences are corpus cases where the two sides differ by a named limit
// (in the PR body), not compared.
var namedDivergences = map[string]string{
	// Python's json module accepts a lone surrogate escape; the shared envelope
	// validator's JSON reader (jiter's rules) refuses it.
	"json syntax 19":      "lone surrogate escape",
	"json syntax 19 json": "lone surrogate escape",
}

// pythonCrash is the exit code the driver reports for a Python traceback.
const pythonCrash = 70

func compareResults(t *testing.T, c pushCase, got, want pushResult, wantName string) bool {
	t.Helper()
	if _, named := namedDivergences[c.Name]; named {
		return true
	}
	if want.Exit == pythonCrash {
		// Python crashed on this input (an uncaught exception): not a behaviour
		// to port; dho must answer something sensible, never crash.
		if got.Exit == 0 && strings.HasPrefix(got.Stdout, "valid:") {
			t.Errorf("%s: dho accepts what Python crashes on", c.Name)
			return false
		}
		return true
	}
	ok := true
	if got.Exit != want.Exit {
		t.Errorf("%s: exit %d, %s exit %d", c.Name, got.Exit, wantName, want.Exit)
		ok = false
	}
	if got.Stdout != want.Stdout {
		t.Errorf("%s: stdout\n%q\n%s stdout\n%q", c.Name, truncate(got.Stdout), wantName, truncate(want.Stdout))
		ok = false
	}
	if want.Stderr != "" && got.Stderr != want.Stderr {
		t.Errorf("%s: stderr\n%q\n%s stderr\n%q", c.Name, truncate(got.Stderr), wantName, truncate(want.Stderr))
		ok = false
	}
	return ok
}

func truncate(text string) string {
	if len(text) > 600 {
		return text[:600] + "..."
	}
	return text
}

// substituteDir turns the corpus' {dir} token into the run's directory.
func substituteDir(c pushCase, dir string) pushCase {
	args := make([]string, len(c.Args))
	for index, arg := range c.Args {
		args[index] = strings.ReplaceAll(arg, "{dir}", dir)
	}
	c.Args = args
	return c
}

func TestPushMatchesTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(pushGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen struct {
		Cases   []pushCase   `json:"cases"`
		Results []pushResult `json:"results"`
	}
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	cases := corpus(t)
	if len(cases) != len(frozen.Cases) {
		t.Fatalf("the corpus has %d cases, the golden %d", len(cases), len(frozen.Cases))
	}
	dir := t.TempDir()
	failures, validated, rejected := 0, 0, 0
	for index, c := range cases {
		if c.Name != frozen.Cases[index].Name {
			t.Fatalf("case %d is %q, the golden has %q", index, c.Name, frozen.Cases[index].Name)
		}
		got := runGo(t, substituteDir(c, dir), dir)
		if strings.HasPrefix(c.Name, "sample") {
			got.Stdout = maskProducerVersion(got.Stdout)
		}
		if !compareResults(t, c, got, frozen.Results[index], "frozen Python") {
			failures++
		}
		if strings.HasPrefix(frozen.Results[index].Stdout, "valid:") || strings.Contains(frozen.Results[index].Stdout, `"valid": true`) {
			validated++
		}
		if strings.Contains(frozen.Results[index].Stdout, "error(s):") {
			rejected++
		}
	}
	if validated < 20 || rejected < 200 {
		t.Fatalf("the golden has %d accepted and %d rejected payloads: it measures too little", validated, rejected)
	}
	_ = failures
}

// TestPushMatchesThePythonProducer runs the corpus through the real Python verbs
// and through dho. With DHO_PUSH_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestPushMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := corpus(t)
	dir := t.TempDir()
	pythonCases := make([]pushCase, len(cases))
	for index, c := range cases {
		pythonCases[index] = substituteDir(c, dir)
	}
	stdin, _ := json.Marshal(map[string]any{"dir": dir, "cases": pythonCases})
	command := exec.Command(python, "-c", pythonPushProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_ENABLED=false")
	command.Stdin = bytes.NewReader(stdin)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	marker := strings.LastIndex(string(output), "RESULT")
	if marker < 0 {
		t.Fatalf("no result from python: %.400s", output)
	}
	var want []pushResult
	if err := json.Unmarshal(output[marker+len("RESULT"):], &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(want), len(cases))
	}
	mismatches := 0
	frozen := make([]pushResult, len(cases))
	for index, c := range cases {
		expected := want[index]
		expected.Stdout = normalize(expected.Stdout, dir)
		expected.Stderr = normalize(expected.Stderr, dir)
		// A sample's producerVersion is the distribution's version in Python and this
		// binary's here; everything else must match.
		if strings.HasPrefix(c.Name, "sample") {
			expected.Stdout = maskProducerVersion(expected.Stdout)
		}
		if !keepStderr(expected.Stderr) {
			expected.Stderr = ""
		}
		got := runGo(t, substituteDir(c, dir), dir)
		if strings.HasPrefix(c.Name, "sample") {
			got.Stdout = maskProducerVersion(got.Stdout)
		}
		if !compareResults(t, c, got, expected, "python") {
			mismatches++
		}
		frozen[index] = expected
	}
	t.Logf("%d cases, %d mismatches", len(cases), mismatches)
	if os.Getenv("DHO_PUSH_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(map[string]any{"cases": cases, "results": frozen}, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(pushGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if mismatches == 0 {
		if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
			if err := os.WriteFile(filepath.Join(proof, "pushcli-offline"), []byte("executed"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func maskProducerVersion(text string) string {
	lines := strings.Split(text, "\n")
	for index, line := range lines {
		if strings.Contains(line, `"producerVersion"`) {
			lines[index] = `      "producerVersion": "<version>"`
		}
	}
	return strings.Join(lines, "\n")
}
