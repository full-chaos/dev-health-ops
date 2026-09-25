package cli

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

//go:embed testdata/root_flags_oracle.py
var rootFlagsOracleProgram string

type typedValue struct {
	T string `json:"t"`
	V string `json:"v"`
}

func tagged(kind, value string) typedValue { return typedValue{T: kind, V: value} }

// rootCorpus is every command line the root parser is compared on. Each ends
// in `fixtures generate` (a leaf both parsers accept with no options) unless
// the case is about what stands there.
func rootCorpus() [][]string {
	cmd := []string{"fixtures", "generate"}
	with := func(args ...string) []string { return append(append([]string{}, args...), cmd...) }
	corpus := [][]string{
		with(),
		with("--org", "R"), with("--org=R"), with("--org="), with("--org", ""), with("--org", "R", "--org", "S"),
		with("--or", "R"), with("--o", "R"), with("--org", "a b"), with("--org", "R="), with("--org", "é"),
		with("--db", "postgresql://u@h/d"), with("--d", "x"), with("--db=x"), with("--analytics-db", "clickhouse://h/d"),
		with("--analytics", "x"), with("--a", "x"),
		with("--log-level", "DEBUG"), with("--log-level", "debug"), with("--log", "WARNING"), with("--log-level=ERROR"),
		with("--log-level", "nope"), with("--log-level", ""),
		with("-l", "openai"), with("-lopenai"), with("-l=openai"), with("--llm-provider", "anthropic"), with("--llm-p", "x"),
		with("-m", "gpt-x"), with("-mgpt-x"), with("--model", "m"), with("--mod", "m"), with("-m", "a", "-m", "b"),
		with("--llm-api-key", "not-a-real-key"), with("--llm-base-url", "http://h"),
		with("--llm-concurrency", "3"), with("--llm-concurrency", " 7 "), with("--llm-concurrency", "1_0"),
		with("--llm-concurrency", "\u0663"), with("--llm-concurrency", "-5"), with("--llm-concurrency", "x"),
		with("--llm-concurrency", "1.5"), with("--llm-concurrency", ""),
		// the quoted value of an argument error is repr(value), control characters escaped
		with("--llm-concurrency", "\x1b[2J"), with("--llm-concurrency", "a\tb\nc\rd"), with("--llm-concurrency", "\x00\x7f"),
		with("--llm-concurrency", "it's"), with("--llm-concurrency", `say "hi"`), with("--llm-concurrency", `both ' and "`),
		with("--llm-concurrency", `back\slash`), with("--llm-concurrency", "é"), with("--llm-concurrency", "\u00a0x"),
		with("--llm-concurrency", "\u200bx"), with("--llm-concurrency", "\U0001F600"), with("--llm-concurrency", "\U000e0001"),
		with("--org", "R", "--db", "D", "--analytics-db", "A", "--log-level", "INFO", "-l", "L", "-m", "M"),
		// missing values and option-like values
		with("--org"), {"--org"}, {"--org", "R"}, {"--org", "--db", "x", "fixtures", "generate"},
		with("--org", "-x"), with("--org", "-"), with("--org", "--"), with("--org=--db"),
		// unknown and ambiguous options
		with("--bogus"), with("--bogus", "x"), with("-z"), with("--l", "x"), with("--llm", "x"), with("--log-"),
		// help
		with("--help"), with("-h"), with("--org", "R", "--help"), with("--org", "R", "-h"),
		{"--org", "R", "-hx", "fixtures", "generate"},
		// "--" and the words after the command
		{"--org", "R", "--", "fixtures", "generate"}, {"--", "fixtures", "generate"},
		// what follows the command is the command's: the root does not look at it
		{"--org", "R", "fixtures", "generate", "--days", "3"}, {"--org", "R", "fixtures", "generate", "--sink", "x", "--with-metrics"},
		{"--org", "R", "--", "--org", "L"}, {"--org", "R", "--"},
	}
	return corpus
}

// rootResult is the parse of the root flags in the oracle's shape: exit codes
// for what refuses, else each root dest with its default when not typed.
func rootResult(argv []string) map[string]any {
	parsed, err := parseRootFlags(argv)
	switch {
	case err != nil:
		result := map[string]any{"stage": "exit", "code": tagged("int", "2")}
		if strings.HasPrefix(err.Msg, "argument --llm-concurrency: invalid int value: ") {
			// The one message dho words itself: the oracle compares its bytes.
			result["msg"] = tagged("str", err.Msg)
		}
		return result
	case parsed.help:
		return map[string]any{"stage": "exit", "code": tagged("int", "0")}
	case len(parsed.rest) == 0 || parsed.rest[0] != "fixtures":
		// No command, or one the tree does not have: dev-hops's `command`
		// positional refuses it.
		return map[string]any{"stage": "exit", "code": tagged("int", "2")}
	}
	get := func(dest string, fallback *typedValue) typedValue {
		if value, typed := parsed.values[dest]; typed {
			return tagged("str", value)
		}
		if fallback == nil {
			return tagged("null", "")
		}
		return *fallback
	}
	info, auto := tagged("str", "INFO"), tagged("str", "auto")
	concurrency := tagged("int", "5")
	if value, typed := parsed.values["llm-concurrency"]; typed {
		n, _ := pythonparity.ParseInt(value)
		concurrency = tagged("int", n.String())
	}
	return map[string]any{"stage": "ok", "ns": map[string]any{
		"log_level": get("log-level", &info), "db": get("db", nil), "analytics_db": get("analytics-db", nil),
		"org": get("org", nil), "llm_provider": get("llm-provider", &auto), "model": get("model", nil),
		"llm_api_key": get("llm-api-key", nil), "llm_base_url": get("llm-base-url", nil), "llm_concurrency": concurrency,
	}}
}

// TestRootFlagsMatchLivePython compares the root parser with the real
// build_parser().parse_args(...) + main()'s _resolve_org over the corpus: the
// exit code of what refuses, and the value of every root option of what parses.
// Named limits: a root value is handed to the command, where a flag typed after
// the command wins (asserted by the dispatch tests, not by this oracle); the
// environment defaults of the root parser (LOG_LEVEL, POSTGRES_URI, ...) stay
// each command's own, so the oracle runs with them unset.
func TestRootFlagsMatchLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR") == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	python := pyoracle.Resolve(t, repoRoot)
	probe, probeErr := exec.Command(python, pyoracle.VersionProbeArgs...).Output()
	pyoracle.RequireDeployed(t, python, probe, probeErr)

	corpus := rootCorpus()
	input, err := json.Marshal(corpus)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", rootFlagsOracleProgram)
	command.Stdin = strings.NewReader(string(input))
	command.Env = append(os.Environ(), "PYTHONHASHSEED=0")
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr))
	}
	var want []map[string]any
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode python answer: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d cases", len(want), len(corpus))
	}
	stages := map[string]int{}
	mismatches := 0
	for index, argv := range corpus {
		got := rootResult(argv)
		if _, compared := got["msg"]; !compared {
			// dho's other refusals are worded by its own dispatcher; only the exit code is compared.
			delete(want[index], "msg")
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want[index])
		stages[fmt.Sprint(got["stage"])]++
		if canonicalJSON(gotJSON) != canonicalJSON(wantJSON) {
			mismatches++
			t.Errorf("case %d %q:\n go     %s\n python %s", index, argv, gotJSON, wantJSON)
		}
	}
	names := make([]string, 0, len(stages))
	for name := range stages {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("%d command lines compared, %d mismatches, stages %v", len(corpus), mismatches, stages)
	// A corpus that only ever parses, or only ever refuses, compares nothing.
	if stages["ok"] < 40 || stages["exit"] < 20 {
		t.Fatalf("the corpus did not reach both outcomes: %v", stages)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"), "cli-root-flags"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func canonicalJSON(raw []byte) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	out, _ := json.Marshal(value)
	return string(out)
}
