package cli

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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
		with("-h", "--llm", "x"), with("--llm", "x", "-h"), with("--help", "--l"), with("--l", "--help"), with("--org", "R", "-h", "--llm"),
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

// The frozen oracle (rootflags_golden_test.go) is an external test package,
// because the venue golden harness it uses imports this package. These are
// its handles on the corpus, the Go side and the Python program.
var (
	RootCorpus             = rootCorpus
	RootResult             = rootResult
	CanonicalJSON          = canonicalJSON
	RootFlagsOracleProgram = func() string { return rootFlagsOracleProgram }
)

// canonicalJSON is raw with its object keys sorted. A number keeps its literal
// text (json.Number): 2 and 2.0, or two integers a float64 cannot tell apart,
// stay different.
func canonicalJSON(raw []byte) string {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return string(raw)
	}
	// Not one JSON value: the text stands for itself, so it equals nothing canonical.
	if _, err := decoder.Token(); err != io.EOF {
		return string(raw)
	}
	out, _ := json.Marshal(value)
	return string(out)
}

// The comparison must not pass a number through float64: an integer and the
// float of the same value, and two integers one float64 apart, are different
// answers.
func TestCanonicalJSONKeepsNumberLiterals(t *testing.T) {
	for name, pair := range map[string][2]string{
		"int and float":           {`{"a":2}`, `{"a":2.0}`},
		"beyond float64 integers": {`{"a":9007199254740992}`, `{"a":9007199254740993}`},
		"exponent":                {`{"a":1000}`, `{"a":1e3}`},
	} {
		if canonicalJSON([]byte(pair[0])) == canonicalJSON([]byte(pair[1])) {
			t.Errorf("%s: %s and %s compare equal", name, pair[0], pair[1])
		}
	}
	if canonicalJSON([]byte(`{"b":1,"a":"x"}`)) != canonicalJSON([]byte(`{"a":"x","b":1}`)) {
		t.Error("key order changes the canonical text")
	}
	// Trailing output after the value is not dropped.
	if canonicalJSON([]byte(`{"a":1} trailing`)) == canonicalJSON([]byte(`{"a":1}`)) || canonicalJSON([]byte(`{"a":1}{"a":2}`)) == canonicalJSON([]byte(`{"a":1}`)) {
		t.Error("text after the JSON value is ignored")
	}
}
