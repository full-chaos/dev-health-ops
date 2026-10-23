package credshape

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func defaultScanner(t testing.TB) *Scanner {
	t.Helper()
	scanner, err := Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	return scanner
}

func exampleStrings(t testing.TB, name string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "jobs", "v1", "examples", name))
	if err != nil {
		t.Fatal(err)
	}
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil {
		t.Fatal(err)
	}
	var out []string
	var walk func(any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			for _, child := range typed {
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		case string:
			out = append(out, typed)
		}
	}
	walk(tree)
	return out
}

func allExampleStrings(t testing.TB) map[string][]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "contracts", "jobs", "v1", "examples", "*.json"))
	if err != nil || len(files) < 24 {
		t.Fatalf("examples: %d files, err %v", len(files), err)
	}
	out := map[string][]string{}
	for _, file := range files {
		out[filepath.Base(file)] = exampleStrings(t, filepath.Base(file))
	}
	return out
}

func TestEveryProbeIsMatchedAndEveryRuleHasAProbe(t *testing.T) {
	scanner := defaultScanner(t)
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	if len(probes) < 30 {
		t.Fatalf("only %d probes", len(probes))
	}
	for _, probe := range probes {
		if scanner.Match(probe) == "" {
			t.Errorf("probe of %d bytes matches no rule", len(probe))
		}
	}
}

func TestBenignLookAlikesAreNotMatched(t *testing.T) {
	scanner := defaultScanner(t)
	for _, value := range []string{
		"", "a", "heartbeat:2026-07-21T12:00:00Z", "job-heartbeat-0001",
		"00000000-0000-4000-8000-000000000001", "2026-07-21T12:00:00Z",
		"sync.provider_unit", "provider:github:full-chaos/dev-health-ops",
		"0123456789abcdef0123456789abcdef01234567", "sha256:" + strings.Repeat("ab", 32),
		"https://example.com/path?x=1", "git@github.com:org/repo.git", "user@example.com",
		"the sky is blue and the task is done", "ask-dev-conversations", "retention:worker_terminal",
		"Bearer", "Basic", "key=value", "eyJ", "ghp_", "AKIA", "glpat-", "sk_live_",
	} {
		if rule := scanner.Match(value); rule != "" {
			t.Errorf("benign value %q matched rule %s", value, rule)
		}
	}
}

func TestPrefilterAgreesWithTheRegexpsAlone(t *testing.T) {
	scanner := defaultScanner(t)
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for _, strs := range allExampleStrings(t) {
		values = append(values, strs...)
	}
	for _, probe := range probes {
		values = append(values, probe, strings.ToUpper(probe), strings.ToLower(probe),
			"x"+probe, probe+" tail", "head "+probe, probe[:len(probe)/2], "\u212a"+probe, "\u017f"+probe,
			strings.ReplaceAll(probe, "s", "\u017f"), strings.ReplaceAll(probe, "k", "\u212a"))
	}
	for _, value := range values {
		if got, want := scanner.Match(value), scanner.matchRegexpOnly(value); got != want {
			t.Errorf("prefilter %q, regexps alone %q (len %d)", got, want, len(value))
		}
	}
}

func FuzzMatchPrefilterAgrees(f *testing.F) {
	scanner, err := Default()
	if err != nil {
		f.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		f.Fatal(err)
	}
	for _, probe := range probes {
		f.Add(probe)
	}
	f.Add("")
	f.Add("heartbeat:2026-07-21T12:00:00Z")
	f.Fuzz(func(t *testing.T, value string) {
		if !utf8.ValidString(value) {
			t.Skip()
		}
		if got, want := scanner.Match(value), scanner.matchRegexpOnly(value); got != want {
			t.Fatalf("prefilter %q, regexps alone %q", got, want)
		}
	})
}

// Deterministic budget proxy: an ordinary job envelope string never reaches a
// regexp, and matching allocates nothing.
func TestFixtureStringsReachNoRegexpAndAllocateNothing(t *testing.T) {
	scanner := defaultScanner(t)
	corpus := allExampleStrings(t)
	if len(corpus["sync.provider_unit.v1.json"]) == 0 {
		t.Fatal("sync.provider_unit corpus is empty")
	}
	var all []string
	for name, values := range corpus {
		for _, value := range values {
			rule, runs := scanner.matchStats(value)
			if rule != "" {
				t.Errorf("%s: fixture string matched %s", name, rule)
			}
			if runs != 0 {
				t.Errorf("%s: a fixture string reached %d regexps", name, runs)
			}
			all = append(all, value)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for _, value := range all {
			scanner.Match(value)
		}
	}); allocs != 0 {
		t.Fatalf("Match allocates %v per run, want 0", allocs)
	}
}

func BenchmarkMatchPerString(b *testing.B) {
	scanner := defaultScanner(b)
	values := exampleStrings(b, "sync.provider_unit.v1.json")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, value := range values {
			scanner.Match(value)
		}
	}
}

func TestNewNeedsAuthDerivedRules(t *testing.T) {
	if _, err := New([]byte(`{"classes":[{"markers":[]}]}`)); err == nil {
		t.Fatal("a classes contract with no markers built a scanner")
	}
	if _, err := New([]byte(`not json`)); err == nil {
		t.Fatal("invalid JSON built a scanner")
	}
}

func TestAnyCredentialShaped(t *testing.T) {
	scanner := defaultScanner(t)
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	if AnyCredentialShaped() || AnyCredentialShaped("", "job-heartbeat-0001", "00000000-0000-4000-8000-000000000001") {
		t.Fatal("benign values counted as credential-shaped")
	}
	for _, probe := range probes {
		if !AnyCredentialShaped("job-heartbeat-0001", probe) {
			t.Fatalf("a probe of %d bytes was not detected", len(probe))
		}
	}
	if !anyCredentialShaped(nil, errors.New("scanner unavailable"), []string{"job-heartbeat-0001"}) {
		t.Fatal("an unavailable scanner must count every value as credential-shaped")
	}
}
