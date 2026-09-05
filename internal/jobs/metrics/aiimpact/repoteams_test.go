package aiimpact

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixtureTeams MUST stay byte-identical to
// testdata/python_repo_teams_oracle.py's TEAMS.
func fixtureTeams() []Team {
	return []Team{
		{ID: "team-star", Name: "Star", RepoPatterns: []string{"acme/**"}},
		{ID: "team-matchall", Name: "MatchAll", RepoPatterns: []string{"*", "**", "/*"}},
		{ID: "team-long", Name: "Long", RepoPatterns: []string{"acme/platform-*"}},
		{ID: "team-tie", Name: "Tie", RepoPatterns: []string{"acme/platfXrm-*"}},
		{ID: "team-exact", Name: "Exact", RepoPatterns: []string{"acme/platform-core"}},
		{ID: "team-case", Name: "Case", RepoPatterns: []string{"  WIDGETS/Alpha  "}},
		{ID: "team-nopat", Name: "NoPat", RepoPatterns: []string{}},
		{ID: "   ", Name: "Blank", RepoPatterns: []string{"blank/*"}},
		{ID: "team-dup-a", Name: "DupA", RepoPatterns: []string{"dup/repo"}},
		{ID: "team-dup-b", Name: "DupB", RepoPatterns: []string{"dup/repo"}},
		{ID: "team-ws", Name: "WS", RepoPatterns: []string{"   "}},
	}
}

var fixtureProbes = []string{
	"acme/anything",
	"acme/platform-x",
	"acme/platform-core",
	"ACME/Platform-Core",
	"  acme/platform-core ",
	"WIDGETS/ALPHA",
	"widgets/alpha",
	"totally/unrelated",
	"blank/thing",
	"dup/repo",
	"",
	"   ",
}

// TestRepoPatternResolverMatchesLivePython compares this port against the
// production builder + resolver over the hostile pattern set documented in the
// oracle script. Every probe is compared, including the ones expected to
// resolve to nothing -- a port that leaked an empty prefix would resolve
// "totally/unrelated" to a team, and only a negative probe can catch that.
func TestRepoPatternResolverMatchesLivePython(t *testing.T) {
	want := runRepoTeamsOracle(t, "ai-impact-repo-teams-golden")
	resolver := BuildRepoPatternResolver(fixtureTeams())

	if len(want) != len(fixtureProbes) {
		t.Fatalf("python answered %d probes, fixture has %d -- the two sides have drifted",
			len(want), len(fixtureProbes))
	}
	for _, probe := range fixtureProbes {
		expected, present := want[probe]
		if !present {
			t.Fatalf("python produced no answer for probe %q; the fixtures have drifted apart", probe)
		}
		got := resolver.Resolve(probe)
		switch {
		case expected == nil && got != nil:
			t.Errorf("probe %q: python resolved to nothing, go resolved to %q", probe, *got)
		case expected != nil && got == nil:
			t.Errorf("probe %q: python resolved to %q, go resolved to nothing", probe, *expected)
		case expected != nil && got != nil && *expected != *got:
			t.Errorf("probe %q: python=%q go=%q", probe, *expected, *got)
		}
	}
}

// TestEmptyReductionPatternsAreDroppedNotMatchAll isolates the single most
// dangerous failure mode, so it is caught even when the live oracle is skipped.
//
// "*", "**" and "/*" all reduce to the empty string. Keeping one as a
// zero-length prefix makes strings.HasPrefix(anything, "") true, silently
// attributing EVERY repository in the org to that team -- a wrong answer that
// looks like a working feature.
func TestEmptyReductionPatternsAreDroppedNotMatchAll(t *testing.T) {
	resolver := BuildRepoPatternResolver([]Team{
		{ID: "team-matchall", Name: "MatchAll", RepoPatterns: []string{"*", "**", "/*"}},
	})
	if len(resolver.prefixes) != 0 {
		t.Fatalf("built %d prefix rules from patterns that all reduce to empty; want 0", len(resolver.prefixes))
	}
	for _, probe := range []string{"anything/at-all", "a", "x/y/z"} {
		if got := resolver.Resolve(probe); got != nil {
			t.Fatalf("probe %q resolved to %q; an empty prefix leaked in and now matches every repo", probe, *got)
		}
	}
}

// TestTrailingStarsAndSlashesStripFully pins the rstrip semantics: rstrip
// removes ALL trailing occurrences, so every spelling below reduces to the
// same prefix. A port using TrimSuffix (one occurrence) would leave "acme/"
// or "acme/*" and stop matching "acme/anything".
func TestTrailingStarsAndSlashesStripFully(t *testing.T) {
	for _, pattern := range []string{"acme/**", "acme/*", "acme/***", "acme*", "acme/"} {
		resolver := BuildRepoPatternResolver([]Team{
			{ID: "t", Name: "T", RepoPatterns: []string{pattern}},
		})
		if pattern == "acme/" {
			// No '*', so this is an EXACT key, not a prefix -- it must NOT
			// match "acme/anything". The negative control for the rule.
			if got := resolver.Resolve("acme/anything"); got != nil {
				t.Fatalf("pattern %q has no '*' and must be exact, but matched a prefix probe", pattern)
			}
			continue
		}
		got := resolver.Resolve("acme/anything")
		if got == nil || *got != "t" {
			t.Fatalf("pattern %q did not reduce to the prefix \"acme\": probe resolved to %v", pattern, got)
		}
	}
}

// TestLongestPrefixWinsAndTiesKeepDeclarationOrder pins the descending-length
// STABLE sort. Longest-match is the intent; stability is what makes an
// equal-length tie deterministic rather than dependent on sort internals.
func TestLongestPrefixWinsAndTiesKeepDeclarationOrder(t *testing.T) {
	resolver := BuildRepoPatternResolver([]Team{
		{ID: "short", Name: "Short", RepoPatterns: []string{"acme/*"}},
		{ID: "long", Name: "Long", RepoPatterns: []string{"acme/platform-*"}},
	})
	got := resolver.Resolve("acme/platform-thing")
	if got == nil || *got != "long" {
		t.Fatalf("longest prefix did not win: %v", got)
	}

	// Equal-length prefixes: the FIRST declared must win.
	tie := BuildRepoPatternResolver([]Team{
		{ID: "first", Name: "First", RepoPatterns: []string{"acme/aaaa-*"}},
		{ID: "second", Name: "Second", RepoPatterns: []string{"acme/aaab-*"}},
	})
	if got := tie.Resolve("acme/aaaa-x"); got == nil || *got != "first" {
		t.Fatalf("equal-length tie resolved to %v, want the first-declared rule", got)
	}
}

func runRepoTeamsOracle(t *testing.T, markerName string) map[string]*string {
	t.Helper()
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required for the live repo-teams Python oracle")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, filepath.Join("testdata", "python_repo_teams_oracle.py"))
	command.Dir = filepath.Join(root, "internal", "jobs", "metrics", "aiimpact")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute production Python oracle: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	output := bytes.TrimSpace(stdout.Bytes())
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	var decoded map[string]*string
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	if len(decoded) == 0 {
		t.Fatal("live Python answered no probes; the oracle is broken")
	}
	if writeErr := os.WriteFile(filepath.Join(proofDirectory, markerName), []byte("executed"), 0o644); writeErr != nil {
		t.Fatalf("write live-python-oracle proof: %v", writeErr)
	}
	return decoded
}

// TestNonASCIIPatternsAreComparedConsistently is codex round chaos-4280-r1's
// finding 6, and the test this file's doc comment cited before it existed
// (the round caught that too: "a PR body naming a test IS a claim").
//
// Reproduces the exact adversarial input the round measured: a capital Sigma
// followed by 31 case-ignorable runes then a cased letter is PAST
// x/text cases.Lower's Final_Sigma lookahead cap, so Lower alone resolves the
// pattern and the repo name to two DIFFERENT strings ("...ς" vs "...σ") even
// though CPython's str.lower() -- and this resolver's own Fold-based key --
// treat them as the same team.
func TestNonASCIIPatternsAreComparedConsistently(t *testing.T) {
	longRun := strings.Repeat(".", 31)
	pattern := "AΣ" + longRun + "B*" // -> prefix "aσ" + longRun + "b" once folded
	repoName := "aσ" + longRun + "b/foo"

	resolver := BuildRepoPatternResolver([]Team{
		{ID: "team-sigma", Name: "Sigma", RepoPatterns: []string{pattern}},
	})
	got := resolver.Resolve(repoName)
	if got == nil || *got != "team-sigma" {
		t.Fatalf("Resolve(%q) with pattern %q = %v, want \"team-sigma\" -- "+
			"x/text's bounded Final_Sigma lookahead (31 case-ignorable runes, "+
			"pythonparity.Lower's doc comment) makes Lower alone disagree with "+
			"CPython here; the fix is comparing via pythonparity.Fold instead",
			repoName, pattern, got)
	}

	// Negative control: a genuinely DIFFERENT repo must still not match, so
	// the fix cannot have degenerated into "everything matches everything."
	if got := resolver.Resolve("totally/unrelated"); got != nil {
		t.Fatalf("Resolve(\"totally/unrelated\") = %v, want nil", got)
	}
}
