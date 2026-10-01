package aiimpact

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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

// TestRepoPatternResolverMatchesLivePython compares (against the frozen production answers) this port against the
// production builder + resolver over the hostile pattern set documented in the
// oracle script. Every probe is compared, including the ones expected to
// resolve to nothing -- a port that leaked an empty prefix would resolve
// "totally/unrelated" to a team, and only a negative probe can catch that.
func TestRepoPatternResolverMatchesLivePython(t *testing.T) {
	want := runRepoTeamsOracle(t)
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

// runRepoTeamsOracle returns the team_id the production builder and resolver gave each probe. The oracle
// script was executed once on the last build that carried the Python sources and its stdout is frozen in
// testdata/golden/repo_teams_oracle.json (recipe in the golden's spec); a frozen run reads it, no Python
// runs. The script's text and the probe set are part of the request, so a changed script or fixture is
// refused until it is recorded again.
func runRepoTeamsOracle(t *testing.T) map[string]*string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	const scriptPath = "internal/jobs/metrics/aiimpact/testdata/python_repo_teams_oracle.py"
	source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scriptPath)))
	if err != nil {
		t.Fatal(err)
	}
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/repo_teams_oracle.json",
		PythonBuild: rotguard.PythonBuild,
		SHA256:      "",
		Recipe: "git worktree add --detach $DIR " + rotguard.PythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/jobs/metrics/aiimpact/ -test '^TestRepoPatternResolverMatchesLivePython$' -python-root $DIR",
	}
	answers := programoracle.Run(t, spec, root, []programoracle.Program{
		programoracle.Script("repo teams oracle", scriptPath, string(source), nil),
	})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the repo teams oracle exited %d (stdout %q)", answers[0].ExitCode, answers[0].Stdout)
	}
	output := bytes.TrimSpace([]byte(answers[0].Stdout))
	if lastLine := bytes.LastIndexByte(output, '\n'); lastLine >= 0 {
		output = output[lastLine+1:]
	}
	var decoded map[string]*string
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode production Python oracle output %q: %v", output, err)
	}
	if len(decoded) == 0 {
		t.Fatal("the recorded Python answered no probes; the oracle is broken")
	}
	return decoded
}

// TestNonASCIIPatternsAreComparedConsistently is codex round chaos-4280-r1's
// finding 6, and the test this file's doc comment cited before it existed
// (the round caught that too: "a PR body naming a test IS a claim").
//
// Reproduces the exact adversarial input the round measured: a capital Sigma
// followed by 31 case-ignorable runes then a cased letter, which was past
// x/text cases.Lower's Final_Sigma lookahead cap before CHAOS-6630. CPython's
// str.lower() and this resolver's Fold-based key treat the pattern and the
// repo name as the same team.
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
			"the resolver compares with pythonparity.Fold, which folds every "+
			"sigma spelling to one value at any distance",
			repoName, pattern, got)
	}

	// Negative control: a genuinely DIFFERENT repo must still not match, so
	// the fix cannot have degenerated into "everything matches everything."
	if got := resolver.Resolve("totally/unrelated"); got != nil {
		t.Fatalf("Resolve(\"totally/unrelated\") = %v, want nil", got)
	}
}
