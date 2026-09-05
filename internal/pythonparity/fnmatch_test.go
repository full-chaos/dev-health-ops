package pythonparity

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// fnMatchCases pair a pattern with a name. They are chosen to separate
// fnmatch from path.Match rather than to demonstrate that globbing works --
// the two agree on ordinary patterns, so a case that both accept proves
// nothing about which one is implemented.
var fnMatchCases = []struct {
	pattern string
	name    string
}{
	// The difference that matters most here: repository names contain "/",
	// and fnmatch's `*` crosses it while path.Match's does not.
	{"*health*", "full-chaos/dev-health-ops"},
	{"*", "org/repo"},
	{"org/*", "org/repo"},
	{"*/repo", "org/repo"},
	{"org*repo", "org/repo"},

	// Character classes, including Python's `!` negation which Go spells `^`.
	{"[abc]at", "cat"},
	{"[!abc]at", "cat"},
	{"[!abc]at", "hat"},
	{"[a-c]at", "bat"},
	{"[!a-c]at", "dat"},

	// A literal `]` as the first class member, and a literal `^`.
	{"[]]x", "]x"},
	{"[^]x", "^x"},

	// Unterminated class: a LITERAL bracket to Python, an error to path.Match.
	{"[abc", "[abc"},
	{"a[", "a["},

	// Single-character wildcard, including across a separator.
	{"?at", "cat"},
	{"a?c", "a/c"},

	// Regex metacharacters in the NAME and in the pattern must stay literal.
	{"a.b", "a.b"},
	{"a.b", "axb"},
	{"a+b", "a+b"},
	{"(x)", "(x)"},
	{"a$b", "a$b"},
	{"a|b", "a|b"},

	// Anchoring: fnmatch matches the WHOLE string, not a prefix.
	{"abc", "abcd"},
	{"abc*", "abcd"},
	{"*abc", "xabc"},

	// Empty pattern and empty name.
	{"", ""},
	{"*", ""},
	{"?", ""},

	// Newline: translate uses (?s:...), so `*` and `?` cross a newline.
	{"a*b", "a\nb"},
	{"a?b", "a\nb"},

	// START ANCHORING. These are the cases the first 28 MISSED: a pattern
	// whose regex can match a proper SUFFIX of the name. Python applies
	// translate() with re.match, which anchors at the start; Go's MatchString
	// does not, so without an explicit \A these all match wrongly. Found by
	// the should_process oracle when the exclude glob "tests/**" swallowed
	// "contests/thing.py".
	{"tests/**", "contests/thing.py"},
	{"tests/*", "contests/thing"},
	{"abc*", "xabc"},
	{"b*", "ab"},
	{"[a-c]at", "xbat"},
	{"?at", "xcat"},
}

func TestFnMatchDiffersFromPathMatchWhereItMust(t *testing.T) {
	// Pins the reason this helper exists. If someone later swaps in
	// path.Match, these cases break loudly instead of silently narrowing
	// which repositories get scanned.
	if !FnMatch("full-chaos/dev-health-ops", "*health*") {
		t.Errorf("fnmatch `*` must cross a slash: *health* should match full-chaos/dev-health-ops")
	}
	// [!abc] excludes 'c', so "cat" must NOT match. Under path.Match this
	// class is read as a literal '!' plus a, b, c and the result flips.
	if FnMatch("cat", "[!abc]at") {
		t.Errorf("[!abc]at must not match cat")
	}
	if !FnMatch("hat", "[!abc]at") {
		t.Errorf("[!abc]at must match hat")
	}
	if !FnMatch("[abc", "[abc") {
		t.Errorf("an unterminated class is a literal bracket in Python")
	}
	// The anchoring case, kept here as well as in the table because it is the
	// one that shipped wrong: an exclude glob must not match a path whose name
	// merely CONTAINS the excluded directory name.
	if FnMatch("contests/thing.py", "tests/**") {
		t.Errorf(`"tests/**" must not match "contests/thing.py" -- fnmatch anchors at the start`)
	}
}

// TestFnMatchMatchesLivePython is the oracle: it compares both the match
// outcome AND the translated expression against CPython.
func TestFnMatchMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE") == "" {
		t.Skip("live Python oracle runs only through the uncached live-oracle gate")
	}
	python := os.Getenv("DEV_HEALTH_PYTHON")
	if python == "" {
		python = "python3"
	}

	// JSON rather than a line-based protocol: two cases carry a newline INSIDE
	// the name, and a line-based encoding silently drops them. The first
	// version of this harness did exactly that and reported "checked 28 of 30"
	// -- the count guard caught it, which is why the guard is there.
	pairs := make([][2]string, 0, len(fnMatchCases))
	for _, tc := range fnMatchCases {
		pairs = append(pairs, [2]string{tc.pattern, tc.name})
	}
	encoded, err := json.Marshal(pairs)
	if err != nil {
		t.Fatalf("encode cases: %v", err)
	}

	script := filepath.Join("testdata", "python_fnmatch_oracle.py")
	command := exec.Command(python, script)
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("python oracle failed: %v", err)
	}

	var got []struct {
		Pattern    string `json:"pattern"`
		Name       string `json:"name"`
		Match      bool   `json:"match"`
		MatchCase  bool   `json:"matchcase"`
		Translated string `json:"translated"`
	}
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(got) != len(fnMatchCases) {
		t.Fatalf("oracle returned %d results for %d cases -- every comparison "+
			"below would be checking a subset it chose for itself",
			len(got), len(fnMatchCases))
	}

	checked := 0
	for i, tc := range fnMatchCases {
		want := got[i]
		if want.Pattern != tc.pattern || want.Name != tc.name {
			t.Fatalf("oracle result %d is for (%q,%q), expected (%q,%q) -- "+
				"the results are not aligned with the inputs",
				i, want.Pattern, want.Name, tc.pattern, tc.name)
		}
		checked++
		if g := FnMatch(tc.name, tc.pattern); g != want.Match {
			t.Errorf("FnMatch(%q, %q) = %v, python %v (translated %q)",
				tc.name, tc.pattern, g, want.Match, want.Translated)
		}
		// fnmatch vs fnmatchcase differ only under a case-folding normcase,
		// which POSIX does not have. If these ever disagree the platform
		// assumption in fnmatch.go is wrong and the helper needs revisiting.
		if want.Match != want.MatchCase {
			t.Errorf("python fnmatch and fnmatchcase disagree on (%q,%q): %v vs %v -- "+
				"the POSIX identity-normcase assumption does not hold here",
				tc.pattern, tc.name, want.Match, want.MatchCase)
		}
	}
	if checked != len(fnMatchCases) {
		t.Fatalf("checked %d of %d cases", checked, len(fnMatchCases))
	}
}
