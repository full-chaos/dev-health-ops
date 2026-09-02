package edges

import (
	"errors"
	"strconv"
	"testing"
)

// The corpus is the audit's gate-1 table plus the boundary set. Every row states
// what PYTHON does, measured against the deployed
// `_parse_pr_dependency_source`, not what we believe it should do.
//
// This function decides which pipeline owns a row, so a divergence is a row one
// pipeline skips and the other never sees — with both conservation checks still
// balancing. That is why the corpus is differential rather than illustrative.
var pythonParityCorpus = []struct {
	name     string
	input    string
	wantRepo string
	wantPR   int
	wantProv string
	wantErr  error
	// python records the observed behaviour of the deployed function, so a
	// reader can see what this row is asserting parity WITH.
	python string
}{
	{"github ordinary", "ghpr:owner/repo#5", "owner/repo", 5, ProviderGitHub, nil, "('owner/repo', 5, 'github')"},
	{"gitlab ordinary", "gitlab:group/proj!42", "group/proj", 42, ProviderGitLab, nil, "('group/proj', 42, 'gitlab')"},

	// --- the five that diverge from strconv.Atoi ---
	{"negative rejected", "ghpr:o/r#-5", "", 0, "", nil, "None (isdigit False)"},
	{"explicit plus rejected", "ghpr:o/r#+5", "", 0, "", nil, "None (isdigit False)"},
	{"arabic-indic accepted", "ghpr:o/r#٥", "o/r", 5, ProviderGitHub, nil, "('o/r', 5, 'github')"},
	{"fullwidth accepted", "ghpr:o/r#５", "o/r", 5, ProviderGitHub, nil, "('o/r', 5, 'github')"},
	{"superscript is the deliberate divergence", "ghpr:o/r#²", "", 0, "", ErrMalformedPRID,
		"RAISES ValueError -- aborts the whole build (CHAOS-4811)"},

	// --- boundary set ---
	{"not pr shaped", "linear:CHAOS-4766", "", 0, "", nil, "None (no prefix)"},
	{"empty", "", "", 0, "", nil, "None"},
	{"prefix only", "ghpr:", "", 0, "", nil, "None (no separator)"},
	{"no separator", "ghpr:owner/repo", "", 0, "", nil, "None"},
	{"empty slug", "ghpr:#5", "", 0, "", nil, "None (repo_slug empty)"},
	{"empty number", "ghpr:o/r#", "", 0, "", nil, "None (isdigit('') False)"},
	{"zero rejected", "ghpr:o/r#0", "", 0, "", nil, "None (pr_number <= 0)"},
	{"whitespace number", "ghpr:o/r# 5", "", 0, "", nil, "None (isdigit False)"},
	{"slug containing the separator", "ghpr:o/r#x#7", "o/r#x", 7, ProviderGitHub, nil,
		"('o/r#x', 7, 'github') -- rsplit takes the LAST separator"},
	{"gitlab separator inside a github id is not special", "ghpr:o/r!5", "", 0, "", nil, "None"},
}

func TestParsePRDependencySourceMatchesPython(t *testing.T) {
	for _, testCase := range pythonParityCorpus {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParsePRDependencySource(testCase.input)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("input %q: err = %v, want %v (python: %s)",
						testCase.input, err, testCase.wantErr, testCase.python)
				}
				return
			}
			if err != nil {
				t.Fatalf("input %q: unexpected err %v (python: %s)", testCase.input, err, testCase.python)
			}
			if got.RepoSlug != testCase.wantRepo || got.PRNumber != testCase.wantPR || got.Provider != testCase.wantProv {
				t.Fatalf("input %q: got %+v, want {%s %d %s} (python: %s)",
					testCase.input, got, testCase.wantRepo, testCase.wantPR, testCase.wantProv, testCase.python)
			}
		})
	}
}

// TestAtoiWouldDivergeInBothDirections pins WHY this package does not use
// strconv.Atoi, so a later simplification that reaches for it fails here with
// the reason rather than silently re-opening the divergence.
func TestAtoiWouldDivergeInBothDirections(t *testing.T) {
	claimedByAtoiOnly := []string{"-5", "+5"}
	for _, number := range claimedByAtoiOnly {
		if _, err := strconv.Atoi(number); err != nil {
			t.Fatalf("premise changed: strconv.Atoi(%q) now rejects; the divergence table is stale", number)
		}
		if isPythonDigitString(number) {
			t.Fatalf("premise changed: python isdigit(%q) now accepts", number)
		}
	}
	claimedByPythonOnly := []string{"٥", "５"}
	for _, number := range claimedByPythonOnly {
		if _, err := strconv.Atoi(number); err == nil {
			t.Fatalf("premise changed: strconv.Atoi(%q) now accepts", number)
		}
		if !isPythonDigitString(number) {
			t.Fatalf("premise changed: python isdigit(%q) now rejects", number)
		}
		value, ok := pythonIntFromDigits(number)
		if !ok || value != 5 {
			t.Fatalf("int(%q) should be 5, got %d ok=%v", number, value, ok)
		}
	}
}

// TestSuperscriptIsTheOnlyDeliberateDivergence keeps the exception closed: a
// value must EITHER agree with Python's accept-set OR be the one ruled
// divergence. Nothing else may return ErrMalformedPRID.
func TestSuperscriptIsTheOnlyDeliberateDivergence(t *testing.T) {
	// isdigit true but no decimal value -- the whole class Python crashes on.
	for _, number := range []string{"²", "³", "¹", "5²"} {
		if !isPythonDigitString(number) {
			t.Fatalf("premise changed: isdigit(%q) now rejects, so it would not reach int()", number)
		}
		if _, ok := pythonIntFromDigits(number); ok {
			t.Fatalf("int(%q) should fail as it does in Python", number)
		}
		if _, err := ParsePRDependencySource("ghpr:o/r#" + number); !errors.Is(err, ErrMalformedPRID) {
			t.Fatalf("ghpr:o/r#%s should be the named divergence, got err=%v", number, err)
		}
	}
	// Everything else that is rejected must be a SILENT skip, never the error --
	// otherwise the "one deliberate divergence" claim is false.
	for _, input := range []string{"ghpr:o/r#-5", "ghpr:o/r#0", "ghpr:o/r#", "linear:X", ""} {
		if _, err := ParsePRDependencySource(input); err != nil {
			t.Fatalf("%q must be a silent skip, not an error: %v", input, err)
		}
	}
}
