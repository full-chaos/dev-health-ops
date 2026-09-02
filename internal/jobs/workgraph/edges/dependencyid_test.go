package edges

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// prDependencyIDParity decodes tests/fixtures/pr_dependency_id_parity.json —
// the DEPLOYED `_parse_pr_dependency_source` called over a shape-varying corpus,
// with its own output frozen.
//
// Generated, not written. The previous version carried Python's behaviour as
// hand-transcribed strings: that asserts the author's transcription rather than
// the reference, and a mistyped expectation is indistinguishable from a correct
// one. Here the expectations ARE the reference's output.
//
// `raises` is a real recorded outcome, not an error in the harness — Python's
// isdigit() accepts characters int() rejects and the conversion is unguarded
// (CHAOS-4811). The corpus varies the SHAPE of the id (prefix present/absent/
// miscased, separator absent or wrong-for-provider, slug empty or containing the
// separator, number ASCII/non-ASCII-decimal/digit-but-not-decimal/signed/zero/
// empty/whitespace), because a corpus varying only "valid vs invalid" misses
// every character-class disagreement between isdigit(), int() and strconv.Atoi.
type prDependencyIDParity struct {
	Schema       string `json:"schema"`
	Observations []struct {
		Input     string      `json:"input"`
		Outcome   string      `json:"outcome"`
		Exception string      `json:"exception"`
		RepoSlug  string      `json:"repo_slug"`
		PRNumber  json.Number `json:"pr_number"`
		Provider  string      `json:"provider"`
	} `json:"observations"`
}

func loadPRIDParity(t *testing.T) prDependencyIDParity {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "pr_dependency_id_parity.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pr-id parity corpus: %v", err)
	}
	var corpus prDependencyIDParity
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("decode pr-id parity corpus: %v", err)
	}
	if corpus.Schema != "pr_dependency_id_parity.v2" {
		t.Fatalf("unexpected corpus schema %q", corpus.Schema)
	}
	// A silently-empty corpus would make every assertion below vacuous.
	// json.Unmarshal of `null` into a slice succeeds and yields nil, so "no
	// error" is not evidence that anything was decoded.
	if len(corpus.Observations) == 0 {
		t.Fatal("pr-id parity corpus decoded to zero observations")
	}
	return corpus
}

func TestParsePRDependencySourceMatchesPython(t *testing.T) {
	corpus := loadPRIDParity(t)
	sawParsed, sawNone, sawRaises := 0, 0, 0

	for _, observation := range corpus.Observations {
		got, err := ParsePRDependencySource(observation.Input)
		switch observation.Outcome {
		case "parsed":
			sawParsed++
			if err != nil {
				t.Errorf("%q: Python parsed it, this port errored: %v", observation.Input, err)
				continue
			}
			if got.RepoSlug != observation.RepoSlug || got.Provider != observation.Provider {
				t.Errorf("%q: got slug=%q provider=%q, Python gave slug=%q provider=%q",
					observation.Input, got.RepoSlug, got.Provider,
					observation.RepoSlug, observation.Provider)
			}
			// The corpus is decoded as json.Number because Python's ints are
			// arbitrary-precision: the reference really does return a 40-digit
			// PR number, and decoding that into an int would fail here rather
			// than in the code under test.
			reference, conversionErr := observation.PRNumber.Int64()
			switch {
			case conversionErr == nil:
				if int64(got.PRNumber) != reference {
					t.Errorf("%q: got PR %d, Python gave %d",
						observation.Input, got.PRNumber, reference)
				}
				if got.NumberExceedsInt64 {
					t.Errorf("%q: flagged as exceeding int64, but Python's value %d fits",
						observation.Input, reference)
				}
			default:
				// Python parsed a real positive integer this port cannot hold.
				// The CLASSIFICATION must still be Python's: the row belongs to
				// the issue<->PR pipeline, and must not be silently handed to
				// the issue<->issue build because a number did not fit.
				if !got.NumberExceedsInt64 {
					t.Errorf("%q: Python parsed %s, which exceeds int64, but this port "+
						"did not flag it", observation.Input, observation.PRNumber)
				}
				if !got.IsPR() {
					t.Errorf("%q: a PR number too large to represent must still classify "+
						"as a PR; this row would be built as an issue<->issue edge that "+
						"the mapping pipeline also owns", observation.Input)
				}
			}
		case "none":
			sawNone++
			if err != nil {
				t.Errorf("%q: Python returned None (a silent skip), this port errored: %v",
					observation.Input, err)
				continue
			}
			if got.IsPR() {
				t.Errorf("%q: Python returned None but this port claimed it as a PR — the row "+
					"would be skipped from the issue<->issue build and owned by neither pipeline",
					observation.Input)
			}
		case "raises":
			sawRaises++
			// THE ONE RULED DIVERGENCE. Python raises an unguarded ValueError
			// and aborts the whole org's build; this port rejects the single row
			// with a named, counted reason.
			if !errors.Is(err, ErrMalformedPRID) {
				t.Errorf("%q: Python raises %s here; this port must reject it as "+
					"ErrMalformedPRID, got err=%v", observation.Input, observation.Exception, err)
			}
		default:
			t.Fatalf("%q: unknown recorded outcome %q", observation.Input, observation.Outcome)
		}
	}

	// Each outcome class must be exercised, or the corpus has silently narrowed.
	if sawParsed == 0 || sawNone == 0 || sawRaises == 0 {
		t.Fatalf("corpus no longer covers every outcome: parsed=%d none=%d raises=%d",
			sawParsed, sawNone, sawRaises)
	}
	t.Logf("reference-derived corpus: %d parsed, %d none, %d raises", sawParsed, sawNone, sawRaises)
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
		value, positive, exceeds, ok := pythonIntFromDigits(number)
		if !ok || value != 5 || !positive || exceeds {
			t.Fatalf("int(%q) should be 5, got %d positive=%v exceeds=%v ok=%v",
				number, value, positive, exceeds, ok)
		}
	}
}

// TestSuperscriptIsThisGatesOnlyDivergence keeps THIS GATE's exception closed.
// Scope is the parse gate, not the port: the port has three divergences, listed
// in Divergences (edges.go). Within this function, a
// value must EITHER agree with Python's accept-set OR be the one ruled
// divergence. Nothing else may return ErrMalformedPRID.
func TestSuperscriptIsThisGatesOnlyDivergence(t *testing.T) {
	// isdigit true but no decimal value -- the whole class Python crashes on.
	for _, number := range []string{"²", "³", "¹", "5²"} {
		if !isPythonDigitString(number) {
			t.Fatalf("premise changed: isdigit(%q) now rejects, so it would not reach int()", number)
		}
		if _, _, _, ok := pythonIntFromDigits(number); ok {
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
