package pythonparity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type fuzzCorpus struct {
	Schema      string `json:"schema"`
	Seed        int    `json:"seed"`
	Measured    string `json:"measured_on"`
	Unicodedata string `json:"unicodedata"`
	Accepted    int    `json:"accepted"`
	Cases       []struct {
		Input   string `json:"input"`
		Verdict string `json:"verdict"`
		UUID    string `json:"uuid"`
		Error   string `json:"error"`
	} `json:"cases"`
}

// TestParseUUIDMatchesPythonOverTheFuzzCorpus replays a seeded, grammar-driven
// corpus measured from the live interpreter.
//
// # Why this exists alongside the curated accept set
//
// The curated corpus can only hold cases a human thought of, and for four
// review rounds nobody thought of the one that mattered: every curated row was
// ASCII and every row that passed the length gate was thirty-two hex digits, so
// the corpus could not see that the gate counts CHARACTERS and that the step
// behind it is `int(hex, 16)` rather than a hex decode.
//
// This corpus is built without that judgement — bodies of roughly the right
// length, then mutations drawn from the classes the normalisation reacts to —
// so it covers combinations of those classes rather than a list of them.
//
// The two are kept separate on purpose. The curated file explains WHY each case
// matters and is meant to be read; this one is a wide net and is not. Neither
// is redundant with the other.
func TestParseUUIDMatchesPythonOverTheFuzzCorpus(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "uuid_fuzz_corpus.json"))
	if err != nil {
		t.Fatalf("read the fuzz corpus: %v", err)
	}
	var corpus fuzzCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("decode the fuzz corpus: %v", err)
	}
	if corpus.Schema != "python_uuid_fuzz_corpus.v1" || len(corpus.Cases) == 0 {
		t.Fatalf("unexpected corpus: schema=%q cases=%d", corpus.Schema, len(corpus.Cases))
	}

	// A corpus that rejects nearly everything exercises the length gate over and
	// over and the grammar behind it never. The generator refuses to write such
	// a corpus; this asserts the same property at replay time, so a hand-edited
	// or truncated fixture cannot quietly turn this test into a no-op.
	if corpus.Accepted < 100 {
		t.Fatalf("corpus has only %d accepted rows: it cannot exercise the int() grammar",
			corpus.Accepted)
	}

	var accepted, rejected int
	// Failures are COUNTED BY DIRECTION rather than fataling on the first one.
	// With four thousand rows, the shape of a regression — "everything with a
	// fullwidth digit now refuses" — is far more useful than its first instance,
	// and the two directions mean different things.
	var wronglyAccepted, wronglyRefused, wrongValue int
	const reportLimit = 10

	for _, testCase := range corpus.Cases {
		parsed, err := ParseUUID(testCase.Input)

		if testCase.Verdict == "REJECT" {
			rejected++
			if err == nil {
				wronglyAccepted++
				if wronglyAccepted <= reportLimit {
					t.Errorf(
						"ACCEPTED WHAT THE REFERENCE REJECTS: ParseUUID(%q) = %s, CPython raises %s",
						testCase.Input, parsed, testCase.Error)
				}
			}
			continue
		}

		accepted++
		if err != nil {
			wronglyRefused++
			if wronglyRefused <= reportLimit {
				t.Errorf("REFUSED WHAT THE REFERENCE ACCEPTS: ParseUUID(%q) -> %v, CPython = %s",
					testCase.Input, err, testCase.UUID)
			}
			continue
		}
		if parsed.String() != testCase.UUID {
			wrongValue++
			if wrongValue <= reportLimit {
				t.Errorf("WRONG VALUE: ParseUUID(%q) = %s, CPython = %s",
					testCase.Input, parsed, testCase.UUID)
			}
		}
	}

	if wronglyAccepted+wronglyRefused+wrongValue > 0 {
		t.Fatalf(
			"%d/%d rows diverge from CPython %s (unicodedata %s): "+
				"%d accepted-what-it-rejects, %d refused-what-it-accepts, %d wrong value",
			wronglyAccepted+wronglyRefused+wrongValue, len(corpus.Cases),
			corpus.Measured, corpus.Unicodedata,
			wronglyAccepted, wronglyRefused, wrongValue)
	}
	t.Logf("agreed with CPython %s on all %d rows (%d accepted, %d rejected)",
		corpus.Measured, len(corpus.Cases), accepted, rejected)
}
