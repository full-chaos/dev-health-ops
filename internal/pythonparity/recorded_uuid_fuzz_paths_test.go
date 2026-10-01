package pythonparity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded UUID fuzz corpus is declared. Claims are the paths
// whose change makes a test of this package fail (backed by the mutation run in the pull request
// that added this file); `seed` is named with its reason. Nothing here needs Python or a Python
// file at test time.

var uuidFuzzClaims = []string{
	".accepted",
	".cases[].error",
	".cases[].input",
	".cases[].uuid",
	".cases[].verdict",
	".measured_on",
	".samples",
	".schema",
	".unicodedata",
}

var uuidFuzzNotClaims = map[string]string{
	".seed": "the generators random seed: the rows it produced are the evidence, the seed only lets a recorder reproduce them",
}

func TestEveryRecordedUUIDFuzzPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "uuid_fuzz_corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, uuidFuzzClaims, uuidFuzzNotClaims)
}

// TestUUIDFuzzCorpusStatesWhatItHolds pins what the corpus says about itself against the rows it
// holds and against the Go table it is replayed with: `accepted` and `samples` are the counted
// rows (the replay test only checked accepted >= 100), every refusal was recorded as CPython's
// ValueError and no accepted row carries an error, and the interpreter and Unicode versions it was
// measured on are the ones pythonint_table.go says it was measured on (the two files are replayed
// together; a re-record of one without the other would split them).
func TestUUIDFuzzCorpusStatesWhatItHolds(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "uuid_fuzz_corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Samples     int    `json:"samples"`
		Accepted    int    `json:"accepted"`
		Measured    string `json:"measured_on"`
		Unicodedata string `json:"unicodedata"`
		Cases       []struct {
			Input   string `json:"input"`
			Verdict string `json:"verdict"`
			Error   string `json:"error"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for _, testCase := range corpus.Cases {
		switch testCase.Verdict {
		case "REJECT":
			if testCase.Error != "ValueError" {
				t.Errorf("rejected row %q: recorded error %q, want CPython's ValueError", testCase.Input, testCase.Error)
			}
		default:
			accepted++
			if testCase.Error != "" {
				t.Errorf("accepted row %q carries the error %q", testCase.Input, testCase.Error)
			}
		}
	}
	if corpus.Accepted != accepted {
		t.Errorf("accepted = %d, the corpus holds %d accepted rows", corpus.Accepted, accepted)
	}
	if corpus.Samples != len(corpus.Cases) {
		t.Errorf("samples = %d, the corpus holds %d rows", corpus.Samples, len(corpus.Cases))
	}

	table, err := os.ReadFile("pythonint_table.go")
	if err != nil {
		t.Fatal(err)
	}
	header := string(table)
	if i := strings.Index(header, "\npackage "); i >= 0 {
		header = header[:i]
	}
	if want := fmt.Sprintf("Measured from CPython %s (unicodedata %s)", corpus.Measured, corpus.Unicodedata); !strings.Contains(header, want) {
		t.Errorf("the corpus was measured on CPython %q / unicodedata %q, but pythonint_table.go's header does not say %q", corpus.Measured, corpus.Unicodedata, want)
	}
}
