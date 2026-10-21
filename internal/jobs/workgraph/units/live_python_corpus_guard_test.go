package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// A rot guard over every frozen corpus of tests/fixtures/ that had a generator
// with a recognised output path.
//
// Every frozen corpus in tests/fixtures/ is a claim about how the deployed
// CPython behaved. The Go tests assert that GO matches the frozen file; this
// asserts that the checked-in fixture is still byte for byte what the
// recorded Python generator rendered.
//
// The list of (generator, fixture) pairs is frozen in
// testdata/corpus_discovery_jobs.json; the renders were executed once, all in
// one golden (testdata/golden/corpus_discovery_rot_guard.json), and a frozen
// run compares the recorded renders with the checked-in fixtures and runs no
// Python.

// corpusJob is one generator and the fixture it declared.
type corpusJob struct {
	Generator string `json:"generator"`
	Fixture   string `json:"fixture"`
}

// TestEveryDiscoverableCorpusStillMatchesLivePython compares the recorded
// render of each generator with the fixture it declared.
func TestEveryDiscoverableCorpusStillMatchesLivePython(t *testing.T) {
	repoRoot := repositoryRootPath(t)
	fixturesDirectory := filepath.Join(repoRoot, "tests", "fixtures")

	raw, err := os.ReadFile(filepath.Join("testdata", "corpus_discovery_jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var jobs []corpusJob
	if err := json.Unmarshal(raw, &jobs); err != nil {
		t.Fatalf("decode the frozen corpus jobs: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("the frozen corpus job list is empty; this guard would pass while guarding nothing")
	}

	generatorsToRun := make([]rotguard.Generator, len(jobs))
	for index, job := range jobs {
		generatorsToRun[index] = rotguard.Generator{Name: job.Generator}
	}
	spec := rotguard.Spec("testdata/golden/corpus_discovery_rot_guard.json", "899cf483964e63531298b171f569cdb4ffb49e6976c9c4c7bca8611904ee4955")
	renders := rotguard.Run(t, spec, generatorsToRun...)
	for index, job := range jobs {
		name, fixtureName, rendered := job.Generator, job.Fixture, renders[index]
		t.Run(name, func(t *testing.T) {
			frozen, err := os.ReadFile(filepath.Join(fixturesDirectory, fixtureName))
			if err != nil {
				t.Fatalf("read the frozen fixture %s: %v", fixtureName, err)
			}

			if rendered != string(frozen) {
				t.Errorf(
					"%s has ROTTED: the frozen fixture no longer matches what the "+
						"recorded interpreter produced.\n"+
						"  fixture:   tests/fixtures/%s\n"+
						"  frozen:    %d bytes\n"+
						"  recorded:  %d bytes\n"+
						"The frozen file records the behaviour the Go port was written "+
						"against, so a change here means the port may now be wrong -- "+
						"read what moved and decide whether the Go side must follow.\n%s",
					name, fixtureName, len(frozen), len(rendered),
					firstDifference(string(frozen), rendered),
				)
			}
		})
	}
	t.Logf("guarded %d corpora", len(jobs))
}

// firstDifference reports where two renderings diverge, with a little context.
// A whole-file diff of a 700KB corpus is unreadable in CI output, and the first
// difference is almost always enough to identify what moved.
func firstDifference(frozen, live string) string {
	limit := len(frozen)
	if len(live) < limit {
		limit = len(live)
	}
	for i := 0; i < limit; i++ {
		if frozen[i] != live[i] {
			start := i - 60
			if start < 0 {
				start = 0
			}
			end := i + 60
			if end > limit {
				end = limit
			}
			return "  first difference at byte " + itoa(i) + ":\n" +
				"    frozen: ..." + strings.ReplaceAll(frozen[start:end], "\n", "\\n") + "...\n" +
				"    live:   ..." + strings.ReplaceAll(live[start:end], "\n", "\\n") + "..."
		}
	}
	return "  identical up to byte " + itoa(limit) + "; the renderings differ only in length"
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
