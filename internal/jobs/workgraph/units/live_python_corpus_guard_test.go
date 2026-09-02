package units

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A rot guard that DISCOVERS the corpora it guards instead of listing them.
//
// # WHY THIS EXISTS
//
// Every frozen corpus in tests/fixtures/ is a claim about how the deployed
// CPython behaves. The Go tests assert that GO matches the frozen file; nothing
// there asserts that PYTHON still does. When the interpreter moves -- a Unicode
// version bump changing category Nd, a CPython release changing sum()'s
// compensation or repr()'s shortest-round-trip -- the frozen file silently
// becomes a record of an interpreter nobody runs, and every test stays green.
//
// The existing guards cover that risk one hand-written test at a time. At the
// time this was added there were 28 generators and 6 guards, so 22 corpora could
// rot with nothing failing -- including four this branch had just added.
//
// That is the same defect this lane fixed in the CI path filter (#2109): a guard
// maintained by ENUMERATION only covers what someone remembered to list, and the
// entry that is missing is missing precisely because nobody thought of it. Its
// absence is indistinguishable from a pass. Adding four more entries would have
// repaired the instance and left the mechanism, which is what guaranteed the
// recurrence last time.
//
// So this discovers its subjects from the filesystem. A corpus added tomorrow is
// guarded the day it lands, with nobody having to remember.
//
// # THE CONVENTION IT KEYS ON
//
// A generator is discoverable when it declares
//
//	OUTPUT_PATH = Path(__file__).parent / "<name>.json"
//
// and accepts --stdout to render without writing. Both hold for every generator
// this lane owns. A generator that does neither is REPORTED rather than failed:
// most belong to other lanes, and failing their build to enforce a convention
// they never agreed to would be the wrong way to ask.
var outputPathPattern = regexp.MustCompile(
	`OUTPUT_PATH\s*=\s*Path\(__file__\)\.parent\s*/\s*"([^"]+)"`,
)

// nonHermeticGenerators names generators that CANNOT run in this guard, with the
// reason. Empty today, and kept so a future non-hermetic generator has a home
// that forces its author to state why rather than silently dropping out of
// coverage.
var nonHermeticGenerators = map[string]string{}

func TestEveryDiscoverableCorpusStillMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}

	repoRoot := repositoryRootPath(t)
	fixturesDirectory := filepath.Join(repoRoot, "tests", "fixtures")

	generators, err := filepath.Glob(filepath.Join(fixturesDirectory, "generate_*.py"))
	if err != nil {
		t.Fatalf("glob generators: %v", err)
	}
	if len(generators) == 0 {
		t.Fatal(
			"no generators found under tests/fixtures/ -- discovery has broken and " +
				"this guard would pass while guarding nothing, which is the exact " +
				"failure it exists to prevent",
		)
	}
	sort.Strings(generators)

	python := workgraphComponentsLivePython(t, repoRoot)

	var (
		guarded     []string
		unguardable []string
	)

	for _, generator := range generators {
		name := filepath.Base(generator)

		if reason, skip := nonHermeticGenerators[name]; skip {
			t.Logf("skipping %s: %s", name, reason)
			continue
		}

		source, err := os.ReadFile(generator)
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		match := outputPathPattern.FindSubmatch(source)
		if match == nil || !strings.Contains(string(source), "--stdout") {
			unguardable = append(unguardable, name)
			continue
		}
		fixtureName := string(match[1])

		t.Run(name, func(t *testing.T) {
			command := exec.Command(python, generator, "--stdout")
			command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
			rendered, err := command.Output()
			if err != nil {
				var stderr []byte
				if exitError, ok := err.(*exec.ExitError); ok {
					stderr = exitError.Stderr
				}
				t.Fatalf("run %s against live Python: %v: %s", name, err, stderr)
			}

			frozen, err := os.ReadFile(filepath.Join(fixturesDirectory, fixtureName))
			if err != nil {
				t.Fatalf("read the frozen fixture %s: %v", fixtureName, err)
			}

			if string(rendered) != string(frozen) {
				// A byte diff is the right comparison: both sides are the same
				// Python-rendered JSON text from the same generator, so any
				// difference is a real change in what the interpreter produces --
				// not a formatting artefact.
				t.Errorf(
					"%s has ROTTED: the frozen fixture no longer matches what the "+
						"deployed interpreter produces.\n"+
						"  fixture:   tests/fixtures/%s\n"+
						"  generator: tests/fixtures/%s\n"+
						"  frozen:    %d bytes\n"+
						"  live:      %d bytes\n"+
						"This is NOT fixed by regenerating without reading the diff. The "+
						"frozen file records the behaviour the Go port was written "+
						"against, so a change here means the port may now be wrong -- "+
						"read what moved, decide whether the Go side must follow, THEN "+
						"regenerate.\n%s",
					name, fixtureName, name, len(frozen), len(rendered),
					firstDifference(string(frozen), string(rendered)),
				)
			}
		})
		guarded = append(guarded, name)
	}

	t.Logf("guarded %d corpora by discovery", len(guarded))
	if len(unguardable) > 0 {
		// Reported, not failed. These are mostly other lanes' generators, and
		// failing their build to enforce a convention they never agreed to
		// would be the wrong way to ask. The number is the point: it is the
		// size of the remaining rot surface.
		t.Logf(
			"%d generator(s) cannot be guarded because they declare no "+
				"OUTPUT_PATH = Path(__file__).parent / \"...\" or accept no "+
				"--stdout; their corpora can rot undetected:\n  %s",
			len(unguardable), strings.Join(unguardable, "\n  "),
		)
	}
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
