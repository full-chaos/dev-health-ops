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
// TWO conventions exist in tests/fixtures/, and the first version of this file
// recognised only one -- the one I happen to write:
//
//	OUTPUT_PATH = Path(__file__).parent / "name.json"     10 generators
//	OUTPUT      = Path(__file__).with_name("name.json")    6 generators
//
// So "discovery" silently skipped six conforming generators, including
// generate_file_hotspots_python_golden.py, whose live comparison passes fine.
// That is the enumeration failure one level up: I replaced a hand-written list
// with a pattern, and then wrote a pattern matching my own house style. Found by
// a codex round, which ran the skipped ones directly and showed they work.
//
// Both forms are matched now. A third form should be ADDED here rather than the
// generator being rewritten to suit the matcher.
// Both quote styles. Python treats '...' and "..." identically, so recognising
// only double quotes made declaredOutputPath answer "no declaration" for a
// perfectly ordinary single-quoted one -- which matters most in
// TestExplicitCorpusPathsAreStillNeeded, where a false "no declaration" means a
// stale explicit entry is never reported. RE2 has no backreferences, so the
// single-quoted forms are separate patterns rather than a matched-quote group.
var outputPathPatterns = []*regexp.Regexp{
	regexp.MustCompile(`OUTPUT_PATH\s*=\s*Path\(__file__\)\.parent\s*/\s*"([^"]+)"`),
	regexp.MustCompile(`OUTPUT_PATH\s*=\s*Path\(__file__\)\.parent\s*/\s*'([^']+)'`),
	regexp.MustCompile(`OUTPUT\w*\s*=\s*Path\(__file__\)\.with_name\(\s*"([^"]+)"\s*\)`),
	regexp.MustCompile(`OUTPUT\w*\s*=\s*Path\(__file__\)\.with_name\(\s*'([^']+)'\s*\)`),
	// argparse `--out` with a REPO-RELATIVE default is also a declaration, just
	// in another syntax. Two generators arrived this way within a day, and
	// naming each in explicitCorpusPaths would be curating instances of a shape
	// the parser can simply read. Only tests/fixtures/ defaults count: a default
	// of /tmp/... names a scratch path, not the committed corpus, and treating
	// it as one would point the comparison at a file that is not in the tree.
	regexp.MustCompile(`add_argument\(\s*"--out"\s*,\s*default\s*=\s*"tests/fixtures/([^"]+)"`),
	regexp.MustCompile(`add_argument\(\s*'--out'\s*,\s*default\s*=\s*'tests/fixtures/([^']+)'`),
}

// explicitCorpusPaths names generators whose corpus path cannot be INFERRED
// from their source, but which are still fully checkable: they accept --stdout
// and their committed corpus is in the tree, so the guard can compare them once
// it is told where to look.
//
// This is deliberately NOT excludedGenerators. That map is for generators that
// cannot RUN (a missing module), and its self-check demands a missingModule.
// An undiscoverable-but-runnable generator has a different problem and needs the
// opposite treatment: naming the path here makes it GUARDED, which shrinks the
// rot surface the ratchet measures, where an exclusion would merely excuse it.
//
// An entry earns its place only while the path is genuinely uninferable --
// TestExplicitCorpusPathsAreStillNeeded below deletes the excuse when the
// generator starts declaring its own path.
var explicitCorpusPaths = map[string]string{
	// Declares its output as an argparse `--out` default of
	// /tmp/build_scope_parity_table.json, so no repo-relative constant exists to
	// match -- but it takes --stdout and its corpus is committed beside it.
	"generate_build_scope_parity_table.py": "build_scope_parity_table.json",
	// Its corpus is a GO PACKAGE's testdata, so it lives beside the package that
	// loads it rather than in tests/fixtures/ -- ordinary Go layout, and not
	// something to bend for this guard. No Path(__file__)-relative declaration can
	// name it, because the file is not beside the generator.
	//
	// Naming it here makes it GUARDED rather than excused: the ratchet's surface
	// shrinks by one instead of the corpus being permitted to rot. The generator
	// runs fine and takes --stdout, so excludedGenerators would be the wrong map
	// (its self-check demands a missingModule).
	"generate_scope_grammar_corpus.py": "../../internal/pythonparity/scopeparity/testdata/corpus_seed1.json",
}

func declaredOutputPath(source []byte) (string, bool) {
	for _, pattern := range outputPathPatterns {
		if match := pattern.FindSubmatch(source); match != nil {
			return string(match[1]), true
		}
	}
	return "", false
}

// corpusPathFor resolves a generator's corpus, by declaration or by explicit map.
func corpusPathFor(name string, source []byte) (string, bool) {
	if path, ok := declaredOutputPath(source); ok {
		return path, true
	}
	if path, ok := explicitCorpusPaths[name]; ok {
		return path, true
	}
	return "", false
}

// maxUnguardableGenerators is a RATCHET, not a target.
//
// Generators that declare no recognised output path or accept no --stdout cannot
// be discovered. Most belong to other lanes, so failing their build to enforce a
// convention they never agreed to is the wrong way to ask -- but letting the
// number grow silently is how the rot surface got to 22 in the first place.
//
// So the count may only go DOWN. Lower this when a generator is brought into the
// convention; if it rises, someone has added an undiscoverable corpus and this
// test says so. Tracked as CHAOS-4849.
const maxUnguardableGenerators = 9

// excludedGenerators names generators that are discoverable but CANNOT run in
// CI, with the reason and the condition that would remove the exclusion.
//
// This map had its first entry added by a codex round, and the finding is the
// counter-argument to this whole file: the hand-written enumeration I replaced
// was LOAD-BEARING in exactly one case. It had accidentally excluded
// generate_effort_golden.py, for a reason nobody had written down, and
// discovery restored it and would have broken go-quality.
//
// So discovery is not strictly better than enumeration. It converts a silent gap
// into a loud failure, which is an improvement only if the genuine exclusions
// are recorded -- and an exclusion that outlives its reason is the same rot as a
// stale divergence entry. Hence TestExcludedGeneratorsAreStillUnrunnable below,
// which fails when an exclusion becomes unnecessary.
var excludedGenerators = map[string]struct {
	reason        string
	missingModule string
	removeWhen    string
}{
	"generate_effort_golden.py": {
		reason: "imports work_graph.investment.materialize, which transitively " +
			"imports httpx2; the CI oracle closure is installed --no-deps and " +
			"pins httpx==0.28.1 but not httpx2, so this raises ModuleNotFoundError",
		missingModule: "httpx2",
		removeWhen:    "httpx2 is added to ci/requirements-live-python-oracles.txt",
	},
	"generate_repo_effort_allocation_golden.py": {
		reason: "same import as generate_effort_golden.py above -- both drive " +
			"functions from work_graph.investment.materialize, so both hit the " +
			"same missing httpx2 transitive",
		missingModule: "httpx2",
		removeWhen:    "httpx2 is added to ci/requirements-live-python-oracles.txt",
	},
	"generate_work_unit_label_golden.py": {
		reason: "same import as generate_effort_golden.py above -- drives " +
			"_resolve_work_unit_label from work_graph.investment.materialize, " +
			"same missing httpx2 transitive",
		missingModule: "httpx2",
		removeWhen:    "httpx2 is added to ci/requirements-live-python-oracles.txt",
	},
	// generate_scope_grammar_corpus.py's exclusion (limits missing from the
	// closure) was removed here CHAOS-4945, once `limits` (and its own
	// transitives, deprecated and wrapt) were added to
	// ci/requirements-live-python-oracles.txt. Its explicitCorpusPaths entry
	// above already named its corpus, so it is guarded by discovery with no
	// further change -- exactly as this comment on that entry predicted, and
	// exactly as main's own now-superseded comment on this entry predicted
	// too ("When limits lands this entry goes and the corpus becomes guarded
	// with no other change") -- resolved by taking the deletion, not the
	// comment expansion, since `limits` landing is this PR's own change.
}

func TestEveryDiscoverableCorpusStillMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	// A PROOF MARKER, because Go's package-level `ok` counts a SKIPPED test as
	// passing. Without one, a skip and a run are indistinguishable in the only
	// output most readers see -- which is not hypothetical: this guard skipped
	// in my own local verification and reported `ok` while the ratchet was
	// failing, and a codex round found what my green had hidden.
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	defer func() {
		if t.Failed() {
			return
		}
		if err := os.WriteFile(
			filepath.Join(proofDirectory, "workgraph-units-corpus-discovery"),
			[]byte("executed"), 0o644,
		); err != nil {
			t.Fatalf("write proof marker: %v", err)
		}
	}()

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

		if excluded, skip := excludedGenerators[name]; skip {
			t.Logf("excluded %s: %s (remove when: %s)", name, excluded.reason, excluded.removeWhen)
			continue
		}

		source, err := os.ReadFile(generator)
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		fixtureName, declared := corpusPathFor(name, source)
		if !declared || !strings.Contains(string(source), "--stdout") {
			unguardable = append(unguardable, name)
			continue
		}

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
		t.Logf(
			"%d generator(s) are undiscoverable (no recognised output-path "+
				"declaration, or no --stdout); their corpora can rot undetected:\n  %s",
			len(unguardable), strings.Join(unguardable, "\n  "),
		)
	}
	// The RATCHET. t.Logf is invisible in non-verbose CI, so the list above
	// informs a human reading the log and nothing else -- which is how the rot
	// surface reached 22 unnoticed. This assertion is the part that acts.
	if len(unguardable) > maxUnguardableGenerators {
		t.Errorf(
			"%d undiscoverable generators, but the ratchet allows at most %d. A "+
				"new corpus has been added that this guard cannot check. Either "+
				"give it a recognised output-path declaration and --stdout, or, "+
				"if it runs but declares no repo-relative path, name its corpus "+
				"in explicitCorpusPaths. Do NOT use excludedGenerators: that map "+
				"is for generators that cannot RUN and its self-check demands a "+
				"missingModule. Do NOT raise the ratchet: it exists to make the "+
				"rot surface monotonically shrink.",
			len(unguardable), maxUnguardableGenerators,
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

// TestExcludedGeneratorsAreStillUnrunnable fails when an exclusion stops being
// necessary.
//
// An exclusion is a claim about the environment, and claims about the
// environment go stale silently. The entry for generate_effort_golden.py exists
// because the CI oracle closure lacks httpx2; the day someone adds it, that
// corpus becomes guardable and the exclusion becomes a hole nobody is watching.
//
// # WHY THIS READS THE REQUIREMENTS FILE AND NOT THE INTERPRETER
//
// The first version of this test imported the module and failed if the import
// succeeded. That has INVERTED polarity: httpx2 is present in a developer's
// local venv, so the test would have been red on every machine and green in the
// only environment the exclusion is about. A check whose result depends on where
// it runs, in the opposite direction to the thing it guards, is worse than no
// check.
//
// The exclusion is a statement about ci/requirements-live-python-oracles.txt, so
// that file is what gets read. Environment-independent, and it tests the exact
// condition recorded in removeWhen.
func TestExcludedGeneratorsAreStillUnrunnable(t *testing.T) {
	if len(excludedGenerators) == 0 {
		t.Skip("no exclusions to verify")
	}

	repoRoot := repositoryRootPath(t)
	closurePath := filepath.Join(repoRoot, "ci", "requirements-live-python-oracles.txt")
	closure, err := os.ReadFile(closurePath)
	if err != nil {
		t.Fatalf("read the live-oracle closure: %v", err)
	}

	for name, excluded := range excludedGenerators {
		if excluded.missingModule == "" {
			t.Errorf(
				"%s is excluded with no missingModule, so the exclusion cannot be "+
					"checked and will outlive its reason silently", name,
			)
			continue
		}
		t.Run(name, func(t *testing.T) {
			for _, line := range strings.Split(string(closure), "\n") {
				requirement := strings.TrimSpace(line)
				if requirement == "" || strings.HasPrefix(requirement, "#") {
					continue
				}
				// Match the distribution name only: "httpx2==1.0" must match
				// while "httpx==0.28.1" must not, which a substring test gets
				// wrong in exactly the direction that matters here.
				distribution := requirement
				if index := strings.IndexAny(distribution, "=<>!~[; "); index >= 0 {
					distribution = distribution[:index]
				}
				if strings.EqualFold(strings.TrimSpace(distribution), excluded.missingModule) {
					t.Errorf(
						"%s is excluded because %q, but %q is NOW in %s. The "+
							"exclusion is stale: remove the entry and let discovery "+
							"guard this corpus. (Recorded removal condition: %s)",
						name, excluded.reason, excluded.missingModule,
						"ci/requirements-live-python-oracles.txt", excluded.removeWhen,
					)
				}
			}
		})
	}
}

// TestExplicitCorpusPathsAreStillNeeded deletes an excuse the moment it stops
// being one.
//
// An explicit entry exists only because the generator declares no inferable
// output path. If it starts declaring one, the entry is now a second source of
// truth that can disagree with the source -- the same rot as a stale exclusion.
func TestExplicitCorpusPathsAreStillNeeded(t *testing.T) {
	if len(explicitCorpusPaths) == 0 {
		t.Skip("no explicit corpus paths to verify")
	}
	fixturesDir := filepath.Join(repositoryRootPath(t), "tests", "fixtures")
	for name, corpus := range explicitCorpusPaths {
		t.Run(name, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join(fixturesDir, name))
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			if declared, ok := declaredOutputPath(source); ok {
				t.Errorf(
					"%s now declares its own output path (%q), so its "+
						"explicitCorpusPaths entry is stale and must be deleted "+
						"rather than left to disagree with the source",
					name, declared,
				)
			}
			if !strings.Contains(string(source), "--stdout") {
				t.Errorf("%s no longer accepts --stdout, so naming its corpus "+
					"cannot make it checkable; move it to the ratchet instead", name)
			}
			if _, err := os.Stat(filepath.Join(fixturesDir, corpus)); err != nil {
				t.Errorf("%s names corpus %q which is not present: %v", name, corpus, err)
			}
		})
	}
}
