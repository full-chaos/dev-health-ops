// Command goldenrecord records the golden files of venue oracles that use
// venueoracle.OpenGolden: the Python plane's answers, executed on the pinned
// Python-bearing build, frozen for a Go-only replay.
//
// Recording is three steps and a golden lands only if all of them pass:
//
//  1. RECORD: the named tests run with DHO_VENUE_GOLDEN_UPDATE=1 against the
//     clean checkout DHO_VENUE_GOLDEN_PYTHON_ROOT names; each writes a
//     candidate (<golden>.recording) from Finish, in the test body.
//  2. REPLAY: the same tests run again in a fresh process with
//     DHO_VENUE_GOLDEN_CANDIDATE=1, which replays the candidate frozen.
//  3. PROMOTE: only if both passed, each candidate is renamed onto its golden and
//     its digest is pinned in the package's tests.
//
// A test failure at any point, including a cleanup that fails after Finish,
// deletes the candidates and leaves every golden as it was.
//
// A venue golden holds the key of the Python settings its test declares
// (venueoracle.Options.PythonEnv). -backfill-python-env adds that key to a
// golden recorded before the key existed. It records nothing and runs no
// Python. It first executes what the golden's test declares now and what it
// declared at the commit that holds the golden's bytes, and refuses a golden
// whose two keys differ or whose recording commit cannot be found or built
// (BackfillPythonEnv). Only then is the candidate the golden with exactly that
// one header field added, and REPLAY and PROMOTE run as above.
//
// Usage, from the repository root:
//
//	go run ./internal/testsupport/venueoracle/goldenrecord \
//	    -pkg ./internal/apiservice/admin/ -test '^TestX$' -python-root <clean worktree at the pinned build>
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const candidateSuffix = ".recording"

// Config is one recording.
type Config struct {
	// Root is the repository root: go test runs there and Package is relative to it.
	Root string
	// Package is the go package pattern of the tests, e.g. ./internal/apiservice/admin/.
	Package string
	// Test is the -run regular expression selecting the oracles to record.
	Test string
	// PythonRoot is the clean checkout at the pinned build Python runs from.
	PythonRoot string
	// AllowDrop lets a re-record remove requests or row comparisons the existing
	// golden holds (an intended change of the oracle). Without it the candidate
	// must keep every request name and row name of the golden it replaces.
	AllowDrop bool
	// PassEnv names ambient variables a recording needs beyond the fixed set
	// (recordingEnv): a credential a producer reads, by name only.
	PassEnv []string
	// Run executes one go test invocation with the extra environment. The
	// default runs `go test -tags=integration` in Root.
	Run func(cfg Config, env []string) error
	// Keys, RecordingCommit and Tree are what BackfillPythonEnv reads the
	// declared Python settings with. Keys returns, for the venue tests Test
	// selects in Package of the source tree, the key of the settings each
	// declares (test name to key); RecordingCommit returns the commit that
	// holds exactly the bytes of a golden (its path relative to Root); Tree
	// returns that commit's source tree and a cleanup. The defaults execute
	// the tests through a go test overlay, ask git, and unpack git archive.
	Keys            func(cfg Config, tree string) (map[string]string, error)
	RecordingCommit func(cfg Config, golden string) (string, error)
	Tree            func(cfg Config, commit string) (string, func(), error)
}

// Result names what one recording promoted.
type Result struct {
	Promoted []Promotion
	// Shown is, for a backfill, one line per golden: its recording commit and
	// the key its test declared there and declares now.
	Shown []string
}

// Promotion is one golden that landed.
type Promotion struct {
	Path      string
	OldDigest string
	NewDigest string
	// Pinned lists the test files whose pinned digest was updated.
	Pinned []string
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Root, "root", ".", "repository root")
	flag.StringVar(&cfg.Package, "pkg", "", "go package of the oracles, relative to the root")
	flag.StringVar(&cfg.Test, "test", "", "-run regular expression of the oracles to record")
	flag.StringVar(&cfg.PythonRoot, "python-root", "", "clean checkout at the pinned Python-bearing build")
	flag.BoolVar(&cfg.AllowDrop, "allow-drop", false, "let a re-record drop requests or row comparisons the existing golden holds")
	passEnv := flag.String("pass-env", "", "comma-separated NAMES of ambient variables to pass to the recording beyond the fixed set (names only, never values)")
	backfill := flag.Bool("backfill-python-env", false, "do not record: add the key of the venue's Python settings to the selected venue goldens that hold none, each only after its test is shown to declare now the settings it declared at the golden's recording commit (no Python runs)")
	flag.Parse()
	for _, name := range strings.Split(*passEnv, ",") {
		if name = strings.TrimSpace(name); name != "" {
			cfg.PassEnv = append(cfg.PassEnv, name)
		}
	}
	if *backfill {
		if cfg.Package == "" || cfg.Test == "" || cfg.PythonRoot != "" {
			fmt.Fprintln(os.Stderr, "goldenrecord: -backfill-python-env takes -pkg and -test, and no -python-root (it runs no Python)")
			os.Exit(2)
		}
		result, err := BackfillPythonEnv(context.Background(), cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "goldenrecord: nothing was changed: %v\n", err)
			os.Exit(1)
		}
		if len(result.Promoted) == 0 {
			fmt.Println("nothing to backfill: every selected venue golden already holds the key of its venue's Python settings")
		}
		for _, line := range result.Shown {
			fmt.Println(line)
		}
		for _, promotion := range result.Promoted {
			fmt.Printf("backfilled %s\n  sha256 %s (was %s)\n", promotion.Path, promotion.NewDigest, orNone(promotion.OldDigest))
		}
		return
	}
	if cfg.Package == "" || cfg.Test == "" || cfg.PythonRoot == "" {
		fmt.Fprintln(os.Stderr, "goldenrecord: -pkg, -test and -python-root are required")
		os.Exit(2)
	}
	result, err := Record(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "goldenrecord: nothing was promoted: %v\n", err)
		os.Exit(1)
	}
	for _, promotion := range result.Promoted {
		fmt.Printf("promoted %s\n  sha256 %s (was %s)\n", promotion.Path, promotion.NewDigest, orNone(promotion.OldDigest))
		for _, file := range promotion.Pinned {
			fmt.Printf("  pinned in %s\n", file)
		}
	}
}

func orNone(digest string) string {
	if digest == "" {
		return "none: a new golden"
	}
	return digest
}

// Record runs the three steps and returns what it promoted, or an error with
// every golden untouched and every candidate deleted.
func Record(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Run == nil {
		cfg.Run = goTest
	}
	packageDir := filepath.Join(cfg.Root, cfg.Package)
	if _, err := os.Stat(packageDir); err != nil {
		return Result{}, err
	}
	stale, err := candidates(packageDir)
	if err != nil {
		return Result{}, err
	}
	if len(stale) > 0 {
		return Result{}, fmt.Errorf("candidates from an earlier run are still on disk (%s): delete them, then record again", stale[0])
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		dir, err := os.MkdirTemp("", "goldenrecord-proof-")
		if err != nil {
			return Result{}, err
		}
		defer os.RemoveAll(dir)
		proofDir = dir
	}
	passed := append([]string{}, cfg.PassEnv...)
	sort.Strings(passed)
	base := []string{"DEV_HEALTH_LIVE_PYTHON_ORACLES=1", "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR=" + proofDir, passedEnvName + "=" + strings.Join(passed, ",")}
	discard := func() { removeAll(packageDir) }

	// A bytecode cache in the pinned checkout can run code older than its source:
	// clear it, and stop the recording writing a new one.
	if err := clearBytecode(filepath.Join(cfg.PythonRoot, "src")); err != nil {
		return Result{}, fmt.Errorf("clearing the bytecode cache of %s: %w", cfg.PythonRoot, err)
	}
	if err := cfg.Run(cfg, append(append([]string{}, base...), "DHO_VENUE_GOLDEN_UPDATE=1", "DHO_VENUE_GOLDEN_PYTHON_ROOT="+cfg.PythonRoot, "PYTHONDONTWRITEBYTECODE=1")); err != nil {
		discard()
		return Result{}, fmt.Errorf("the recording run failed (a golden is only recorded from a run that passed every check, cleanups included): %w", err)
	}
	found, err := candidates(packageDir)
	if err != nil {
		discard()
		return Result{}, err
	}
	if len(found) == 0 {
		return Result{}, errors.New("the recording run passed but wrote no candidate: the selected tests do not use venueoracle.OpenGolden or did not reach Finish")
	}
	// The bytes the replay is about to check are the bytes that get promoted.
	replayed := map[string][]byte{}
	for _, candidate := range found {
		raw, err := os.ReadFile(candidate)
		if err != nil {
			discard()
			return Result{}, err
		}
		replayed[candidate] = raw
	}
	if err := cfg.Run(cfg, append(append([]string{}, base...), "DHO_VENUE_GOLDEN_CANDIDATE=1")); err != nil {
		discard()
		return Result{}, fmt.Errorf("the fresh-process replay of the candidates failed: %w", err)
	}
	for _, candidate := range found {
		after, err := os.ReadFile(candidate)
		if err != nil || !bytes.Equal(after, replayed[candidate]) {
			discard()
			return Result{}, fmt.Errorf("the candidate %s changed after it was replayed: only the replayed bytes may be promoted", candidate)
		}
	}

	plan, result, err := planPromotion(packageDir, found, replayed, cfg.AllowDrop)
	if err != nil {
		discard()
		return Result{}, err
	}
	if err := apply(plan); err != nil {
		discard()
		return Result{}, err
	}
	for _, candidate := range found {
		_ = os.Remove(candidate)
	}
	return result, nil
}

// edit is one file the promotion writes; before is what it held (existed false
// when it did not exist), so a failed promotion can put it back.
type edit struct {
	path    string
	before  []byte
	existed bool
	after   []byte
}

// placeholder is what the test of a golden that does not exist yet pins until
// the first record: "PIN:" and the golden's file name without its extension.
func placeholder(final string) string {
	name := filepath.Base(final)
	return "PIN:" + strings.TrimSuffix(name, filepath.Ext(name))
}

// planPromotion computes every write the promotion makes, in memory, before
// any is made: each golden's bytes and the pin of its digest in the package's
// tests. A golden no test pins is an error (a promoted golden the frozen replay
// would refuse is not a success).
func planPromotion(packageDir string, found []string, replayed map[string][]byte, allowDrop bool) ([]edit, Result, error) {
	testFiles, err := filepath.Glob(filepath.Join(packageDir, "*_test.go"))
	if err != nil {
		return nil, Result{}, err
	}
	contents := map[string]string{}
	original := map[string][]byte{}
	for _, file := range testFiles {
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, Result{}, err
		}
		original[file] = raw
		contents[file] = string(raw)
	}
	var plan []edit
	var result Result
	for _, candidate := range found {
		final := strings.TrimSuffix(candidate, candidateSuffix)
		promotion := Promotion{Path: final, NewDigest: digest(replayed[candidate])}
		golden := edit{path: final, after: replayed[candidate]}
		if raw, err := os.ReadFile(final); err == nil {
			golden.before, golden.existed = raw, true
			promotion.OldDigest = digest(raw)
			if dropped := droppedCoverage(raw, replayed[candidate]); len(dropped) > 0 && !allowDrop {
				return nil, Result{}, fmt.Errorf("the candidate for %s no longer holds what the golden it replaces holds: %s; a re-record must not silently remove coverage (an intended removal: -allow-drop)", final, strings.Join(dropped, ", "))
			}
		}
		needle := promotion.OldDigest
		if needle == "" {
			needle = placeholder(final)
		}
		for _, file := range testFiles {
			if strings.Contains(contents[file], needle) {
				contents[file] = strings.ReplaceAll(contents[file], needle, promotion.NewDigest)
				promotion.Pinned = append(promotion.Pinned, file)
			}
		}
		if len(promotion.Pinned) == 0 {
			return nil, Result{}, fmt.Errorf("no test in %s pins %s (looked for %s): a golden no test pins would be refused by the frozen replay; for a new golden give its spec the digest %q", packageDir, final, needle, placeholder(final))
		}
		plan = append(plan, golden)
		result.Promoted = append(result.Promoted, promotion)
	}
	for _, file := range testFiles {
		if contents[file] != string(original[file]) {
			plan = append(plan, edit{path: file, before: original[file], existed: true, after: []byte(contents[file])})
		}
	}
	return plan, result, nil
}

// apply makes the planned writes, each by a temporary file renamed into place,
// and puts every file back as it was if any write fails.
func apply(plan []edit) error {
	for index, step := range plan {
		if err := writeAtomic(step.path, step.after); err != nil {
			rollback(plan[:index])
			return fmt.Errorf("promotion failed at %s (everything written before it was put back): %w", step.path, err)
		}
	}
	return nil
}

func rollback(done []edit) {
	for index := len(done) - 1; index >= 0; index-- {
		step := done[index]
		if step.existed {
			_ = writeAtomic(step.path, step.before)
		} else {
			_ = os.Remove(step.path)
		}
	}
}

func writeAtomic(path string, content []byte) error {
	temp, err := os.CreateTemp(filepath.Dir(path), ".goldenrecord-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	if _, err := temp.Write(content); err != nil {
		temp.Close()
		os.Remove(name)
		return err
	}
	if err := temp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Chmod(name, 0o644); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// candidates lists the candidate files under dir.
func candidates(dir string) ([]string, error) {
	var found []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, candidateSuffix) {
			found = append(found, path)
		}
		return nil
	})
	return found, err
}

// BackfillPythonEnv adds the key of its venue's Python settings to the header
// of each selected venue golden that was recorded before a golden kept that
// key. It records nothing and runs no Python, and it adds a key only where it
// can show the golden was recorded under the settings its test declares now:
//
//  1. It reads the key each selected venue test declares now (Config.Keys).
//  2. For each golden of those tests that holds no key, it finds the commit
//     that holds exactly the golden's bytes (the recording commit), builds
//     that commit's tree and reads the key the test declared there.
//  3. A golden whose two keys differ, whose test declared no venue settings at
//     its recording commit, or whose recording commit cannot be found or built
//     is refused, and nothing is changed: record it again.
//  4. Otherwise the candidate is the golden with exactly that one header
//     field added; the frozen tests replay the candidates in a fresh process,
//     and only then are they promoted and re-pinned.
func BackfillPythonEnv(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Run == nil {
		cfg.Run = goTest
	}
	if cfg.Keys == nil {
		cfg.Keys = declaredKeys
	}
	if cfg.RecordingCommit == nil {
		cfg.RecordingCommit = recordingCommit
	}
	if cfg.Tree == nil {
		cfg.Tree = commitTree
	}
	packageDir := filepath.Join(cfg.Root, cfg.Package)
	if _, err := os.Stat(packageDir); err != nil {
		return Result{}, err
	}
	stale, err := candidates(packageDir)
	if err != nil {
		return Result{}, err
	}
	if len(stale) > 0 {
		return Result{}, fmt.Errorf("candidates from an earlier run are still on disk (%s): delete them, then run again", stale[0])
	}
	now, err := cfg.Keys(cfg, cfg.Root)
	if err != nil {
		return Result{}, fmt.Errorf("the Python settings the selected tests declare now could not be read: %w", err)
	}
	keyless, err := keylessGoldens(packageDir, now)
	if err != nil {
		return Result{}, err
	}
	if len(keyless) == 0 {
		return Result{}, nil
	}
	// Every golden is checked before anything is written.
	atCommit := map[string]map[string]string{}
	written := map[string][]byte{}
	tests := map[string]bool{}
	var found, shown []string
	for _, golden := range keyless {
		relative, err := filepath.Rel(cfg.Root, golden.path)
		if err != nil {
			return Result{}, err
		}
		commit, err := cfg.RecordingCommit(cfg, relative)
		if err != nil {
			return Result{}, fmt.Errorf("golden %s: its recording commit was not found (%w): the key is not added; record it again", relative, err)
		}
		then, known := atCommit[commit]
		if !known {
			tree, cleanup, err := cfg.Tree(cfg, commit)
			if err != nil {
				return Result{}, fmt.Errorf("golden %s: the tree of its recording commit %s could not be built (%w): the key is not added; record it again", relative, commit, err)
			}
			then, err = cfg.Keys(cfg, tree)
			cleanup()
			if err != nil {
				return Result{}, fmt.Errorf("golden %s: the Python settings its test declared at its recording commit %s could not be read (%w): the key is not added; record it again", relative, commit, err)
			}
			atCommit[commit] = then
		}
		recorded, declared := then[golden.test]
		if !declared {
			return Result{}, fmt.Errorf("golden %s: its test %s declared no venue settings at its recording commit %s: the key is not added; record it again", relative, golden.test, commit)
		}
		if recorded != now[golden.test] {
			return Result{}, fmt.Errorf("golden %s was recorded at %s under other Python settings than its test %s declares now (key %.12s then, %.12s now): its answers are not the ones the real Python api gives under the settings of today, so the key is not added; record it again",
				relative, commit, golden.test, recorded, now[golden.test])
		}
		candidate, err := venueoracle.WithPythonEnvKey(golden.raw, now[golden.test])
		if err != nil {
			return Result{}, fmt.Errorf("golden %s: %w", relative, err)
		}
		if err := onlyPythonEnvAdded(golden.raw, candidate); err != nil {
			return Result{}, fmt.Errorf("golden %s: %w", relative, err)
		}
		shown = append(shown, fmt.Sprintf("%s: recorded at %s; its test %s declared there the Python settings it declares now (key %.12s)", relative, commit, golden.test, recorded))
		path := golden.path + candidateSuffix
		written[path] = candidate
		found = append(found, path)
		tests[strings.SplitN(golden.test, "/", 2)[0]] = true
	}
	discard := func() { removeAll(packageDir) }
	for _, path := range found {
		if err := os.WriteFile(path, written[path], 0o644); err != nil {
			discard()
			return Result{}, err
		}
	}
	proofDir, err := os.MkdirTemp("", "goldenrecord-proof-")
	if err != nil {
		discard()
		return Result{}, err
	}
	defer os.RemoveAll(proofDir)
	// The replay runs the tests of the backfilled goldens only: a test whose
	// golden already holds its key has no candidate to replay.
	names := make([]string, 0, len(tests))
	for name := range tests {
		names = append(names, regexp.QuoteMeta(name))
	}
	sort.Strings(names)
	replay := cfg
	replay.Test = "^(" + strings.Join(names, "|") + ")$"
	if err := cfg.Run(replay, []string{"DEV_HEALTH_LIVE_PYTHON_ORACLES=1", "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR=" + proofDir, "DHO_VENUE_GOLDEN_CANDIDATE=1"}); err != nil {
		discard()
		return Result{}, fmt.Errorf("the fresh-process replay of the candidates failed: %w", err)
	}
	for _, candidate := range found {
		after, err := os.ReadFile(candidate)
		if err != nil || !bytes.Equal(after, written[candidate]) {
			discard()
			return Result{}, fmt.Errorf("the candidate %s changed after it was replayed: only the replayed bytes may be promoted", candidate)
		}
	}
	plan, result, err := planPromotion(packageDir, found, written, false)
	if err != nil {
		discard()
		return Result{}, err
	}
	if err := apply(plan); err != nil {
		discard()
		return Result{}, err
	}
	for _, candidate := range found {
		_ = os.Remove(candidate)
	}
	result.Shown = shown
	return result, nil
}

// keylessGolden is a venue golden that holds no key of its venue's Python settings.
type keylessGolden struct {
	path, test string
	raw        []byte
}

// keylessGoldens lists, in path order, the golden files under dir that hold no
// key and belong to a test in keys (a venue test the run selected). A golden
// of another test is not a venue golden of this run and is left alone.
func keylessGoldens(dir string, keys map[string]string) ([]keylessGolden, error) {
	var out []keylessGolden
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var file struct {
			Header struct {
				Test        string `json:"test"`
				PythonBuild string `json:"python_build"`
				PythonEnv   string `json:"python_env"`
			} `json:"header"`
		}
		if json.Unmarshal(raw, &file) != nil || file.Header.PythonBuild == "" || file.Header.PythonEnv != "" {
			return nil
		}
		if _, selected := keys[file.Header.Test]; selected {
			out = append(out, keylessGolden{path: path, test: file.Header.Test, raw: raw})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, err
}

// reportEnv names the file the overlay's report function appends to.
const reportEnv = "DHO_VENUE_PYTHON_ENV_REPORT"

// reportSource is a file declaredKeys adds to package venueoracle through a go
// test overlay. It is never part of a tree: with it, Start writes the key of
// the settings the test declares and stops the test before anything is built.
const reportSource = `package venueoracle

import (
	"fmt"
	"os"
	"testing"
)

func venuePythonEnvReport(t *testing.T, options Options) {
	key, err := pythonEnvKey(pythonPlaneEnv(options, nil))
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(os.Getenv("` + reportEnv + `"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(file, "%s\t%s\n", t.Name(), key); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	t.SkipNow()
}
`

// startHead is where the report call goes: the first statement of Start.
var startHead = regexp.MustCompile(`func Start\(t \*testing\.T, ctx context\.Context, options Options\) \*Venue \{\n\tt\.Helper\(\)\n`)

// legacyPlaneEnv is the statement with which venueoracle.Start built the
// Python plane's environment in every commit before a golden kept its key. A
// golden recorded then got exactly this environment around its test's JWT key
// and PythonEnv; pythonPlaneEnv of the working tree must give the same one (a
// test in package venueoracle holds that), so the key the working tree
// computes for a test's Options is the key of what the recording ran under.
const legacyPlaneEnv = `v.pythonEnv = append([]string{"PYTHONPATH=" + filepath.Join(options.Root, "src"), "POSTGRES_URI=" + async,
		"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=" + v.PythonValkeyURI,
		"CLICKHOUSE_URI=" + v.AdminClickHouseHTTPURI(t, v.PythonClickHouseDB)},
		options.PythonEnv...)`

var (
	planeEnvStatement = regexp.MustCompile(`(?s)v\.pythonEnv = append\(\[\]string\{.*?options\.PythonEnv\.\.\.\)`)
	planeEnvFunction  = regexp.MustCompile(`(?s)\nfunc pythonPlaneEnv\(options Options, perRun map\[string\]string\) \[\]string \{.*?\n\}\n`)
	commentLine       = regexp.MustCompile(`(?m)^\s*//.*$`)
	blank             = regexp.MustCompile(`\s+`)
)

// codeText is source with its comment lines and all white space taken out.
func codeText(source string) string {
	return blank.ReplaceAllString(commentLine.ReplaceAllString(source, ""), "")
}

// harnessEnvErr is an error unless the harness of tree hands the Python plane
// the environment the harness of root does. A tree from before the key built
// it in Start with the legacy statement; a later tree builds it in
// pythonPlaneEnv, which must be the function of root. A tree that does
// neither cannot be shown to have recorded under today's harness settings.
func harnessEnvErr(root, tree string) error {
	harness := filepath.Join("internal", "testsupport", "venueoracle")
	start, err := os.ReadFile(filepath.Join(tree, harness, "venueoracle.go"))
	if err != nil {
		return err
	}
	if statement := planeEnvStatement.Find(start); statement != nil {
		if codeText(string(statement)) != codeText(legacyPlaneEnv) {
			return errors.New("the harness of that commit handed the Python plane another environment than the one known for goldens from before the key")
		}
		return nil
	}
	theirs, err := os.ReadFile(filepath.Join(tree, harness, "pythonenv.go"))
	if err != nil {
		return fmt.Errorf("the harness of that commit builds the Python plane's environment in a way this tool does not know: %w", err)
	}
	ours, err := os.ReadFile(filepath.Join(root, harness, "pythonenv.go"))
	if err != nil {
		return err
	}
	then, now := planeEnvFunction.Find(theirs), planeEnvFunction.Find(ours)
	if then == nil || now == nil || codeText(string(then)) != codeText(string(now)) {
		return errors.New("the harness of that commit handed the Python plane another environment than the harness of the working tree does")
	}
	return nil
}

// declaredKeys is the default Config.Keys: it runs the selected tests of the
// package in tree with a go test overlay that makes venueoracle.Start write
// the key of the Python plane's environment for the running test's Options
// (pythonEnvKey over pythonPlaneEnv) and stop, so it executes what each test
// declares and builds no venue. The two functions are always the ones of
// Root, so the keys of two trees compare; that the harness of tree handed the
// Python plane the environment those functions describe is checked first
// (harnessEnvErr).
func declaredKeys(cfg Config, tree string) (map[string]string, error) {
	work, err := os.MkdirTemp("", "goldenrecord-keys-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)
	tree, err = filepath.Abs(tree)
	if err != nil {
		return nil, err
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	if tree != root {
		if err := harnessEnvErr(root, tree); err != nil {
			return nil, err
		}
	}
	harness := filepath.Join(tree, "internal", "testsupport", "venueoracle")
	source, err := os.ReadFile(filepath.Join(harness, "venueoracle.go"))
	if err != nil {
		return nil, err
	}
	at := startHead.FindIndex(source)
	if at == nil {
		return nil, fmt.Errorf("%s has no venueoracle.Start of the form the report is put into", tree)
	}
	patched := append(append(append([]byte{}, source[:at[1]]...), "\tvenuePythonEnvReport(t, options)\n"...), source[at[1]:]...)
	keyFunction, err := filepath.Abs(filepath.Join(cfg.Root, "internal", "testsupport", "venueoracle", "pythonenv.go"))
	if err != nil {
		return nil, err
	}
	overlay, err := json.Marshal(map[string]map[string]string{"Replace": {
		filepath.Join(harness, "venueoracle.go"):          filepath.Join(work, "venueoracle.go"),
		filepath.Join(harness, "zz_python_env_report.go"): filepath.Join(work, "report.go"),
		filepath.Join(harness, "pythonenv.go"):            keyFunction,
	}})
	if err != nil {
		return nil, err
	}
	for name, content := range map[string][]byte{"venueoracle.go": patched, "report.go": []byte(reportSource), "overlay.json": overlay} {
		if err := os.WriteFile(filepath.Join(work, name), content, 0o644); err != nil {
			return nil, err
		}
	}
	report := filepath.Join(work, "report.tsv")
	command := exec.Command("go", "test", "-tags=integration", "-count=1", "-overlay", filepath.Join(work, "overlay.json"), "-run", cfg.Test, cfg.Package)
	command.Dir = tree
	command.Env = append(recordingEnv(os.Environ(), nil), "DEV_HEALTH_LIVE_PYTHON_ORACLES=1", reportEnv+"="+report)
	if output, err := command.CombinedOutput(); err != nil {
		text := string(output)
		if len(text) > 4000 {
			text = text[len(text)-4000:]
		}
		return nil, fmt.Errorf("go test of %s in %s: %w\n%s", cfg.Package, tree, err, text)
	}
	raw, err := os.ReadFile(report)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	keys := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		test, key, ok := strings.Cut(line, "\t")
		if !ok || !keyText.MatchString(key) {
			return nil, fmt.Errorf("the report line %q is not a test and a key", line)
		}
		if earlier, seen := keys[test]; seen && earlier != key {
			return nil, fmt.Errorf("test %s reported two keys", test)
		}
		keys[test] = key
	}
	return keys, nil
}

// git runs one git command in root and returns its trimmed output.
func git(root string, args ...string) (string, error) {
	command := exec.Command("git", args...)
	command.Dir = root
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(output)), nil
}

// recordingCommit is the default Config.RecordingCommit: the newest commit in
// the history of HEAD that changed golden and holds exactly the bytes the
// file has on disk. A golden that is in no commit as it is on disk has no
// recording commit.
func recordingCommit(cfg Config, golden string) (string, error) {
	path := filepath.ToSlash(golden)
	want, err := git(cfg.Root, "hash-object", "--", path)
	if err != nil {
		return "", err
	}
	history, err := git(cfg.Root, "log", "--format=%H", "--", path)
	if err != nil {
		return "", err
	}
	commit, found := newestCommitHolding(want, strings.Fields(history), func(commit string) (string, bool) {
		blob, err := git(cfg.Root, "rev-parse", "--verify", "--quiet", commit+":"+path)
		return blob, err == nil
	})
	if !found {
		return "", errors.New("no commit holds the golden as it is on disk")
	}
	return commit, nil
}

// newestCommitHolding is the first commit of history (newest first) whose blob
// of the file is want.
func newestCommitHolding(want string, history []string, blobAt func(commit string) (string, bool)) (string, bool) {
	for _, commit := range history {
		if blob, ok := blobAt(commit); ok && blob == want {
			return commit, true
		}
	}
	return "", false
}

// commitTree is the default Config.Tree: the files of commit, from git
// archive, in a temporary directory. It writes nothing to the repository.
func commitTree(cfg Config, commit string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "goldenrecord-tree-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	command := exec.Command("git", "archive", "--format=tar", commit)
	command.Dir = cfg.Root
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stream, err := command.StdoutPipe()
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if err := command.Start(); err != nil {
		cleanup()
		return "", nil, err
	}
	extractErr := extractTar(stream, dir)
	if extractErr != nil {
		_, _ = io.Copy(io.Discard, stream)
	}
	if err := command.Wait(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("git archive %s: %w: %s", commit, err, strings.TrimSpace(stderr.String()))
	}
	if extractErr != nil {
		cleanup()
		return "", nil, extractErr
	}
	return dir, cleanup, nil
}

// extractTar writes the directories, files and symbolic links of a tar stream under dir.
func extractTar(stream io.Reader, dir string) error {
	reader := tar.NewReader(stream)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		if !filepath.IsLocal(header.Name) {
			return fmt.Errorf("the archive names a path outside the tree: %q", header.Name)
		}
		target := filepath.Join(dir, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(header.Mode)&0o777|0o600)
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, reader); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
		default:
			return fmt.Errorf("the archive holds %q of a kind this tool does not write (%c)", header.Name, header.Typeflag)
		}
	}
}

var keyText = regexp.MustCompile(`^[0-9a-f]{64}$`)

// onlyPythonEnvAdded is an error unless candidate is original with exactly
// one change: header.python_env added, holding a key.
func onlyPythonEnvAdded(original, candidate []byte) error {
	decode := func(raw []byte) (map[string]any, error) {
		var out map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&out); err != nil {
			return nil, err
		}
		return out, nil
	}
	before, err := decode(original)
	if err != nil {
		return fmt.Errorf("the golden does not decode: %w", err)
	}
	after, err := decode(candidate)
	if err != nil {
		return fmt.Errorf("the candidate does not decode: %w", err)
	}
	header, _ := after["header"].(map[string]any)
	key, _ := header["python_env"].(string)
	if !keyText.MatchString(key) {
		return errors.New("the candidate's header holds no python_env key")
	}
	if oldHeader, _ := before["header"].(map[string]any); oldHeader["python_env"] != nil {
		return errors.New("the golden already holds a python_env key: a backfill never replaces one")
	}
	delete(header, "python_env")
	if !reflect.DeepEqual(before, after) {
		return errors.New("the candidate differs from the golden in more than the python_env key of its header: a backfill touches no answer")
	}
	return nil
}

func removeAll(dir string) {
	found, _ := candidates(dir)
	for _, path := range found {
		_ = os.Remove(path)
	}
}

// passedNames are the ambient variables a recording and its replay run under,
// by exact name: what the Go toolchain, the container runtime and the network
// need. Nothing else of the ambient environment reaches the tests (no family
// is passed by prefix), so no ambient variable can shape a producer's answers
// unseen: a producer's configuration is set by its test, where the request key
// or the test text holds it; a credential is passed by name with -pass-env and
// must be declared by the golden (GoldenSpec.PassEnv), which records the name.
// The interpreter is the pinned checkout's own (DEV_HEALTH_PYTHON is not
// passed), and the harness switches are set by the verb itself.
var passedNames = map[string]bool{
	"PATH": true, "HOME": true, "USER": true, "LOGNAME": true, "TMPDIR": true, "XDG_RUNTIME_DIR": true,
	"GOPATH": true, "GOROOT": true, "GOCACHE": true, "GOMODCACHE": true, "GOFLAGS": true, "GOPROXY": true,
	"GONOPROXY": true, "GOSUMDB": true, "GONOSUMDB": true, "GOPRIVATE": true, "GOINSECURE": true, "GOTMPDIR": true,
	"GOWORK": true, "GOTOOLCHAIN": true, "GOMAXPROCS": true, "GOENV": true, "GOEXPERIMENT": true, "CGO_ENABLED": true,
	"SSL_CERT_FILE": true, "SSL_CERT_DIR": true,
	"HTTP_PROXY": true, "HTTPS_PROXY": true, "NO_PROXY": true, "http_proxy": true, "https_proxy": true, "no_proxy": true,
	"DOCKER_HOST": true, "DOCKER_CONFIG": true, "DOCKER_CERT_PATH": true, "DOCKER_TLS_VERIFY": true,
	"DOCKER_API_VERSION": true, "DOCKER_CONTEXT": true,
	"TESTCONTAINERS_RYUK_DISABLED": true, "TESTCONTAINERS_HOST_OVERRIDE": true,
	"TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE": true, "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX": true,
}

// passedEnvName tells the recording which names -pass-env passed, so each
// golden can require exactly the names it declares.
const passedEnvName = "DHO_VENUE_GOLDEN_PASSED_ENV"

// recordingEnv is ambient reduced to the passed names and the names in extra,
// with a fixed UTF-8 locale. The verb's own variables are added by its caller.
func recordingEnv(ambient, extra []string) []string {
	named := map[string]bool{}
	for _, name := range extra {
		named[name] = true
	}
	var out []string
	for _, entry := range ambient {
		name, _, _ := strings.Cut(entry, "=")
		if passedNames[name] || named[name] {
			out = append(out, entry)
		}
	}
	return append(out, "LANG=C.UTF-8", "LC_ALL=C.UTF-8")
}

// goTest is the default runner.
func goTest(cfg Config, env []string) error {
	command := exec.Command("go", "test", "-tags=integration", "-count=1", "-timeout", "120m", "-v", "-run", cfg.Test, cfg.Package)
	command.Dir = cfg.Root
	command.Env = append(recordingEnv(os.Environ(), cfg.PassEnv), env...)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}

// coverage is what a golden covers: its requests by name and its row snapshots by name.
type coverage struct {
	Requests []struct {
		Name string `json:"name"`
	} `json:"requests"`
	Rows map[string]json.RawMessage `json:"rows"`
}

// droppedCoverage lists the request and row names the old golden holds that the
// new one does not. An old file that cannot be read holds nothing to keep.
func droppedCoverage(oldRaw, newRaw []byte) []string {
	var oldCoverage, newCoverage coverage
	if json.Unmarshal(oldRaw, &oldCoverage) != nil {
		return nil
	}
	if json.Unmarshal(newRaw, &newCoverage) != nil {
		return []string{"the candidate is not readable"}
	}
	have := map[string]bool{}
	for _, request := range newCoverage.Requests {
		have["request "+request.Name] = true
	}
	for name := range newCoverage.Rows {
		have["rows "+name] = true
	}
	var dropped []string
	for _, request := range oldCoverage.Requests {
		if !have["request "+request.Name] {
			dropped = append(dropped, fmt.Sprintf("request %q", request.Name))
		}
	}
	for name := range oldCoverage.Rows {
		if !have["rows "+name] {
			dropped = append(dropped, fmt.Sprintf("row comparison %q", name))
		}
	}
	sort.Strings(dropped)
	return dropped
}

// clearBytecode removes every __pycache__ directory and .pyc file under dir.
func clearBytecode(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		return err
	}
	return filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case entry.IsDir() && entry.Name() == "__pycache__":
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			return filepath.SkipDir
		case !entry.IsDir() && strings.HasSuffix(path, ".pyc"):
			return os.Remove(path)
		}
		return nil
	})
}
