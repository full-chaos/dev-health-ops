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
// Usage, from the repository root:
//
//	go run ./internal/testsupport/venueoracle/goldenrecord \
//	    -pkg ./internal/apiservice/admin/ -test '^TestX$' -python-root <clean worktree at the pinned build>
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	"sort"
	"strings"
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
}

// Result names what one recording promoted.
type Result struct {
	Promoted []Promotion
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
	flag.Parse()
	for _, name := range strings.Split(*passEnv, ",") {
		if name = strings.TrimSpace(name); name != "" {
			cfg.PassEnv = append(cfg.PassEnv, name)
		}
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
	// Both recording runs get this one list: they may differ in time only.
	recordEnv := append(append([]string{}, base...), "DHO_VENUE_GOLDEN_UPDATE=1", "DHO_VENUE_GOLDEN_PYTHON_ROOT="+cfg.PythonRoot, "PYTHONDONTWRITEBYTECODE=1")

	// A bytecode cache in the pinned checkout can run code older than its source:
	// clear it, and stop the recording writing a new one.
	if err := clearBytecode(filepath.Join(cfg.PythonRoot, "src")); err != nil {
		return Result{}, fmt.Errorf("clearing the bytecode cache of %s: %w", cfg.PythonRoot, err)
	}
	if err := cfg.Run(cfg, recordEnv); err != nil {
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
	// Record a second time, in a fresh process, from the same producer: a value
	// that differs between the two runs is a per-run value (a random id, a
	// timestamp, a minted token) the golden would pin to one run. It needs a
	// pin in the test or a typed placeholder (GoldenSpec.Scrub), and the
	// candidates are compared as written, after projection and scrub.
	for _, candidate := range found {
		if err := os.Remove(candidate); err != nil {
			discard()
			return Result{}, err
		}
	}
	if err := cfg.Run(cfg, recordEnv); err != nil {
		discard()
		return Result{}, fmt.Errorf("the second recording run failed: %w", err)
	}
	second, err := candidates(packageDir)
	if err != nil {
		discard()
		return Result{}, err
	}
	if err := compareRuns(replayed, second); err != nil {
		discard()
		return Result{}, err
	}
	for _, candidate := range found {
		if err := os.WriteFile(candidate, replayed[candidate], 0o644); err != nil {
			discard()
			return Result{}, err
		}
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

// compareRuns is an error unless the second recording wrote exactly the
// candidates of the first, byte for byte. The error names the first request
// (or row comparison) and the field that differs, with the length and a digest
// prefix of each value, never the value: a per-run value may be a secret.
func compareRuns(first map[string][]byte, secondPaths []string) error {
	second := map[string][]byte{}
	for _, path := range secondPaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		second[path] = raw
	}
	var names []string
	for path := range first {
		names = append(names, path)
	}
	sort.Strings(names)
	for _, path := range names {
		other, ok := second[path]
		if !ok {
			return fmt.Errorf("the second recording run wrote no candidate %s: a recording that is not repeatable is not recorded", path)
		}
		if bytes.Equal(first[path], other) {
			continue
		}
		return fmt.Errorf("the two recording runs of %s differ at %s: a value that changes between runs cannot be pinned; make it deterministic in the test (stable ids, a fixed clock) or turn it into a typed placeholder with GoldenSpec.Scrub", path, firstDifference(first[path], other))
	}
	for path := range second {
		if _, ok := first[path]; !ok {
			return fmt.Errorf("the second recording run wrote a candidate %s the first did not", path)
		}
	}
	return nil
}

// maxReported is how many differences a refusal lists before it counts the rest.
const maxReported = 5

// goldenDoc is the part of a golden file the comparison reads.
type goldenDoc struct {
	Header   map[string]any `json:"header"`
	Requests []struct {
		Name    string            `json:"name"`
		Status  json.Number       `json:"status"`
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
	} `json:"requests"`
	Rows map[string]struct {
		Rows string `json:"rows"`
	} `json:"rows"`
}

func parseDoc(raw []byte) (goldenDoc, bool) {
	var doc goldenDoc
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return doc, decoder.Decode(&doc) == nil
}

// firstDifference lists up to maxReported differences between two golden
// files, then the count of the rest. A value is never printed (a per-run reset
// token has no shape a redaction could match): each side is shown as its kind,
// length and the head of its sha256.
func firstDifference(a, b []byte) string {
	left, lok := parseDoc(a)
	right, rok := parseDoc(b)
	if !lok || !rok {
		return "the file (not a golden file)"
	}
	var found []string
	add := func(where string, x, y string) {
		found = append(found, fmt.Sprintf("%s (first run: %s, second run: %s)", where, describe(x), describe(y)))
	}
	names := map[string]bool{}
	for k := range left.Header {
		names[k] = true
	}
	for k := range right.Header {
		names[k] = true
	}
	for _, k := range sortedKeys(names) {
		if x, y := fmt.Sprint(left.Header[k]), fmt.Sprint(right.Header[k]); x != y {
			add("header."+k, x, y)
		}
	}
	if len(left.Requests) != len(right.Requests) {
		found = append(found, fmt.Sprintf("requests count (first run: %d, second run: %d)", len(left.Requests), len(right.Requests)))
	}
	for i := 0; i < len(left.Requests) && i < len(right.Requests); i++ {
		x, y := left.Requests[i], right.Requests[i]
		where := "request " + x.Name
		if x.Name != y.Name {
			add(fmt.Sprintf("request #%d name", i), x.Name, y.Name)
			continue
		}
		if x.Status.String() != y.Status.String() {
			found = append(found, fmt.Sprintf("%s status (first run: %s, second run: %s)", where, x.Status, y.Status))
		}
		hn := map[string]bool{}
		for k := range x.Headers {
			hn[k] = true
		}
		for k := range y.Headers {
			hn[k] = true
		}
		for _, k := range sortedKeys(hn) {
			if x.Headers[k] != y.Headers[k] {
				add(where+" header "+k, x.Headers[k], y.Headers[k])
			}
		}
		if x.Body != y.Body {
			add(where+" body "+bodyDifference(x.Body, y.Body), x.Body, y.Body)
		}
	}
	rn := map[string]bool{}
	for k := range left.Rows {
		rn[k] = true
	}
	for k := range right.Rows {
		rn[k] = true
	}
	for _, k := range sortedKeys(rn) {
		if x, y := left.Rows[k].Rows, right.Rows[k].Rows; x != y {
			add("rows "+k+" "+lineDifference(x, y), x, y)
		}
	}
	if len(found) == 0 {
		return "the file (bytes differ, compared fields equal)"
	}
	out := strings.Join(found[:min(len(found), maxReported)], "; ")
	if len(found) > maxReported {
		out += fmt.Sprintf("; and %d more", len(found)-maxReported)
	}
	return out
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// bodyDifference is the JSON path of the first differing leaf of two bodies,
// after unpacking a packed body, else the first differing line.
func bodyDifference(x, y string) string {
	if a, ok := unpack(x); ok {
		x = a
	}
	if b, ok := unpack(y); ok {
		y = b
	}
	var left, right any
	ld, rd := json.NewDecoder(strings.NewReader(x)), json.NewDecoder(strings.NewReader(y))
	ld.UseNumber()
	rd.UseNumber()
	if ld.Decode(&left) == nil && rd.Decode(&right) == nil {
		if path, ok := firstLeaf(left, right, "$"); ok {
			return path
		}
	}
	return lineDifference(x, y)
}

// unpack undoes venueoracle.PackBody (gzip, then base64, behind the prefix).
func unpack(body string) (string, bool) {
	encoded, ok := strings.CutPrefix(body, "gzip+base64:")
	if !ok {
		return "", false
	}
	packed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", false
	}
	reader, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		return "", false
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func lineDifference(x, y string) string {
	left, right := strings.Split(x, "\n"), strings.Split(y, "\n")
	for i := 0; i < len(left) || i < len(right); i++ {
		var a, b string
		if i < len(left) {
			a = left[i]
		}
		if i < len(right) {
			b = right[i]
		}
		if a != b {
			return fmt.Sprintf("line %d", i+1)
		}
	}
	return "line 1"
}

// firstLeaf walks two parsed JSON values in key order and returns the path of
// the first leaf whose literal text differs.
func firstLeaf(a, b any, path string) (string, bool) {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok {
			return path, true
		}
		keys := map[string]bool{}
		for k := range x {
			keys[k] = true
		}
		for k := range y {
			keys[k] = true
		}
		for _, k := range sortedKeys(keys) {
			xv, xok := x[k]
			yv, yok := y[k]
			// A key can be a secret too: it is named by its digest, never as written.
			label := path + ".<key sha256 " + keyDigest(k) + ">"
			if !xok || !yok {
				return label, true
			}
			if p, ok := firstLeaf(xv, yv, label); ok {
				return p, true
			}
		}
		return "", false
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return path + "[length]", true
		}
		for i := range x {
			if p, ok := firstLeaf(x[i], y[i], fmt.Sprintf("%s[%d]", path, i)); ok {
				return p, true
			}
		}
		return "", false
	default:
		if fmt.Sprintf("%T:%v", a, a) == fmt.Sprintf("%T:%v", b, b) {
			return "", false
		}
		return path, true
	}
}

// describe is a value's length and the head of its digest, never the value.
func describe(text string) string {
	sum := sha256.Sum256([]byte(text))
	return fmt.Sprintf("%d bytes, sha256 %s", len(text), hex.EncodeToString(sum[:])[:8])
}

// keyDigest is the head of a JSON key's sha256.
func keyDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:8]
}
