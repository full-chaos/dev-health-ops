package devhealth_test

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The hook is changed only by serial fake-helm tests. Production test calls
// always construct their command through helmCommand, preserving namespace pinning.
var renderMemoHelmPath string
var helmRenderMemo = newRenderMemo()
var renderMemoTotals struct{ hits, misses, uncached, distinct atomic.Uint64 }

type renderMemoEntry struct {
	once sync.Once
	out  []byte
	err  error
}

type renderMemo struct {
	mu       sync.Mutex
	entries  map[[32]byte]*renderMemoEntry
	versions map[string]*renderMemoEntry
	slots    chan struct{}
	// run executes one helm process; nil means cmd.CombinedOutput. Cells replace it to count and hold calls in Go.
	run func(*exec.Cmd) ([]byte, error)
}

// renderMemoBound is read once, when a memo is built: HELM_RENDER_PARALLEL when it is a positive number, else
// min(GOMAXPROCS, NumCPU). It is never read again, so no earlier call can fix the capacity before a caller sets it.
func renderMemoBound() int {
	n := min(runtime.GOMAXPROCS(0), runtime.NumCPU())
	if override, err := strconv.Atoi(os.Getenv("HELM_RENDER_PARALLEL")); err == nil && override > 0 {
		n = override
	}
	return n
}

func newRenderMemo() *renderMemo { return newRenderMemoWithBound(renderMemoBound()) }

func newRenderMemoWithBound(n int) *renderMemo {
	return &renderMemo{entries: make(map[[32]byte]*renderMemoEntry), versions: make(map[string]*renderMemoEntry), slots: make(chan struct{}, n)}
}

func (m *renderMemo) live(cmd *exec.Cmd) ([]byte, error) {
	m.slots <- struct{}{}
	defer func() { <-m.slots }()
	if m.run != nil {
		return m.run(cmd)
	}
	return cmd.CombinedOutput()
}

func helmOutput(args ...string) ([]byte, error) {
	cmd := helmCommand(args...)
	if renderMemoHelmPath != "" {
		cmd.Path = renderMemoHelmPath
		cmd.Args[0] = renderMemoHelmPath
		cmd.Err = nil // helm may be absent; the serial cells supply their own binary.
	}
	return helmRenderMemo.output(cmd)
}

func (m *renderMemo) output(cmd *exec.Cmd) ([]byte, error) {
	if os.Getenv("HELM_NO_RENDER_MEMO") == "1" || cmd.Stdin != nil || cmd.Env != nil || cmd.Dir != "" {
		renderMemoTotals.uncached.Add(1)
		return m.live(cmd)
	}
	key, err := m.key(cmd)
	if err != nil { // Unrecognized inputs or unreadable files must never reuse output.
		renderMemoTotals.uncached.Add(1)
		return m.live(cmd)
	}
	m.mu.Lock()
	entry, hit := m.entries[key]
	if !hit {
		entry = &renderMemoEntry{}
		m.entries[key] = entry
		renderMemoTotals.misses.Add(1)
		renderMemoTotals.distinct.Add(1)
	} else {
		renderMemoTotals.hits.Add(1)
	}
	m.mu.Unlock()
	entry.once.Do(func() { entry.out, entry.err = m.live(cmd) })
	return append([]byte(nil), entry.out...), entry.err
}

func renderMemoCountsLine() string {
	return fmt.Sprintf("render-memo: hits=%d misses=%d uncached=%d distinct=%d",
		renderMemoTotals.hits.Load(), renderMemoTotals.misses.Load(), renderMemoTotals.uncached.Load(), renderMemoTotals.distinct.Load())
}

// Length prefixes distinguish adjacent fields, paths and empty values.
func renderMemoField(h hash.Hash, b []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(b)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(b)
}

func (m *renderMemo) key(cmd *exec.Cmd) ([32]byte, error) {
	var key [32]byte
	chart, files, err := renderMemoInputs(cmd.Args[1:])
	if err != nil {
		return key, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return key, err
	}
	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return key, err
	}
	h := sha256.New()
	renderMemoField(h, []byte(cwd))
	for _, arg := range cmd.Args[1:] {
		renderMemoField(h, []byte(arg))
	}
	chart, err = filepath.Abs(chart)
	if err != nil {
		return key, err
	}
	info, err := os.Stat(chart)
	if err != nil {
		return key, err
	}
	if !info.IsDir() {
		return key, fmt.Errorf("chart is not a local directory")
	}
	renderMemoField(h, []byte(chart))
	if err := renderMemoTree(h, chart, ".", map[string]bool{}); err != nil {
		return key, err
	}
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			return key, err
		}
		renderMemoField(h, []byte(path))
		renderMemoField(h, b)
	}
	var env []string
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if strings.HasPrefix(name, "HELM_") || strings.HasPrefix(name, "KUBE") {
			env = append(env, value)
		}
	}
	sort.Strings(env)
	for _, value := range env {
		renderMemoField(h, []byte(value))
	}
	path, err := filepath.Abs(cmd.Path)
	if err != nil {
		return key, err
	}
	renderMemoField(h, []byte(path))
	m.mu.Lock()
	version := m.versions[path]
	if version == nil {
		version = &renderMemoEntry{}
		m.versions[path] = version
	}
	m.mu.Unlock()
	version.once.Do(func() { version.out, version.err = m.live(exec.Command(path, "version", "--short")) })
	if version.err != nil {
		return key, version.err
	}
	renderMemoField(h, version.out)
	copy(key[:], h.Sum(nil))
	return key, nil
}

// ReadDir is sorted; Stat follows links, including linked directories and
// subcharts. Ancestor tracking fails open on cycles instead of hanging.
func renderMemoTree(h hash.Hash, path, relative string, ancestors map[string]bool) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		real, err := filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		if ancestors[real] {
			return fmt.Errorf("chart symlink cycle")
		}
		ancestors[real] = true
		defer delete(ancestors, real)
		children, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		renderMemoField(h, []byte("dir:"+relative))
		for _, child := range children {
			if err := renderMemoTree(h, filepath.Join(path, child.Name()), filepath.Join(relative, child.Name()), ancestors); err != nil {
				return err
			}
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("non-regular chart input")
	}
	if strings.HasSuffix(info.Name(), "_test.go") {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	renderMemoField(h, []byte("file:"+relative))
	renderMemoField(h, b)
	return nil
}

// Only flags whose inputs are understood belong here. File flags are hashed;
// inline --set-json is JSON, not a filename. Every unknown flag fails open.
func renderMemoInputs(args []string) (string, []string, error) {
	bad := func() (string, []string, error) { return "", nil, fmt.Errorf("unsupported helm argv") }
	start := 1
	if len(args) == 0 {
		return bad()
	}
	switch args[0] {
	case "template":
	case "show":
		if len(args) < 2 || args[1] != "values" {
			return bad()
		}
		start = 2
	default:
		return bad()
	}
	var positional, files, showOnly []string
	for i := start; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		flag, value, equals := strings.Cut(arg, "=")
		switch flag {
		case "--skip-schema-validation":
			if equals && value != "true" && value != "false" {
				return bad()
			}
			continue
		case "--namespace", "-n", "--set", "--set-string", "--set-json", "--set-file", "-f", "--values", "--show-only":
		default:
			return bad()
		}
		if !equals {
			i++
			if i >= len(args) {
				return bad()
			}
			value = args[i]
		}
		switch flag {
		case "-f", "--values":
			for _, path := range strings.Split(value, ",") {
				if path == "" || path == "-" {
					return bad()
				}
				files = append(files, path)
			}
		case "--set-file":
			// Conservatively reject escaped/list forms rather than mis-hash a path.
			if strings.ContainsAny(value, `\{}`) {
				return bad()
			}
			for _, assignment := range strings.Split(value, ",") {
				name, path, ok := strings.Cut(assignment, "=")
				if !ok || name == "" || path == "" || path == "-" {
					return bad()
				}
				files = append(files, path)
			}
		case "--show-only":
			showOnly = append(showOnly, strings.Split(value, ",")...)
		}
	}
	if len(positional) == 0 || len(positional) > 2 || (args[0] == "show" && len(positional) != 1) {
		return bad()
	}
	chart := positional[len(positional)-1]
	if strings.Contains(chart, "://") {
		return bad()
	}
	for _, path := range showOnly {
		clean := filepath.Clean(path)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return bad()
		}
	}
	return chart, files, nil
}

// Serial cells isolate their fake binary, memo and environment. The hook avoids
// depending on an installed Helm or on the package TestMain's PATH shim.
func fakeRenderMemo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/bash
state=STATE
if [ "$1" = version ]; then printf 'v-fake\n'; exit 0; fi
lock() { until mkdir "$state/lock" 2>/dev/null; do sleep 0.005; done; }
lock
n=0; [ ! -f "$state/active" ] || read -r n < "$state/active"
n=$((n+1)); printf '%s\n' "$n" > "$state/active"
max=0; [ ! -f "$state/max" ] || read -r max < "$state/max"
[ "$n" -le "$max" ] || printf '%s\n' "$n" > "$state/max"
printf 'render\n' >> "$state/calls"
rmdir "$state/lock"
finish() {
 lock
 read -r n < "$state/active"
 printf '%s\n' "$((n-1))" > "$state/active"
 rmdir "$state/lock"
}
trap finish EXIT
if [ -f "$state/barrier" ]; then
 read -r bn bg bt < "$state/barrier"
 if [ ! -f "$state/released" ]; then
  waited=0
  until [ -f "$state/released" ]; do
   lock; cur=0; [ ! -f "$state/active" ] || read -r cur < "$state/active"; rmdir "$state/lock"
   if [ "$cur" -ge "$bn" ]; then : > "$state/released"; break; fi
   waited=$((waited+1))
   if [ "$waited" -gt $((bt*100)) ]; then : > "$state/barrier-timeout"; : > "$state/released"; break; fi
   sleep 0.01
  done
  sleep "$bg"
 fi
else
 sleep 0.04
fi
printf '%s\n' "$@"
chart=''
fail=0
while [ "$#" -gt 0 ]; do
 case "$1" in
 template|show|values) shift ;;
 --namespace|-n|--set|--set-string|--set-json|--show-only|--post-renderer|--kube-context|--kubeconfig)
  [ "${2:-}" != fail=true ] || fail=1
  shift 2 ;;
 -f|--values) if [ "$2" = - ]; then cat; else cat "$2"; fi; shift 2 ;;
 --values=*|-f=*) cat "${1#*=}"; shift ;;
 --set-file) cat "${2#*=}"; shift 2 ;;
 --*) shift ;;
 *) [ ! -d "$1" ] || chart="$1"; shift ;;
 esac
done
if [ -n "$chart" ]; then
 find -L "$chart" -type f ! -name '*_test.go' -print0 | sort -z | xargs -0 cat | sha256sum
fi
if [ "$fail" = 1 ]; then printf 'fake render failed\n' >&2; exit 7; fi
`
	script = strings.ReplaceAll(script, "STATE", strconv.Quote(state))
	path := filepath.Join(dir, "helm")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HELM_NO_RENDER_MEMO", "0")
	previousPath, previousMemo := renderMemoHelmPath, helmRenderMemo
	renderMemoHelmPath, helmRenderMemo = path, newRenderMemo()
	t.Cleanup(func() { renderMemoHelmPath, helmRenderMemo = previousPath, previousMemo })
	chart := filepath.Join(dir, "chart")
	if err := os.Mkdir(chart, 0o700); err != nil {
		t.Fatal(err)
	}
	memoWrite(t, filepath.Join(chart, "Chart.yaml"), "name: fake\nversion: 0.1.0\n")
	memoWrite(t, filepath.Join(chart, "values.yaml"), "value: before\n")
	return chart, state
}

func memoWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func memoRender(t *testing.T, chart string, args ...string) []byte {
	t.Helper()
	out, err := helmOutput(append([]string{"template", "memo", chart}, args...)...)
	if err != nil {
		t.Fatalf("fake render: %v\n%s", err, out)
	}
	return out
}

func memoCalls(t *testing.T, state string) int {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "calls"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(b), "render\n")
}

func TestRenderMemoHitsIdenticalCalls(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	first := memoRender(t, chart)
	original := string(first)
	first[0] = '!'
	if got := string(memoRender(t, chart)); got != original {
		t.Fatal("cached bytes were not copied")
	}
	if n := memoCalls(t, state); n != 1 {
		t.Fatalf("renders=%d, want 1", n)
	}
}

func TestRenderMemoMissesDifferentArgv(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	a := memoRender(t, chart, "--set", "value=one")
	b := memoRender(t, chart, "--set", "value=two")
	if string(a) == string(b) || memoCalls(t, state) != 2 {
		t.Fatal("different argv reused output")
	}
}

func TestRenderMemoMissesChangedValuesFileBytes(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	path := filepath.Join(t.TempDir(), "external.yaml")
	for _, flag := range []string{"-f", "--values", "--values=", "--set-file"} {
		memoWrite(t, path, "first")
		args := []string{flag, path}
		if flag == "--set-file" {
			args[1] = "value=" + path
		}
		if flag == "--values=" {
			args = []string{flag + path}
		}
		before := memoCalls(t, state)
		a := memoRender(t, chart, args...)
		memoWrite(t, path, "second")
		b := memoRender(t, chart, args...)
		if string(a) == string(b) || memoCalls(t, state)-before != 2 {
			t.Fatalf("%s did not invalidate", flag)
		}
	}
}

func memoCopyChart(t *testing.T) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "chart-copy")
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		target := filepath.Join(dst, path)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestRenderMemoSeesAChartEditBetweenCalls(t *testing.T) {
	_, state := fakeRenderMemo(t)
	chart := memoCopyChart(t)
	a := memoRender(t, chart)
	path := filepath.Join(chart, "templates", "configmap.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	memoWrite(t, path, string(b)+"\n# edited between identical calls\n")
	second := memoRender(t, chart)
	if string(a) == string(second) || memoCalls(t, state) != 2 {
		t.Fatal("template edit reused output")
	}
}

func TestRenderMemoFollowsSymlinkedChartAndSubcharts(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	link := filepath.Join(t.TempDir(), "chart-link")
	if err := os.Symlink(chart, link); err != nil {
		t.Fatal(err)
	}
	// Also follow directory links *inside* the chart, not just its root.
	child := t.TempDir()
	if err := os.Symlink(child, filepath.Join(chart, "charts")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(child, "child.yaml")
	memoWrite(t, path, "before")
	a := memoRender(t, link)
	memoWrite(t, path, "after")
	b := memoRender(t, link)
	if string(a) == string(b) || memoCalls(t, state) != 2 {
		t.Fatal("symlinked subchart edit reused output")
	}
}

func TestRenderMemoHashesEveryChartInput(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	for _, name := range []string{"Chart.yaml", "values.yaml", "values.schema.json", "charts/child/Chart.yaml", "testdata/input"} {
		path := filepath.Join(chart, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		memoWrite(t, path, "before")
		before := memoCalls(t, state)
		a := memoRender(t, chart)
		memoWrite(t, path, "after")
		b := memoRender(t, chart)
		if string(a) == string(b) || memoCalls(t, state)-before != 2 {
			t.Fatalf("edit to %s reused output", name)
		}
	}
}

func TestRenderMemoUnsupportedInputsStayUncached(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	for _, args := range [][]string{
		{"template", "memo", chart, "--unknown"},
		{"template", "memo", chart, "--post-renderer", "/not/hashed"},
		{"template", "memo", chart, "--kube-context", "other"},
		{"template", "memo", chart, "--kubeconfig", "/not/hashed"},
		{"template", "memo", chart, "--show-only", "../outside"},
		{"template", "memo", filepath.Join(chart, "absent")},
		{"list"}, {"show", "chart", chart},
	} {
		before := memoCalls(t, state)
		for range 2 {
			if _, err := helmOutput(args...); err != nil {
				t.Fatal(err)
			}
		}
		if memoCalls(t, state)-before != 2 {
			t.Fatalf("unsupported args were cached: %v", args)
		}
	}
	for _, custom := range []string{"stdin", "env", "dir", "values stdin"} {
		before := memoCalls(t, state)
		for range 2 {
			args := []string{"template", "memo", chart}
			if custom == "values stdin" {
				args = append(args, "-f", "-")
			}
			cmd := helmCommand(args...)
			cmd.Path, cmd.Args[0], cmd.Err = renderMemoHelmPath, renderMemoHelmPath, nil
			switch custom {
			case "stdin":
				cmd.Stdin = strings.NewReader("stdin")
			case "env":
				cmd.Env = os.Environ()
			case "dir":
				cmd.Dir = chart
				// nil stdin still proves -f - cannot be memoized.
			}
			if _, err := helmRenderMemo.output(cmd); err != nil {
				t.Fatal(err)
			}
		}
		if memoCalls(t, state)-before != 2 {
			t.Fatalf("custom %s cached", custom)
		}
	}
}

func TestRenderMemoCachesFailuresAsFailures(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	for range 2 {
		out, err := helmOutput("template", "memo", chart, "--set", "fail=true")
		if err == nil || !strings.Contains(string(out), "fake render failed") {
			t.Fatalf("failure lost: %v %s", err, out)
		}
	}
	if memoCalls(t, state) != 1 {
		t.Fatal("failure was not cached")
	}
}

// semaphoreProbe is what probeRenderSemaphore measured in Go, around the process the semaphore guards.
type semaphoreProbe struct {
	capacity, max, renders int
	timedOut               bool
	inFlightAtTimeout      int
}

func memoChartDir(t *testing.T) string {
	t.Helper()
	chart := filepath.Join(t.TempDir(), "chart")
	if err := os.Mkdir(chart, 0o700); err != nil {
		t.Fatal(err)
	}
	memoWrite(t, filepath.Join(chart, "Chart.yaml"), "name: fake\nversion: 0.1.0\n")
	memoWrite(t, filepath.Join(chart, "values.yaml"), "value: before\n")
	return chart
}

// probeRenderSemaphore sends 20 calls through m (half bypass the memo with an unknown flag, half are memoized: both
// must use the same semaphore) and counts the live "processes" in Go. The first calls wait on a channel until
// `want` are live at once (an event, not a poll; a Go deadline fails the wait and records how many were live), then
// the released calls are held for `window`; a call above `want` closes `third` at once, so a missing bound shows as
// max > want without any file, lock or sleep-based counting.
func probeRenderSemaphore(t *testing.T, m *renderMemo, chart string, want int, deadline, window time.Duration) semaphoreProbe {
	t.Helper()
	t.Setenv("HELM_NO_RENDER_MEMO", "0")
	var cur, max, renders, inFlightAtTimeout atomic.Int32
	var timedOut atomic.Bool
	reached, third := make(chan struct{}), make(chan struct{})
	var reachedOnce, thirdOnce sync.Once
	m.run = func(cmd *exec.Cmd) ([]byte, error) {
		if len(cmd.Args) > 1 && cmd.Args[1] == "version" {
			return []byte("v-fake\n"), nil
		}
		n := cur.Add(1)
		defer cur.Add(-1)
		renders.Add(1)
		for {
			old := max.Load()
			if n <= old || max.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-reached:
			return []byte("rendered\n"), nil
		default:
		}
		if int(n) >= want {
			reachedOnce.Do(func() { close(reached) })
		}
		if int(n) > want {
			thirdOnce.Do(func() { close(third) })
		}
		timer := time.NewTimer(deadline)
		select {
		case <-reached:
			timer.Stop()
		case <-timer.C:
			timedOut.Store(true)
			inFlightAtTimeout.Store(cur.Load())
			reachedOnce.Do(func() { close(reached) })
		}
		hold := time.NewTimer(window)
		select {
		case <-third:
			hold.Stop()
		case <-hold.C:
		}
		return []byte("rendered\n"), nil
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 20 {
		wg.Go(func() {
			<-start
			flag := "--set"
			if i%2 == 0 {
				flag = "--unknown"
			}
			cmd := exec.Command("helm", "template", "memo", chart, flag, fmt.Sprintf("value=%d", i))
			if out, err := m.output(cmd); err != nil {
				t.Errorf("%v: %s", err, out)
			}
		})
	}
	close(start)
	wg.Wait()
	return semaphoreProbe{capacity: cap(m.slots), max: int(max.Load()), renders: int(renders.Load()),
		timedOut: timedOut.Load(), inFlightAtTimeout: int(inFlightAtTimeout.Load())}
}

func TestRenderMemoSemaphoreBoundsLiveProcesses(t *testing.T) {
	m := newRenderMemoWithBound(2)
	got := probeRenderSemaphore(t, m, memoChartDir(t), 2, 10*time.Second, 250*time.Millisecond)
	if got.timedOut {
		t.Fatalf("only %d render(s) were live after the deadline: the semaphore never let two overlap (capacity %d)", got.inFlightAtTimeout, got.capacity)
	}
	if got.capacity != 2 || got.max != 2 || got.renders != 20 {
		t.Fatalf("capacity=%d max=%d renders=%d, want capacity=2 max=2 renders=20", got.capacity, got.max, got.renders)
	}
}

// Red cells: the probe reports each failure shape seen on the hosted runners from a memo built with the wrong bound.
func TestRenderMemoProbeSeesACapacityAboveTheBound(t *testing.T) {
	got := probeRenderSemaphore(t, newRenderMemoWithBound(3), memoChartDir(t), 2, 5*time.Second, 150*time.Millisecond)
	if got.timedOut || got.capacity != 3 || got.max != 3 {
		t.Fatalf("a memo of capacity 3 measured capacity=%d max=%d timedOut=%v, want capacity=3 max=3 (the shape of max=3 want 2)", got.capacity, got.max, got.timedOut)
	}
}

func TestRenderMemoProbeSeesACapacityBelowTheBound(t *testing.T) {
	got := probeRenderSemaphore(t, newRenderMemoWithBound(1), memoChartDir(t), 2, 300*time.Millisecond, 150*time.Millisecond)
	if !got.timedOut || got.inFlightAtTimeout != 1 || got.max != 1 {
		t.Fatalf("a memo of capacity 1 measured timedOut=%v inFlight=%d max=%d, want timedOut with 1 in flight (the shape of the barrier timeout)", got.timedOut, got.inFlightAtTimeout, got.max)
	}
}

func TestRenderMemoCapacityIsFixedAtConstruction(t *testing.T) {
	t.Setenv("HELM_RENDER_PARALLEL", "3")
	m := newRenderMemo()
	if cap(m.slots) != 3 {
		t.Fatalf("capacity %d, want 3 from HELM_RENDER_PARALLEL at construction", cap(m.slots))
	}
	// A call made before the caller changes its mind must not move the capacity (the old lazy limit read it at first use).
	m.run = func(*exec.Cmd) ([]byte, error) { return nil, nil }
	if _, err := m.live(exec.Command("helm", "version", "--short")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HELM_RENDER_PARALLEL", "5")
	if _, err := m.live(exec.Command("helm", "version", "--short")); err != nil {
		t.Fatal(err)
	}
	if cap(m.slots) != 3 {
		t.Fatalf("capacity %d after the env changed, want 3 (fixed at construction)", cap(m.slots))
	}
}

func TestRenderMemoFailingCallReleasesItsSlot(t *testing.T) {
	m := newRenderMemoWithBound(1)
	failed := false
	m.run = func(*exec.Cmd) ([]byte, error) {
		if !failed {
			failed = true
			return []byte("boom\n"), errors.New("render failed")
		}
		return []byte("ok\n"), nil
	}
	if _, err := m.live(exec.Command("helm", "template", "x")); err == nil {
		t.Fatal("the first call must fail")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if out, err := m.live(exec.Command("helm", "template", "y")); err != nil || string(out) != "ok\n" {
			t.Errorf("second call = %q, %v", out, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the slot of the failed call was never released: the next call is blocked")
	}
}

func TestRenderMemoConcurrentIdenticalCallsRunOnce(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() { memoRender(t, chart) })
	}
	wg.Wait()
	if memoCalls(t, state) != 1 {
		t.Fatal("identical concurrent calls rendered more than once")
	}
}

func TestRenderMemoCanBeDisabled(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	t.Setenv("HELM_NO_RENDER_MEMO", "1")
	for range 2 {
		memoRender(t, chart)
	}
	if memoCalls(t, state) != 2 {
		t.Fatal("memo-off reused output")
	}
}

func TestRenderMemoEnvironmentIsPartOfKey(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	memoRender(t, chart)
	for _, name := range []string{"HELM_MEMO_CELL", "KUBECONFIG", "KUBE_CONTEXT"} {
		before := memoCalls(t, state)
		t.Setenv(name, "changed")
		memoRender(t, chart)
		if memoCalls(t, state)-before != 1 {
			t.Fatalf("%s missing from key", name)
		}
	}
}

func TestRenderMemoCountsLine(t *testing.T) {
	chart, state := fakeRenderMemo(t)
	hits, misses, uncached, distinct := renderMemoTotals.hits.Load(), renderMemoTotals.misses.Load(), renderMemoTotals.uncached.Load(), renderMemoTotals.distinct.Load()
	memoRender(t, chart)
	memoRender(t, chart)
	t.Setenv("HELM_NO_RENDER_MEMO", "1")
	memoRender(t, chart)
	want := fmt.Sprintf("render-memo: hits=%d misses=%d uncached=%d distinct=%d", hits+1, misses+1, uncached+1, distinct+1)
	if got := renderMemoCountsLine(); got != want {
		t.Fatalf("counts=%q, want %q", got, want)
	}
	if memoCalls(t, state) != 2 {
		t.Fatal("counts disagree with actual executions")
	}
	// Exercise TestMain itself with no selected tests, proving the empty memo prints
	// exactly once. This child runs no chart renders.
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("empty child: %v %s", err, b)
	}
	line := "render-memo: hits=0 misses=0 uncached=0 distinct=0"
	if strings.Count(string(b), "render-memo:") != 1 || !strings.Contains(string(b), line) {
		t.Fatalf("empty package counts: %s", b)
	}
}

// A one-P runtime must permit only one live render even on a many-core host.
// The existing override cell independently proves HELM_RENDER_PARALLEL wins.
func TestRenderMemoDefaultSemaphoreHonorsGOMAXPROCS(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	chart, state := fakeRenderMemo(t)
	t.Setenv("HELM_RENDER_PARALLEL", "")
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 6 {
		wg.Go(func() {
			<-start
			if out, err := helmOutput("template", "memo", chart, "--unknown", fmt.Sprintf("value=%d", i)); err != nil {
				t.Errorf("%v: %s", err, out)
			}
		})
	}
	close(start)
	wg.Wait()
	b, err := os.ReadFile(filepath.Join(state, "max"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(b)) != "1" || memoCalls(t, state) != 6 {
		t.Fatalf("max=%s renders=%d, want max=1 and renders=6", b, memoCalls(t, state))
	}
}
