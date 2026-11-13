package checkgo_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The packages shards run each integration go test call with -json into GO_INTEGRATION_LEG_JSON_DIR and print the plain
// package-list log from that file (INTEGRATION_LEG_JQ). These cells run tc_trap .. run_integration_go_test of
// ci/check_go.sh against a stand-in go that prints a fixed event stream and exits with a fixed status.

type legEvent struct {
	ImportPath string  `json:"ImportPath,omitempty"`
	Action     string  `json:"Action"`
	Package    string  `json:"Package,omitempty"`
	Test       string  `json:"Test,omitempty"`
	Output     string  `json:"Output,omitempty"`
	Elapsed    float64 `json:"Elapsed,omitempty"`
}

func lev(action, pkg, test, output string) legEvent {
	e := legEvent{Action: action, Package: pkg, Test: test, Output: output}
	if action == "pass" || action == "fail" || action == "skip" {
		e.Elapsed = 1.5
	}
	return e
}

func legStream(t *testing.T, events ...legEvent) string {
	t.Helper()
	var b strings.Builder
	for _, e := range events {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
		b.WriteByte('\n')
	}
	return b.String()
}

const (
	legPkgA = "example.com/m/a"
	legPkgB = "example.com/m/b"
	legPkgC = "example.com/m/c"
)

// legPassStream: two passing packages; plain go test prints legPassPlain for them.
func legPassStream(t *testing.T) string {
	return legStream(t,
		lev("start", legPkgA, "", ""), lev("run", legPkgA, "TestA", ""),
		lev("output", legPkgA, "TestA", "=== RUN   TestA\n"), lev("output", legPkgA, "TestA", "    a_test.go:9: noisy log\n"),
		lev("output", legPkgA, "TestA", "--- PASS: TestA (1.20s)\n"), lev("pass", legPkgA, "TestA", ""),
		lev("output", legPkgA, "", "PASS\n"), lev("output", legPkgA, "", "ok  \texample.com/m/a\t1.234s\n"), lev("pass", legPkgA, "", ""),
		lev("start", legPkgB, "", ""), lev("output", legPkgB, "", "PASS\n"),
		lev("output", legPkgB, "", "ok  \texample.com/m/b\t2.500s [no tests to run]\n"), lev("pass", legPkgB, "", ""))
}

const legPassPlain = "ok  \texample.com/m/a\t1.234s\nok  \texample.com/m/b\t2.500s [no tests to run]\n"

type legRun struct {
	code                 int
	stdout, stderr, argv string
	jsonDir, pids        string
}

type legOpts struct {
	withDir    bool
	jqFilter   string // replaces INTEGRATION_LEG_JQ when set
	goDelay    string // the go stand-in sleeps this long (sleep(1) words) before it writes its stream
	slowRead   bool   // each read of the renderer starts 2 s late (a cat stand-in)
	readNap    bool   // each read of the renderer naps 1 s after it ends, so go test writes and exits during a read pass
	hangJq     bool   // the renderer's jq never reads (it sleeps); its jq and cat stand-ins record their pids
	uutilsTail bool   // a tail stand-in first on PATH that, like uutils tail, ends on a dead --pid with no last read
	mutateOld  string // planted defect: old -> new in ci/check_go.sh
	mutateNew  string
}

func runIntegrationLeg(t *testing.T, stream string, goRC int, o legOpts) legRun {
	t.Helper()
	text, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(text)
	if o.mutateOld != "" {
		if strings.Count(s, o.mutateOld) != 1 {
			t.Fatalf("planted defect did not land: %q", o.mutateOld)
		}
		s = strings.Replace(s, o.mutateOld, o.mutateNew, 1)
	}
	start := strings.Index(s, "tc_trap() {")
	fn := strings.Index(s, "run_integration_go_test() {")
	if start < 0 || fn < start {
		t.Fatal("tc_trap / run_integration_go_test not found in ci/check_go.sh")
	}
	end := fn + strings.Index(s[fn:], "\n}\n") + 3
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(dir, "events.json")
	argv := filepath.Join(dir, "argv")
	if err := os.WriteFile(events, []byte(stream), 0o644); err != nil {
		t.Fatal(err)
	}
	realCat, err := exec.LookPath("cat")
	if err != nil {
		t.Fatal(err)
	}
	delay := ""
	if o.goDelay != "" {
		delay = "sleep " + o.goDelay + "\n"
	}
	// the go stand-in reads its stream with the real cat, so a cat stand-in only ever stands in for the renderer's reads
	fakeGo := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argv + "\n" + delay + realCat + " " + events + "\nexit " + strconv.Itoa(goRC) + "\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	standIn := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	switch {
	case o.slowRead:
		standIn("cat", "#!/bin/sh\nsleep 2\nexec "+realCat+" \"$@\"\n")
	case o.readNap:
		standIn("cat", "#!/bin/sh\n"+realCat+" \"$@\" || exit $?\nsleep 1\n")
	}
	if o.uutilsTail {
		// -n +1 -f --pid=P -- FILE: one first read, then each pass ends at once when P is dead, before any read
		standIn("tail", "#!/bin/bash\nfor a in \"$@\"; do case \"$a\" in --pid=*) pid=\"${a#--pid=}\" ;; esac; done\n"+
			"exec 3<\"${!#}\" || exit 1\n"+realCat+" <&3\nwhile :; do\n  sleep 0.2\n  kill -0 \"${pid}\" 2>/dev/null || exit 0\n  "+realCat+" <&3\ndone\n")
	}
	pids := filepath.Join(dir, "renderer-pids")
	if o.hangJq {
		realJq, err := exec.LookPath("jq")
		if err != nil {
			t.Fatal(err)
		}
		// only the renderer's jq (-nR) sleeps; the end-signal count is the real jq
		standIn("jq", "#!/bin/sh\nif [ \"$1\" = -nR ]; then echo $$ >> "+pids+"; exec sleep 300; fi\nexec "+realJq+" \"$@\"\n")
		standIn("cat", "#!/bin/sh\necho $$ >> "+pids+"\nexec "+realCat+" \"$@\"\n")
		// whatever outcome, no stand-in this test started outlives it: stop each recorded pid, never a pattern
		t.Cleanup(func() {
			raw, _ := os.ReadFile(pids)
			for _, f := range strings.Fields(string(raw)) {
				if pid, err := strconv.Atoi(f); err == nil && pid > 1 {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		})
	}
	override := ""
	if o.jqFilter != "" {
		override = "INTEGRATION_LEG_JQ='" + o.jqFilter + "'\n"
	}
	script := "set -uo pipefail\nSCRIPT_DIR=" + dir + "\nROOT=" + dir + "\nGO_ENV_OFF=(env)\nINTEGRATION_TEST_ENV=()\n" +
		"die() { echo \"$*\" >&2; exit 2; }\n" + jobBudgetSnippet(t) + "\n" + s[start:end] + override +
		"run_integration_go_test . -run '^(TestA)$' ./a ./b\nr=$?\nprintf 'RETURNED rc=%s\\n' \"${r}\"\nexit \"${r}\"\n"
	jsonDir := filepath.Join(dir, "legjson")
	extra := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "TESTCONTAINERS_SESSION_ID=sesscell1234"}
	if o.withDir {
		extra = append(extra, "GO_INTEGRATION_LEG_JSON_DIR="+jsonDir)
	}
	if o.hangJq {
		extra = append(extra, "DEV_HEALTH_GO_RENDER_WAIT_SECS=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", script)
	// a renderer left running would hold stdout open: bound the wait for it, the run then fails instead of hanging
	cmd.WaitDelay = 10 * time.Second
	cmd.Env = inheritedEnv(append(append([]string{}, deadlineEnvNames...), "GO_INTEGRATION_LEG_JSON_DIR"), extra...)
	var so, se strings.Builder
	cmd.Stdout, cmd.Stderr = &so, &se
	r := legRun{jsonDir: jsonDir, pids: pids}
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		r.code = ee.ExitCode()
	}
	if ctx.Err() != nil {
		t.Fatalf("the harness hit its 60 s bound\nstdout:\n%s\nstderr:\n%s", so.String(), se.String())
	}
	raw, _ := os.ReadFile(argv)
	r.stdout, r.stderr, r.argv = so.String(), se.String(), strings.TrimSpace(string(raw))
	return r
}

func TestIntegrationLegJSONPrintsThePlainLogOfPassingPackages(t *testing.T) {
	t.Parallel()
	stream := legPassStream(t)
	r := runIntegrationLeg(t, stream, 0, legOpts{withDir: true})
	if r.code != 0 || r.stdout != legPassPlain+"RETURNED rc=0\n" {
		t.Errorf("rc=%d stdout=%q; want rc 0 and exactly the plain go test lines, then RETURNED\nstderr:\n%s", r.code, r.stdout, r.stderr)
	}
	if !strings.Contains(r.stderr, "2 of 2 package(s) have a final result") {
		t.Errorf("no end-signal line on stderr:\n%s", r.stderr)
	}
	if !strings.Contains(" "+r.argv+" ", " -json ") {
		t.Errorf("go test argv %q lacks -json", r.argv)
	}
	got, err := os.ReadFile(filepath.Join(r.jsonDir, "integration-packages-g1.json"))
	if err != nil || string(got) != stream {
		t.Errorf("the -json file is not go test's stdout byte for byte (err %v):\n%s", err, got)
	}
}

func TestIntegrationLegJSONRedCellsKeepGoTestStatusAndWords(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		stream []legEvent
		rc     int
		want   []string
	}{
		{"test FAIL", []legEvent{
			lev("start", legPkgA, "", ""), lev("run", legPkgA, "TestA", ""), lev("output", legPkgA, "TestA", "=== RUN   TestA\n"),
			lev("output", legPkgA, "TestA", "    a_test.go:9: want 1, got 2\n"), lev("output", legPkgA, "TestA", "--- FAIL: TestA (0.10s)\n"),
			lev("fail", legPkgA, "TestA", ""), lev("output", legPkgA, "", "FAIL\n"), lev("output", legPkgA, "", "FAIL\texample.com/m/a\t0.150s\n"),
			lev("fail", legPkgA, "", ""), lev("output", legPkgB, "", "ok  \texample.com/m/b\t0.300s\n"), lev("pass", legPkgB, "", ""),
		}, 1, []string{"--- FAIL: TestA (0.10s)\n", "want 1, got 2\n", "\nFAIL\texample.com/m/a\t0.150s\n", "ok  \texample.com/m/b\t0.300s\n"}},
		{"panic", []legEvent{
			lev("start", legPkgA, "", ""), lev("output", legPkgA, "TestA", "=== RUN   TestA\n"), lev("output", legPkgA, "TestA", "--- FAIL: TestA (0.00s)\n"),
			lev("output", legPkgA, "TestA", "panic: boom [recovered]\n"), lev("output", legPkgA, "TestA", "goroutine 7 [running]:\n"),
			lev("fail", legPkgA, "TestA", ""), lev("output", legPkgA, "", "FAIL\texample.com/m/a\t0.020s\n"), lev("fail", legPkgA, "", ""),
		}, 1, []string{"panic: boom [recovered]\n", "goroutine 7 [running]:\n", "FAIL\texample.com/m/a\t0.020s\n"}},
		{"build failure", []legEvent{
			{ImportPath: "example.com/m/c [example.com/m/c.test]", Action: "build-output", Output: "# example.com/m/c [example.com/m/c.test]\n"},
			{ImportPath: "example.com/m/c [example.com/m/c.test]", Action: "build-output", Output: "c/c_test.go:7:2: undefined: missingThing\n"},
			{ImportPath: "example.com/m/c [example.com/m/c.test]", Action: "build-fail"},
			lev("start", legPkgC, "", ""), lev("output", legPkgC, "", "FAIL\texample.com/m/c [build failed]\n"), lev("fail", legPkgC, "", ""),
		}, 1, []string{"# example.com/m/c [example.com/m/c.test]\n", "c/c_test.go:7:2: undefined: missingThing\n", "FAIL\texample.com/m/c [build failed]\n"}},
		{"timeout", []legEvent{
			lev("start", legPkgA, "", ""), lev("run", legPkgA, "TestHang", ""), lev("output", legPkgA, "TestHang", "=== RUN   TestHang\n"),
			lev("output", legPkgA, "", "panic: test timed out after 2s\n"), lev("output", legPkgA, "", "\trunning tests:\n"),
			lev("output", legPkgA, "", "\t\tTestHang (2s)\n"), lev("output", legPkgA, "", "FAIL\texample.com/m/a\t2.010s\n"), lev("fail", legPkgA, "", ""),
		}, 1, []string{"panic: test timed out after 2s\n", "\t\tTestHang (2s)\n", "FAIL\texample.com/m/a\t2.010s\n"}},
		{"killed mid-package", []legEvent{
			lev("start", legPkgA, "", ""), lev("output", legPkgA, "TestHang", "=== RUN   TestHang\n"), lev("output", legPkgA, "TestHang", "    still waiting\n"),
		}, 137, []string{"=== RUN   TestHang\n", "    still waiting\n"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := runIntegrationLeg(t, legStream(t, c.stream...), c.rc, legOpts{withDir: true})
			if r.code != c.rc || !strings.HasSuffix(r.stdout, "RETURNED rc="+strconv.Itoa(c.rc)+"\n") {
				t.Errorf("rc=%d; want go test's %d\nstdout:\n%s\nstderr:\n%s", r.code, c.rc, r.stdout, r.stderr)
			}
			for _, w := range c.want {
				if !strings.Contains(r.stdout, w) {
					t.Errorf("the log lacks %q\nstdout:\n%s", w, r.stdout)
				}
			}
		})
	}
}

func TestIntegrationLegJSONUnsetRunsPlainGoTest(t *testing.T) {
	t.Parallel()
	stream := legPassStream(t)
	r := runIntegrationLeg(t, stream, 0, legOpts{})
	if strings.Contains(" "+r.argv+" ", " -json ") || !strings.HasPrefix(r.argv, "test -mod=readonly -trimpath -tags=integration -count=1 -timeout=") ||
		!strings.HasSuffix(r.argv, " -run ^(TestA)$ ./a ./b") {
		t.Errorf("go test argv %q; want the plain call: no -json, -run and the packages last", r.argv)
	}
	if r.code != 0 || r.stdout != stream+"RETURNED rc=0\n" {
		t.Errorf("rc=%d; want go test's own stdout, untouched\nstdout:\n%s", r.code, r.stdout)
	}
	if _, err := os.Stat(r.jsonDir); !os.IsNotExist(err) {
		t.Errorf("a run without GO_INTEGRATION_LEG_JSON_DIR made %s (err %v)", r.jsonDir, err)
	}
}

// legTruncStream: go test passed, but the stream ends before b's final result.
func legTruncStream(t *testing.T) string {
	return legStream(t, lev("start", legPkgA, "", ""), lev("output", legPkgA, "", "ok  \texample.com/m/a\t1.000s\n"), lev("pass", legPkgA, "", ""),
		lev("start", legPkgB, "", ""), lev("output", legPkgB, "TestB", "=== RUN   TestB\n"))
}

func TestIntegrationLegJSONTruncatedStreamFailsAGreenCall(t *testing.T) {
	t.Parallel()
	r := runIntegrationLeg(t, legTruncStream(t), 0, legOpts{withDir: true})
	if r.code != 70 || !strings.Contains(r.stderr, "1 of 2 package(s) have a final result") || !strings.Contains(r.stderr, "truncated") {
		t.Errorf("rc=%d; want 70 with the truncation named\nstderr:\n%s", r.code, r.stderr)
	}
}

func TestIntegrationLegJSONBrokenRendererKeepsGoTestStatus(t *testing.T) {
	t.Parallel()
	r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, jqFilter: "this is not jq ("})
	if r.code != 0 || !strings.Contains(r.stderr, "renderer failed") || !strings.Contains(r.stdout, `"Action":"pass"`) {
		t.Errorf("rc=%d; want 0 (go test's), the failure named on stderr and the raw events in the log\nstdout:\n%s\nstderr:\n%s", r.code, r.stdout, r.stderr)
	}
}

func TestIntegrationLegJSONReturnsAfterALateRenderer(t *testing.T) {
	t.Parallel()
	r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, slowRead: true})
	if r.code != 0 || r.stdout != legPassPlain+"RETURNED rc=0\n" {
		t.Errorf("rc=%d stdout=%q; want the whole plain log before the call returns", r.code, r.stdout)
	}
}

// legTimeoutStream is the timed-out package of the red cells; legTimeoutWords are the words its log must carry.
func legTimeoutStream(t *testing.T) string {
	return legStream(t, lev("start", legPkgA, "", ""), lev("run", legPkgA, "TestHang", ""), lev("output", legPkgA, "TestHang", "=== RUN   TestHang\n"),
		lev("output", legPkgA, "", "panic: test timed out after 2s\n"), lev("output", legPkgA, "", "\trunning tests:\n"),
		lev("output", legPkgA, "", "\t\tTestHang (2s)\n"), lev("output", legPkgA, "", "FAIL\texample.com/m/a\t2.010s\n"), lev("fail", legPkgA, "", ""))
}

var legTimeoutWords = []string{"panic: test timed out after 2s\n", "\t\tTestHang (2s)\n", "FAIL\texample.com/m/a\t2.010s\n"}

func legMissingWords(r legRun, rc int) string {
	if r.code != rc || !strings.HasSuffix(r.stdout, "RETURNED rc="+strconv.Itoa(rc)+"\n") {
		return "rc " + strconv.Itoa(r.code) + ", want " + strconv.Itoa(rc)
	}
	for _, w := range legTimeoutWords {
		if !strings.Contains(r.stdout, w) {
			return "the log lacks " + strconv.Quote(w)
		}
	}
	return ""
}

// A go test that writes its stream and exits at once, 200 times: every rendered log carries the words (the renderer
// never ends before its last read of the file).
func TestIntegrationLegJSONWriteAndExitAtOnceKeepsTheWords(t *testing.T) {
	t.Parallel()
	const runs, width = 200, 8
	stream := legTimeoutStream(t)
	var mu sync.Mutex
	var misses []string
	slots := make(chan struct{}, width)
	var wg sync.WaitGroup
	for i := 0; i < runs; i++ {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			r := runIntegrationLeg(t, stream, 1, legOpts{withDir: true})
			if m := legMissingWords(r, 1); m != "" {
				mu.Lock()
				misses = append(misses, "run "+strconv.Itoa(i)+": "+m+"\nstdout:\n"+r.stdout+"stderr:\n"+r.stderr)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if len(misses) > 0 {
		t.Errorf("%d of %d runs lost rendered words; first:\n%s", len(misses), runs, misses[0])
	}
}

// uutils tail (ubuntu-26.04 runners) ends on a dead --pid with no last read: the renderer must not depend on any tail.
func TestIntegrationLegJSONDoesNotDependOnTailPid(t *testing.T) {
	t.Parallel()
	r := runIntegrationLeg(t, legTimeoutStream(t), 1, legOpts{withDir: true, uutilsTail: true, goDelay: "0.5"})
	if m := legMissingWords(r, 1); m != "" {
		t.Errorf("%s\nstdout:\n%s\nstderr:\n%s", m, r.stdout, r.stderr)
	}
}

// go test writes and exits while a read pass of the renderer runs: the renderer reads once more after the end.
func TestIntegrationLegJSONReadsOnceMoreAfterTheEnd(t *testing.T) {
	t.Parallel()
	r := runIntegrationLeg(t, legTimeoutStream(t), 1, legOpts{withDir: true, readNap: true, goDelay: "0.2"})
	if m := legMissingWords(r, 1); m != "" {
		t.Errorf("%s\nstdout:\n%s\nstderr:\n%s", m, r.stdout, r.stderr)
	}
}

// legRendererAlive lists the recorded renderer pids and the pids the bound line names still alive, polled up to 2 s (a
// killed child may still be reaped).
func legRendererAlive(t *testing.T, pidsFile, stderr string) (recorded int, alive []int) {
	t.Helper()
	raw, _ := os.ReadFile(pidsFile)
	named := ""
	if i := strings.Index(stderr, "stopping it (pids "); i >= 0 {
		named = stderr[i+len("stopping it (pids "):]
		named = named[:strings.IndexByte(named+")", ')')]
	}
	recorded = len(strings.Fields(string(raw)))
	var pids []int
	for _, f := range strings.Fields(string(raw) + " " + named) {
		if pid, err := strconv.Atoi(f); err == nil && pid > 1 {
			pids = append(pids, pid)
		}
	}
	for i := 0; i < 20; i++ {
		alive = alive[:0]
		for _, pid := range pids {
			if syscall.Kill(pid, 0) == nil {
				alive = append(alive, pid)
			}
		}
		if len(alive) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return recorded, alive
}

// legPadStream is legPassStream with more output than a pipe holds, so a renderer whose jq never reads is still
// writing at the bound.
func legPadStream(t *testing.T) string {
	pad := make([]legEvent, 0, 1500)
	for i := 0; i < 1500; i++ {
		pad = append(pad, lev("output", legPkgA, "TestA", "    a_test.go:9: "+strings.Repeat("x", 100)+"\n"))
	}
	return legStream(t, pad...) + legPassStream(t)
}

func TestIntegrationLegJSONStopsARendererPastItsBound(t *testing.T) {
	t.Parallel()
	for _, rc := range []int{0, 1} {
		t.Run("go test rc "+strconv.Itoa(rc), func(t *testing.T) {
			t.Parallel()
			r := runIntegrationLeg(t, legPadStream(t), rc, legOpts{withDir: true, hangJq: true})
			if r.code != rc || !strings.Contains(r.stdout, "RETURNED rc="+strconv.Itoa(rc)+"\n") {
				t.Errorf("rc=%d; want go test's %d\nstdout:\n%s\nstderr:\n%s", r.code, rc, r.stdout, r.stderr)
			}
			if !strings.Contains(r.stdout, `"Action":"pass","Package":"example.com/m/a"`) {
				t.Errorf("the raw events are not in the log past the bound\nstdout:\n%s", r.stdout)
			}
			if n, alive := legRendererAlive(t, r.pids, r.stderr); n < 2 || len(alive) > 0 || !strings.Contains(r.stderr, "stopping it (pids ") {
				t.Errorf("renderer: %d stand-ins recorded, still alive %v; want jq and a read recorded, the bound line, none left\nstderr:\n%s", n, alive, r.stderr)
			}
		})
	}
}

func TestIntegrationLegJSONPlantedDefectsAreCaught(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, old, new string
		run            func(t *testing.T, old, new string) bool // true when the defect is seen
	}{
		{"a passing package prints all its output", `if $e.Action == "fail" then $all`, `if true then $all`, func(t *testing.T, old, new string) bool {
			r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, mutateOld: old, mutateNew: new})
			return r.stdout != legPassPlain+"RETURNED rc=0\n"
		}},
		{"build output dropped", `elif $e.Action == "raw" or $e.Action == "build-output" then`, `elif $e.Action == "raw" then`, func(t *testing.T, old, new string) bool {
			r := runIntegrationLeg(t, legStream(t,
				legEvent{ImportPath: "example.com/m/c [example.com/m/c.test]", Action: "build-output", Output: "c/c_test.go:7:2: undefined: missingThing\n"},
				lev("output", legPkgC, "", "FAIL\texample.com/m/c [build failed]\n"), lev("fail", legPkgC, "", "")), 1, legOpts{withDir: true, mutateOld: old, mutateNew: new})
			return !strings.Contains(r.stdout, "undefined: missingThing")
		}},
		{"the renderer's status becomes the call's", `render_rc=$((jq_rc != 0 ? jq_rc : read_rc))`, `render_rc=$((jq_rc != 0 ? jq_rc : read_rc)); rc=${render_rc}`, func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, jqFilter: "this is not jq (", mutateOld: old, mutateNew: new}).code != 0
		}},
		{"a truncated stream stays green", "      rc=70\n    fi\n  fi\n", "      :\n    fi\n  fi\n", func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legTruncStream(t), 0, legOpts{withDir: true, mutateOld: old, mutateNew: new}).code == 0
		}},
		{"the raw events are not printed past the bound", "      wait \"${render_pid}\" 2>/dev/null || true\n      cat -- \"${json}\" || true\n", "      wait \"${render_pid}\" 2>/dev/null || true\n", func(t *testing.T, old, new string) bool {
			r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, hangJq: true, mutateOld: old, mutateNew: new})
			return !strings.Contains(r.stdout, `"Action":"pass","Package":"example.com/m/a"`)
		}},
		{"no wait for the renderer", `    for ((render_tick = 0; render_tick < render_wait * 5; render_tick++)); do`, `    render_wait=0; for ((render_tick = 0; render_tick < render_wait * 5; render_tick++)); do`, func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, slowRead: true, mutateOld: old, mutateNew: new}).stdout != legPassPlain+"RETURNED rc=0\n"
		}},
		{"the end flag is read after the last read", "        if [ -e \"${done_flag}\" ] || ! kill -0 \"$$\" 2>/dev/null; then last=1; fi\n        cat <&3 || exit 2\n", "        cat <&3 || exit 2\n        if [ -e \"${done_flag}\" ] || ! kill -0 \"$$\" 2>/dev/null; then last=1; fi\n", func(t *testing.T, old, new string) bool {
			return legMissingWords(runIntegrationLeg(t, legTimeoutStream(t), 1, legOpts{withDir: true, readNap: true, goDelay: "0.2", mutateOld: old, mutateNew: new}), 1) != ""
		}},
		{"the end flag before go test ends", "  wait \"${pid}\" || rc=$?\n  trap - INT TERM\n  if [ -n \"${render_pid}\" ]; then\n", "  [ -z \"${done_flag}\" ] || : >\"${done_flag}\"\n  wait \"${pid}\" || rc=$?\n  trap - INT TERM\n  if [ -n \"${render_pid}\" ]; then\n", func(t *testing.T, old, new string) bool {
			return legMissingWords(runIntegrationLeg(t, legTimeoutStream(t), 1, legOpts{withDir: true, goDelay: "1", mutateOld: old, mutateNew: new}), 1) != ""
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if !c.run(t, c.old, c.new) {
				t.Fatal("planted defect was NOT caught")
			}
		})
	}
}
