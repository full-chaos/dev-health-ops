package checkgo_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
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
	withDir   bool
	jqFilter  string // replaces INTEGRATION_LEG_JQ when set
	slowTail  bool   // the renderer's tail starts 2 s late
	hangTail  bool   // the renderer never ends (its tail sleeps); the tail and jq stand-ins record their pids
	mutateOld string // planted defect: old -> new in ci/check_go.sh
	mutateNew string
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
	fakeGo := "#!/bin/sh\nprintf '%s\\n' \"$*\" > " + argv + "\ncat " + events + "\nexit " + strconv.Itoa(goRC) + "\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fakeGo), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "reap-testcontainers-session.sh"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if o.slowTail {
		realTail, err := exec.LookPath("tail")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, "tail"), []byte("#!/bin/sh\nsleep 2\nexec "+realTail+" \"$@\"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	pids := filepath.Join(dir, "renderer-pids")
	if o.hangTail {
		realJq, err := exec.LookPath("jq")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, "tail"), []byte("#!/bin/sh\necho $$ >> "+pids+"\nexec sleep 300\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bin, "jq"), []byte("#!/bin/sh\necho $$ >> "+pids+"\nexec "+realJq+" \"$@\"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
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
	if o.hangTail {
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
	r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, slowTail: true})
	if r.code != 0 || r.stdout != legPassPlain+"RETURNED rc=0\n" {
		t.Errorf("rc=%d stdout=%q; want the whole plain log before the call returns", r.code, r.stdout)
	}
}

// legRendererAlive lists the recorded renderer pids still alive, polled up to 2 s (a killed child may still be reaped).
func legRendererAlive(t *testing.T, pidsFile string) (recorded int, alive []int) {
	t.Helper()
	raw, _ := os.ReadFile(pidsFile)
	var pids []int
	for _, f := range strings.Fields(string(raw)) {
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
	return len(pids), alive
}

func TestIntegrationLegJSONStopsARendererPastItsBound(t *testing.T) {
	t.Parallel()
	for _, rc := range []int{0, 1} {
		t.Run("go test rc "+strconv.Itoa(rc), func(t *testing.T) {
			t.Parallel()
			r := runIntegrationLeg(t, legPassStream(t), rc, legOpts{withDir: true, hangTail: true})
			if r.code != rc || !strings.Contains(r.stdout, "RETURNED rc="+strconv.Itoa(rc)+"\n") {
				t.Errorf("rc=%d; want go test's %d\nstdout:\n%s\nstderr:\n%s", r.code, rc, r.stdout, r.stderr)
			}
			if !strings.Contains(r.stdout, `"Action":"pass","Package":"example.com/m/a"`) {
				t.Errorf("the raw events are not in the log past the bound\nstdout:\n%s", r.stdout)
			}
			if n, alive := legRendererAlive(t, r.pids); n < 2 || len(alive) > 0 {
				t.Errorf("renderer stand-ins: %d recorded, still alive %v; want both recorded and none left", n, alive)
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
		{"the renderer's status becomes the call's", `render_rc=$((jq_rc != 0 ? jq_rc : tail_rc))`, `render_rc=$((jq_rc != 0 ? jq_rc : tail_rc)); rc=${render_rc}`, func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, jqFilter: "this is not jq (", mutateOld: old, mutateNew: new}).code != 0
		}},
		{"a truncated stream stays green", "      rc=70\n    fi\n  fi\n", "      :\n    fi\n  fi\n", func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legTruncStream(t), 0, legOpts{withDir: true, mutateOld: old, mutateNew: new}).code == 0
		}},
		{"the raw events are not printed past the bound", "      wait \"${render_pid}\" 2>/dev/null || true\n      cat -- \"${json}\" || true\n", "      wait \"${render_pid}\" 2>/dev/null || true\n", func(t *testing.T, old, new string) bool {
			r := runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, hangTail: true, mutateOld: old, mutateNew: new})
			return !strings.Contains(r.stdout, `"Action":"pass","Package":"example.com/m/a"`)
		}},
		{"no wait for the renderer", `    for ((render_tick = 0; render_tick < render_wait * 5; render_tick++)); do`, `    render_wait=0; for ((render_tick = 0; render_tick < render_wait * 5; render_tick++)); do`, func(t *testing.T, old, new string) bool {
			return runIntegrationLeg(t, legPassStream(t), 0, legOpts{withDir: true, slowTail: true, mutateOld: old, mutateNew: new}).stdout != legPassPlain+"RETURNED rc=0\n"
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
