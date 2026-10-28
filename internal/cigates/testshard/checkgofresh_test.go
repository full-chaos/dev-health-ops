package testshard_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11725: CHECK_GO_FRESH=1 puts GOFLAGS=-count=1 on every go call of ci/check_go.sh in a plain local run, so no test
// result comes from the test cache. Under CI=true or GITHUB_ACTIONS=true the script refuses the knob before any go test runs.

const (
	freshOnLine      = "check_go.sh: CHECK_GO_FRESH=1: GOFLAGS=-count=1 on every go call (test cache off, local only)\n"
	freshRefusedLine = "ERROR: CHECK_GO_FRESH=1 is for local runs only; refused because CI=true or GITHUB_ACTIONS=true"
	freshBadLine     = "ERROR: CHECK_GO_FRESH must be 1 or unset, got 'yes'"
)

// goFlagsOfCalls runs ci/check_go.sh with the stand-in go and returns the run and the GOFLAGS value of each go test call.
func goFlagsOfCalls(t *testing.T, env []string, args ...string) (legRun, []string) {
	t.Helper()
	envLog := filepath.Join(t.TempDir(), "go_env.log")
	if err := os.WriteFile(envLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	run := runCheckGo(t, append(env, "FAKE_GO_ENV_LOG="+envLog), false, args...)
	b, err := os.ReadFile(envLog)
	if err != nil {
		t.Fatal(err)
	}
	var flags []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			flags = append(flags, strings.TrimPrefix(l, "GOFLAGS="))
		}
	}
	return run, flags
}

func TestTheFreshKnobPutsCountOneOnEveryGoTestOfTheTestAndRaceLegs(t *testing.T) {
	t.Parallel()
	local := []string{"CI=", "GITHUB_ACTIONS="}
	cases := []struct {
		name string
		env  []string
		want string
	}{
		{"knob on", []string{"CHECK_GO_FRESH=1"}, "-count=1"},
		{"knob on with an ambient GOFLAGS", []string{"CHECK_GO_FRESH=1", "GOFLAGS=-tags=integration"}, "-count=1"},
		{"knob off", []string{"CHECK_GO_FRESH="}, ""},
	}
	// The test leg's slice 350 of 350 holds no race-excluded row, so the stand-in's canned run passes it.
	for _, leg := range [][]string{{"ci-leg", "test", "350", "350"}, {"ci-leg", "race", "1", fmt.Sprint(shardCount)}} {
		for _, c := range cases {
			run, flags := goFlagsOfCalls(t, append(append([]string{}, local...), c.env...), leg...)
			if run.Err != nil {
				t.Fatalf("%v, %s: %v\n%s", leg, c.name, run.Err, run.Stderr)
			}
			if len(flags) == 0 || len(flags) != len(run.Calls) {
				t.Fatalf("%v, %s: %d GOFLAGS record(s) for %d go test call(s): the scan saw nothing", leg, c.name, len(flags), len(run.Calls))
			}
			for i, f := range flags {
				if f != c.want {
					t.Errorf("%v, %s: go test %s ran with GOFLAGS=%q, want %q", leg, c.name, run.Calls[i], f, c.want)
				}
			}
			if on := strings.Contains(run.Stderr, freshOnLine); on != (c.want != "") {
				t.Errorf("%v, %s: the knob line printed=%v, want %v: %s", leg, c.name, on, c.want != "", run.Stderr)
			}
		}
	}
}

type envIRun struct {
	stdout, stderr string
	code           int
	testCalls      []string
	listCalls      []string
}

// checkGoEnvI runs ci/check_go.sh with the stand-in go and ONLY the given environment (as env -i would), plus the paths
// the harness and go need.
func checkGoEnvI(t *testing.T, env []string, args ...string) envIRun {
	t.Helper()
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fmt.Sprintf(standInGo, realGo)), 0o755); err != nil {
		t.Fatal(err)
	}
	log, listLog := filepath.Join(dir, "go_test.log"), filepath.Join(dir, "go_list.log")
	for _, f := range []string{log, listLog} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	goEnv, err := exec.Command(realGo, "env", "GOCACHE", "GOMODCACHE", "GOPATH").Output()
	if err != nil {
		t.Fatal(err)
	}
	paths := strings.Split(strings.TrimSpace(string(goEnv)), "\n")
	if len(paths) != 3 {
		t.Fatalf("go env GOCACHE GOMODCACHE GOPATH: %q", goEnv)
	}
	base := []string{
		"PATH=" + bin + string(os.PathListSeparator) + filepath.Dir(realGo) + string(os.PathListSeparator) + "/usr/bin:/bin",
		"HOME=" + dir, "TMPDIR=" + dir, "GOCACHE=" + paths[0], "GOMODCACHE=" + paths[1], "GOPATH=" + paths[2], "GOPROXY=off",
		"FAKE_GO_LOG=" + log, "FAKE_GO_LIST_LOG=" + listLog,
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "ci", "check_go.sh")}, args...)...)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(base, env...)
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	code := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatal(err)
		}
		code = exitErr.ExitCode()
	}
	lines := func(p string) []string {
		b, _ := os.ReadFile(p)
		var r []string
		for _, l := range strings.Split(string(b), "\n") {
			if l != "" {
				r = append(r, l)
			}
		}
		return r
	}
	return envIRun{out.String(), errb.String(), code, lines(log), lines(listLog)}
}

func TestTheFreshKnobIsRefusedOnAHostedLikeRunner(t *testing.T) {
	t.Parallel()
	hosted := func(ci, actions string) []string {
		return []string{"CI=" + ci, "GITHUB_ACTIONS=" + actions, "GITHUB_RUN_ID=1", "RUNNER_TEMP=" + t.TempDir(), "CHECK_GO_FRESH=1"}
	}
	for _, c := range []struct {
		name string
		env  []string
	}{
		{"CI and GITHUB_ACTIONS", hosted("true", "true")},
		{"CI alone", hosted("true", "")},
		{"GITHUB_ACTIONS alone", hosted("", "true")},
	} {
		run := checkGoEnvI(t, c.env, "ci-leg", "test", "1", fmt.Sprint(shardCount))
		if run.code != 2 || !strings.Contains(run.stderr, freshRefusedLine) {
			t.Errorf("%s: want exit 2 and the refusal, got exit %d: %s", c.name, run.code, run.stderr)
		}
		if len(run.testCalls) != 0 || len(run.listCalls) != 0 {
			t.Errorf("%s: the refused knob still ran %d go test call(s) and %d listing(s)", c.name, len(run.testCalls), len(run.listCalls))
		}
		if strings.Contains(run.stderr, freshOnLine) {
			t.Errorf("%s: the refused knob printed the on line: %s", c.name, run.stderr)
		}
	}
	on := checkGoEnvI(t, []string{"CHECK_GO_FRESH=1"}, "help")
	if on.code != 0 || !strings.Contains(on.stderr, freshOnLine) {
		t.Errorf("a plain local run with the knob: want exit 0 and the on line, got exit %d: %s", on.code, on.stderr)
	}
	bad := checkGoEnvI(t, []string{"CHECK_GO_FRESH=yes"}, "help")
	if bad.code != 2 || !strings.Contains(bad.stderr, freshBadLine) {
		t.Errorf("CHECK_GO_FRESH=yes: want exit 2 and %q, got exit %d: %s", freshBadLine, bad.code, bad.stderr)
	}
	for _, value := range []string{"", "0"} {
		off := checkGoEnvI(t, append(hosted("true", "true")[:4], "CHECK_GO_FRESH="+value), "help")
		if off.code != 0 || strings.Contains(off.stderr, "CHECK_GO_FRESH") {
			t.Errorf("CHECK_GO_FRESH=%q on a hosted-like runner: want exit 0 and no knob line, got exit %d: %s", value, off.code, off.stderr)
		}
	}
}

func TestAFileReadByAChildProcessIsServedFromTheTestCacheUnlessCountIsOne(t *testing.T) {
	t.Parallel()
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	mod, cache := t.TempDir(), t.TempDir()
	for name, text := range map[string]string{
		"go.mod": "module freshprobe\n\ngo 1.27\n",
		"probe_test.go": "package freshprobe\n\nimport (\n\t\"os/exec\"\n\t\"strings\"\n\t\"testing\"\n)\n\n" +
			"func TestTheTemplateSaysA(t *testing.T) {\n\tout, err := exec.Command(\"cat\", \"tpl.txt\").Output()\n" +
			"\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\tif strings.TrimSpace(string(out)) != \"A\" {\n\t\tt.Fatalf(\"tpl.txt = %q\", out)\n\t}\n}\n",
		"tpl.txt": "A\n",
	} {
		if err := os.WriteFile(filepath.Join(mod, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	goTest := func(goflags string) (string, error) {
		cmd := exec.Command(realGo, "test", ".")
		cmd.Dir = mod
		cmd.Env = append(os.Environ(), "GOCACHE="+cache, "GOFLAGS="+goflags, "GOWORK=off", "GOPROXY=off", "GOTOOLCHAIN=local")
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := goTest(""); err != nil || strings.Contains(out, "(cached)") {
		t.Fatalf("first run: want a fresh pass, got %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(mod, "tpl.txt"), []byte("B\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := goTest(""); err != nil || !strings.Contains(out, "(cached)") {
		t.Fatalf("after a template-only edit the cached pass is the class this guards: got %v: %s", err, out)
	}
	if out, err := goTest("-count=1"); err == nil || !strings.Contains(out, "tpl.txt = ") {
		t.Fatalf("GOFLAGS=-count=1 must run the test again and see the edit: got %v: %s", err, out)
	}
}
