package testshard_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
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
	// CHAOS-13838: every go test of these legs gets ONE GOFLAGS value: "-trimpath" (the warm cache's key for the go the tests run
	// themselves), and with the knob exactly "-count=1 -trimpath". An ambient GOFLAGS never reaches them.
	cases := []struct {
		name string
		env  []string
		want string
		knob bool
	}{
		{"knob on", []string{"CHECK_GO_FRESH=1"}, "-count=1 -trimpath", true},
		{"knob on with an ambient GOFLAGS", []string{"CHECK_GO_FRESH=1", "GOFLAGS=-tags=integration"}, "-count=1 -trimpath", true},
		{"knob off", []string{"CHECK_GO_FRESH="}, "-trimpath", false},
		{"knob off with an ambient GOFLAGS", []string{"CHECK_GO_FRESH=", "GOFLAGS=-tags=integration"}, "-trimpath", false},
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
			if on := strings.Contains(run.Stderr, freshOnLine); on != c.knob {
				t.Errorf("%v, %s: the knob line printed=%v, want %v: %s", leg, c.name, on, c.knob, run.Stderr)
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

func TestNoGoCallEnvHoldsTheFreshKnobItself(t *testing.T) {
	t.Parallel()
	freshLog := filepath.Join(t.TempDir(), "go_fresh.log")
	if err := os.WriteFile(freshLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	run, flags := goFlagsOfCalls(t, []string{"CI=", "GITHUB_ACTIONS=", "CHECK_GO_FRESH=1", "FAKE_GO_FRESH_LOG=" + freshLog}, "ci-leg", "test", "350", "350")
	if run.Err != nil {
		t.Fatalf("%v\n%s", run.Err, run.Stderr)
	}
	b, err := os.ReadFile(freshLog)
	if err != nil {
		t.Fatal(err)
	}
	seen := strings.Fields(string(b))
	if len(seen) == 0 || len(seen) != len(run.Calls) || len(flags) != len(seen) {
		t.Fatalf("%d fresh record(s), %d GOFLAGS record(s) for %d go test call(s): the scan saw nothing", len(seen), len(flags), len(run.Calls))
	}
	for i, s := range seen {
		if s != "unset" {
			t.Errorf("go test %s ran with CHECK_GO_FRESH in its environment: a child check_go.sh would inherit the knob", run.Calls[i])
		}
		if flags[i] != "-count=1 -trimpath" {
			t.Errorf("go test %s ran with GOFLAGS=%q, want \"-count=1 -trimpath\": the knob must still work", run.Calls[i], flags[i])
		}
	}
}

// legGoflagsSites maps each function of ci/check_go.sh that sets GOFLAGS="${LEG_TEST_GOFLAGS}" to how often it does (top level
// counts as "").
func legGoflagsSites(source string) map[string]int {
	fn := regexp.MustCompile(`^([a-z_][a-z0-9_]*)\(\) \{$`)
	sites, cur := map[string]int{}, ""
	for _, line := range strings.Split(source, "\n") {
		if m := fn.FindStringSubmatch(line); m != nil {
			cur = m[1]
		} else if line == "}" {
			cur = ""
		}
		sites[cur] += strings.Count(line, `GOFLAGS="${LEG_TEST_GOFLAGS}"`)
		if sites[cur] == 0 {
			delete(sites, cur)
		}
	}
	return sites
}

// CHAOS-13838: the one GOFLAGS value is set at the three go test calls of the test and race legs and nowhere else, so the vet,
// build, lint and integration calls keep their own keys and the ambient scrub (GO_ENV_OFF) stays the rule for every other call.
func TestTheLegGoflagsAreSetOnlyAtTheThreeLegGoTestCalls(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(b)
	want := map[string]int{"go_test_leg": 1, "go_test_json_run_t": 1, "check_providersync_race_shard": 1}
	if got := legGoflagsSites(source); !reflect.DeepEqual(got, want) {
		t.Errorf("GOFLAGS=\"${LEG_TEST_GOFLAGS}\" is set in %v, want exactly %v", got, want)
	}
	for _, line := range []string{"LEG_TEST_GOFLAGS=-trimpath", `if [ "${CHECK_GO_FRESH:-}" = 1 ]; then LEG_TEST_GOFLAGS="-count=1 -trimpath"; fi`} {
		if strings.Count(source, "\n"+line+"\n") != 1 {
			t.Errorf("ci/check_go.sh must define the leg GOFLAGS once as %q", line)
		}
	}
	for _, c := range []struct{ name, from, to string }{
		{"the providersync slice loses it", `"${GO_ENV_OFF[@]}" GOFLAGS="${LEG_TEST_GOFLAGS}" GOWORK=off go test -mod=readonly -trimpath -race`, `"${GO_ENV_OFF[@]}" GOWORK=off go test -mod=readonly -trimpath -race`},
		{"the vet stage gains it", "\ncheck_vet() {\n", "\ncheck_vet() {\n  : GOFLAGS=\"${LEG_TEST_GOFLAGS}\"\n"},
	} {
		planted := strings.Replace(source, c.from, c.to, 1)
		if planted == source {
			t.Fatalf("%s: the text to plant is not in ci/check_go.sh", c.name)
		}
		if reflect.DeepEqual(legGoflagsSites(planted), want) {
			t.Errorf("%s: the site guard still holds", c.name)
		}
	}
}
