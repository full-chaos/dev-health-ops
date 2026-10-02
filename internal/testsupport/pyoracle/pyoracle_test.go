package pyoracle

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// TestInterpreterOverrideWins asserts DEV_HEALTH_PYTHON beats every other
// rule, including an existing repo .venv.
func TestInterpreterOverrideWins(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "/override/python")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != "/override/python" {
		t.Fatalf("path = %q, want the DEV_HEALTH_PYTHON override", path)
	}
	if !strings.Contains(rule, "DEV_HEALTH_PYTHON") {
		t.Fatalf("rule = %q, want it to name DEV_HEALTH_PYTHON", rule)
	}
}

// TestInterpreterPythonAliasWinsOverVenv asserts the deprecated PYTHON alias
// still beats the repo .venv when DEV_HEALTH_PYTHON is unset -- it is a
// fallback for the override rule, not for the venv rule.
func TestInterpreterPythonAliasWinsOverVenv(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "/alias/python")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != "/alias/python" {
		t.Fatalf("path = %q, want the PYTHON alias override", path)
	}
	if !strings.Contains(rule, "PYTHON") {
		t.Fatalf("rule = %q, want it to name the PYTHON alias", rule)
	}
}

// TestInterpreterFindsVenv asserts the repo .venv is used when neither env
// var is set.
func TestInterpreterFindsVenv(t *testing.T) {
	root := t.TempDir()
	venv := filepath.Join(root, ".venv", "bin", "python")
	if err := os.MkdirAll(filepath.Dir(venv), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(venv, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Fatalf("Interpreter: %v", err)
	}
	if path != venv {
		t.Fatalf("path = %q, want the repo venv %q", path, venv)
	}
	if !strings.Contains(rule, "venv") {
		t.Fatalf("rule = %q, want it to name the venv", rule)
	}
}

// TestInterpreterFallsBackToPath asserts PATH's python3 is used only when
// neither env var is set and no repo .venv exists.
func TestInterpreterFallsBackToPath(t *testing.T) {
	root := t.TempDir() // no .venv under here
	t.Setenv("DEV_HEALTH_PYTHON", "")
	t.Setenv("PYTHON", "")

	path, rule, err := Interpreter(root)
	if err != nil {
		t.Skipf("python3 not on PATH in this environment: %v", err)
	}
	if path == "" {
		t.Fatal("path is empty despite no error")
	}
	if !strings.Contains(rule, "PATH") {
		t.Fatalf("rule = %q, want it to name PATH", rule)
	}
}

// TestRunErrorNamesInterpreter asserts a run failure's message names the
// interpreter path, so a missing dependency cannot be misread as an oracle
// divergence.
func TestRunErrorNamesInterpreter(t *testing.T) {
	underlying := &fakeExitError{msg: "exit status 1"}
	wrapped := RunError("/some/venv/bin/python", underlying, []byte("ModuleNotFoundError: No module named 'httpx'"))
	if wrapped == nil {
		t.Fatal("RunError returned nil for a non-nil error")
	}
	if !strings.Contains(wrapped.Error(), "/some/venv/bin/python") {
		t.Fatalf("error %q does not name the interpreter path", wrapped.Error())
	}
	if !strings.Contains(wrapped.Error(), "ModuleNotFoundError") {
		t.Fatalf("error %q dropped the underlying output", wrapped.Error())
	}
}

func TestRunErrorNilPassthrough(t *testing.T) {
	if err := RunError("/some/python", nil, nil); err != nil {
		t.Fatalf("RunError(nil) = %v, want nil", err)
	}
}

type fakeExitError struct{ msg string }

func (e *fakeExitError) Error() string { return e.msg }

// TestDeployedVersionError pins the version gate at its boundary: the
// deployed release and newer pass; the release before it, an older major, an
// unparsable answer and a probe that failed to run are all refused.
func TestDeployedVersionError(t *testing.T) {
	cases := []struct {
		name    string
		output  string
		runErr  error
		wantErr bool
	}{
		{"deployed release", "3.14\n", nil, false},
		{"newer minor", "3.15\n", nil, false},
		{"newer major", "4.0\n", nil, false},
		{"one minor before", "3.13\n", nil, true},
		{"hosted runner release", "3.12\n", nil, true},
		{"older major", "2.7\n", nil, true},
		{"unparsable", "not-a-version\n", nil, true},
		{"empty answer", "", nil, true},
		{"probe failed", "3.14\n", os.ErrNotExist, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := DeployedVersionError("/x/python", []byte(tc.output), tc.runErr)
			if (err != nil) != tc.wantErr {
				t.Fatalf("output %q: err = %v, wantErr %v", tc.output, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "/x/python") {
				t.Fatalf("error %q does not name the interpreter", err)
			}
		})
	}
}

// A recording must not depend on the day's shell: ClosedEnv names its variables and inherits none
// (CHAOS-7471; an inherited AUTO_RUN_MIGRATIONS, SERVICE_NAME or OPERATIONAL_ORDERING_CONTRACT can change a
// recorded answer under the same header).
func TestClosedEnvInheritsNothingAndCarriesTheExtras(t *testing.T) {
	for _, name := range []string{"AUTO_RUN_MIGRATIONS", "SERVICE_NAME", "SERVICE_VERSION", "OPERATIONAL_ORDERING_CONTRACT", "LOG_LEVEL"} {
		t.Setenv(name, "ambient-value")
	}
	env := ClosedEnv("/repo", "CLICKHOUSE_URI=dsn", "OPERATIONAL_ORDERING_CONTRACT=2")
	got := map[string]string{}
	for _, entry := range env {
		name, value, _ := strings.Cut(entry, "=")
		if _, dup := got[name]; dup {
			t.Fatalf("%s is set twice in the closed environment", name)
		}
		got[name] = value
	}
	for _, name := range []string{"AUTO_RUN_MIGRATIONS", "SERVICE_NAME", "SERVICE_VERSION", "LOG_LEVEL"} {
		if _, inherited := got[name]; inherited {
			t.Errorf("the closed environment inherited %s", name)
		}
	}
	if got["OPERATIONAL_ORDERING_CONTRACT"] != "2" || got["CLICKHOUSE_URI"] != "dsn" || got["PYTHONPATH"] != filepath.Join("/repo", "src") || got["PYTHONHASHSEED"] != "0" {
		t.Errorf("the closed environment lost a named variable: %v", got)
	}
}

// The recorders of the goldens that were hand-recorded with the whole shell environment run Python
// through ClosedEnv and never inherit it: a closed list of the test files, each of which must call
// ClosedEnv and must not call os.Environ().
func TestTheClosedEnvironmentRecordersDoNotInheritTheEnvironment(t *testing.T) {
	for _, file := range []string{
		"../../operationalbackfill/backfill_integration_test.go",
	} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if strings.Contains(text, "os.Environ()") {
			t.Errorf("%s inherits the environment (os.Environ()): a recording must run in pyoracle.ClosedEnv", file)
		}
		if !strings.Contains(text, "pyoracle.ClosedEnv(") {
			t.Errorf("%s does not run Python through pyoracle.ClosedEnv", file)
		}
	}
}

// refusingInterpreter is a stand-in interpreter that behaves like the real one
// under a recording test's guard: with PYTHONHOME set it cannot start (it says
// why on standard error and exits 1); otherwise it writes the environment it
// got to envFile and prints version.
func refusingInterpreter(t *testing.T, version string) (python, envFile string) {
	t.Helper()
	dir := t.TempDir()
	envFile = filepath.Join(dir, "env")
	python = filepath.Join(dir, "python3")
	script := "#!/bin/sh\nif [ -n \"${PYTHONHOME+set}\" ]; then echo \"Fatal Python error: Failed to import encodings module; PYTHONHOME = '$PYTHONHOME'\" >&2; exit 1; fi\n" +
		"env > '" + envFile + "'\nprintf '%s\\n' '" + version + "'\n"
	if err := os.WriteFile(python, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return python, envFile
}

// The version probe is a Python child like any other: it runs in the closed
// environment and takes nothing of the process, so the variable a recording
// test holds to stop every inheriting Python child does not stop the probe.
func TestTheVersionProbeRunsInTheClosedEnvironment(t *testing.T) {
	python, envFile := refusingInterpreter(t, "3.14")
	root := t.TempDir()
	t.Setenv("PYTHONHOME", "/a-directory-that-does-not-exist")
	t.Setenv("AMBIENT_OF_THE_DAY", "1")
	if err := probeDeployed(t, python, root); err != nil {
		t.Fatalf("the probe did not pass under a recording test's guard: %v", err)
	}
	raw, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		// The shell adds its own bookkeeping; only what the caller gave it counts.
		if name, _, _ := strings.Cut(line, "="); name == "PWD" || name == "SHLVL" || name == "_" || name == "OLDPWD" {
			continue
		}
		got[line] = true
	}
	want := map[string]bool{}
	for _, entry := range ClosedEnv(root) {
		want[entry] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the probe's environment:\n got  %v\n want %v (ClosedEnv, nothing else)", got, want)
	}
}

// A probe that cannot start says why: the end of the interpreter's standard
// error is in the error, not only its exit status.
func TestAProbeThatCannotStartSaysWhatTheInterpreterWrote(t *testing.T) {
	dir := t.TempDir()
	python := filepath.Join(dir, "python3")
	if err := os.WriteFile(python, []byte("#!/bin/sh\necho 'Fatal Python error: the reason is here' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := probeDeployed(t, python, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "Fatal Python error: the reason is here") || !strings.Contains(err.Error(), "exit status 1") {
		t.Fatalf("the error does not hold what the interpreter wrote: %v", err)
	}
	old, _ := refusingInterpreter(t, "3.12")
	if err := probeDeployed(t, old, t.TempDir()); err == nil || !strings.Contains(err.Error(), `resolved Python "3.12"`) {
		t.Fatalf("an older release was not refused by what it said: %v", err)
	}
}

// In a recording a test that is not on the closed list resolves a stand-in
// that runs nothing, and a package on it, a run that is no recording, and the
// launcher get the interpreter.
func TestInARecordingATestOutsideTheClosedListIsGivenAnInterpreterThatRefuses(t *testing.T) {
	real := filepath.Join(t.TempDir(), "real-python")
	if err := os.WriteFile(real, []byte("#!/bin/sh\necho real\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", real)
	root := t.TempDir()

	t.Setenv(RecordingEnv, "1")
	standIn := Resolve(t, root)
	if standIn == real {
		t.Fatal("a test outside the closed list was given the real interpreter in a recording")
	}
	out, err := exec.Command(standIn).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 97 || !strings.Contains(string(out), "producer launcher") {
		t.Fatalf("the stand-in ran as %v with %q, want exit 97 and the way out", err, out)
	}
	if got := ResolveLauncher(t, root); got != real {
		t.Fatalf("the launcher was given %s, want the real interpreter", got)
	}

	t.Chdir(filepath.Join(repoRootOf(t), "internal", "pgmigrate"))
	if got := Resolve(t, root); got != real {
		t.Fatalf("a package on the closed list was given %s in a recording, want the real interpreter", got)
	}

	// The match is exact: a package UNDER a listed one is not on the list
	// (internal/apiservice is listed, internal/apiservice/admin is not).
	t.Chdir(filepath.Join(repoRootOf(t), "internal", "apiservice", "admin"))
	if got := Resolve(t, root); got == real {
		t.Fatal("a package under a listed one was given the real interpreter in a recording")
	}

	t.Setenv(RecordingEnv, "")
	t.Chdir(filepath.Join(repoRootOf(t), "internal", "testsupport", "pyoracle"))
	if got := Resolve(t, root); got != real {
		t.Fatalf("outside a recording the test was given %s, want the real interpreter", got)
	}
}

func repoRootOf(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if filepath.Dir(dir) == dir {
			t.Fatal("no go.mod")
		}
	}
}

// The closed list names directories that exist (it only shrinks: ci/ratchets.tsv holds its size to the merge base).
func TestTheClosedListOfOwnLaunchPackagesOnlyShrinks(t *testing.T) {
	list := UnconvertedOwnLaunch()
	if len(list) == 0 {
		t.Fatal("the closed list is empty: this test then checks nothing; when the last package is converted, delete the list and this test together")
	}
	root := repoRootOf(t)
	for _, dir := range list {
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(dir))); err != nil || !info.IsDir() {
			t.Errorf("%s is on the closed list and is not a directory: a converted or removed package leaves the list", dir)
		}
	}
}

// unconvertedDayOne is the closed list as it was when the guard began
// (CHAOS-7707). An entry that is not in it is a package added to the list,
// which this change cannot do: a package that starts its own Python child
// converts to the launcher instead. Removing a line from the list is allowed,
// so a swap (one out, one in) and a raise of the ceiling both fail here.
var unconvertedDayOne = map[string]bool{
	"internal/adminops":                true,
	"internal/api/externalingest":      true,
	"internal/api/legacyingest":        true,
	"internal/apiservice":              true,
	"internal/apiservice/customerpush": true,
	"internal/backfillrun":             true,
	"internal/chmigrate":               true,
	"internal/fixturescli":             true,
	"internal/maintenancecli":          true,
	"internal/metricscli":              true,
	"internal/operationalbackfill":     true,
	"internal/pgmigrate":               true,
	"internal/providersync":            true,
	"internal/pushcli":                 true,
}

func TestTheClosedListHoldsNoPackageThatWasNotThereOnDayOne(t *testing.T) {
	for _, dir := range UnconvertedOwnLaunch() {
		if !unconvertedDayOne[dir] {
			t.Errorf("%s is on unconverted_own_launch.txt and was not on the day-one list: convert its Python launch to the producer's launcher instead of listing it", dir)
		}
	}
}

// The closed child environment is pinned as a literal. Every golden was
// recorded under it and no key holds its constants, so a change here changes
// no golden's key and no replay refuses: the answers of the old environment
// would be served as the new one's.
func TestTheClosedEnvironmentIsPinnedAsAnyGoldenWasRecordedUnderIt(t *testing.T) {
	want := []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8", "LC_ALL=C.UTF-8", "TZ=UTC",
		"PYTHONHASHSEED=0", "PYTHONDONTWRITEBYTECODE=1",
		"PYTHONPATH=/ROOT/src",
		"OTEL_ENABLED=false",
	}
	got := ClosedEnv("/ROOT")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ClosedEnv changed:\n got %q\nwant %q\nevery golden was recorded under the old closed environment: re-record all of them, then move this pin", got, want)
	}
}

// Interpreter itself refuses in a recording for a package that is not on the
// closed list: a new test that resolves the interpreter's path by itself and
// starts it with the environment it built is not let through by skipping Resolve.
func TestInterpreterRefusesInARecordingOutsideTheClosedList(t *testing.T) {
	real := filepath.Join(t.TempDir(), "real-python")
	if err := os.WriteFile(real, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEV_HEALTH_PYTHON", real)
	root := t.TempDir()

	t.Setenv(RecordingEnv, "1")
	if path, _, err := Interpreter(root); err == nil {
		t.Fatalf("Interpreter gave %s in a recording to a package outside the closed list", path)
	}
	// A package UNDER a listed one is outside it (exact match).
	t.Chdir(filepath.Join(repoRootOf(t), "internal", "apiservice", "admin"))
	if _, _, err := Interpreter(root); err == nil {
		t.Fatal("Interpreter gave the interpreter to a package under a listed one")
	}
	t.Chdir(filepath.Join(repoRootOf(t), "internal", "pgmigrate"))
	if path, _, err := Interpreter(root); err != nil || path != real {
		t.Fatalf("a package on the closed list: %s, %v", path, err)
	}
	t.Chdir(filepath.Join(repoRootOf(t), "internal", "testsupport", "pyoracle"))
	if got := ResolveLauncher(t, root); got != real {
		t.Fatalf("the launcher resolve was refused: %s", got)
	}
	t.Setenv(RecordingEnv, "")
	if path, _, err := Interpreter(root); err != nil || path != real {
		t.Fatalf("outside a recording: %s, %v", path, err)
	}
}

// A LookPath or a launch of a python name cannot be refused in a recording (it is the
// standard library), so the files that start a Python they looked up by name
// (or start it by that name) are a frozen set: a new file that does is RED until it goes through the
// producer's launcher. The exceptions below never record a golden (CHAOS-7820).
var lookPathPythonDayOne = map[string]bool{
	"internal/testsupport/pyoracle/pyoracle.go":             true, // the resolver itself
	"internal/testsupport/venueoracle/venueoracle.go":       true, // the launcher's PATH check
	"internal/apiservice/admin/orgdeletion_targets_test.go": true,
	"internal/pgmigrate/preflight_test.go":                  true,
	// Launches by the name "python3" (the venue puts the interpreter's
	// directory first on PATH): started with the process environment, a
	// recording's poison (PYTHONHOME) stops these, so none records today.
	"internal/testsupport/programoracle/programoracle.go":                              true,
	"internal/apiservice/metricsvenue/counter_parity_venue_oracle_integration_test.go": true,
	"internal/testsupport/venueoracle/producer.go":                                     true,
}

func TestNoNewFileLooksPythonUpByName(t *testing.T) {
	root := repoRootOf(t)
	pattern := regexp.MustCompile(`exec\.(LookPath|Command)\(\s*"python|exec\.CommandContext\([^,]+,\s*"python`)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".venv", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if pattern.Match(raw) && !lookPathPythonDayOne[rel] {
			t.Errorf("%s looks Python up by name (exec.LookPath): a recording must start Python only through the producer's launcher (venueoracle Producer.Command)", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A listed exception that no longer looks Python up leaves the set.
	for rel := range lookPathPythonDayOne {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err == nil && !pattern.Match(raw) {
			t.Errorf("%s no longer looks Python up by name: remove it from lookPathPythonDayOne", rel)
		}
	}
}

// NOT seen by the sweep above (named in the PR body): a name built at run time
// (a variable, a concatenation), a launch through a shell, a helper that takes
// the name as a parameter, and a launch of the interpreter by a path.
