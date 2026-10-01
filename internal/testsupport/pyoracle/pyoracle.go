// Package pyoracle resolves the interpreter every live-Python oracle test in
// this repository compares Go against.
//
// It exists because two policies grew independently: chschema resolved
// DEV_HEALTH_PYTHON, then the repo's checked-out .venv, then PATH; every
// other oracle honoured a differently-named PYTHON variable and fell back
// straight to a bare "python3" on PATH, skipping the venv entirely. Setting
// one variable fixed some oracles and left the rest pointed at whatever
// system interpreter happened to be first on PATH -- which reports a missing
// dependency as if it were a Go/Python divergence, because the failure text
// never says which interpreter ran.
//
// One policy, one primary variable: DEV_HEALTH_PYTHON, then the deprecated
// PYTHON alias, then the repo's checked-out .venv, then PATH.
package pyoracle

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Interpreter resolves the interpreter a live-Python oracle should run
// against, given the repository root (the directory a caller would also use
// to build PYTHONPATH). It reports which rule matched, so callers can make
// the resolution visible even on a passing run.
//
// In a recording (RecordingEnv) a test that is not on the closed list of
// packages that still launch their own Python (unconverted_own_launch.txt)
// is refused here as well as in Resolve: the interpreter's path is the one
// thing an own-launch site needs, and a refusal on Resolve alone left every
// caller of Interpreter outside the guard. The producer's launcher resolves
// through ResolveLauncher, which is not refused.
func Interpreter(root string) (path string, rule string, err error) {
	if os.Getenv(RecordingEnv) == "1" {
		if dir, dirErr := testPackageDir(); dirErr != nil || !unconverted(dir) {
			return "", "", fmt.Errorf("a recording starts Python only through the producer launcher (venueoracle Producer.Command with golden.Produce); package %q resolves an interpreter by itself: convert the site", dir)
		}
	}
	return interpreter(root)
}

// interpreter is Interpreter's resolution, with no recording guard.
func interpreter(root string) (path string, rule string, err error) {
	if override := os.Getenv("DEV_HEALTH_PYTHON"); override != "" {
		return override, "DEV_HEALTH_PYTHON override", nil
	}
	if alias := os.Getenv("PYTHON"); alias != "" {
		return alias, "PYTHON override (deprecated alias of DEV_HEALTH_PYTHON)", nil
	}
	venv := filepath.Join(root, ".venv", "bin", "python")
	if info, statErr := os.Stat(venv); statErr == nil && !info.IsDir() {
		return venv, "repo .venv at " + venv, nil
	}
	found, lookErr := exec.LookPath("python3")
	if lookErr != nil {
		return "", "", fmt.Errorf(
			"no Python to run the live oracle: %s does not exist, "+
				"DEV_HEALTH_PYTHON and PYTHON are both unset, and python3 is not on PATH: %w",
			venv, lookErr)
	}
	return found, "python3 on PATH", nil
}

// Resolve is the standard entry point for a live-Python oracle test: it
// resolves the interpreter under root, logs the resolved path and which rule
// matched (so a wrong-but-passing run is never silent), and fails the test
// naming the interpreter search when none can be found.
//
// In a recording (the record verb sets RecordingEnv for its runs) a test that
// is not on the closed list of packages that still launch their own Python
// (unconverted_own_launch.txt) gets a stand-in that refuses to run: a Python
// child a test starts itself would copy the process's environment, and only the
// producer's launcher (venueoracle's Producer.Command, which resolves through
// ResolveLauncher) starts Python in a recording.
func Resolve(t *testing.T, root string) string {
	t.Helper()
	if os.Getenv(RecordingEnv) == "1" {
		if dir, err := testPackageDir(); err != nil || !unconverted(dir) {
			return refusingStandIn(t, dir, err)
		}
	}
	return ResolveLauncher(t, root)
}

// ResolveLauncher is Resolve for the code that starts Python for a recording:
// the producer's launcher and the venue. It gives the real interpreter in a
// recording too.
func ResolveLauncher(t *testing.T, root string) string {
	t.Helper()
	python, rule, err := interpreter(root)
	if err != nil {
		t.Fatalf("pyoracle: %v", err)
	}
	t.Logf("pyoracle: resolved interpreter %s (%s)", python, rule)
	return python
}

// RecordingEnv is the variable the record verb sets for the runs that execute
// Python (venueoracle's goldenUpdateEnv; a test of venueoracle holds the two
// equal).
const RecordingEnv = "DHO_VENUE_GOLDEN_UPDATE"

// unconvertedOwnLaunchCeiling is how many packages the closed list holds. It
// only goes down.
const unconvertedOwnLaunchCeiling = 8

//go:embed unconverted_own_launch.txt
var unconvertedOwnLaunch string

// UnconvertedOwnLaunch is the closed list: repository directories of the test
// packages that may still start their own Python child in a recording.
func UnconvertedOwnLaunch() []string {
	var list []string
	for _, line := range strings.Split(unconvertedOwnLaunch, "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			list = append(list, line)
		}
	}
	return list
}

func unconverted(dir string) bool {
	for _, entry := range UnconvertedOwnLaunch() {
		if entry == dir {
			return true
		}
	}
	return false
}

// testPackageDir is the repository-relative directory of the package under
// test (a test runs in its package's directory).
func testPackageDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			rel, relErr := filepath.Rel(dir, wd)
			return filepath.ToSlash(rel), relErr
		}
		if filepath.Dir(dir) == dir {
			return "", fmt.Errorf("no go.mod above %s", wd)
		}
	}
}

// refusingStandIn writes a stand-in "interpreter" that runs nothing and
// exits 97 with a message that says what to do, and returns its path. A test
// that starts it as a Python child fails at that child, not later.
func refusingStandIn(t *testing.T, dir string, cause error) string {
	t.Helper()
	why := "package " + dir + " starts its own Python child in a recording"
	if cause != nil {
		why = "the package of this test cannot be found (" + cause.Error() + ")"
	}
	shim := filepath.Join(t.TempDir(), "python")
	script := "#!/bin/sh\necho 'a recording starts Python only through the producer launcher (venueoracle Producer.Command with golden.Produce): " + strings.ReplaceAll(why, "'", "") + "; convert the site' >&2\nexit 97\n"
	if err := os.WriteFile(shim, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Logf("pyoracle: a recording resolves no interpreter for %s: the stand-in %s refuses to run", dir, shim)
	return shim
}

// RunError wraps an error from executing a resolved interpreter so the
// message names the interpreter path. Without it, a missing dependency or
// import failure reads exactly like an oracle divergence, because both
// print as a bare test failure with no mention of which Python answered.
func RunError(python string, err error, output []byte) error {
	if err == nil {
		return nil
	}
	if len(output) > 0 {
		return fmt.Errorf("interpreter %s: %w: %s", python, err, output)
	}
	return fmt.Errorf("interpreter %s: %w", python, err)
}

// DeployedMajor and DeployedMinor are the interpreter the api ships on
// (pyproject.toml requires-python = ">=3.14"). A live oracle compares Go
// against Python's behaviour, and that behaviour moves between releases
// (json.decoder's error text, datetime parsing edge cases), so an older
// interpreter measures the wrong Python, not the code under test.
const (
	DeployedMajor = 3
	DeployedMinor = 14
)

// versionProbeProgram is the program that makes an interpreter print its
// "major.minor" version. They are not exported: the probe is a Python child
// like any other, and this package starts it (RequireDeployed), in the closed
// environment, so no test starts it with the environment it inherited.
const versionProbeProgram = "import sys; print('%d.%d' % sys.version_info[:2])"

// DeployedVersionError reports why an interpreter that answered the version
// probe with output (or failed with runErr) is not at least the deployed
// release, or nil when it is. It judges what the interpreter SAID, never its
// path, so a bare "python3" that resolves to a hosted runner's 3.12 is
// refused by what it is.
func DeployedVersionError(python string, output []byte, runErr error) error {
	if runErr != nil {
		return fmt.Errorf("read the interpreter version of %s: %w", python, runErr)
	}
	version := strings.TrimSpace(string(output))
	var major, minor int
	if _, scanErr := fmt.Sscanf(version, "%d.%d", &major, &minor); scanErr != nil ||
		major < DeployedMajor || (major == DeployedMajor && minor < DeployedMinor) {
		return fmt.Errorf("live oracle resolved Python %q at %s; it needs the deployed %d.%d (set DEV_HEALTH_PYTHON to the repo .venv interpreter)",
			version, python, DeployedMajor, DeployedMinor)
	}
	return nil
}

// probeDeployed asks the interpreter at python for its version and reports why
// it is not at least the deployed release, or nil when it is. The probe runs
// in ClosedEnv(root) and takes nothing of the process's environment: a probe
// that inherits it answers for the shell of the day, and a recording test
// holds a variable in its process that stops every Python child that inherits
// it (venueoracle's producer guard). When the interpreter fails to start, the
// error holds the end of what it wrote to standard error, which says why.
//
// The interpreter is started as every harness child is: under the constant
// name "python3", looked up through PATH with the interpreter's own directory
// put first for the rest of the test (what an activated virtualenv does), and
// only when that lookup gives the python3 of that directory.
func probeDeployed(t *testing.T, python, root string) error {
	t.Helper()
	python3, err := python3Beside(python)
	if err != nil {
		return err
	}
	t.Setenv("PATH", filepath.Dir(python3)+string(os.PathListSeparator)+os.Getenv("PATH"))
	command := exec.Command("python3", "-c", versionProbeProgram)
	if command.Err != nil || command.Path != python3 {
		return fmt.Errorf("read the interpreter version of %s: python3 through PATH is %q (%v), not that interpreter", python, command.Path, command.Err)
	}
	// The closed environment's PATH does not hold the interpreter's
	// directory: the child is started under its full path, so it finds its
	// own installed packages.
	command.Args[0] = python3
	command.Env = ClosedEnv(root)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, runErr := command.Output()
	if runErr != nil {
		said := strings.TrimSpace(stderr.String())
		if len(said) > probeStderrLimit {
			said = "..." + said[len(said)-probeStderrLimit:]
		}
		if said != "" {
			runErr = fmt.Errorf("%w; it wrote: %s", runErr, said)
		}
	}
	return DeployedVersionError(python3, output, runErr)
}

// python3Beside is the absolute path of the python3 in the directory of the
// interpreter at python (a name is looked up through PATH first). Every
// virtualenv and every Python 3 install directory holds one.
func python3Beside(python string) (string, error) {
	if !filepath.IsAbs(python) {
		found, err := exec.LookPath(python)
		if err != nil {
			return "", fmt.Errorf("interpreter %q: %w", python, err)
		}
		python = found
	}
	dir, err := filepath.Abs(filepath.Dir(python))
	if err != nil {
		return "", err
	}
	python3 := filepath.Join(dir, "python3")
	info, err := os.Stat(python3)
	if err != nil {
		return "", fmt.Errorf("interpreter %q: no python3 beside it: %w", python, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("interpreter %q: %s is not an executable file", python, python3)
	}
	return python3, nil
}

// probeStderrLimit is how much of a failed probe's standard error an error
// message carries.
const probeStderrLimit = 600

// RequireDeployed fails (never skips) the test when the interpreter at python
// is older than the deployed release or does not start. Call it right after
// Resolve in every oracle whose answer depends on the Python release; root is
// the checkout the oracle's Python runs from.
func RequireDeployed(t *testing.T, python, root string) {
	t.Helper()
	if err := probeDeployed(t, python, root); err != nil {
		t.Fatalf("pyoracle: %v", err)
	}
}

// RootEnv names the variable that points a recording at a clean checkout of the pinned
// Python build; unset, the Python side runs from the repository the test runs in.
const RootEnv = "DHO_PYTHON_ROOT"

// Root is the repository root the Python side of a live oracle runs from: the checkout RootEnv
// names (a clean worktree at the commit a golden is pinned to), else the repository the test
// runs in (two directories above the package of the calling test).
func Root(t *testing.T) string {
	t.Helper()
	if override := os.Getenv(RootEnv); override != "" {
		root, err := filepath.Abs(override)
		if err != nil {
			t.Fatalf("pyoracle: %s=%q: %v", RootEnv, override, err)
		}
		return root
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// ClosedEnv is the environment a recording runs Python in: a fixed, named set and nothing
// inherited, so no variable of the day (AUTO_RUN_MIGRATIONS, SERVICE_NAME, OPERATIONAL_ORDERING_CONTRACT,
// LOG_LEVEL...) can shape a recorded answer under the same header. extra entries (NAME=value) are the
// ones the scenario sets on purpose: the DSN, the ordering contract.
func ClosedEnv(root string, extra ...string) []string {
	env := []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8", "LC_ALL=C.UTF-8", "TZ=UTC",
		"PYTHONHASHSEED=0", "PYTHONDONTWRITEBYTECODE=1",
		"PYTHONPATH=" + filepath.Join(root, "src"),
		"OTEL_ENABLED=false",
	}
	return append(env, extra...)
}
