//go:build !race

package checkgo_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-13767: go_test_cover_args is the one seam through which a non-race test leg of a main push writes a cover profile.

func coverArgsFunction(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?ms)^go_test_cover_args\(\) \{\n.*?^\}\n`).Find(body)
	if m == nil {
		t.Fatal("go_test_cover_args is not defined in ci/check_go.sh")
	}
	return string(m)
}

func runCoverArgs(t *testing.T, dir string, args ...string) string {
	t.Helper()
	script := coverArgsFunction(t) + "\ngo_test_cover_args \"$@\"\n"
	cmd := exec.Command("bash", append([]string{"-c", script, "x"}, args...)...)
	cmd.Env = append(os.Environ(), "GO_TEST_COVER_DIR="+dir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go_test_cover_args failed: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func TestCoverArgsAreEmptyWithoutDirAndForRaceCalls(t *testing.T) {
	t.Parallel()
	if got := runCoverArgs(t, "", "./pkg"); got != "" {
		t.Errorf("no GO_TEST_COVER_DIR: want no flag, got %q", got)
	}
	if got := runCoverArgs(t, t.TempDir(), "-race", "./pkg"); got != "" {
		t.Errorf("a -race call: want no flag, got %q", got)
	}
}

func TestCoverArgsGiveEachCallItsOwnProfileFile(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "go-cover")
	a := runCoverArgs(t, dir, "-run", "^X$", "./pkg")
	b := runCoverArgs(t, dir, "./pkg")
	re := regexp.MustCompile("^-coverprofile=" + regexp.QuoteMeta(dir) + `/cover-[A-Za-z0-9]+\.out$`)
	if !re.MatchString(a) || !re.MatchString(b) {
		t.Fatalf("want -coverprofile=<dir>/cover-<id>.out, got %q and %q", a, b)
	}
	if a == b {
		t.Fatalf("two calls share one profile file: %q", a)
	}
}

func TestOnlyTheNonRaceTestLegCallsCarryTheCoverFlag(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile(scriptPath(t, "ci/check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	for _, l := range strings.Split(string(body), "\n") {
		if strings.Contains(l, "$(go_test_cover_args") {
			sites = append(sites, l)
			if strings.Contains(l, "-race") {
				t.Errorf("a race call carries the cover flag: %s", l)
			}
		}
	}
	if len(sites) != 2 {
		t.Fatalf("want exactly 2 call sites (go_test_leg, go_test_json_run_t), got %d", len(sites))
	}
}
