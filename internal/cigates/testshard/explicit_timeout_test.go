package testshard_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10359: every `go test` of the sharded test leg and of the race-excluded census run names its -timeout, so a slow run panics
// with the names of the tests that were running (a goroutine dump) before the 30 min job cap cancels the job with no output.

var explicitTimeoutFunctions = []string{"check_test_shard", "go_test_json_run_t"}

// A stage of the leg is a plain go test line or a go_test_leg call (CHAOS-11684: the helper adds -json and the rest of the flags).
var goTestWithoutTimeout = regexp.MustCompile(`go test (?:[^\n]*\s)?-mod=readonly|go_test_leg "`)

// functionBody is the text of the shell function name in script, from its opening line to the first line that is a lone `}`.
func functionBody(t *testing.T, script, name string) string {
	t.Helper()
	start := strings.Index(script, "\n"+name+"() {\n")
	if start < 0 {
		t.Fatalf("function %s not found in ci/check_go.sh", name)
	}
	rest := script[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("function %s has no closing brace", name)
	}
	return rest[:end]
}

// callsMissingTimeout lists the go test lines of the named functions of script that carry no `-timeout`, and the number of go
// test lines seen (zero is a failure of the caller: nothing was checked).
func callsMissingTimeout(t *testing.T, script string) (missing []string, seen int) {
	t.Helper()
	for _, name := range explicitTimeoutFunctions {
		for _, line := range strings.Split(functionBody(t, script, name), "\n") {
			if !goTestWithoutTimeout.MatchString(line) || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			seen++
			if !strings.Contains(line, " -timeout ") && !strings.Contains(line, " -timeout=") {
				missing = append(missing, name+": "+strings.TrimSpace(line))
			}
		}
	}
	return missing, seen
}

func readCheckGo(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "check_go.sh"))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestTheShardedTestLegNamesEveryGoTestTimeout(t *testing.T) {
	missing, seen := callsMissingTimeout(t, readCheckGo(t))
	if seen != 4 {
		t.Fatalf("saw %d go test line(s) in %v, want 4 (the batch, the merge_group security batch, the heavy package alone, the one fresh census run): the scan is stale", seen, explicitTimeoutFunctions)
	}
	for _, line := range missing {
		t.Errorf("go test without an explicit -timeout: %s", line)
	}
}

func TestAGoTestLineWithoutATimeoutIsReported(t *testing.T) {
	script := strings.Replace(readCheckGo(t), "-timeout \"$(cigates_go_test_timeout ${rest})\" ${rest}", "${rest}", 1)
	missing, _ := callsMissingTimeout(t, script)
	if len(missing) != 1 || !strings.Contains(missing[0], "check_test_shard") {
		t.Fatalf("want exactly the planted check_test_shard line, got %v", missing)
	}
	script = strings.Replace(readCheckGo(t), "-json -count=1 -timeout \"${timeout}\"", "-json -count=1", 1)
	missing, _ = callsMissingTimeout(t, script)
	if len(missing) != 1 || !strings.Contains(missing[0], "go_test_json_run_t") {
		t.Fatalf("want exactly the planted census line, got %v", missing)
	}
}

// CHAOS-11762 interim: the census call of a shard takes 20m only when its package list holds internal/cigates/checkgo, else 10m;
// the timeout is an argument of the helper, never read from the environment (the behaviour is run in internal/cigates/checkgo).
func TestTheCensusCallTimeoutIs20mOnlyForACheckgoList(t *testing.T) {
	script := readCheckGo(t)
	for _, want := range []string{
		"    rows_timeout=10m\n",
		`grep -qx -- "${CHECKGO_PACKAGE}"; then rows_timeout=20m; fi`,
		`go_test_json_run_t "${rows_timeout}" "${json}" "${log}" ${rest_rows}`,
		"go_test_json_run_t 10m \"${json}\" \"${log}\" \"$@\"",
		`CHECKGO_PACKAGE="github.com/full-chaos/dev-health-ops/internal/cigates/checkgo"`,
	} {
		if n := strings.Count(script, want); n != 1 {
			t.Errorf("ci/check_go.sh holds %d copies of %q, want 1", n, want)
		}
	}
	if strings.Contains(script, "GO_TEST_JSON_TIMEOUT") {
		t.Error("ci/check_go.sh reads a timeout from the environment")
	}
}
