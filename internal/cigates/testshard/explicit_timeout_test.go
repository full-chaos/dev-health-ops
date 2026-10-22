package testshard_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10359: every `go test` of the sharded test leg and of the race-excluded census names its -timeout, so a slow run panics
// with the names of the tests that were running (a goroutine dump) before the 30 min job cap cancels the job with no output.

var explicitTimeoutFunctions = []string{"check_test_shard", "check_race_excluded_ran"}

var goTestWithoutTimeout = regexp.MustCompile(`go test (?:[^\n]*\s)?-mod=readonly`)

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
	if seen != 3 {
		t.Fatalf("saw %d go test line(s) in %v, want 3 (the batch, the heavy package alone, one census row): the scan is stale", seen, explicitTimeoutFunctions)
	}
	for _, line := range missing {
		t.Errorf("go test without an explicit -timeout: %s", line)
	}
}

func TestAGoTestLineWithoutATimeoutIsReported(t *testing.T) {
	script := strings.Replace(readCheckGo(t), "-trimpath -timeout 10m ${rest}", "-trimpath ${rest}", 1)
	missing, _ := callsMissingTimeout(t, script)
	if len(missing) != 1 || !strings.Contains(missing[0], "check_test_shard") {
		t.Fatalf("want exactly the planted check_test_shard line, got %v", missing)
	}
	script = strings.Replace(readCheckGo(t), "-count=1 -timeout 5m -v", "-count=1 -v", 1)
	missing, _ = callsMissingTimeout(t, script)
	if len(missing) != 1 || !strings.Contains(missing[0], "check_race_excluded_ran") {
		t.Fatalf("want exactly the planted census line, got %v", missing)
	}
}
