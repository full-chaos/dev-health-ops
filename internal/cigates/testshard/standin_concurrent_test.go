package testshard_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// The legs run go test calls concurrently, and a call's argv can be longer than the 4096 bytes bash writes at once: two stand-in
// calls must never tear each other's FAKE_GO_LOG line, and line k of FAKE_GO_ENV_LOG must stay the GOFLAGS of the call on line k.
func TestConcurrentLongStandInCallsAreRecordedWholeAndInStep(t *testing.T) {
	t.Parallel()
	const writers, rounds, argsPerCall = 4, 50, 512
	falseBin, err := exec.LookPath("false")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	standIn := filepath.Join(dir, "go")
	if err := os.WriteFile(standIn, []byte(fmt.Sprintf(standInGo, falseBin)), 0o755); err != nil {
		t.Fatal(err)
	}
	log, envLog := filepath.Join(dir, "go_test.log"), filepath.Join(dir, "go_env.log")
	var base []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "FAKE_GO_") && !strings.HasPrefix(kv, "GOFLAGS=") {
			base = append(base, kv)
		}
	}
	base = append(base, "FAKE_GO_LOG="+log, "FAKE_GO_ENV_LOG="+envLog, "FAKE_GO_LIST_LOG="+filepath.Join(dir, "go_list.log"))

	args := make([][]string, writers)
	want := map[string]int{}
	for w := range args {
		args[w] = []string{"test", "-count=1"}
		for i := 0; i < argsPerCall; i++ {
			args[w] = append(args[w], fmt.Sprintf("w%d-%0190d", w, i))
		}
		want[strings.Join(args[w], " ")] = w
	}
	if n := len(strings.Join(args[0], " ")); n < 8*1024 {
		t.Fatalf("a call's argv is %d bytes, want more than 8 KiB", n)
	}

	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				cmd := exec.Command(standIn)
				cmd.Args = append([]string{"go"}, args[w]...)
				cmd.Env = append(append([]string{}, base...), fmt.Sprintf("GOFLAGS=-w%d", w))
				cmd.Stdout = io.Discard
				var stderr strings.Builder
				cmd.Stderr = &stderr
				if err := cmd.Run(); err != nil {
					errs[w] = fmt.Errorf("%v: %s", err, stderr.String())
				}
			}(w)
		}
		wg.Wait()
		for w, err := range errs {
			if err != nil {
				t.Fatalf("round %d, call %d: the stand-in failed: %v", r, w, err)
			}
		}
	}

	read := func(p string) []string {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	}
	lines, envs := read(log), read(envLog)
	torn, outOfStep := 0, 0
	for k, l := range lines {
		w, ok := want[l]
		if !ok {
			torn++
			continue
		}
		if k >= len(envs) || envs[k] != fmt.Sprintf("GOFLAGS=-w%d", w) {
			outOfStep++
		}
	}
	if torn != 0 || outOfStep != 0 || len(lines) != writers*rounds || len(envs) != writers*rounds {
		t.Errorf("%d/%d recorded go test lines torn, %d whole lines out of step with FAKE_GO_ENV_LOG; %d go test lines and %d env lines for %d calls",
			torn, len(lines), outOfStep, len(lines), len(envs), writers*rounds)
	}
}
