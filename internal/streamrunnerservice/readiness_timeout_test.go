package streamrunnerservice

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

// readinessTestCheckTimeout bounds a HUNG readiness check in these tests. It is
// NOT an assertion: no test here relies on a check timing out, and a check that
// answers does so in microseconds. Sub-second values (100 ms, 500 ms) fail under
// `-race` on a CPU-starved runner because the check goroutine wakes late and
// reports TimedOut:true (CHAOS-6725, extended here by CHAOS-6727).
const readinessTestCheckTimeout = 10 * time.Second

// The test files of this package never build a registry with a sub-second
// check timeout again (the source-level guard for the class above).
func TestNoStreamRunnerServiceTestBuildsASubSecondReadinessTimeout(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob test files: %v (%d files)", err, len(files))
	}
	pattern := regexp.MustCompile(`health\.NewRegistry\(\s*\d+\s*\*\s*time\.(Millisecond|Microsecond|Nanosecond)\s*\)`)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if loc := pattern.FindIndex(raw); loc != nil {
			t.Errorf("%s builds a health registry with a sub-second check timeout (%q); use readinessTestCheckTimeout", file, raw[loc[0]:loc[1]])
		}
	}
}
