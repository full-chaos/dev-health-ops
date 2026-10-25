package containers

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// HarnessTimingFileEnv names a file that receives one line per harness phase
// (start, close, schema), so container cost can be measured against test time.
// Unset: nothing is written.
const HarnessTimingFileEnv = "DEV_HEALTH_TEST_HARNESS_TIMING_FILE"

var timingMu sync.Mutex

// TimePhase starts a timer for one harness phase; the returned func records it.
func TimePhase(phase, engine, mode string) func() {
	path := strings.TrimSpace(os.Getenv(HarnessTimingFileEnv))
	if path == "" {
		return func() {}
	}
	began := time.Now()
	return func() { writeTiming(path, phase, engine, mode, time.Since(began)) }
}

func writeTiming(path, phase, engine, mode string, elapsed time.Duration) {
	timingMu.Lock()
	defer timingMu.Unlock()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "containers: harness timing file: %v\n", err)
		return
	}
	defer file.Close()
	fmt.Fprintf(file, "%s\t%s\t%s\t%d\n", phase, engine, mode, elapsed.Milliseconds())
}
