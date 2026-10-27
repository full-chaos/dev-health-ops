package containers

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// HarnessTimingEnv set to "1" writes one stderr line per harness phase (start,
// close, schema), so container cost can be measured against test time:
//
//	containers: harness-timing <phase> <engine> <mode> <ms>
//
// Tab-separated after the prefix. Unset: nothing is written.
const HarnessTimingEnv = "DEV_HEALTH_TEST_HARNESS_TIMING"

const harnessTimingPrefix = "containers: harness-timing"

var (
	timingMu  sync.Mutex
	timingLog io.Writer = os.Stderr
)

// TimePhase starts a timer for one harness phase; the returned func records it.
func TimePhase(phase, engine, mode string) func() {
	if strings.TrimSpace(os.Getenv(HarnessTimingEnv)) != "1" {
		return func() {}
	}
	began := time.Now()
	return func() {
		timingMu.Lock()
		defer timingMu.Unlock()
		fmt.Fprintf(timingLog, "%s\t%s\t%s\t%s\t%d\n", harnessTimingPrefix, phase, engine, mode, time.Since(began).Milliseconds())
	}
}
