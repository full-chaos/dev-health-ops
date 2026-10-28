package workerservice

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// CHAOS-11790: drainRig.drain builds a new slog handler over rig.logs on every call, and slog serialises writes
// only per handler, so two drains of one rig write the one buffer with no common lock. The sink must hold its own.

// drainLogs is what drainRig.logs holds.
type drainLogs = bytes.Buffer // STUB (red): an unguarded buffer

func newDrainLogs() *drainLogs { return &drainLogs{} }

func TestDrainLogSinkSurvivesTwoHandlersWritingConcurrently(t *testing.T) {
	const lines = 200
	sink := newDrainLogs()
	var writers sync.WaitGroup
	for writer := 0; writer < 2; writer++ {
		logger := slog.New(slog.NewJSONHandler(io.Writer(sink), nil))
		writers.Add(1)
		go func() {
			defer writers.Done()
			for line := 0; line < lines; line++ {
				logger.Info("touched_days_drain.pass", "n", line)
			}
		}()
	}
	writers.Wait()
	if got := strings.Count(sink.String(), "\n"); got != 2*lines {
		t.Fatalf("lines = %d, want %d", got, 2*lines)
	}
}
