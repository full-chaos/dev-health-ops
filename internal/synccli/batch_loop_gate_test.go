package synccli

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func gitlabLoopHarness(n int) *batchHarness {
	h := &batchHarness{}
	for i := range n {
		h.repos = append(h.repos, providersync.ListedRepository{Name: "r", FullName: "acme/r", ProjectID: int64(i + 1)})
	}
	return h
}

var gitlabLoopArgs = []string{"--provider", "gitlab", "-s", "acme/*", "--auth", "tok", "--group", "acme", "--batch-size", "1"}

// A run whose goroutine starts late (as under -race on a loaded runner) must not lower the observed
// peak: every run of the loop is still in flight at once, so the peak is the frozen one.
func TestLoopHoldPeakSurvivesALateStart(t *testing.T) {
	h := gitlabLoopHarness(5)
	h.beforeStart = func(run providersync.InProcessRun) {
		if run.SourceExternalID == "5" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	var gate *peakGate
	h.hold, gate = loopHold(h, 5, loopGateLimit)
	code, _, stderr := runVerb(t, "deployments", h.executor(), append(gitlabLoopArgs, "--max-concurrent", "99999999999999999999"), inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if gate.timedOut() || h.peak != 5 {
		t.Fatalf("a late start changed the loop shape: peak %d, want 5 (gate timed out: %v)", h.peak, gate.timedOut())
	}
}

// A loop that runs fewer at once than expected never reaches the peak: the gate must time out and say
// so (the oracle fails the case on it), not release the runs as if the peak had been reached.
func TestLoopHoldTimesOutBelowTheExpectedPeak(t *testing.T) {
	h := gitlabLoopHarness(5)
	var gate *peakGate
	h.hold, gate = loopHold(h, 5, 100*time.Millisecond)
	code, _, stderr := runVerb(t, "deployments", h.executor(), append(gitlabLoopArgs, "--max-concurrent", "2"), inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !gate.timedOut() || h.peak != 2 {
		t.Fatalf("gate timed out %v with peak %d; want a timeout with peak 2", gate.timedOut(), h.peak)
	}
}

// Once the expected peak was reached, a run does not wait at all.
func TestLoopHoldDoesNotWaitOnceThePeakIsReached(t *testing.T) {
	h := gitlabLoopHarness(5)
	var gate *peakGate
	h.hold, gate = loopHold(h, 1, loopGateLimit)
	began := time.Now()
	code, _, stderr := runVerb(t, "deployments", h.executor(), append(gitlabLoopArgs, "--max-concurrent", "1"), inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if gate.timedOut() || h.peak != 1 {
		t.Fatalf("gate timed out %v with peak %d; want peak 1 and no timeout", gate.timedOut(), h.peak)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("five runs at an already-reached peak took %s", took)
	}
}
