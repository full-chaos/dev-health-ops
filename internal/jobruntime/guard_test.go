package jobruntime

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

// A job already in the queue whose stored arguments carry a credential-shaped
// value is cancelled with a bounded reason; the handler never runs and the value
// reaches no log line, error or observer record.
func TestAdapterCancelsAQueuedJobWhoseArgumentsCarryACredentialShape(t *testing.T) {
	t.Parallel()
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	sentinel := probes[0]
	observer := &recordingObserver{}
	logs := &bytes.Buffer{}
	invoked := false
	claim := &recordingClaim{state: ClaimProceed}
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
		invoked = true
		return nil
	}), observer, claim, &recordingLease{}, logs)
	job := retentionJob(t, 1)
	job.EncodedArgs = []byte(strings.Replace(string(job.EncodedArgs), "corr-test-1", sentinel, 1))
	if !strings.Contains(string(job.EncodedArgs), sentinel) {
		t.Fatal("job was not poisoned")
	}

	err = adapter.Work(context.Background(), job)
	if err == nil {
		t.Fatal("a job carrying a credential-shaped value was worked")
	}
	if invoked {
		t.Fatal("handler ran")
	}
	if len(observer.deterministicReasons) != 1 || observer.deterministicReasons[0] != ReasonCredentialShapedValue {
		t.Fatalf("deterministic reasons = %v, want [ReasonCredentialShapedValue]", observer.deterministicReasons)
	}
	for name, text := range map[string]string{
		"error":    err.Error(),
		"logs":     logs.String(),
		"observer": fmt.Sprintf("%+v", *observer),
		"claim":    fmt.Sprintf("%+v", *claim),
	} {
		if strings.Contains(text, sentinel) || strings.Contains(text, sentinel[len(sentinel)/2:]) {
			t.Errorf("%s carries the value", name)
		}
	}
}
