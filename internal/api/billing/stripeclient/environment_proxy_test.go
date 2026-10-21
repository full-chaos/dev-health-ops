package stripeclient

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		provider := New(Options{Key: "sk_test_" + proxycapture.Canary, BaseURL: "http://upstream.invalid"})
		_, _ = provider.RawGet(ctx, "/v1/balance")
	}).AssertNothingReached(t)
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
