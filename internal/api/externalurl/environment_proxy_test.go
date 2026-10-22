package externalurl

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestGuardedTransportNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
		if response, err := (&http.Client{Transport: GuardedTransport()}).Do(request); err == nil {
			_ = response.Body.Close()
		}
	}).AssertNothingReached(t)
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
