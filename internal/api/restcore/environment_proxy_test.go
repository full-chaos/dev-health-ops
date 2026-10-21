package restcore

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultCoreClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		core := Core{Provider: "github", Headers: map[string]string{"Authorization": "Bearer " + proxycapture.Canary}, MaxRetries: 1}
		_, _ = core.Get(ctx, proxycapture.Target, "environment_proxy")
	}).AssertNothingReached(t)
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
