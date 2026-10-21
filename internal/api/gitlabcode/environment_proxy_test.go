package gitlabcode

import (
	"context"
	"math/big"
	"os"
	"os/exec"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestTheDefaultListingClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		client := Client{BaseURL: proxycapture.Target, Token: proxycapture.Canary}
		_, _ = client.request(ctx, proxycapture.Target+"api/v4/projects", "environment_proxy", "", big.NewInt(1))
	}).AssertNothingReached(t)
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
