package atlassianteams

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func sendBasicCanary(ctx context.Context, client *http.Client) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
	if err != nil {
		panic(err)
	}
	request.SetBasicAuth("someone@example.com", proxycapture.Canary)
	request.Header.Set("X-Canary", proxycapture.Canary)
	if response, err := client.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

func TestTheGatewayClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) { sendBasicCanary(ctx, GatewayHTTPClient(5*time.Second)) }).AssertNothingReached(t)
}

func TestTheTenantInfoDefaultClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		tenant, err := url.Parse(proxycapture.Target)
		if err != nil {
			panic(err)
		}
		_, _ = fetchTenantCloudID(ctx, nil, tenant)
	}).AssertNothingReached(t)
}

func TestTheVendoredDefaultClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		sendBasicCanary(ctx, atlassian.NewDefaultHTTPClient(5*time.Second))
	}).AssertNothingReached(t)
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
