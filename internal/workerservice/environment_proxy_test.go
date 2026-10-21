package workerservice

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func sendCanaryWith(ctx context.Context, client *http.Client) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
	if response, err := client.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

func TestTheDefaultAtlassianTeamsDoerNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) { sendCanaryWith(ctx, defaultAtlassianTeamsDoer()) }).AssertNothingReached(t)
}

func TestTheContractDeadlineClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, func(ctx context.Context) {
		sendCanaryWith(ctx, contractDeadlineHTTPClient(5*time.Second))
	}).AssertNothingReached(t)
}
