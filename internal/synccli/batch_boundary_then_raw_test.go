package synccli

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The sink Boundary and the provider credential are one redaction: the sink
// login's escaped spelling "svc:" ends where the token "A7kZ2" starts.
func TestBatchListFailureRedactsTheSinkAndTheCredentialAsOneUnion(t *testing.T) {
	deps := InlineDeps{List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
		return nil, errors.New("svc%3A7kZ2qq")
	}}
	plan := Plan{Provider: "github", SinkURI: "clickhouse://svc%3A:zz9q@c/d"}
	run := providersync.InProcessRun{Credential: map[string]string{"token": "A7kZ2"}}
	err := runBatch(context.Background(), deps, plan, nil, run, cli.Env{})
	if err == nil || err.Error() != "github: list repositories: [REDACTED]qq" {
		t.Errorf("got %v", err)
	}
}
