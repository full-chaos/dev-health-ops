package providersync

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The deployment routes log a provider's deployment id on a failed or partial
// lookup. An id that is not 1-64 bytes of [A-Za-z0-9_-] never reaches the
// line; the line records deployment_id_dropped=true instead.
func TestGitLabDeploymentLogsDropAMalformedProviderID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&output, slog.LevelInfo))
	defer slog.SetDefault(previous)
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[]`},
		{body: `[{"id":"CANARY provider content","iid":7,"status":"success","created_at":"2026-07-22T10:00:00Z","sha":"main","ref":"main","deployable":"wrong-shape"}]`},
		{status: 400, body: `{"message":"nope"}`},
	}}
	_, err := (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"), providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(output.String())
	if strings.Contains(output.String(), "CANARY provider content") || !strings.Contains(output.String(), `"deployment_id_dropped":true`) {
		t.Fatal("provider-assigned ID bypassed shape validation")
	}
}

type overlongDeploymentIDDoer struct {
	delegate gitHubDeploymentsDoer
	id       string
}

func (d *overlongDeploymentIDDoer) Do(r *http.Request) (*http.Response, error) {
	response, err := d.delegate.Do(r)
	if err != nil {
		return response, err
	}
	if r.URL.Path == "/repos/acme/api/deployments" {
		b, _ := io.ReadAll(response.Body)
		response.Body.Close()
		response.Body = io.NopCloser(strings.NewReader(strings.ReplaceAll(string(b), `"id":101`, `"id":`+d.id)))
	}
	return response, nil
}
func TestGitHubDeploymentLogsDropAnOverlongProviderID(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&output, slog.LevelInfo))
	defer slog.SetDefault(previous)
	id := strings.Repeat("7", 65)
	doer := &overlongDeploymentIDDoer{id: id}
	_, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"), providerfoundation.Credential{}, gitHubRepositoryClient(t, doer, "https://api.github.com"), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(output.String())
	if strings.Contains(output.String(), id) || !strings.Contains(output.String(), `"deployment_id_dropped":true`) {
		t.Fatal("65-byte provider-assigned ID bypassed shape validation")
	}
}
