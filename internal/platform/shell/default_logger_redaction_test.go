package shell

import (
	"bytes"
	"context"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestWorkerPackageLevelLogCallsWriteThroughTheRedactingHandler runs a worker
// through Execute, the entry point of dev-health-worker, the stream-runner,
// the scheduler and the reconciler, and logs from inside it the way worker
// packages do: package-level slog, slog.Default(), and the standard log
// package, each carrying a provider 400 whose body holds a credential. Every
// line lands in the worker's JSON stream with the credential and the body
// gone, and the error class, status and path kept.
//
// Not parallel: Execute installs the process default logger for its run.
func TestWorkerPackageLevelLogCallsWriteThroughTheRedactingHandler(t *testing.T) {
	providerErr := &providerfoundation.ProviderError{
		Class:      providerfoundation.ErrorPermanent,
		StatusCode: 400,
		Path:       "/repos/octo/hello/pulls/7",
		Body:       `{"token":"canary-body-token","message":"canary-body-message"}`,
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{
		Service: "dev-health-worker",
		ConfigureDependencies: func(context.Context, config.Config, *health.Registry) ([]lifecycle.Component, error) {
			slog.Warn("github_deployments.pull_request_lookup_failed", "deployment_id", 7, "cause", providerErr.Error())
			slog.Default().Warn("provider_body_seen", "detail", `provider said {"access_token":"canary-json"} for ?private_token=canary-query&page=2`)
			slog.Warn("provider_error", "error", errors.Join(errors.New("first"), providerErr))
			log.Printf("stdlib line Authorization: Bearer canary-stdlib client_secret=canary-kv")
			return nil, errors.New("stop after logging")
		},
	}, nil, testLookup(map[string]string{"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0"}), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected the configure error to stop the process")
	}
	combined := stdout.String() + stderr.String()
	if strings.Contains(combined, "canary") {
		t.Fatalf("a protected value or the body reached the output:\n%s", combined)
	}
	for _, want := range []string{
		`"msg":"github_deployments.pull_request_lookup_failed"`,
		`provider request failed: permanent status=400 path=/repos/octo/hello/pulls/7`,
		`"msg":"provider_body_seen"`,
		`page=2`,
		`"msg":"provider_error"`,
		`stdlib line`,
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout lacks %q:\n%s", want, stdout.String())
		}
	}
}
