package accessverify

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func TestNewJWKSKeys_NeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runJWKSProxyCaptureChild, func(_ context.Context) {
		for _, supplied := range []*http.Client{nil, {}} {
			cfg := JWKSConfig("https://upstream.invalid")
			cfg.HTTPClient = supplied
			client, err := NewJWKSKeys(cfg, "svc", noop.NewMeterProvider().Meter("test"))
			if err != nil {
				t.Fatal(err)
			}
			if key, err := client.ResolveKey("kid-k"); key != nil || err == nil {
				t.Fatalf("an unreachable issuer: key %v err %v, want a failed fetch", key, err)
			}
			if supplied != nil && supplied.Transport != nil {
				t.Fatal("NewJWKSKeys changed the supplied client's nil Transport")
			}
		}
	}).AssertNothingReached(t)

	issuer := newCfgIssuer(t)
	issuer.publish(t, "kid-k")
	supplied := issuer.srv.Client()
	transport := supplied.Transport
	cfg := JWKSConfig(issuer.srv.URL)
	cfg.HTTPClient = supplied
	client, err := NewJWKSKeys(cfg, "svc", noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	wantAdmitted(t, "the supplied TLS transport", client, "kid-k")
	if supplied.Transport != transport {
		t.Fatal("NewJWKSKeys changed the supplied TLS client's Transport")
	}
}

// Re-run this test binary so net/http first reads the capture proxy's environment.
func runJWKSProxyCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
