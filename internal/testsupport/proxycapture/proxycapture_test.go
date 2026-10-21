package proxycapture

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func send(ctx context.Context, client *http.Client, target string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		panic(err)
	}
	request.Header.Set("Authorization", "Bearer "+Canary)
	if response, err := client.Do(request); err == nil {
		_ = response.Body.Close()
	}
}

// The control the whole method rests on: a client on http.DefaultTransport hands its request, credential included, to
// the proxy the child's environment names. If this stops being seen, every "nothing reached" row proves nothing.
func TestADefaultTransportClientReachesTheCaptureWithItsCredential(t *testing.T) {
	capture := Check(t, runCaptureChild, func(ctx context.Context) { send(ctx, &http.Client{}, Target) })
	if os.Getenv(childEnv) != "" {
		return
	}
	t.Logf("capture: %+v", capture)
	if capture.Connections == 0 || !capture.SawCanary {
		t.Fatalf("a DefaultTransport client did not reach the capture proxy with its credential: %+v", capture)
	}
}

func TestADefaultTransportClientTunnelsHTTPSThroughTheCapture(t *testing.T) {
	capture := Check(t, runCaptureChild, func(ctx context.Context) { send(ctx, &http.Client{}, "https://upstream.invalid/") })
	if os.Getenv(childEnv) != "" {
		return
	}
	t.Logf("capture: %+v", capture)
	if capture.Connections == 0 || !strings.HasPrefix(capture.FirstLines[0], "CONNECT upstream.invalid:443") {
		t.Fatalf("a DefaultTransport client did not ask the capture proxy for a tunnel: %+v", capture)
	}
}

func TestATransportWithoutProxyReachesNothing(t *testing.T) {
	Check(t, runCaptureChild, func(ctx context.Context) {
		send(ctx, &http.Client{Transport: &http.Transport{}}, Target)
		send(ctx, &http.Client{Transport: &http.Transport{}}, "https://upstream.invalid/")
	}).AssertNothingReached(t)
}

func TestTheRunPatternAnchorsEverySubtestLevel(t *testing.T) {
	if got, want := runPattern("TestA/b.c"), `^TestA$/^b\.c$`; got != want {
		t.Fatalf("runPattern = %q, want %q", got, want)
	}
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
