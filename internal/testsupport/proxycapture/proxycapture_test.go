package proxycapture

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func proxyAddress(env []string) string {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "HTTPS_PROXY=http://"); ok {
			return value
		}
	}
	return ""
}

// A gRPC client of another test that redials a reused 127.0.0.1 port the capture proxy now holds sends an HTTP/2
// preface, not a proxy request: it is a foreign dial, never a proxy dial.
func TestAForeignGRPCRedialOfTheCapturePortIsNotAProxyDial(t *testing.T) {
	runner := func(args, env []string) ([]byte, error) {
		conn, err := grpc.NewClient(proxyAddress(env), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Invoke(ctx, "/probe.Probe/Call", struct{}{}, &struct{}{})
		return runCaptureChild(args, env)
	}
	capture := Check(t, runner, func(ctx context.Context) {})
	if os.Getenv(childEnv) != "" {
		return
	}
	t.Logf("capture: %+v", capture)
	if capture.Foreign == 0 {
		t.Fatalf("the redial did not reach the capture port: %+v", capture)
	}
	capture.AssertNothingReached(t)
}

func TestAPlantedConnectThroughTheEnvironmentProxyIsAProxyDial(t *testing.T) {
	runner := func(args, env []string) ([]byte, error) {
		conn, err := net.Dial("tcp", proxyAddress(env))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = conn.Write([]byte("CONNECT upstream.invalid:4317 HTTP/1.1\r\nHost: upstream.invalid:4317\r\n\r\n"))
		buffer := make([]byte, 64)
		_, _ = conn.Read(buffer)
		_ = conn.Close()
		return runCaptureChild(args, env)
	}
	capture := Check(t, runner, func(ctx context.Context) {})
	if os.Getenv(childEnv) != "" {
		return
	}
	if capture.Connections != 1 || capture.Foreign != 0 {
		t.Fatalf("a planted CONNECT is not a single proxy dial: %+v", capture)
	}
}

func TestOnlyAConnectOrAnAbsoluteURLRequestIsAProxyDial(t *testing.T) {
	for line, want := range map[string]bool{
		"CONNECT upstream.invalid:443 HTTP/1.1":    true,
		"GET http://upstream.invalid/ HTTP/1.1":    true,
		"POST https://upstream.invalid/x HTTP/1.1": true,
		"PRI * HTTP/2.0":         false,
		"GET /path HTTP/1.1":     false,
		"":                       false,
		"\x16\x03\x01 tls hello": false,
	} {
		if got := isProxyDial(line, false); got != want {
			t.Errorf("isProxyDial(%q) = %v, want %v", line, got, want)
		}
	}
}

// A real proxy request split over several writes is still one proxy dial.
func TestASplitProxyRequestIsAProxyDial(t *testing.T) {
	runner := func(args, env []string) ([]byte, error) {
		conn, err := net.Dial("tcp", proxyAddress(env))
		if err != nil {
			t.Fatal(err)
		}
		for _, part := range []string{"CO", "NNECT upstream.invalid:4", "317 HTTP/1.1\r\nHost: x\r\n", "\r\n"} {
			_, _ = conn.Write([]byte(part))
			time.Sleep(20 * time.Millisecond)
		}
		buffer := make([]byte, 64)
		_, _ = conn.Read(buffer)
		_ = conn.Close()
		return runCaptureChild(args, env)
	}
	capture := Check(t, runner, func(ctx context.Context) {})
	if os.Getenv(childEnv) != "" {
		return
	}
	if capture.Connections != 1 || capture.Foreign != 0 {
		t.Fatalf("a split CONNECT is not a single proxy dial: %+v", capture)
	}
}

func TestAVeryLongProxyRequestLineIsAProxyDial(t *testing.T) {
	long := "GET http://upstream.invalid/issues?id=" + strings.Repeat("7ee2dd36-45e9-4d60-8d7b-000000000000&id=", 30000) + "x HTTP/1.1"
	if !isProxyDial(long, false) {
		t.Fatal("a long complete request line is a proxy dial")
	}
	if !isProxyDial(long[:1<<20], true) || isProxyDial(long[:1<<20], false) {
		t.Fatal("a line cut at the read bound is judged on its target alone")
	}
}
