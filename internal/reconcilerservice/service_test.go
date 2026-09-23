package reconcilerservice

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func TestReconcilerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if reconcilerSpec.Service != "dev-health-reconciler" {
		t.Fatalf("service = %q", reconcilerSpec.Service)
	}
	if reconcilerSpec.ConfigureDependenciesWithLogger == nil || reconcilerSpec.ConfigureDependencies != nil {
		t.Fatal("reconciler logger-aware dependency configuration is not exclusively wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithLogger(
		context.Background(),
		config.Config{},
		registry,
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithLogger() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no runtime pools without database configuration", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "job_registry", "posture_manifest_lockstep", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerOperatorStaysLiveWhenDependenciesAreMissing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lookup := func(key string) (string, bool) {
		values := map[string]string{
			"DEV_HEALTH_HTTP_ADDR":        address,
			"DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s",
		}
		value, ok := values[key]
		return value, ok
	}
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- shell.Execute(ctx, reconcilerSpec, nil, secrets.LookupEnv(lookup), shell.IO{
			Stdout: &stdout,
			Stderr: &stderr,
		})
	}()

	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/healthz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("healthz status = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("operator HTTP did not start: %v logs=%s stderr=%s", requestErr, stdout.String(), stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Get("http://" + address + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shell exit = %d logs=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operator HTTP did not stop after cancellation")
	}
}

// TestRunHealthcheckMirrorsReadyz pins CHAOS-4239's Compose healthcheck
// probe: runHealthcheck's exit code must be exactly the /readyz status code
// it observed, since Docker's exec-form healthcheck (required by the
// distroless, shell-less runtime image -- see runHealthcheck's doc comment)
// has no other way to learn the process's own readiness.
func TestRunHealthcheckMirrorsReadyz(t *testing.T) {
	for name, testCase := range map[string]struct {
		handler  http.HandlerFunc
		wantCode int
	}{
		"ready": {
			handler: func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusOK)
			},
			wantCode: 0,
		},
		"not ready": {
			handler: func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(http.StatusServiceUnavailable)
			},
			wantCode: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/readyz" {
					t.Fatalf("healthcheck probed %q, want /readyz", request.URL.Path)
				}
				testCase.handler(response, request)
			}))
			defer server.Close()

			_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			lookup := func(key string) (string, bool) {
				if key == "DEV_HEALTH_HTTP_ADDR" {
					return ":" + port, true
				}
				return "", false
			}

			if code := runHealthcheck(lookup); code != testCase.wantCode {
				t.Fatalf("runHealthcheck() = %d, want %d", code, testCase.wantCode)
			}
		})
	}
}

// TestRunHealthcheckFailsClosedWithNothingListening proves the probe does
// not hang or panic, and reports unhealthy, when the process's own HTTP
// server is not answering at all -- the shape a genuinely wedged process (or
// one that has not started yet) would take.
func TestRunHealthcheckFailsClosedWithNothingListening(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	lookup := func(key string) (string, bool) {
		if key == "DEV_HEALTH_HTTP_ADDR" {
			return address, true
		}
		return "", false
	}

	if code := runHealthcheck(lookup); code != 1 {
		t.Fatalf("runHealthcheck() = %d, want 1 with nothing listening", code)
	}
}

// TestReconcilerCommandRunsThePinnedSpec runs `dho reconciler --help` through
// the dho dispatch and requires the --unreclaimable-sweep flag, which the
// option registry declares for the dev-health-reconciler service alone. A
// Command whose Run executed any other spec (or none) would not list it.
func TestReconcilerCommandRunsThePinnedSpec(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args:   []string{"reconciler", "--help"},
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 0 {
		t.Fatalf("dho reconciler --help = %d, want 0\nstderr: %s", code, stderr.String())
	}
	if help := stdout.String() + stderr.String(); !strings.Contains(help, "--unreclaimable-sweep") {
		t.Fatalf("dho reconciler --help does not list the reconciler-only --unreclaimable-sweep flag:\n%s", help)
	}
}

// TestReconcilerHealthcheckIsAChildVerb runs `dho reconciler healthcheck`
// through the dho dispatch (the exec-form Compose probe): its exit code is the
// probe's result, and any argument after it is a usage error.
func TestReconcilerHealthcheckIsAChildVerb(t *testing.T) {
	for name, testCase := range map[string]struct {
		status   int
		extra    []string
		wantCode int
	}{
		"ready":          {status: http.StatusOK, wantCode: 0},
		"not ready":      {status: http.StatusServiceUnavailable, wantCode: 1},
		"extra argument": {status: http.StatusOK, extra: []string{"--now"}, wantCode: 2},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/readyz" {
					t.Errorf("healthcheck probed %q, want /readyz", request.URL.Path)
				}
				response.WriteHeader(testCase.status)
			}))
			defer server.Close()
			_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
				Args: append([]string{"reconciler", "healthcheck"}, testCase.extra...),
				Lookup: func(key string) (string, bool) {
					if key == "DEV_HEALTH_HTTP_ADDR" {
						return ":" + port, true
					}
					return "", false
				},
				Stderr: &stderr,
			})
			if code != testCase.wantCode {
				t.Fatalf("dho reconciler healthcheck%v = %d, want %d\nstderr: %s", testCase.extra, code, testCase.wantCode, stderr.String())
			}
		})
	}
}
