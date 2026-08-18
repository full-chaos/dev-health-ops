package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

func testLookup(values map[string]string) secrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

type failingComponent struct {
	err error
}

func (failingComponent) Name() string { return "failing-test-component" }
func (component failingComponent) Start(context.Context) error {
	return component.err
}
func (failingComponent) Shutdown(context.Context) error { return nil }

func TestVersionFlagReportsMetadataWithoutLoadingRuntimeConfig(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{Service: "dev-health-worker"}, []string{"--version"}, testLookup(map[string]string{
		"POSTGRES_URI":      "not a valid URI",
		"POSTGRES_URI_FILE": "/also-conflicting",
	}), IO{Stdout: &stdout, Stderr: &stderr})
	if code != 0 {
		t.Fatalf("version failed: code=%d stderr=%s", code, stderr.String())
	}
	var info version.Info
	if err := json.Unmarshal(stdout.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Service != "dev-health-worker" || info.GoVersion == "" {
		t.Fatalf("incomplete version output: %#v", info)
	}
}

func TestConfigurationFailureIsSanitized(t *testing.T) {
	t.Parallel()

	secret := "postgres://user:do-not-print@"
	var stderr bytes.Buffer
	code := Execute(context.Background(), Spec{Service: "dev-health-worker"}, nil, testLookup(map[string]string{
		"POSTGRES_URI": secret,
	}), IO{Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected invalid config to fail")
	}
	if strings.Contains(stderr.String(), secret) || strings.Contains(stderr.String(), "do-not-print") {
		t.Fatalf("configuration error leaked secret: %s", stderr.String())
	}
}

// TestProfileResolutionOwnsFlagEnvDefaultAndMembership pins the resolution
// order the shell took over from internal/platform/config (CHAOS-3875):
// --profile beats DEV_HEALTH_PROFILE beats the declared default, the result
// must be a declared profile, and a service that declares none accepts no
// profile at all.
func TestProfileResolutionOwnsFlagEnvDefaultAndMembership(t *testing.T) {
	t.Parallel()

	streamSpec := Spec{
		Service:        "dev-health-stream-runner",
		Profiles:       []string{"ingest", "external", "pagerduty"},
		DefaultProfile: "ingest",
	}

	for name, test := range map[string]struct {
		spec     Spec
		selected *string
		env      map[string]string
		want     string
		wantErr  bool
	}{
		"flag beats environment": {
			spec: streamSpec, selected: ptr("external"),
			env:  map[string]string{"DEV_HEALTH_PROFILE": "pagerduty"},
			want: "external",
		},
		"environment beats default": {
			spec: streamSpec, selected: ptr(""),
			env:  map[string]string{"DEV_HEALTH_PROFILE": "pagerduty"},
			want: "pagerduty",
		},
		"default when neither is set": {
			spec: streamSpec, selected: ptr(""), want: "ingest",
		},
		"blank environment falls through to the default": {
			spec: streamSpec, selected: ptr(""),
			env:  map[string]string{"DEV_HEALTH_PROFILE": "   "},
			want: "ingest",
		},
		"undeclared flag value is rejected": {
			spec: streamSpec, selected: ptr("archive"), wantErr: true,
		},
		"undeclared environment value is rejected": {
			spec: streamSpec, selected: ptr(""),
			env:     map[string]string{"DEV_HEALTH_PROFILE": "archive"},
			wantErr: true,
		},
		"profile-free service accepts none": {
			spec: Spec{Service: "dev-health-worker"}, selected: nil, want: "",
		},
		"profile-free service rejects one": {
			spec: Spec{Service: "dev-health-worker"}, selected: ptr("ingest"),
			wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveProfile(test.spec, test.selected, testLookup(test.env))
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolveProfile accepted %v, want an error", test.selected)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("profile = %q, want %q", got, test.want)
			}
		})
	}
}

func ptr(value string) *string { return &value }

func TestDependencyConfigurationFailureIsCategorizedWithoutLoggingErrorText(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{
		Service: "dev-health-worker",
		ConfigureDependencies: func(
			context.Context,
			config.Config,
			*health.Registry,
		) ([]lifecycle.Component, error) {
			return nil, errors.New("dial https://ch.internal/db?password=do-not-print")
		},
	}, nil, testLookup(nil), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected dependency configuration to fail")
	}
	combined := stdout.String() + stderr.String()
	for _, forbidden := range []string{"https://", "do-not-print", "ch.internal"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("dependency error leaked %q: %s", forbidden, combined)
		}
	}
	if !strings.Contains(combined, "dependency_configuration_failed") {
		t.Fatalf("dependency failure omitted safe category: %s", combined)
	}
}

func TestLoggerAwareDependencyConfigurationReceivesShellJSONLogger(t *testing.T) {
	t.Parallel()

	var received *slog.Logger
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{
		Service: "dev-health-worker",
		ConfigureDependenciesWithLogger: func(
			_ context.Context,
			_ config.Config,
			_ *health.Registry,
			logger *slog.Logger,
		) ([]lifecycle.Component, error) {
			received = logger
			logger.Info("dependency logger injected", "logger_injected", true)
			return nil, errors.New("stop after logger injection")
		},
	}, nil, testLookup(nil), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 || received == nil {
		t.Fatalf("logger-aware dependency configuration code=%d logger=%v", code, received)
	}
	if !strings.Contains(stdout.String(), `"logger_injected":true`) {
		t.Fatalf("logger-aware callback did not use shell JSON logger: %s", stdout.String())
	}
}

func TestShellRejectsAmbiguousDependencyCallbacks(t *testing.T) {
	t.Parallel()

	legacyCalled := false
	loggerAwareCalled := false
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{
		Service: "dev-health-worker",
		ConfigureDependencies: func(
			context.Context,
			config.Config,
			*health.Registry,
		) ([]lifecycle.Component, error) {
			legacyCalled = true
			return nil, nil
		},
		ConfigureDependenciesWithLogger: func(
			context.Context,
			config.Config,
			*health.Registry,
			*slog.Logger,
		) ([]lifecycle.Component, error) {
			loggerAwareCalled = true
			return nil, nil
		},
	}, nil, testLookup(nil), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 || legacyCalled || loggerAwareCalled {
		t.Fatalf(
			"ambiguous callbacks code=%d legacy=%v logger-aware=%v",
			code,
			legacyCalled,
			loggerAwareCalled,
		)
	}
	if !strings.Contains(stdout.String()+stderr.String(), "ambiguous_dependency_configuration") {
		t.Fatalf("ambiguous callback failure was not categorized: %s", stdout.String()+stderr.String())
	}
}

func TestRuntimeFailureIsCategorizedWithoutLoggingComponentErrorText(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), Spec{
		Service: "dev-health-worker",
		ConfigureDependencies: func(
			_ context.Context,
			_ config.Config,
			registry *health.Registry,
		) ([]lifecycle.Component, error) {
			if err := registry.RegisterRequired(
				"test_dependency",
				func(context.Context) error { return nil },
			); err != nil {
				return nil, err
			}
			return []lifecycle.Component{failingComponent{
				err: errors.New("dial https://ch.internal/db?password=do-not-print"),
			}}, nil
		},
	}, nil, testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
	}), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected runtime component to fail")
	}
	combined := stdout.String() + stderr.String()
	for _, forbidden := range []string{"https://", "do-not-print", "ch.internal"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("runtime error leaked %q: %s", forbidden, combined)
		}
	}
	if !strings.Contains(combined, "runtime_failure") {
		t.Fatalf("runtime failure omitted safe category: %s", combined)
	}
}

func TestShellStartsEndpointsAndTerminatesCleanly(t *testing.T) {
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
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Execute(ctx, Spec{
			Service: "dev-health-worker",
			ConfigureDependencies: func(
				_ context.Context,
				_ config.Config,
				registry *health.Registry,
			) ([]lifecycle.Component, error) {
				return nil, registry.RegisterRequired(
					"test_dependency",
					func(context.Context) error { return nil },
				)
			},
		}, nil, testLookup(map[string]string{
			"DEV_HEALTH_HTTP_ADDR":        address,
			"DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s",
		}), IO{Stdout: &stdout, Stderr: &stderr})
	}()

	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/readyz")
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
			if response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("expected ready endpoint, got %d", response.StatusCode)
			}
		}
		if time.Now().After(deadline) {
			if requestErr == nil {
				t.Fatalf("shell did not become ready: status=%d logs=%s stderr=%s", response.StatusCode, stdout.String(), stderr.String())
			}
			t.Fatalf("shell did not start: %v logs=%s stderr=%s", requestErr, stdout.String(), stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, path := range []string{"/healthz", "/metrics"} {
		response, err := client.Get(fmt.Sprintf("http://%s%s", address, path))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %d", path, response.StatusCode)
		}
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shell exit=%d logs=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shell did not terminate after cancellation")
	}
	if strings.Contains(stdout.String(), "postgres://") {
		t.Fatalf("startup logs exposed a DSN: %s", stdout.String())
	}
}
