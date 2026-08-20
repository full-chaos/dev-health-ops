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
	"slices"
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

// asyncFailingComponent starts cleanly and then reports a failure on its
// Errors() channel, exercising lifecycle.Runtime's asynchronous
// component-failure path (as opposed to failingComponent's synchronous
// Start-failure path) end to end through the shell.
type asyncFailingComponent struct {
	name  string
	errCh chan error
}

func (component asyncFailingComponent) Name() string         { return component.name }
func (asyncFailingComponent) Start(context.Context) error    { return nil }
func (asyncFailingComponent) Shutdown(context.Context) error { return nil }
func (component asyncFailingComponent) Errors() <-chan error { return component.errCh }

// reasonedComponentError is a component-supplied error carrying a bounded
// DependencyReason(), the same shape dependency adapters use on the
// configure path (CHAOS-3873), but returned from the runtime path instead.
type reasonedComponentError struct{ reason string }

func (reasonedComponentError) Error() string                { return "dependency unavailable" }
func (err reasonedComponentError) DependencyReason() string { return err.reason }

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

// TestRuntimeFailureCarriesComponentNameAndCause pins CHAOS-3906: an
// operator reading the shell's runtime-failure log must be able to see
// which component failed and why, not just an opaque category.
func TestRuntimeFailureCarriesComponentNameAndCause(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 1)
	errCh <- errors.New("dial dependency: connection refused")
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
			return []lifecycle.Component{asyncFailingComponent{
				name:  "sync-worker-loop",
				errCh: errCh,
			}}, nil
		},
	}, nil, testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
	}), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected async component failure to fail the process")
	}
	combined := stdout.String() + stderr.String()
	if !strings.Contains(combined, "sync-worker-loop") {
		t.Fatalf("runtime failure log omitted the failing component name: %s", combined)
	}
	if !strings.Contains(combined, "connection refused") {
		t.Fatalf("runtime failure log omitted the cause: %s", combined)
	}
	if !strings.Contains(combined, "component_failure") {
		t.Fatalf("runtime component failure omitted its category: %s", combined)
	}
}

// TestRuntimeFailureRedactsComponentErrorDSN proves the redacting slog
// handler (internal/platform/logging.NewJSON) still scrubs a DSN carried by
// a component's runtime-path error, which is what makes attaching the raw
// error (rather than discarding it) safe (CHAOS-3873).
func TestRuntimeFailureRedactsComponentErrorDSN(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 1)
	errCh <- fmt.Errorf("dial postgres://user:do-not-print@ch.internal/db")
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
			return []lifecycle.Component{asyncFailingComponent{
				name:  "async-dsn-component",
				errCh: errCh,
			}}, nil
		},
	}, nil, testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
	}), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected async component failure to fail the process")
	}
	combined := stdout.String() + stderr.String()
	for _, forbidden := range []string{"postgres://", "do-not-print", "ch.internal"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("runtime component failure leaked %q: %s", forbidden, combined)
		}
	}
	// This is the assertion that actually proves the handler did the
	// scrubbing rather than the DSN simply never being attached: it only
	// appears once the component's cause is attached as a real attribute
	// value and passed through the redacting ReplaceAttr handler.
	if !strings.Contains(combined, "component async-dsn-component: dial [REDACTED]") {
		t.Fatalf("runtime failure log did not show a redacted (not discarded) cause: %s", combined)
	}
}

// TestRuntimeFailureHonoursComponentDependencyReason pins CHAOS-3906's core
// asymmetry fix: a bounded DependencyReason() from a component was already
// honoured on the configure path (CHAOS-3873) but discarded on the runtime
// path. It must now be honoured there too.
func TestRuntimeFailureHonoursComponentDependencyReason(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 1)
	errCh <- reasonedComponentError{reason: "queue_backend_unreachable"}
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
			return []lifecycle.Component{asyncFailingComponent{
				name:  "reasoned-worker",
				errCh: errCh,
			}}, nil
		},
	}, nil, testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
	}), IO{Stdout: &stdout, Stderr: &stderr})
	if code == 0 {
		t.Fatal("expected async component failure to fail the process")
	}
	combined := stdout.String() + stderr.String()
	if !strings.Contains(combined, `"reason":"queue_backend_unreachable"`) {
		t.Fatalf("runtime failure did not honour the component's DependencyReason: %s", combined)
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

// TestUnknownFlagIsRejectedAtStartup is the acceptance test for "a typo'd
// option fails loudly at startup" (CHAOS-4020). The environment surface this
// replaces has no equivalent: a misspelled variable name is indistinguishable
// from an unset one, which is how a typo'd OTEL_SERVICE_NAMEi sat inert in
// production. The exit status must be the argument-error status, not a
// configuration or runtime failure, and the process must never reach dependency
// construction.
func TestUnknownFlagIsRejectedAtStartup(t *testing.T) {
	for _, args := range [][]string{
		{"--queeues=metrics"},
		{"--otel-service-namei=worker"},
		{"--concurrency=metrics=1", "--not-a-flag"},
		{"--worker-group"}, // present but missing its value
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			constructed := false
			var stdout, stderr bytes.Buffer
			code := Execute(
				t.Context(),
				Spec{
					Service:       "dev-health-worker",
					RequireQueues: true,
					ConfigureDependencies: func(
						context.Context, config.Config, *health.Registry,
					) ([]lifecycle.Component, error) {
						constructed = true
						return nil, nil
					},
				},
				args,
				testLookup(nil),
				IO{Stdout: &stdout, Stderr: &stderr},
			)
			if code != 2 {
				t.Fatalf("exit code = %d, want 2 for an argument error", code)
			}
			if constructed {
				t.Fatal("dependency construction must never run for an unparseable command line")
			}
			if !strings.Contains(stderr.String(), "--help") {
				t.Errorf("the failure must point at --help, got %q", stderr.String())
			}
		})
	}
}

// TestHelpListsEveryOptionAndExitsCleanly keeps --help the single discovery
// surface: an operator configuring a worker for the first time must be able to
// read the whole option set, including the environment fallback of each option
// and the route vocabulary, without exit status noise.
func TestHelpListsEveryOptionAndExitsCleanly(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		t.Run(flag, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Execute(
				t.Context(),
				Spec{Service: "dev-health-worker", RequireQueues: true},
				[]string{flag},
				testLookup(nil),
				IO{Stdout: &stdout, Stderr: &stderr},
			)
			if code != 0 {
				t.Fatalf("exit code = %d, want 0 for --help", code)
			}
			help := stdout.String()
			for _, fragment := range []string{
				"-Q, --queues",
				"-c, --concurrency",
				"DEV_HEALTH_QUEUE_CONCURRENCY",
				"PROVIDER ROUTE SWITCHES",
				"POSTGRES_URI",
				"environment only",
			} {
				if !strings.Contains(help, fragment) {
					t.Errorf("--help is missing %q", fragment)
				}
			}
		})
	}
}

// TestShortAndLongQueueFlagsAreOneSetting proves -q and --queues are aliases of
// a single value rather than two settings that could disagree, and that the
// deprecated --queue-concurrency spelling still reaches the same place.
func TestShortAndLongQueueFlagsAreOneSetting(t *testing.T) {
	for name, args := range map[string][]string{
		// -Q is Celery's spelling and the canonical short form here.
		"celery short":     {"-Q", "metrics,reports", "-c", "metrics=2,reports=1"},
		"short":            {"-q", "metrics,reports", "-c", "metrics=2,reports=1"},
		"long":             {"--queues=metrics,reports", "--concurrency=metrics=2,reports=1"},
		"repeated":         {"-q", "metrics", "-q", "reports", "-c", "metrics=2", "-c", "reports=1"},
		"deprecated alias": {"--queues=metrics,reports", "--queue-concurrency=metrics=2,reports=1"},
		"mixed celery":     {"-Q", "metrics", "-q", "reports", "-c", "metrics=2,reports=1"},
	} {
		t.Run(name, func(t *testing.T) {
			var observed config.Config
			var stdout, stderr bytes.Buffer
			code := Execute(
				t.Context(),
				Spec{
					Service:       "dev-health-worker",
					RequireQueues: true,
					ConfigureDependencies: func(
						_ context.Context, cfg config.Config, _ *health.Registry,
					) ([]lifecycle.Component, error) {
						observed = cfg
						return nil, errors.New("stop before running")
					},
				},
				args,
				testLookup(nil),
				IO{Stdout: &stdout, Stderr: &stderr},
			)
			if code != 1 {
				t.Fatalf("exit code = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if !slices.Equal(observed.Queues, []string{"metrics", "reports"}) {
				t.Fatalf("queues = %v", observed.Queues)
			}
			if observed.WorkerQueueConcurrency["metrics"] != 2 ||
				observed.WorkerQueueConcurrency["reports"] != 1 {
				t.Fatalf("concurrency = %v", observed.WorkerQueueConcurrency)
			}
		})
	}
}

// TestFlagsBeatEnvironmentThroughTheShell exercises the whole precedence chain
// end to end, and proves the shadowed environment value is reported rather than
// fatal -- the property that lets configuration move into `command:` one
// surface at a time while host .env files still carry the old variables.
func TestFlagsBeatEnvironmentThroughTheShell(t *testing.T) {
	var observed config.Config
	var stdout, stderr bytes.Buffer
	code := Execute(
		t.Context(),
		Spec{
			Service:       "dev-health-worker",
			RequireQueues: true,
			ConfigureDependencies: func(
				_ context.Context, cfg config.Config, _ *health.Registry,
			) ([]lifecycle.Component, error) {
				observed = cfg
				return nil, errors.New("stop before running")
			},
		},
		[]string{
			"-q", "metrics",
			"-c", "metrics=2",
			"--worker-group=from-flag",
			"--log-level=warn",
		},
		testLookup(map[string]string{
			"DEV_HEALTH_WORKER_GROUP": "from-environment",
			"DEV_HEALTH_LOG_LEVEL":    "debug",
			"DEV_HEALTH_HTTP_ADDR":    "127.0.0.1:9099",
		}),
		IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 1 {
		t.Fatalf("exit code = %d; stderr=%s", code, stderr.String())
	}
	if observed.WorkerGroup != "from-flag" {
		t.Fatalf("worker group = %q, want the flag value", observed.WorkerGroup)
	}
	if observed.LogLevel != slog.LevelWarn {
		t.Fatalf("log level = %s, want the flag value", observed.LogLevel)
	}
	// The setting with no flag still resolves from the environment.
	if observed.HTTPAddress != "127.0.0.1:9099" {
		t.Fatalf("http address = %q, want the environment fallback", observed.HTTPAddress)
	}
	// ...and is named in a startup warning so it is not silently invisible.
	if !slices.Equal(observed.EnvOnlySettings, []string{"http-addr"}) {
		t.Fatalf("EnvOnlySettings = %v, want only http-addr", observed.EnvOnlySettings)
	}
	if !strings.Contains(stdout.String(), "configuration supplied through environment variables") {
		t.Error("an environment-configured setting must produce a startup warning")
	}
}

// TestCredentialsAreNotAcceptedAsFlags keeps DSNs off the command line at the
// parse layer, not merely by convention: a --postgres-uri flag would place a
// credential in `ps` output and in `docker compose config`.
func TestCredentialsAreNotAcceptedAsFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute(
		t.Context(),
		Spec{Service: "dev-health-worker", RequireQueues: true},
		[]string{"--postgres-uri=postgres://operator:hunter2-pw@db.internal/devhealth"},
		testLookup(nil),
		IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 2 {
		t.Fatalf("exit code = %d, want a rejected argument", code)
	}
	// The refusal names the flag, never the value behind it: a rejected
	// credential must not be copied into the process log by the rejection.
	if strings.Contains(stderr.String()+stdout.String(), "hunter2-pw") {
		t.Error("the rejection must not echo the credential it refused")
	}
	if !strings.Contains(stderr.String(), "postgres-uri") {
		t.Errorf("the rejection must name the offending flag, got %q", stderr.String())
	}
}

// TestCeleryLogLevelAliasIsAcceptedAndWins pins the other spelling the Python
// Celery worker CLI uses. The Go worker mirrors that CLI, so an operator moving
// between the two fleets should not have to remember which one hyphenates.
func TestCeleryLogLevelAliasIsAcceptedAndWins(t *testing.T) {
	var observed config.Config
	var stdout, stderr bytes.Buffer
	code := Execute(
		t.Context(),
		Spec{
			Service:       "dev-health-worker",
			RequireQueues: true,
			ConfigureDependencies: func(
				_ context.Context, cfg config.Config, _ *health.Registry,
			) ([]lifecycle.Component, error) {
				observed = cfg
				return nil, errors.New("stop before running")
			},
		},
		[]string{"-Q", "metrics", "-c", "metrics=1", "--loglevel=warn"},
		testLookup(map[string]string{"DEV_HEALTH_LOG_LEVEL": "debug"}),
		IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 1 {
		t.Fatalf("exit code = %d; stderr=%s", code, stderr.String())
	}
	if observed.LogLevel != slog.LevelWarn {
		t.Fatalf("log level = %s, want the --loglevel flag value", observed.LogLevel)
	}
}

// TestRoutesFlagIsGone is a deliberate negative: provider route enablement is
// not a CLI surface. A worker executes the queues it subscribes to, and the
// forty WORKER_*_ENABLED switches survive only as the producer/executor
// agreement. Re-adding a --routes flag should fail here.
func TestRoutesFlagIsGone(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute(
		t.Context(),
		Spec{Service: "dev-health-worker", RequireQueues: true},
		[]string{"-Q", "metrics", "-c", "metrics=1", "--routes=github/prs"},
		testLookup(nil),
		IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2 for an undefined flag", code)
	}
}

// TestBlankFlagDoesNotShadowTheEnvironment is the regression guard for a defect
// an adversarial review found that my own blank-value test could not: that test
// asserted a blank flag resolves to the DEFAULT, with no environment value
// present. Both "blank is ignored" and "blank overrides with nothing" satisfy
// that, so it passed while the second was true.
//
// The distinguishing case needs a NON-BLANK environment value to shadow. On
// main a blank argument was trimmed and fell through to the environment; an
// empty override that won instead would silently drop a deployment's shutdown
// budget to the 30s package default and flip ShutdownTimeoutExplicit, which
// feeds the drain-budget branch in cmd/dev-health-worker.
func TestBlankFlagDoesNotShadowTheEnvironment(t *testing.T) {
	var observed config.Config
	var stdout, stderr bytes.Buffer
	code := Execute(
		t.Context(),
		Spec{
			Service:       "dev-health-worker",
			RequireQueues: true,
			ConfigureDependencies: func(
				_ context.Context, cfg config.Config, _ *health.Registry,
			) ([]lifecycle.Component, error) {
				observed = cfg
				return nil, errors.New("stop before running")
			},
		},
		[]string{
			"-Q", "heartbeat",
			"-c", "heartbeat=1",
			"--shutdown-timeout=",
			"--worker-group=",
			"--log-level=",
		},
		testLookup(map[string]string{
			"DEV_HEALTH_SHUTDOWN_TIMEOUT": "7260s",
			"DEV_HEALTH_WORKER_GROUP":     "ops",
			"DEV_HEALTH_LOG_LEVEL":        "warn",
		}),
		IO{Stdout: &stdout, Stderr: &stderr},
	)
	if code != 1 {
		t.Fatalf("exit code = %d; stderr=%s", code, stderr.String())
	}
	if observed.ShutdownTimeout != 7260*time.Second {
		t.Fatalf("shutdown timeout = %s, want the environment's 7260s", observed.ShutdownTimeout)
	}
	if !observed.ShutdownTimeoutExplicit {
		t.Error("an environment-supplied timeout must still count as an operator choice")
	}
	if observed.WorkerGroup != "ops" {
		t.Fatalf("worker group = %q, want the environment's ops", observed.WorkerGroup)
	}
	if observed.LogLevel != slog.LevelWarn {
		t.Fatalf("log level = %s, want the environment's warn", observed.LogLevel)
	}
}
