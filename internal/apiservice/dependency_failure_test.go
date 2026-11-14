package apiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// testCredential is planted in every DSN below; no log line may contain it.
const testCredential = "planted-credential-6369"

func reachableDatabase(cfg config.Config) config.Config {
	cfg.APIDatabaseURI = secrets.NewValue("postgres://api:" + testCredential + "@127.0.0.1:1/app")
	cfg.APIJWTIssuer, cfg.APIJWTAudience = "dev-health-ops", "dev-health-api"
	return cfg
}

// TestStartupDependencyFailuresNameTheDependency injects one fault at each
// start-up construction site and requires configure to fail with that site's
// reason code (the one field the shell logs) and to log one line naming the
// dependency, the reason and the underlying error -- with no credential in it.
func TestStartupDependencyFailuresNameTheDependency(t *testing.T) {
	for name, testCase := range map[string]struct {
		cfg        func(t *testing.T) config.Config
		root       string
		dependency string
		reason     string
		errorText  string
	}{
		"postgres": {
			cfg: func(*testing.T) config.Config {
				cfg := reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"})
				cfg.APIDatabaseURI = secrets.NewValue("mysql://api:" + testCredential + "@127.0.0.1:1/app")
				return cfg
			},
			dependency: "api_postgres", reason: "api_postgres_open_failed",
			errorText: "invalid PostgreSQL connection configuration",
		},
		"job registry": {
			cfg:        func(*testing.T) config.Config { return reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}) },
			root:       "no-such-contract-root",
			dependency: "webhook_job_registry", reason: "api_job_registry_load_failed",
			errorText: "load job contracts for webhook intake",
		},
		"protection config": {
			cfg: func(*testing.T) config.Config {
				cfg := reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"})
				cfg.APIJWTAudience = "acr-api"
				return cfg
			},
			dependency: "api_access_token_verifier", reason: "api_protection_config_failed",
			errorText: "is an ACR audience",
		},
		"valkey": {
			cfg: func(t *testing.T) config.Config {
				return config.Config{APIAddress: "127.0.0.1:0", ValkeyURI: secrets.NewValue("redis://:" + testCredential + "@" + closedAddress(t) + "/1")}
			},
			dependency: "api_valkey", reason: "api_valkey_open_failed",
			errorText: "Valkey readiness check failed",
		},
		"clickhouse": {
			cfg: func(t *testing.T) config.Config {
				return config.Config{APIAddress: "127.0.0.1:0", APIClickHouseURI: secrets.NewValue("clickhouse://api:" + testCredential + "@" + closedAddress(t) + "/default")}
			},
			dependency: "api_clickhouse", reason: "api_clickhouse_open_failed",
			errorText: "ClickHouse readiness check failed",
		},
		"server": {
			cfg:        func(*testing.T) config.Config { return config.Config{} },
			dependency: "api_server", reason: "api_server_config_failed",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if testCase.root != "" {
				saved := contractRoot
				contractRoot = testCase.root
				t.Cleanup(func() { contractRoot = saved })
			}
			var logs bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			components, err := configure(ctx, testCase.cfg(t), health.NewRegistry(time.Second), slog.New(slog.NewJSONHandler(&logs, nil)))
			closeComponents(components)
			var coded interface{ DependencyReason() string }
			if !errors.As(err, &coded) || coded.DependencyReason() != testCase.reason {
				t.Fatalf("configure error = %v, want reason %q", err, testCase.reason)
			}
			var record map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var candidate map[string]any
				if json.Unmarshal([]byte(line), &candidate) == nil && candidate["msg"] == "api dependency configuration failed" {
					record = candidate
				}
			}
			if record == nil {
				t.Fatalf("no failure line logged:\n%s", logs.String())
			}
			if record["dependency"] != testCase.dependency || record["reason"] != testCase.reason {
				t.Fatalf("logged dependency=%v reason=%v, want %s %s", record["dependency"], record["reason"], testCase.dependency, testCase.reason)
			}
			if logged, _ := record["error"].(string); logged == "" || !strings.Contains(logged, testCase.errorText) {
				t.Fatalf("logged error %q, want it to contain %q", logged, testCase.errorText)
			}
			if strings.Contains(logs.String(), testCredential) {
				t.Fatalf("a credential reached the log:\n%s", logs.String())
			}
		})
	}
}

// TestShellLogsTheStartupDependencyReason runs `dho api` through the dho
// dispatch with an unreachable ClickHouse login: the process exits 1 and its
// "configure runtime dependencies" line now carries the reason code.
func TestShellLogsTheStartupDependencyReason(t *testing.T) {
	code, output, left := shellDependencyFailure(t)
	if code != 1 {
		t.Fatalf("dho api = %d, want 1\n%s", code, output)
	}
	if left > 0 {
		t.Fatalf("the shell left %d tracing batch processor(s) running past this test", left)
	}
	// Each record is checked on its own: the shell's "configure runtime
	// dependencies" line must carry the reason itself, and the adapter's
	// line must name the dependency.
	records := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil {
			if message, _ := record["msg"].(string); message != "" {
				records[message] = record
			}
		}
	}
	shellLine := records["configure runtime dependencies"]
	if shellLine == nil || shellLine["reason"] != "api_clickhouse_open_failed" || shellLine["error_category"] != "dependency_configuration_failed" {
		t.Fatalf("the shell line does not carry the reason: %v\n%s", shellLine, output)
	}
	adapterLine := records["api dependency configuration failed"]
	if adapterLine == nil || adapterLine["dependency"] != "api_clickhouse" || adapterLine["reason"] != "api_clickhouse_open_failed" {
		t.Fatalf("the adapter line does not name the dependency: %v\n%s", adapterLine, output)
	}
	if strings.Contains(output, testCredential) {
		t.Fatalf("a credential reached the log:\n%s", output)
	}
}

// shellDependencyFailure runs `dho api` with an unreachable ClickHouse login: it returns the exit code, the output and
// how many tracing batch processors the run left behind. The shell returns on the dependency failure before its runtime
// owns the tracing component, so a provider it built would keep its batch processor running after the test; that
// goroutine's export errors write the global log while a later test reads a buffer it installed there (CHAOS-13690).
// Tracing off: no provider, no goroutine left.
func shellDependencyFailure(t *testing.T) (code int, output string, left int) {
	t.Helper()
	t.Setenv("OTEL_ENABLED", "false")
	batchers := batchSpanProcessors()
	env := map[string]string{
		"API_ADDRESS":          "127.0.0.1:0",
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
		"API_CLICKHOUSE_URI":   "clickhouse://api:" + testCredential + "@" + closedAddress(t) + "/default",
	}
	var stdout, stderr bytes.Buffer
	code = cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args:   []string{"api"},
		Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String() + stderr.String(), batchSpanProcessors() - batchers
}

// CHAOS-13690: the shell run of the test above leaves nothing that writes the global log under a later test. The cell
// forces the timing the CI race needed luck for: batch exports every millisecond, each one failing at once against a
// closed port, and a span through the global tracer the shell installs. It then points the global log at a buffer and
// reads it while the exports would fail, as TestExternalIngestWrongClassMatrixPublicSchemaRows does. A batch processor
// left running reports each failure through otel.Handle -> log.Print into that buffer: a data race under -race, and the
// leak count below.
func TestTheShellRunLeavesNothingWritingTheGlobalLog(t *testing.T) {
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+closedAddress(t))
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	code, output, left := shellDependencyFailure(t)
	if code != 1 {
		t.Fatalf("dho api = %d, want 1\n%s", code, output)
	}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	_, span := otel.Tracer("chaos-13690").Start(context.Background(), "probe")
	span.End()
	for deadline := time.Now().Add(300 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		_ = logs.String()
	}
	if left > 0 {
		t.Fatalf("the shell left %d tracing batch processor(s) running; the global log read:\n%s", left, logs.String())
	}
}

// CHAOS-6663: the Go api verifies a process license instead of refusing to
// start with LICENSE_KEY and LICENSE_PUBLIC_KEY set. One that does not verify
// is the community tier with a warning naming the reason -- start-up goes on,
// and neither value reaches the log.
func TestBuildDepsStartsWithAnUnverifiableProcessLicense(t *testing.T) {
	const licenseValue, publicValue = "signed-key-value-6663", "public-key-value-6663"
	t.Setenv("LICENSE_KEY", licenseValue)
	t.Setenv("LICENSE_PUBLIC_KEY", publicValue)
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
	licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise"})
	var logs bytes.Buffer
	_, components, err := buildDeps(context.Background(), config.Config{}, health.NewRegistry(time.Second), slog.New(slog.NewJSONHandler(&logs, nil)), AuthProxyConfig{}, false)
	closeComponents(components)
	if err != nil {
		t.Fatalf("buildDeps refused to start: %v", err)
	}
	if got := licensing.ProcessTier(); got != "community" {
		t.Fatalf("process tier = %q, want community", got)
	}
	output := logs.String()
	if !strings.Contains(output, `"reason":"invalid_public_key"`) {
		t.Fatalf("no warning naming the reason:\n%s", output)
	}
	if strings.Contains(output, licenseValue) || strings.Contains(output, publicValue) {
		t.Fatalf("a license variable value reached the log:\n%s", output)
	}
}

// batchSpanProcessors counts the OTel batch span processor goroutines now running.
func batchSpanProcessors() int {
	buffer := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buffer, true)
		if n < len(buffer) {
			return strings.Count(string(buffer[:n]), "sdk/trace.(*batchSpanProcessor).processQueue")
		}
		buffer = make([]byte, 2*len(buffer))
	}
}
