package apiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// testCredential is planted in every DSN below; no log line may contain it.
const testCredential = "planted-credential-6369"

func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func reachableDatabase(cfg config.Config) config.Config {
	cfg.APIDatabaseURI = secrets.NewValue("postgres://api:" + testCredential + "@127.0.0.1:1/app")
	cfg.APIJWTSecret = secrets.NewValue(strings.Repeat("fixture-key-", 3))
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
		"access-token verifier": {
			cfg: func(*testing.T) config.Config {
				cfg := reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"})
				cfg.APIJWTSecret = secrets.Value{}
				return cfg
			},
			dependency: "api_access_token_verifier", reason: "api_protection_config_failed",
			errorText: "JWT_SECRET_KEY is required",
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
	env := map[string]string{
		"API_ADDRESS":          "127.0.0.1:0",
		"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0",
		"API_CLICKHOUSE_URI":   "clickhouse://api:" + testCredential + "@" + closedAddress(t) + "/default",
	}
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args:   []string{"api"},
		Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
		Stdout: &stdout,
		Stderr: &stderr,
	})
	output := stdout.String() + stderr.String()
	if code != 1 {
		t.Fatalf("dho api = %d, want 1\n%s", code, output)
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
