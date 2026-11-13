package apiservice

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestGoAPIStartsWithOneWarnWhenTheStateSecretsShareAValue: SSO_STATE_SECRET equal to GITHUB_APP_STATE_SECRET is
// config hygiene, never a refused start (product rule 3); go-api logs exactly one WARN that names both keys and
// never a value, and distinct keys log none.
func TestGoAPIStartsWithOneWarnWhenTheStateSecretsShareAValue(t *testing.T) {
	same := strings.Repeat("shared-state-key-", 3)
	other := strings.Repeat("github-state-key-", 3)
	for name, tc := range map[string]struct {
		github string
		want   int
	}{"equal": {same, 1}, "distinct": {other, 0}} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{
				"API_DATABASE_URI":        "postgresql://devhealth_api@127.0.0.1:1/devhealth?connect_timeout=1",
				"API_DATABASE_ROLE":       "devhealth_api",
				"DEV_HEALTH_API_ADDR":     "127.0.0.1:0",
				"SSO_STATE_SECRET":        same,
				"GITHUB_APP_STATE_SECRET": tc.github,
			}
			cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
				value, ok := env[key]
				return value, ok
			}})
			if err != nil {
				t.Fatalf("load with %s state keys: %v", name, err)
			}
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			components, err := configureChecked(context.Background(), cfg, health.NewRegistry(time.Second), logger)
			if err != nil {
				t.Fatalf("go-api does not start with %s state keys: %v", name, err)
			}
			defer closeComponents(components)
			if strings.Contains(logs.String(), same) || strings.Contains(logs.String(), other) {
				t.Fatal("a log line carries a state key value")
			}
			warns := 0
			scanner := bufio.NewScanner(&logs)
			scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
			for scanner.Scan() {
				var record struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
				}
				if json.Unmarshal(scanner.Bytes(), &record) == nil && record.Level == "WARN" &&
					strings.Contains(record.Msg, "SSO_STATE_SECRET") && strings.Contains(record.Msg, "GITHUB_APP_STATE_SECRET") {
					warns++
				}
			}
			if warns != tc.want {
				t.Fatalf("%s state keys: %d WARN lines name both keys, want %d", name, warns, tc.want)
			}
		})
	}
}
