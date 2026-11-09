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

// TestGoAPIWarnsOnceWhenTheRetiredJWTSecretKeyIsSet: a JWT_SECRET_KEY left in go-api's environment never stops the
// start (D6465); it logs exactly one WARN that names it, and an unset key logs none.
func TestGoAPIWarnsOnceWhenTheRetiredJWTSecretKeyIsSet(t *testing.T) {
	leftover := strings.Repeat("leftover-hs256-", 3)
	for name, want := range map[string]int{"set": 1, "unset": 0} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{
				"API_DATABASE_URI":    "postgresql://devhealth_api@127.0.0.1:1/devhealth?connect_timeout=1",
				"API_DATABASE_ROLE":   "devhealth_api",
				"DEV_HEALTH_API_ADDR": "127.0.0.1:0",
			}
			if want == 1 {
				env["JWT_SECRET_KEY"] = leftover
				t.Setenv("JWT_SECRET_KEY", leftover)
			} else {
				t.Setenv("JWT_SECRET_KEY", "")
			}
			cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
				value, ok := env[key]
				return value, ok
			}})
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			components, err := configureChecked(context.Background(), cfg, health.NewRegistry(time.Second), logger)
			if err != nil {
				t.Fatalf("go-api does not start with JWT_SECRET_KEY %s: %v", name, err)
			}
			defer closeComponents(components)
			if strings.Contains(logs.String(), leftover) {
				t.Fatal("a log line carries the JWT_SECRET_KEY value")
			}
			warns := 0
			scanner := bufio.NewScanner(&logs)
			scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
			for scanner.Scan() {
				var record struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
				}
				if json.Unmarshal(scanner.Bytes(), &record) == nil && record.Level == "WARN" && strings.Contains(record.Msg, "JWT_SECRET_KEY") {
					warns++
				}
			}
			if warns != want {
				t.Fatalf("JWT_SECRET_KEY %s: %d WARN lines name it, want %d", name, warns, want)
			}
		})
	}
}
