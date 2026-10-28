package apiservice

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const orgDeletePreflightKey = "orgdelete_clickhouse_preflight"

// startLog builds go-api the way it starts and returns everything it logged.
func startLog(t *testing.T, cfg config.Config) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var logs bytes.Buffer
	components, err := configureWith(ctx, cfg, health.NewRegistry(time.Second), slog.New(slog.NewJSONHandler(&logs, nil)), nil)
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	return logs.String()
}

// preflightLines are the start log lines that carry the org deletion's
// ClickHouse preflight result.
func preflightLines(logs string) []string {
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, `"`+orgDeletePreflightKey+`"`) {
			out = append(out, line)
		}
	}
	return out
}

// go-api runs the org deletion's ClickHouse preflight once at start, does not
// fail on it, and logs one line orgdelete_clickhouse_preflight=refused with the
// named reason -- never the DSN, the login or its password.
func TestStartLogsARefusedOrgDeletionPreflight(t *testing.T) {
	const password = "pw-preflight-start-5c1e"
	for _, tc := range []struct {
		name, reason string
		uri          string
	}{
		{"no CLICKHOUSE_URI", "dsn_unset", ""},
		{"a DSN that cannot connect", "open_failed", "clickhouse://purger_login:" + password + "@127.0.0.1:1/default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{APIAddress: "127.0.0.1:0"}
			if tc.uri != "" {
				cfg.ClickHouseURI = secrets.NewValue(tc.uri)
			}
			logs := startLog(t, cfg)
			lines := preflightLines(logs)
			if len(lines) != 1 {
				t.Fatalf("%d preflight line(s), want 1:\n%s", len(lines), logs)
			}
			for _, want := range []string{`"` + orgDeletePreflightKey + `":"refused"`, `"reason":"` + tc.reason + `"`} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("line %s lacks %s", lines[0], want)
				}
			}
			for _, secret := range []string{password, "purger_login", "127.0.0.1:1"} {
				if strings.Contains(logs, secret) {
					t.Errorf("the start log carries %q", secret)
				}
			}
		})
	}
}
