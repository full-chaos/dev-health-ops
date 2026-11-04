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
)

// A set AUTH_ORGS_OWNED is read at go-api start, ignored, and reported with one ERROR line before any other step.
func TestConfigureReportsTheRemovedOrgsOwnedKey(t *testing.T) {
	for _, value := range []string{"true", "false", "zz-not-a-switch-4471"} {
		t.Setenv("AUTH_ORGS_OWNED", value)
		t.Setenv("AUTH_PROXY_URL", "not a url")
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		_, err := configureWith(context.Background(), config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), logger, nil)
		if err == nil || !strings.Contains(err.Error(), "AUTH_PROXY_URL") {
			t.Fatalf("%q: configure stopped at %v, want the proxy URL step", value, err)
		}
		errorLines := 0
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, `"level":"ERROR"`) && strings.Contains(line, "AUTH_ORGS_OWNED") {
				errorLines++
				if strings.Contains(line, "zz-not-a-switch") {
					t.Errorf("%q: the value reached the log", value)
				}
			}
		}
		if errorLines != 1 {
			t.Errorf("%q: %d ERROR lines name the key, want 1; logs: %s", value, errorLines, logs.String())
		}
	}
}
