package apiservice

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// Product rule 3 (no start refusal for config hygiene, D6465): a value of AUTH_PROXY_IMPERSONATION that is neither
// true nor false does not stop go-api. The switch is off, and one WARN names the variable and the value's length,
// never the value.
func TestAnInvalidImpersonationValueStartsWithTheSwitchOffAndOneWarning(t *testing.T) {
	const bad = "yes-please"
	for name, env := range map[string]map[string]string{
		"with the url and sessions": {EnvAuthProxyURL: "http://auth-service:8095", AuthProxyFlagEnv(AuthSessions): "true", EnvAuthProxyImpersonation: bad},
		"without a url":             {EnvAuthProxyImpersonation: bad},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := ParseAuthProxyConfig(lookupFrom(env))
			if err != nil {
				t.Fatalf("start refused: %v", err)
			}
			if cfg.Impersonation || cfg.ImpersonationOn() {
				t.Fatal("the switch is on")
			}
			var out bytes.Buffer
			cfg.WarnInvalidImpersonation(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)))
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != 1 || !strings.Contains(lines[0], `"level":"WARN"`) || !strings.Contains(lines[0], `"env":"AUTH_PROXY_IMPERSONATION"`) ||
				!strings.Contains(lines[0], `"value_length":10`) || strings.Contains(out.String(), bad) {
				t.Errorf("log %q, want one WARN naming %s and value_length %d, without the value", out.String(), EnvAuthProxyImpersonation, len(bad))
			}
		})
	}
	for _, value := range []string{"", " ", "true", "false"} {
		cfg := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: "http://auth-service:8095", EnvAuthProxyImpersonation: value})
		var out bytes.Buffer
		cfg.WarnInvalidImpersonation(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)))
		if out.Len() != 0 {
			t.Errorf("value %q: logged %q, want nothing", value, out.String())
		}
	}
}
