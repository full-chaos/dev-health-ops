package apiservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func configureTrusted(t *testing.T, trusted string, set bool) (string, error) {
	t.Helper()
	t.Setenv("TRUSTED_PROXIES_NETWORK_FENCED", "false")
	t.Setenv("TRUSTED_PROXIES", trusted)
	if !set {
		unsetEnv(t, "TRUSTED_PROXIES")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	components, err := configure(ctx, config.Config{APIAddress: "127.0.0.1:0"}, health.NewRegistry(time.Second), slog.New(slog.NewTextHandler(&out, nil)))
	closeComponents(components)
	return out.String(), err
}

// A TRUSTED_PROXIES entry go-api cannot read stops go-api at start with its own reason: dropping it would trust what
// is left (a dropped "!" exclusion trusts the node addresses again) and say nothing.
func TestConfigureRefusesABadTrustedProxyList(t *testing.T) {
	for _, list := range []string{
		"10.42.0.0/24,bogus", "10.42.0.0/24,", "*", "0.0.0.0/0", "::/0", "::ffff:10.42.0.0/120", "::ffff:10.42.0.7",
		"10.42.0.5/24", "10.42.0.0/24,!10.43.0.1", "10.42.0.0/24,!10.42.0.0/31", "!10.42.0.1",
	} {
		_, err := configureTrusted(t, list, true)
		var coded interface{ DependencyReason() string }
		if !errors.As(err, &coded) || coded.DependencyReason() != "api_trusted_proxies_invalid" {
			t.Errorf("TRUSTED_PROXIES=%q: err = %v, want reason api_trusted_proxies_invalid", list, err)
			continue
		}
		if !strings.Contains(err.Error(), "TRUSTED_PROXIES entry") {
			t.Errorf("TRUSTED_PROXIES=%q: err = %v, want the entry named by the variable and its index", list, err)
		}
	}
}

// The start line carries the counts, so a readback can tell the parser that knows "!" from one that drops it.
func TestConfigureLogsTheTrustedProxyCounts(t *testing.T) {
	cases := []struct {
		list string
		set  bool
		want string
	}{
		{"10.42.0.0/24,!10.42.0.0,!10.42.0.1", true, "entries=1 exclusions=2"},
		{"10.42.0.0/24", true, "entries=1 exclusions=0"},
		{"10.42.0.6, 192.0.2.1", true, "entries=2 exclusions=0"},
		{"", true, "entries=0 exclusions=0"},
		{"", false, "entries=0 exclusions=0"},
	}
	for _, tc := range cases {
		got, err := configureTrusted(t, tc.list, tc.set)
		var coded interface{ DependencyReason() string }
		if errors.As(err, &coded) && coded.DependencyReason() == "api_trusted_proxies_invalid" {
			t.Errorf("TRUSTED_PROXIES=%q (set %v): refused: %v", tc.list, tc.set, err)
			continue
		}
		if !strings.Contains(got, "api: trusted proxies "+tc.want) {
			t.Errorf("TRUSTED_PROXIES=%q (set %v): log = %q, want the line with %q", tc.list, tc.set, got, tc.want)
		}
	}
}

// unsetEnv removes name for the rest of the test; t.Setenv first, so the old value comes back after it.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
}
