package apiservice

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

func TestParseACRAccessProjectScopeDefaultsOff(t *testing.T) {
	if EnvACRAccessProjectScope != "ACR_ACCESS_PROJECT_SCOPE" {
		t.Fatalf("EnvACRAccessProjectScope = %q", EnvACRAccessProjectScope)
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		want bool
	}{
		"unset": {map[string]string{}, false},
		"empty": {map[string]string{EnvACRAccessProjectScope: " "}, false},
		"false": {map[string]string{EnvACRAccessProjectScope: "false"}, false},
		"true":  {map[string]string{EnvACRAccessProjectScope: "true"}, true},
		"TRUE":  {map[string]string{EnvACRAccessProjectScope: " TRUE "}, true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ParseACRAccessProjectScope(func(k string) (string, bool) { v, ok := tc.env[k]; return v, ok })
			if err != nil || got != tc.want {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	_, err := ParseACRAccessProjectScope(func(k string) (string, bool) { return "yes", k == EnvACRAccessProjectScope })
	if err == nil || !strings.Contains(err.Error(), EnvACRAccessProjectScope) {
		t.Fatalf("an invalid value: %v, want an error naming %s", err, EnvACRAccessProjectScope)
	}
}

// The flag is read where go-api starts and reaches the built Deps: an invalid
// value stops start-up with its own reason, "true" turns the project scope on,
// and unset leaves it off.
func TestConfigureReadsTheACRProjectScopeFlag(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run := func(t *testing.T) (bool, error) {
		t.Helper()
		var seen bool
		components, err := configureWith(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second),
			quietLogger(), func(deps *Deps) { seen = deps.ACRAccessProjectScope })
		closeComponents(components)
		return seen, err
	}
	t.Setenv(EnvACRAccessProjectScope, "maybe")
	_, err := run(t)
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "api_acr_project_scope_config_invalid" {
		t.Fatalf("an invalid flag: err = %v, want reason api_acr_project_scope_config_invalid", err)
	}
	for value, want := range map[string]bool{"true": true, "": false} {
		t.Setenv(EnvACRAccessProjectScope, value)
		got, err := run(t)
		if err != nil || got != want {
			t.Fatalf("%s=%q: Deps.ACRAccessProjectScope = %v, %v; want %v", EnvACRAccessProjectScope, value, got, err, want)
		}
	}
}
