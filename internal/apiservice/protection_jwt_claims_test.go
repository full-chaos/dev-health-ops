package apiservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A set-but-empty JWT_ISSUER or JWT_AUDIENCE reaches the api as "" (the
// config layer keeps a present empty value, as the Python api does). The
// api must stop at startup and name the key, never run with the claim check
// switched off.
func TestAPIStartupRefusesEmptyJWTClaimByKeyName(t *testing.T) {
	secret := secrets.NewValue(strings.Repeat("fixture-key-", 3))
	cases := []struct {
		name     string
		issuer   string
		audience string
		want     string
		reason   string
	}{
		{name: "empty issuer", issuer: "", audience: "dev-health-api", want: "JWT_ISSUER", reason: "api_jwt_issuer_empty"},
		{name: "empty audience", issuer: "dev-health-ops", audience: "", want: "JWT_AUDIENCE", reason: "api_jwt_audience_empty"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			// No database: the refusal must not wait for one. The shell logs
			// only the reason code of the returned error, so the key name must
			// be in the dependencyFailure log line.
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			_, err := Spec.ConfigureDependenciesWithLogger(context.Background(), config.Config{
				APIAddress: "127.0.0.1:0", APIJWTSecret: secret, APIJWTIssuer: test.issuer, APIJWTAudience: test.audience,
			}, health.NewRegistry(time.Second), logger)
			var coded interface{ DependencyReason() string }
			if !errors.As(err, &coded) || coded.DependencyReason() != test.reason {
				t.Fatalf("error %v, want reason %s", err, test.reason)
			}
			if !strings.Contains(logs.String(), test.want) {
				t.Fatalf("log %q does not name %s", logs.String(), test.want)
			}
		})
	}
}
