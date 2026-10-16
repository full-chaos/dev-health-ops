package apiservice

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A set-but-empty JWT_ISSUER or JWT_AUDIENCE reaches the api as "" (the
// config layer keeps a present empty value, as the Python api does). The
// api must stop at startup and name the key, never run with the claim check
// switched off.
func TestNewProtectionRefusesEmptyJWTClaimByKeyName(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	secret := secrets.NewValue(strings.Repeat("fixture-key-", 3))
	cases := []struct {
		name     string
		issuer   string
		audience string
		want     string
	}{
		{name: "empty issuer", issuer: "", audience: "dev-health-api", want: "JWT_ISSUER"},
		{name: "empty audience", issuer: "dev-health-ops", audience: "", want: "JWT_AUDIENCE"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := newProtection(config.Config{
				APIJWTSecret: secret, APIJWTIssuer: test.issuer, APIJWTAudience: test.audience,
			}, nil, logger)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error %v, want one naming %s", err, test.want)
			}
		})
	}
}
