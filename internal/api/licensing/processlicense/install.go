package processlicense

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The variables LicenseManager.initialize reads (the same names), each
// also accepted as NAME_FILE through secrets.Resolve.
const (
	LicenseKeyEnv       = "LICENSE_KEY"
	LicensePublicKeyEnv = "LICENSE_PUBLIC_KEY"
)

// Install is the api lifespan's LicenseManager.initialize() for the Go api:
// it resolves LICENSE_PUBLIC_KEY and LICENSE_KEY, verifies the license at
// now (unix seconds) and installs the payload with
// licensing.SetProcessLicense; anything short of a license in force
// installs nil, the community tier. It never fails start-up (the Python
// lifespan swallows every licensing error into the community tier too). Every
// outcome is logged with its reason. Logs name the variables, the reason and
// lengths only: never a key, a license, or any part of them.
//
// It returns the installed license (nil for community) and the reason
// ("" when a license is in force). Nothing configured is the one community
// outcome logged at info (the SaaS shape); every other one is a warning.
func Install(ctx context.Context, lookup secrets.LookupEnv, logger *slog.Logger, now int64) (*licensing.ProcessLicense, Reason) {
	license, reason := resolve(ctx, lookup, logger, now)
	licensing.SetProcessLicense(license)
	return license, reason
}

// Reasons for a community tier that are not a verification failure.
const (
	ReasonUnset         Reason = "unset"
	ReasonResolveFailed Reason = "variable_unreadable"
	ReasonKeyOnly       Reason = "public_key_unset"
	ReasonPublicKeyOnly Reason = "license_key_unset"
)

func resolve(ctx context.Context, lookup secrets.LookupEnv, logger *slog.Logger, now int64) (*licensing.ProcessLicense, Reason) {
	community := func(reason Reason, attrs ...slog.Attr) (*licensing.ProcessLicense, Reason) {
		attrs = append([]slog.Attr{slog.String("reason", string(reason)), slog.String("tier", "community")}, attrs...)
		logger.LogAttrs(ctx, slog.LevelWarn, "api process license not in force; using community tier", attrs...)
		return nil, reason
	}
	publicKey, publicSet, publicErr := secrets.Resolve(LicensePublicKeyEnv, lookup)
	licenseKey, keySet, keyErr := secrets.Resolve(LicenseKeyEnv, lookup)
	// secrets.Resolve's errors name the variable and a fixed class only.
	if publicErr != nil {
		return community(ReasonResolveFailed, slog.String("variable", LicensePublicKeyEnv), slog.String("error", publicErr.Error()))
	}
	if keyErr != nil {
		return community(ReasonResolveFailed, slog.String("variable", LicenseKeyEnv), slog.String("error", keyErr.Error()))
	}
	switch {
	case !publicSet && !keySet:
		logger.LogAttrs(ctx, slog.LevelInfo, "api process license not configured; using community tier",
			slog.String("reason", string(ReasonUnset)), slog.String("tier", "community"))
		return nil, ReasonUnset
	case !publicSet:
		return community(ReasonKeyOnly, slog.String("set", LicenseKeyEnv), slog.String("unset", LicensePublicKeyEnv))
	case !keySet:
		return community(ReasonPublicKeyOnly, slog.String("set", LicensePublicKeyEnv), slog.String("unset", LicenseKeyEnv))
	}
	verifier, err := NewVerifier(publicKey.Reveal())
	if err != nil {
		return community(ReasonInvalidPublicKey, slog.Int("public_key_length", len(publicKey.Reveal())))
	}
	result := verifier.Validate(licenseKey.Reveal(), now)
	if result.License == nil {
		return community(result.Reason, slog.Int("license_key_length", len(licenseKey.Reveal())))
	}
	level := slog.LevelInfo
	message := "api process license in force"
	if result.License.InGracePeriod {
		level, message = slog.LevelWarn, "api process license in force in its grace period (expired)"
	}
	logger.LogAttrs(ctx, level, message,
		slog.String("tier", result.License.Tier),
		slog.Bool("in_grace_period", result.License.InGracePeriod))
	return result.License, ""
}
