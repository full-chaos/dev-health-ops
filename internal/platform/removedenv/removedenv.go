// Package removedenv reports an environment key that no binary reads any more.
package removedenv

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// OrgsOwnedKey is the removed organizations-owned switch.
const OrgsOwnedKey = "AUTH_ORGS_OWNED"

const (
	meterName            = "github.com/full-chaos/dev-health-ops/internal/platform/removedenv"
	orgsOwnedIgnoredName = "dev_health_auth_orgs_owned_ignored"
)

// ObserveOrgsOwned reports whether OrgsOwnedKey is set. The key changes no behaviour: a set key, of any value (an empty
// or blank value too), is ignored as if unset. It writes one ERROR line that names the key and never its value, and
// sets the gauge to 1 (0 when the key is unset). Each binary calls it once at start. The long-running services call it
// after the process meter provider is installed, so the gauge is exported on their /metrics. The one-shot commands
// (auth-migrate, orgs-backfill) install no provider and push nothing: they report by the ERROR line only.
func ObserveOrgsOwned(ctx context.Context, lookup func(string) (string, bool), logger *slog.Logger) bool {
	_, set := lookup(OrgsOwnedKey)
	if set {
		if logger == nil {
			logger = slog.Default()
		}
		logger.ErrorContext(ctx, "removed environment key is set and ignored; unset it",
			"key", OrgsOwnedKey, "reason", "auth phase 5 is cancelled; public.organizations and public.memberships stay the authority")
	}
	recordIgnored(ctx, set)
	return set
}

func recordIgnored(ctx context.Context, set bool) {
	defer func() { _ = recover() }()
	gauge, err := otel.GetMeterProvider().Meter(meterName).Int64Gauge(orgsOwnedIgnoredName,
		metric.WithDescription("1 when the removed key AUTH_ORGS_OWNED is set and ignored, else 0"))
	if err != nil {
		return
	}
	value := int64(0)
	if set {
		value = 1
	}
	gauge.Record(ctx, value)
}
