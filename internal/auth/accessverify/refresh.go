package accessverify

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

const (
	RefreshAcceptedMetric = "devhealth_auth_refresh_token_accepted_total"
	ClassLegacyRefresh    = "ops_refresh_token"
)

// RefreshCounter counts HS256 refresh tokens a service accepted.
type RefreshCounter struct{}

func NewRefreshCounter(meter metric.Meter, service string) (*RefreshCounter, error) {
	return &RefreshCounter{}, nil
}

func (c *RefreshCounter) Accepted(ctx context.Context) {}
