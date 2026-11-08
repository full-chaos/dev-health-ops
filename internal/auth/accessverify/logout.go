package accessverify

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	LegacyLogoutAcceptedMetric = "devhealth_auth_legacy_logout_accepted_total"
	ClassLegacyRefresh         = "ops_refresh_token"
)

// LogoutCounter counts the HS256 refresh tokens a logout route verified with the
// legacy key, by service and credential class: the one HS256 refresh path left.
type LogoutCounter struct {
	logouts metric.Int64Counter
	service string
}

// NewLogoutCounter registers the counter on meter and pre-registers the
// service's series at 0.
func NewLogoutCounter(meter metric.Meter, service string) (*LogoutCounter, error) {
	logouts, err := meter.Int64Counter(LegacyLogoutAcceptedMetric,
		metric.WithDescription("HS256 refresh tokens verified with the legacy key at logout, by service and credential class."))
	if err != nil {
		return nil, err
	}
	logouts.Add(context.Background(), 0, metric.WithAttributes(attribute.String("service", service), attribute.String("class", ClassLegacyRefresh)))
	return &LogoutCounter{logouts: logouts, service: service}, nil
}

// Accepted counts one verified logout token. A nil counter counts nothing.
func (c *LogoutCounter) Accepted(ctx context.Context) {
	if c == nil {
		return
	}
	c.logouts.Add(ctx, 1, metric.WithAttributes(attribute.String("service", c.service), attribute.String("class", ClassLegacyRefresh)))
}
