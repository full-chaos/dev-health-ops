package accessverify

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	RefreshAcceptedMetric = "devhealth_auth_refresh_token_accepted_total"
	ClassLegacyRefresh    = "ops_refresh_token"
)

// RefreshCounter counts the HS256 refresh tokens one service accepted, by
// service and credential class. The D2 hold reads it beside the access-token
// counter, so a refresh path cannot keep HS256 alive unseen.
type RefreshCounter struct {
	counter metric.Int64Counter
	service string
}

// NewRefreshCounter registers the counter on meter and pre-registers the
// service's series at 0.
func NewRefreshCounter(meter metric.Meter, service string) (*RefreshCounter, error) {
	counter, err := meter.Int64Counter(RefreshAcceptedMetric,
		metric.WithDescription("HS256 refresh tokens accepted, by service and credential class."))
	if err != nil {
		return nil, err
	}
	counter.Add(context.Background(), 0, metric.WithAttributes(attribute.String("service", service), attribute.String("class", ClassLegacyRefresh)))
	return &RefreshCounter{counter: counter, service: service}, nil
}

// Accepted counts one accepted refresh token. A nil counter counts nothing.
func (c *RefreshCounter) Accepted(ctx context.Context) {
	if c == nil {
		return
	}
	c.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("service", c.service), attribute.String("class", ClassLegacyRefresh)))
}
