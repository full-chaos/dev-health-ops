package authapi

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"
)

const revocationLagMetric = "auth.session.revocation_to_refusal_seconds"

// revocationLagWindow bounds what is observed: an older revoke is not a lag
// of this refusal, it is a stale credential tried again.
const revocationLagWindow = time.Hour

var revocationLagBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900, 3600}

func registerRevocationLag(meter metric.Meter) (metric.Float64Histogram, error) {
	return meter.Float64Histogram(revocationLagMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Seconds from a session's revocation to a verifier refusing it as revoked, up to one hour. No labels."),
		metric.WithExplicitBucketBoundaries(revocationLagBuckets...))
}

// observeRevocationLag records now - revokedAt at a refusal of a revoked
// session. A nil revokedAt records nothing; a revoke ahead of the clock reads 0.
func (rec recorder) observeRevocationLag(ctx context.Context, revokedAt *time.Time, now time.Time) {
	if rec.revocationLag == nil || revokedAt == nil {
		return
	}
	lag := now.Sub(*revokedAt)
	if lag > revocationLagWindow {
		return
	}
	if lag < 0 {
		lag = 0
	}
	rec.revocationLag.Record(ctx, lag.Seconds())
}
