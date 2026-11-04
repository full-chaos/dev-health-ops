package authapi

import (
	"context"
	"github.com/google/uuid"
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

// revokedSeen carries, out of go-api's token decision, that it refused a token
// because the session row its sid names is revoked and belongs to the token's
// user. The decision returns only a bare refusal.
type revokedSeen struct {
	user, owner, session uuid.UUID
	userSet, revoked     bool
}

type revokedSeenKey struct{}

func withRevokedSeen(ctx context.Context, seen *revokedSeen) context.Context {
	return context.WithValue(ctx, revokedSeenKey{}, seen)
}

func revokedSeenOf(ctx context.Context) *revokedSeen {
	seen, _ := ctx.Value(revokedSeenKey{}).(*revokedSeen)
	if seen == nil {
		return &revokedSeen{}
	}
	return seen
}

func (s *revokedSeen) subject(id uuid.UUID) { s.user, s.userSet = id, true }

func (s *revokedSeen) revokedRow(session, owner uuid.UUID) {
	s.session, s.owner, s.revoked = session, owner, true
}

// ownRowRevoked is the session id when the revoked row seen is the token user's own.
func (s *revokedSeen) ownRowRevoked() (uuid.UUID, bool) {
	return s.session, s.revoked && s.userSet && s.user == s.owner
}

// observeOwnRowRevocation records the lag of a /validate refusal that go-api's
// decision made on the session row's own revoke. The time comes from
// auth-service's own session read; a failed read records nothing.
func (h validateHandler) observeOwnRowRevocation(ctx context.Context, seen *revokedSeen) {
	id, ok := seen.ownRowRevoked()
	if !ok {
		return
	}
	record, err := h.Sessions.Current(ctx, id)
	if err != nil {
		return
	}
	h.rec.observeRevocationLag(ctx, record.RevokedAt, h.Issuer.now())
}
