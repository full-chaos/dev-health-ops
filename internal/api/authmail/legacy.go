package authmail

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

const LegacyLinkAcceptedMetric = "devhealth_auth_legacy_link_accepted_total"

// The purpose label of a link token: the flow that accepted it.
const (
	PurposeInvite            = "invite"
	PurposeEmailVerification = "email_verification"
	PurposePasswordReset     = "password_reset"
)

var purposes = []string{PurposeInvite, PurposeEmailVerification, PurposePasswordReset}

// LegacyLinkCounter counts the link tokens accepted only by the legacy chain
// key (JWT_SECRET_KEY, else SETTINGS_ENCRYPTION_KEY) while a link secret signs,
// by service and purpose. The HS256 removal reads it: a link it counts stops
// working when that key goes.
type LegacyLinkCounter struct {
	links   metric.Int64Counter
	service string
}

// NewLegacyLinkCounter registers the counter on meter and pre-registers every
// purpose's series of service at 0.
func NewLegacyLinkCounter(meter metric.Meter, service string) (*LegacyLinkCounter, error) {
	links, err := meter.Int64Counter(LegacyLinkAcceptedMetric,
		metric.WithDescription("Link tokens accepted with the legacy chain key while a link secret signs, by service and purpose."))
	if err != nil {
		return nil, err
	}
	for _, purpose := range purposes {
		links.Add(context.Background(), 0, metric.WithAttributes(attribute.String("service", service), attribute.String("purpose", purpose)))
	}
	return &LegacyLinkCounter{links: links, service: service}, nil
}

// Accepted counts one legacy-key link token. A nil counter counts nothing.
func (c *LegacyLinkCounter) Accepted(ctx context.Context, purpose string) {
	if c == nil {
		return
	}
	c.links.Add(ctx, 1, metric.WithAttributes(attribute.String("service", c.service), attribute.String("purpose", purpose)))
}

var legacyLinkAccepted atomic.Pointer[LegacyLinkCounter]

// SetLegacyLinkAccepted sets the counter Valid counts legacy-key link tokens with.
func SetLegacyLinkAccepted(counter *LegacyLinkCounter) { legacyLinkAccepted.Store(counter) }

// Valid is signedtoken.ValidAny over AcceptSecrets, in the same order: the
// signing key and the previous link secret first, then the legacy chain key.
// A token only the legacy key signs is counted under purpose. With no link
// secret set, the legacy key is the signing key and nothing is counted.
func (c Config) Valid(ctx context.Context, purpose, token string) (bool, error) {
	current := []string{c.Secret(), c.PreviousLinkSecret}
	ok, err := signedtoken.ValidAny(token, current)
	if err != nil || ok {
		return ok, err
	}
	// A legacy key equal to a current one refused the token above, so only a
	// key that is not current can accept it here.
	if c.TokenSecret == "" {
		return false, nil
	}
	ok, err = signedtoken.Valid(token, c.TokenSecret)
	if ok {
		legacyLinkAccepted.Load().Accepted(ctx, purpose)
	}
	return ok, err
}
