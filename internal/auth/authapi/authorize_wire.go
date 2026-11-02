package authapi

import (
	"crypto/rand"
	"slices"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

const (
	authorizationDecisionSchema      = "authorization_decision.v1"
	authorizationBatchResponseSchema = "authorization_batch_response.v1"
	decisionIDPrefix                 = "dec_"
	decisionIDLength                 = 26
	decisionIDAlphabet               = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
	expiresAtLayout                  = "2006-01-02T15:04:05.000000Z07:00"
)

// authorizeStateFailures are the Reasons that mean the decision point could
// not read the state it needs. They never reach the wire as a decision: the
// whole call is a 503.
var authorizeStateFailures = []authorization.Reason{
	authorization.ReasonGrantReadFailed,
	authorization.ReasonClockUnavailable,
}

// wireReasonCode is the decision reason_code of a Reason that reaches the
// wire: the Reason's own token. ok is false for a state failure and for a
// value outside authorization.AllReasons.
func wireReasonCode(reason authorization.Reason) (string, bool) {
	if slices.Contains(authorizeStateFailures, reason) || !slices.Contains(authorization.AllReasons(), reason) {
		return "", false
	}
	return string(reason), true
}

// AuthorizeWireReasonCodes is the closed decision reason_code set the
// authorize routes emit, sorted.
func AuthorizeWireReasonCodes() []string {
	var codes []string
	for _, reason := range authorization.AllReasons() {
		if code, ok := wireReasonCode(reason); ok {
			codes = append(codes, code)
		}
	}
	slices.Sort(codes)
	return codes
}

type decisionDocument struct {
	SchemaVersion  string  `json:"schema_version"`
	DecisionID     string  `json:"decision_id"`
	Allowed        bool    `json:"allowed"`
	ReasonCode     string  `json:"reason_code"`
	PrincipalID    string  `json:"principal_id"`
	OrganizationID *string `json:"organization_id"`
	PolicyRevision int     `json:"policy_revision"`
	ExpiresAt      string  `json:"expires_at"`
}

type batchDecision struct {
	Index    int              `json:"index"`
	Decision decisionDocument `json:"decision"`
}

type batchDocument struct {
	SchemaVersion string          `json:"schema_version"`
	Decisions     []batchDecision `json:"decisions"`
}

// newDecisionID is a server-minted decision id: dec_ and 26 characters drawn
// uniformly from the 64-character alphabet.
func newDecisionID() (string, error) {
	raw := make([]byte, decisionIDLength)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	out := make([]byte, 0, len(decisionIDPrefix)+decisionIDLength)
	out = append(out, decisionIDPrefix...)
	for _, b := range raw {
		out = append(out, decisionIDAlphabet[b&63])
	}
	return string(out), nil
}

// decisionExpiry is when a decision stops being usable. A deny, and an allow
// the decision core gives no expiry (catalog class none, and decision_bound
// until the decision cache sets its bound), expire at the decision time, so
// nothing is cached. A repository allow keeps the core's expiry.
func decisionExpiry(d authorization.Decision, now int64) time.Time {
	if d.Allowed && d.ExpiresAtUnixNano > now {
		return time.Unix(0, d.ExpiresAtUnixNano).UTC()
	}
	return time.Unix(0, now).UTC()
}
