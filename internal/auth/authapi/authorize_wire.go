package authapi

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

const (
	authorizationDecisionSchema      = "authorization_decision.v1"
	authorizationBatchResponseSchema = "authorization_batch_response.v1"
	decisionIDPrefix                 = "dec_"
	decisionIDLength                 = 26
	decisionIDAlphabet               = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
	expiresAtLayout                  = "2006-01-02T15:04:05.000000Z07:00"
	decisionIDLogKey                 = "decision_id"
	// maxDecisionIDDraws bounds the redraw of ids the log redactor would change (about 1e-5 of draws).
	maxDecisionIDDraws = 8
)

// authorizeStateFailures are the Reasons that mean the decision point could
// not read the state it needs. They never reach the wire as a decision: the
// whole call is a 503.
var authorizeStateFailures = []authorization.Reason{
	authorization.ReasonGrantReadFailed,
	authorization.ReasonClockUnavailable,
}

// internalOnlyReasons are Reasons the wire does not name: each answers with the
// wire code of its value here. allowed_by_platform_admin answers allowed_by_role,
// the code every released policy-enforcement client accepts as an allow; the log
// line keeps the internal Reason.
var internalOnlyReasons = map[authorization.Reason]authorization.Reason{
	authorization.ReasonAllowedByPlatformAdmin: authorization.ReasonAllowedByRole,
}

// wireReasonCode is the decision reason_code of a Reason that reaches the
// wire: the Reason's own token, or its internalOnlyReasons code. ok is false
// for a state failure and for a value outside authorization.AllReasons.
func wireReasonCode(reason authorization.Reason) (string, bool) {
	if slices.Contains(authorizeStateFailures, reason) || !slices.Contains(authorization.AllReasons(), reason) {
		return "", false
	}
	if code, ok := internalOnlyReasons[reason]; ok {
		return string(code), true
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
	return slices.Compact(codes)
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
	// Obligations is present only on an allow that carries at least one.
	Obligations *wireObligations `json:"obligations,omitempty"`
}

// maxRepositoryAllowlist is the bound of repository_allowlist in authorization_decision.v1 (maxItems), held
// equal to the schema by a test; the decision core denies a list over it, this is the second check.
const maxRepositoryAllowlist = authorization.MaxRepositoryAllowlist

// wireObligations is the obligations member of a decision. Every member is a
// pointer: RepositoryAllowlist points at a non-nil slice, so an empty list (NONE)
// is written as [] and never omitted, which a consumer would read as unrestricted.
type wireObligations struct {
	TenantScope         *string   `json:"tenant_scope,omitempty"`
	RepositoryScope     *string   `json:"repository_scope,omitempty"`
	RepositoryAllowlist *[]string `json:"repository_allowlist,omitempty"`
	Redaction           *[]string `json:"redaction,omitempty"`
	MinimumAssurance    *string   `json:"minimum_assurance,omitempty"`
}

var errObligationsInvalid = errors.New("authapi: a decision carries obligations that are not per contract")

// redactionClasses are the registered redaction class ids
// (vocabulary/redaction-classes.v1.json); a defect in the embedded file panics.
var redactionClasses = func() map[string]bool {
	var reg struct {
		Classes []struct {
			ID string `json:"id"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(vocabulary.RedactionClassesV1, &reg); err != nil || len(reg.Classes) == 0 {
		panic("authapi: the redaction class registry is not readable")
	}
	out := map[string]bool{}
	for _, c := range reg.Classes {
		out[c.ID] = true
	}
	return out
}()

// wireObligationsOf maps a decision's obligations to the wire. A deny carries
// none, and an obligation outside the contract (a repeated, malformed or "*" key,
// more than maxRepositoryAllowlist keys, a repository_scope other than
// organization or one beside an allowlist, an unregistered class, an unknown
// assurance level, a tenant scope that is not the question's organization) is an
// error: the caller does not serve the decision.
func wireObligationsOf(d authorization.Decision, organizationID string) (*wireObligations, error) {
	o := d.Obligations
	if o == nil {
		return nil, nil
	}
	if !d.Allowed {
		return nil, errObligationsInvalid
	}
	var w wireObligations
	if o.TenantScope != "" {
		if organizationID == "" || o.TenantScope != organizationID {
			return nil, errObligationsInvalid
		}
		scope := o.TenantScope
		w.TenantScope = &scope
	}
	if o.RepositoryScope != "" {
		if o.RepositoryScope != authorization.RepositoryScopeOrganization || o.RepositoryRestricted || len(o.RepositoryAllowlist) > 0 {
			return nil, errObligationsInvalid
		}
		scope := o.RepositoryScope
		w.RepositoryScope = &scope
	}
	if o.RepositoryRestricted || len(o.RepositoryAllowlist) > 0 {
		if len(o.RepositoryAllowlist) > maxRepositoryAllowlist {
			return nil, errObligationsInvalid
		}
		keys := make([]string, 0, len(o.RepositoryAllowlist))
		for _, key := range o.RepositoryAllowlist {
			if key == "*" || !authorizeRepositoryKeyPattern.MatchString(key) || slices.Contains(keys, key) {
				return nil, errObligationsInvalid
			}
			keys = append(keys, key)
		}
		w.RepositoryAllowlist = &keys
	}
	if len(o.Redaction) > 0 {
		classes := make([]string, 0, len(o.Redaction))
		for _, class := range o.Redaction {
			if !redactionClasses[class] || slices.Contains(classes, class) {
				return nil, errObligationsInvalid
			}
			classes = append(classes, class)
		}
		w.Redaction = &classes
	}
	if o.MinimumAssurance != "" {
		if !slices.Contains([]string{"aal1", "aal2", "aal3"}, o.MinimumAssurance) {
			return nil, errObligationsInvalid
		}
		level := o.MinimumAssurance
		w.MinimumAssurance = &level
	}
	if w == (wireObligations{}) {
		return nil, nil
	}
	return &w, nil
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
