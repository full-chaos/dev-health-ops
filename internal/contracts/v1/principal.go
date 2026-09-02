package contractsv1

import (
	"encoding/json"
	"fmt"
	"time"
)

// PrincipalSurface is the wire surface name for a resolved principal; it is
// also the stem of contracts/auth/v1/jsonschema/principal.v1.schema.json.
const PrincipalSurface = "principal.v1"

// PrincipalSchemaVersion is the only schema_version this package accepts.
//
// Three different version-shaped numbers exist in this system and conflating
// them is the trap: this one and the envelope's `v` claim version the
// CONTRACT; Principal.TokenVersion versions THE PRINCIPAL'S CREDENTIALS; and
// the ".v1" in the surface name versions the API SURFACE. They move
// independently.
const PrincipalSchemaVersion = "principal.v1"

// Impersonation is an active impersonation session.
//
// ACP-ADR-03 bounds the delegated/impersonation session at 15 minutes and
// independently revocable, shorter than the base session (G-52). ExpiresAt
// here and the enclosing Principal.ExpiresAt bound different things and
// neither widens the other: the effective deadline is the EARLIER of the
// two, so a caller checking only one of them is checking the wrong one half
// the time. Use Principal.EffectiveDeadline rather than picking by hand.
type Impersonation struct {
	// ImpersonatedBy is the principal id of the OPERATOR acting as this
	// principal. The surrounding PrincipalID/OrganizationID/Role stay the
	// TARGET's identity; this is the only field naming the operator.
	ImpersonatedBy string    `json:"impersonated_by"`
	StartedAt      time.Time `json:"started_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// Revisions are the monotonic revisions a principal was resolved against.
//
// ACP-ADR-05 decision 3 requires every allow-cache key to bind all four
// (G-31) so that a revision bump invalidates by construction rather than by
// an explicit purge. A cache key built from a subset is a cache a bump
// cannot invalidate.
type Revisions struct {
	PolicyRevision     int64 `json:"policy_revision"`
	MembershipRevision int64 `json:"membership_revision"`
	GrantRevision      int64 `json:"grant_revision"`
	// EntitlementRevision says WHICH entitlement snapshot this resolution
	// saw. It never says what that snapshot granted -- carrying the revision
	// is what lets a consumer bind its cache correctly while the principal
	// itself stays free of entitlement, which ACP-ADR-07 decision 2 requires.
	EntitlementRevision int64 `json:"entitlement_revision"`
}

// Principal is a resolved principal.v1 document.
//
// DELIBERATELY ABSENT, and load-bearing: there is no Tier and no
// LicensedFeatures. ACP-ADR-07 decision 2 (Accepted 2026-09-02) makes
// entitlement an input to a decision and never a claim in a credential, and
// G-14 forbids entitlement travelling inside a credential by name. The
// existing effective-principal envelope carries both today
// (cmd/query-api/internal/principal/claims.go:38-39) and ADR-07 deprecates
// them from it at its next `v` bump. A reader diffing this struct against
// that one will find two fields missing: that absence IS the ADR, not an
// oversight to repair. Entitlement reaches a relying party through an
// authorization decision.
type Principal struct {
	SchemaVersion string `json:"schema_version"`
	PrincipalID   string `json:"principal_id"`
	// PrincipalType is one of the six values in the Wave 0 closed vocabulary
	// (contracts/auth/v1/credential-classes.schema.json). Unknown denies
	// (ACP-ADR-05 decision 1); the schema enforces it, so a value here has
	// already been checked.
	PrincipalType string `json:"principal_type"`
	// OrganizationID is nil ONLY for a genuinely org-less principal (an
	// "infrastructure" principal acting on the platform itself). A relying
	// party that scopes by organization must treat nil as "no organization"
	// and DENY -- never as "any organization".
	OrganizationID *string `json:"organization_id"`
	// Role is present for continuity and audit legibility and is NOT an
	// authorization input: ACP-ADR-05 decision 4 requires an explicit action
	// name at the call site and forbids an is_admin-style check as the final
	// decision (G-27). Authorize on Permissions or on a decision, never here.
	Role        *string  `json:"role"`
	Permissions []string `json:"permissions"`
	// IsSuperuser on its own is a CLAIM. IsSuperuserVerified is the half
	// carrying current-state evidence; a superuser-gated path must require
	// BOTH. IsSuperuser true with IsSuperuserVerified false is a legitimate,
	// expressible state meaning "claimed but unverified" -- deny on it.
	IsSuperuser         bool           `json:"is_superuser"`
	IsSuperuserVerified bool           `json:"is_superuser_verified"`
	TokenVersion        int64          `json:"token_version"`
	Impersonation       *Impersonation `json:"impersonation"`
	Revisions           Revisions      `json:"revisions"`
	IssuedAt            time.Time      `json:"issued_at"`
	// ExpiresAt is enforced by the relying party. ACP-ADR-03 removes the
	// effective-principal envelope's per-call TTL override outright: a
	// security bound any call site may widen is not a bound.
	ExpiresAt time.Time `json:"expires_at"`
	Issuer    string    `json:"issuer"`
	Audience  string    `json:"audience"`
}

// PrincipalFromWire validates raw against principal.v1 and then decodes it.
//
// The order is the point. A decoder that reads fields first and validates
// afterwards has already made decisions on unvalidated input by the time the
// validator speaks. Validation runs against the DECODED JSON VALUE, not
// against this struct, so the contract is checked on the bytes that actually
// arrived rather than on Go's rendering of them -- decoding into the struct
// first would silently drop any field the struct does not declare, which is
// exactly the divergence additionalProperties:false is there to catch.
func PrincipalFromWire(root string, raw []byte) (*Principal, error) {
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("%s: not valid JSON: %w", PrincipalSurface, err)
	}
	if err := Validate(root, PrincipalSurface, document); err != nil {
		return nil, err
	}
	var principal Principal
	if err := json.Unmarshal(raw, &principal); err != nil {
		return nil, fmt.Errorf("%s: decoding validated document: %w", PrincipalSurface, err)
	}
	return &principal, nil
}

// HasPermission is a membership test against the resolved action set.
//
// This is a lookup, NOT an authorization decision. ACP-ADR-05 decision 4
// requires an explicit action name at the call site; ACP-ADR-07 decision 3
// makes entitlement and authorization independent gates that must BOTH pass
// (a role grants no product, a paid product grants no action). Take the real
// decision from the authorization surface, which binds both.
func (p *Principal) HasPermission(action string) bool {
	for _, granted := range p.Permissions {
		if granted == action {
			return true
		}
	}
	return false
}

// IsImpersonated reports whether an operator is acting as this principal.
func (p *Principal) IsImpersonated() bool { return p.Impersonation != nil }

// EffectiveDeadline returns the earlier of the principal's own expiry and, if
// an impersonation session is active, that session's expiry.
//
// Exists so the "which expiry bounds this request" question has one answer
// with one call site, instead of being re-derived (and got wrong in one
// direction) at each. ACP-ADR-03 requires the delegated session to be
// strictly shorter than the base session, but this function does not assume
// that holds on the wire -- it takes the minimum rather than trusting the
// ordering, because a bound that relies on an invariant it does not check is
// only a bound while the invariant lasts.
func (p *Principal) EffectiveDeadline() time.Time {
	deadline := p.ExpiresAt
	if p.Impersonation != nil && p.Impersonation.ExpiresAt.Before(deadline) {
		deadline = p.Impersonation.ExpiresAt
	}
	return deadline
}
