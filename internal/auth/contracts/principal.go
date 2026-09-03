package contracts

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
// TRD section 21: contract evolution is additive within v1; breaking
// semantics require v2. Distinct from the three other version-shaped values
// in this system -- the effective-principal envelope's `v` claim, a
// principal's credential generation counter, and the "v1" path segment --
// which move independently.
const PrincipalSchemaVersion = "principal.v1"

// Credential is the credential that authenticated a principal.
//
// Present because TRD section 2 principle 1 makes credential class part of
// the route contract, and G-2/G-3 forbid treating every bearer value as one
// class or discovering the class by validator fan-out. The credential
// authenticates a principal but IS NOT the principal (TRD section 8 rule 2).
type Credential struct {
	// Class is a value from the closed vocabulary: the Wave 0 frozen 30 in
	// contracts/auth/v1/credential-classes.json plus the three new platform
	// credentials TRD section 11 defines. Unknown denies (G-26). The schema
	// enforces it, so a value here has already been checked.
	Class string `json:"class"`
	// CredentialID identifies the credential INSTANCE. Never the secret:
	// G-16 forbids a credential value in any fixture, log, trace, metric
	// label or audit row, and G-17 makes plaintext secrets one-time output.
	CredentialID string `json:"credential_id"`
	Issuer       string `json:"issuer"`
	Audience     string `json:"audience"`
	// Scopes bound what the credential may be used for. A DIFFERENT
	// vocabulary from authorization actions (G-5: authentication is not
	// authorization). Empty is meaningful and never means unrestricted.
	Scopes []string `json:"scopes"`
}

// Authentication records how and when the principal authenticated.
//
// Assurance is an authorization input, not decoration: G-31 names it as part
// of an allow-cache key, and G-51 requires step-up gating for high-risk
// actions during impersonation.
type Authentication struct {
	// Methods are RFC 8176 amr names, restricted by the schema to the subset
	// this platform's described flows can produce.
	Methods []string `json:"methods"`
	// AuthenticatedAt is when the principal actually authenticated, which is
	// NOT Principal.IssuedAt: one authentication event can produce many
	// resolved principal documents, and a step-up decision needs the age of
	// the authentication rather than the age of this document.
	AuthenticatedAt time.Time `json:"authenticated_at"`
	// Assurance is a NIST SP 800-63B level: aal1, aal2 or aal3.
	Assurance string `json:"assurance"`
}

// Delegation is one hop in the actor chain.
//
// ACP-ADR-03 bounds a delegated session at 15 minutes; G-52 requires an
// independent id, a reason, an explicit permitted action set, and independent
// revocability. G-50: while delegated, permissions derive from the effective
// subject -- real-actor platform authority does not implicitly authorize
// target actions.
type Delegation struct {
	// ActorPrincipalID is the REAL actor acting as the effective subject.
	ActorPrincipalID string    `json:"actor_principal_id"`
	DelegationID     string    `json:"delegation_id"`
	Reason           string    `json:"reason"`
	StartedAt        time.Time `json:"started_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	// PermittedActions is the bounded set this delegation may exercise.
	// Empty means the delegation authorizes nothing by itself -- a real
	// state, never "unrestricted".
	PermittedActions []string `json:"permitted_actions"`
}

// Principal is a resolved principal.v1 document (TRD section 8).
//
// FOUR DELIBERATE OMISSIONS, none an oversight to repair:
//
//   - no Tier / LicensedFeatures -- ACP-ADR-07 decision 2, G-14, and TRD
//     section 8's own rule "Grants and entitlements are not identity claims".
//   - no Permissions -- TRD section 11 and G-14 forbid embedding a complete
//     mutable permission set in a credential; ask the authorization surface,
//     which also binds entitlement as an independent gate.
//   - no Role -- ACP-ADR-05 decision 4 and G-27 require an explicit action at
//     the call site, never a broad role check as the final decision.
//   - no IsSuperuser -- TRD section 3 and G-23 move platform authority to a
//     separate role/action namespace resolved against CURRENT state.
//
// The existing effective-principal envelope
// (cmd/query-api/internal/principal/claims.go) carries all four today. It is
// the compatibility implementation this migration retires; a reader diffing
// against it will find four things "missing", and the absence is the design.
type Principal struct {
	SchemaVersion string `json:"schema_version"`
	// PrincipalID is the stable authorization subject (TRD section 8 rule 1,
	// G-20) -- never an email, username or token prefix.
	PrincipalID   string `json:"principal_id"`
	PrincipalType string `json:"principal_type"`
	// SubjectID is the underlying subject record, SEPARATE from PrincipalID
	// by design: authorization keys on PrincipalID, never on this. nil where
	// the principal has no distinct underlying subject.
	SubjectID *string `json:"subject_id"`
	// OrganizationID is nil only for a genuinely org-less principal. A
	// relying party that scopes by organization must treat nil as "no
	// organization" and DENY, never as "any organization" (G-22).
	OrganizationID *string        `json:"organization_id"`
	Credential     Credential     `json:"credential"`
	Authentication Authentication `json:"authentication"`
	// ActorChain is server-derived and append-only (TRD section 8 rule 4).
	// Empty means acting as itself. A chain rather than a single impersonator
	// because G-49 requires the real actor and the effective subject both to
	// survive downstream, and delegation nests.
	ActorChain []Delegation `json:"actor_chain"`
	// All four revisions are required by G-31's allow-cache key. TRD section
	// 8's example shows only membership and policy; that example is
	// under-specified against the guardrail, which is stated as an acceptance
	// criterion rather than a suggestion.
	MembershipRevision  int64     `json:"membership_revision"`
	PolicyRevision      int64     `json:"policy_revision"`
	GrantRevision       int64     `json:"grant_revision"`
	EntitlementRevision int64     `json:"entitlement_revision"`
	IssuedAt            time.Time `json:"issued_at"`
	// ExpiresAt is enforced by the relying party. ACP-ADR-03 removes the
	// envelope's per-call TTL override outright: a security bound any call
	// site may widen is not a bound.
	ExpiresAt time.Time `json:"expires_at"`
}

// PrincipalFromWire validates raw against principal.v1 and then decodes it.
//
// The order is the point. Validation runs against the DECODED JSON VALUE, not
// against this struct, so the contract is checked on the bytes that actually
// arrived rather than on Go's rendering of them -- decoding into the struct
// first would silently drop any field the struct does not declare, which is
// exactly the divergence additionalProperties:false exists to catch.
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

// IsDelegated reports whether at least one actor is acting as this principal.
func (p *Principal) IsDelegated() bool { return len(p.ActorChain) > 0 }

// RealActorPrincipalID returns the originating real actor, or the principal
// itself when not delegated.
//
// G-49 requires the real actor and the effective subject to stay
// distinguishable through token, decision, audit and downstream execution.
// One named accessor so that question has one answer instead of being
// re-derived, and got wrong, at each call site.
func (p *Principal) RealActorPrincipalID() string {
	if len(p.ActorChain) > 0 {
		return p.ActorChain[0].ActorPrincipalID
	}
	return p.PrincipalID
}

// EffectiveDeadline returns the earliest of the principal's own expiry and
// every delegation's.
//
// Takes the minimum rather than trusting ACP-ADR-03's requirement that a
// delegated session be strictly shorter than the base session: a bound that
// relies on an invariant it does not check is only a bound while the
// invariant holds.
func (p *Principal) EffectiveDeadline() time.Time {
	deadline := p.ExpiresAt
	for _, hop := range p.ActorChain {
		if hop.ExpiresAt.Before(deadline) {
			deadline = hop.ExpiresAt
		}
	}
	return deadline
}

// CacheKeyDimensions returns every dimension G-31 requires an allow-cache key
// to bind, from the principal's side.
//
// G-31: "An allow cache key includes principal, actor chain,
// credential/session, organization, action, resource, policy revision,
// membership/grant revisions, entitlement revision when applicable, and
// relevant assurance." Action and resource belong to the decision rather than
// the principal, so a caller appends those; everything the PRINCIPAL
// contributes is produced here, in one place, so a consumer cannot bind a
// subset by accident. A key built from a subset is a cache a revision bump
// cannot invalidate.
func (p *Principal) CacheKeyDimensions() []string {
	organization := "<nil>"
	if p.OrganizationID != nil {
		organization = *p.OrganizationID
	}
	dimensions := []string{p.PrincipalID}
	for _, hop := range p.ActorChain {
		dimensions = append(dimensions, "dlg="+hop.DelegationID)
	}
	return append(dimensions,
		"cred="+p.Credential.CredentialID,
		"org="+organization,
		fmt.Sprintf("policy=%d", p.PolicyRevision),
		fmt.Sprintf("membership=%d", p.MembershipRevision),
		fmt.Sprintf("grant=%d", p.GrantRevision),
		fmt.Sprintf("entitlement=%d", p.EntitlementRevision),
		"assurance="+p.Authentication.Assurance,
	)
}
