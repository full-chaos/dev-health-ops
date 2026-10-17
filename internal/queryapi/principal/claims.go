// Package principal is the Go VERIFIER half of the effective-principal
// envelope contract (CHAOS-4366 Wave 0, plan §3/§7 open decision 1).
//
// The Python edge (src/dev_health_ops/api/graphql/principal_envelope.py)
// mints a short-lived, audience-bound, EdDSA-signed JWT reproducing the
// graphql/authz.py + graphql/app.py + services/auth.py auth contract:
// user identity, org, permissions, superuser state, active impersonation,
// licensed features/tier, and token_version. query-api trusts this
// envelope rather than independently re-deriving auth state from
// Postgres/Valkey (plan §3's "auth stays put initially").
//
// Claims mirrors that envelope's v1 claim schema field-for-field. See
// SupportedSchemaVersion's doc comment for the versioning contract.
package principal

import "github.com/golang-jwt/jwt/v5"

// SupportedSchemaVersion is the only envelope claim-schema version (`v`)
// this verifier accepts. The issuer bumps `v` whenever a claim is added,
// removed, or its meaning changes (principal_envelope.py's module
// docstring); a verifier that didn't check `v` would silently accept a
// schema it wasn't written against. Verify rejects any other value.
const SupportedSchemaVersion = 1

// Claims is the effective-principal envelope's v1 claim schema. Field
// names and JSON tags match principal_envelope.py's
// EffectivePrincipalEnvelopeClaims exactly; iss/sub/aud/exp/iat/jti come
// from the embedded jwt.RegisteredClaims (sub = user id, matching the
// Python side's `sub` claim).
type Claims struct {
	SchemaVersion       int      `json:"v"`
	OrgID               string   `json:"org_id"`
	Role                string   `json:"role"`
	IsSuperuser         bool     `json:"is_superuser"`
	IsSuperuserVerified bool     `json:"is_superuser_verified"`
	Permissions         []string `json:"permissions"`
	TokenVersion        int      `json:"token_version"`
	Tier                string   `json:"tier"`
	LicensedFeatures    []string `json:"licensed_features"`
	ImpersonatedBy      *string  `json:"impersonated_by,omitempty"`
	ImpersonationActive bool     `json:"impersonation_active"`
	jwt.RegisteredClaims
}

// G-14 position (CHAOS-4738): Permissions, Tier and LicensedFeatures stay on
// the wire. The envelope lives 60 seconds and is minted per request, so stale
// entitlement rides along for at most that long; it is an authorization input
// the issuer computed, not a session credential. The superuser flags are the
// exception that matters: Verify clears them unless both are present and no
// impersonation is active (see enforceSuperuserDiscipline), so no consumer
// reads an unproven grant.

// UserID returns the envelope subject -- the authenticated user's id.
// Exists so callers read "user id" as a named concept rather than
// reaching into the embedded RegisteredClaims.Subject directly.
func (c *Claims) UserID() string {
	return c.Subject
}
