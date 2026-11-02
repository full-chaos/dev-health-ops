package contracts

import "time"

// ServiceAccountTokenSurface names the service-account-token.v1 wire contract.
const ServiceAccountTokenSurface = "service-account-token.v1"

// ServiceAccountTokenClass is the credential class of a service-account-token.v1 token.
const ServiceAccountTokenClass = "service_account_token"

// ServiceAccountTokenTyp is the exact JWS typ of a service account token (RFC
// 8725 explicit typing). It differs from at+jwt and workload+jwt, so no verifier
// reads it as a user or a workload token.
const ServiceAccountTokenTyp = "sa+jwt"

// ServiceAccountTokenAudience is the one audience a service account token
// names. The schema fixes it; vocabulary/audiences.json carries the class on it alone.
const ServiceAccountTokenAudience = "dev-health-api"

// ServiceAccountTokenPrincipalType is the principal_type of a service account token.
const ServiceAccountTokenPrincipalType = "service_account"

// ServiceAccountTokenLifetimeCeiling is the ceiling on exp - iat of a service
// account token. It is the one place the value lives; the schema names it,
// the corpus pins it.
const ServiceAccountTokenLifetimeCeiling = 300 * time.Second

// ServiceAccountTokenHeader is the JWS protected header of a service-account-token.v1 token.
type ServiceAccountTokenHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// ServiceAccountTokenClaims are the claims of a service-account-token.v1
// token: the account (sub), its organization (org_id) and the key row it came
// from (cid). No session, no scope and no audience list.
type ServiceAccountTokenClaims struct {
	Iss           string `json:"iss"`
	Aud           string `json:"aud"`
	Sub           string `json:"sub"`
	PrincipalType string `json:"principal_type"`
	OrgID         string `json:"org_id"`
	Cid           string `json:"cid"`
	Jti           string `json:"jti"`
	Iat           int64  `json:"iat"`
	Nbf           int64  `json:"nbf"`
	Exp           int64  `json:"exp"`
}

// ServiceAccountToken is one service-account-token.v1 document.
type ServiceAccountToken struct {
	Header ServiceAccountTokenHeader `json:"header"`
	Claims ServiceAccountTokenClaims `json:"claims"`
}

// ParseServiceAccountToken validates and parses a service-account-token.v1
// document: the decoded protected header and claims of a service account
// token, after its signature was checked.
//
// Beyond the schema it refuses what a schema cannot say: a duplicate member
// anywhere, a NumericDate in decimal form, nbf before iat, exp not after nbf,
// and a lifetime over ServiceAccountTokenLifetimeCeiling. Every refusal is a
// CredentialRefusal: the surface, a stage and a rule code, never a value.
func ParseServiceAccountToken(root string, raw []byte) (*ServiceAccountToken, error) {
	token, err := parseServiceAccountToken(root, raw)
	if err != nil {
		return nil, refuseCredential(ServiceAccountTokenSurface, err)
	}
	return token, nil
}

func parseServiceAccountToken(root string, raw []byte) (*ServiceAccountToken, error) {
	var token ServiceAccountToken
	if err := decodeWire(root, ServiceAccountTokenSurface, raw, &token); err != nil {
		return nil, err
	}
	c := token.Claims
	if c.Nbf < c.Iat {
		return nil, serviceAccountTokenRule("nbf_before_iat")
	}
	if c.Exp <= c.Nbf {
		return nil, serviceAccountTokenRule("exp_not_after_nbf")
	}
	if c.Exp-c.Iat > int64(ServiceAccountTokenLifetimeCeiling/time.Second) {
		return nil, serviceAccountTokenRule("lifetime_over_ceiling")
	}
	return &token, nil
}

func serviceAccountTokenRule(rule string) error {
	return &CredentialRefusal{Surface: ServiceAccountTokenSurface, Stage: StageClientRule, Rule: rule}
}

// ServiceAccountTokenLifetime is the whole-second lifetime of a token issued at
// now for a key that expires at keyExpiresAt: the ceiling, or the time the key
// has left when that is shorter, so a token never outlives its key. A result
// under one second (an expired key, or an unknown expiry, which fails closed)
// means no token is issued.
func ServiceAccountTokenLifetime(now, keyExpiresAt time.Time) time.Duration {
	if keyExpiresAt.IsZero() {
		return 0
	}
	lifetime := ServiceAccountTokenLifetimeCeiling
	if left := keyExpiresAt.Sub(now).Truncate(time.Second); left < lifetime {
		lifetime = left
	}
	if lifetime < time.Second {
		return 0
	}
	return lifetime
}
