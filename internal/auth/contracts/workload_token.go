package contracts

import "time"

// WorkloadTokenSurface names the workload-token.v1 wire contract.
const WorkloadTokenSurface = "workload-token.v1"

// WorkloadTokenClass is the credential class of a workload-token.v1 token.
const WorkloadTokenClass = "workload_token"

// WorkloadTokenTyp is the exact JWS typ of a workload token (RFC 8725 explicit
// typing). It differs from the user access token's at+jwt.
const WorkloadTokenTyp = "workload+jwt"

// WorkloadTokenLifetimeCeiling is the ceiling on exp - iat of a workload token.
// It is the one place the value lives; the schema names it, the corpus pins it.
const WorkloadTokenLifetimeCeiling = 300 * time.Second

// WorkloadTokenHeader is the JWS protected header of a workload-token.v1 token.
type WorkloadTokenHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// WorkloadTokenClaims are the claims of a workload-token.v1 token: no
// organization, no session and no scope.
type WorkloadTokenClaims struct {
	Iss           string `json:"iss"`
	Aud           string `json:"aud"`
	Sub           string `json:"sub"`
	PrincipalType string `json:"principal_type"`
	Jti           string `json:"jti"`
	Iat           int64  `json:"iat"`
	Nbf           int64  `json:"nbf"`
	Exp           int64  `json:"exp"`
}

// WorkloadToken is one workload-token.v1 document.
type WorkloadToken struct {
	Header WorkloadTokenHeader `json:"header"`
	Claims WorkloadTokenClaims `json:"claims"`
}

// ParseWorkloadToken validates and parses a workload-token.v1 document: the
// decoded protected header and claims of a workload token, after its
// signature was checked.
//
// Beyond the schema it refuses what a schema cannot say: a duplicate member
// anywhere, a NumericDate in decimal form, nbf before iat, exp not after nbf,
// and a lifetime over WorkloadTokenLifetimeCeiling. Every refusal is a
// CredentialRefusal: the surface, a stage and a rule code, never a value.
func ParseWorkloadToken(root string, raw []byte) (*WorkloadToken, error) {
	token, err := parseWorkloadToken(root, raw)
	if err != nil {
		return nil, refuseCredential(WorkloadTokenSurface, err)
	}
	return token, nil
}

func parseWorkloadToken(root string, raw []byte) (*WorkloadToken, error) {
	var token WorkloadToken
	if err := decodeWire(root, WorkloadTokenSurface, raw, &token); err != nil {
		return nil, err
	}
	c := token.Claims
	if c.Nbf < c.Iat {
		return nil, workloadTokenRule("nbf_before_iat")
	}
	if c.Exp <= c.Nbf {
		return nil, workloadTokenRule("exp_not_after_nbf")
	}
	if c.Exp-c.Iat > int64(WorkloadTokenLifetimeCeiling/time.Second) {
		return nil, workloadTokenRule("lifetime_over_ceiling")
	}
	return &token, nil
}

func workloadTokenRule(rule string) error {
	return &CredentialRefusal{Surface: WorkloadTokenSurface, Stage: StageClientRule, Rule: rule}
}
