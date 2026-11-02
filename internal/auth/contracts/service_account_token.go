package contracts

import (
	"errors"
	"time"
)

const ServiceAccountTokenSurface = "service-account-token.v1"

const ServiceAccountTokenClass = "service_account_token"

const ServiceAccountTokenTyp = "sa+jwt"

const ServiceAccountTokenAudience = "dev-health-api"

const ServiceAccountTokenPrincipalType = "service_account"

const ServiceAccountTokenLifetimeCeiling = 0 * time.Second

type ServiceAccountTokenHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

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

type ServiceAccountToken struct {
	Header ServiceAccountTokenHeader `json:"header"`
	Claims ServiceAccountTokenClaims `json:"claims"`
}

func ParseServiceAccountToken(root string, raw []byte) (*ServiceAccountToken, error) {
	return nil, errors.New("not implemented")
}

func ServiceAccountTokenLifetime(now, keyExpiresAt time.Time) time.Duration {
	return 0
}
