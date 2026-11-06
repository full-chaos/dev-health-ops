package authapi

import (
	"errors"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// errLegacyRefreshShape is a verified go-api refresh token without a UUID sub,
// a jti, or an org_id that is absent, empty or a UUID.
var errLegacyRefreshShape = errors.New("authapi: refresh token has no usable sub, jti or org_id")

// errLegacyKeyAbsent is any go-api refresh token once this Issuer holds no
// legacy key: nothing is verified and no claim is read.
var errLegacyKeyAbsent = errors.New("authapi: no legacy key to verify a go-api refresh token with")

// VerifyLegacyRefresh checks token as go-api's refresh route does
// (validate_token(token, "refresh"), same key, issuer and audience as the
// access token) and reads its sub, org_id and jti.
func (i *Issuer) VerifyLegacyRefresh(token string) (session.LegacyRefresh, error) {
	if i.verifier == nil {
		return session.LegacyRefresh{}, errLegacyKeyAbsent
	}
	claims, err := i.verifier.VerifyType(token, edgetoken.RefreshType)
	if err != nil {
		return session.LegacyRefresh{}, err
	}
	sub, _ := claims["sub"].(string)
	principal, err := uuid.Parse(sub)
	if err != nil {
		return session.LegacyRefresh{}, errLegacyRefreshShape
	}
	jti, _ := claims["jti"].(string)
	if jti == "" {
		return session.LegacyRefresh{}, errLegacyRefreshShape
	}
	out := session.LegacyRefresh{PrincipalID: principal, JTI: contracts.NewSecretString(jti)}
	switch org := claims["org_id"].(type) {
	case nil:
	case string:
		if org != "" {
			id, err := uuid.Parse(org)
			if err != nil {
				return session.LegacyRefresh{}, errLegacyRefreshShape
			}
			out.OrganizationID = &id
		}
	default:
		return session.LegacyRefresh{}, errLegacyRefreshShape
	}
	return out, nil
}

// legacyRefreshAccepted counts each HS256 refresh token the compat refresh route accepts; nil counts nothing.
// A package variable, not an Issuer field: endpoint-profile anchors pin line numbers in issuer.go.
var legacyRefreshAccepted atomic.Pointer[accessverify.RefreshCounter]

// SetRefreshAccepted sets the counter the compat refresh route counts accepted HS256 refresh tokens with.
func SetRefreshAccepted(counter *accessverify.RefreshCounter) { legacyRefreshAccepted.Store(counter) }

// legacyLogoutAccepted counts each HS256 refresh token the compat logout route verifies; nil counts nothing.
var legacyLogoutAccepted atomic.Pointer[accessverify.LogoutCounter]

// SetLogoutAccepted sets the counter the compat logout route counts verified HS256 refresh tokens with.
func SetLogoutAccepted(counter *accessverify.LogoutCounter) { legacyLogoutAccepted.Store(counter) }
