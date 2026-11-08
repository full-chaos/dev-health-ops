package authapi

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// legacyMinter mints go-api's HS256 access token for a test that needs a bearer of the retired class. No
// production code mints one.
type legacyMinter struct {
	signer *edgetoken.Signer
	now    func() time.Time
}

func newLegacyMinter(secret, issuer, audience string) (*legacyMinter, error) {
	signer, err := edgetoken.NewSigner(secret, issuer, audience)
	if err != nil {
		return nil, err
	}
	return &legacyMinter{signer: signer, now: time.Now}, nil
}

func (m *legacyMinter) Access(grant Grant) (string, error) {
	now := m.now()
	expires, err := grantExpiry(grant, now, edgetoken.AccessLifetime)
	if err != nil {
		return "", err
	}
	org := ""
	if grant.OrganizationID != nil {
		org = grant.OrganizationID.String()
	}
	sid := session.WireSessionID(grant.SessionID)
	var actor *string
	if grant.Actor != nil {
		text := grant.Actor.String()
		actor = &text
	}
	token, err := m.signer.AccessUntil(edgetoken.AccessClaims{
		UserID: grant.PrincipalID.String(), Email: grant.User.Email, OrgID: org, Role: grant.Role,
		IsSuperuser: grant.User.IsSuperuser, Username: grant.User.Username, FullName: grant.User.FullName,
		TokenVersion: grant.User.TokenVersion, ImpersonatingUserID: actor, SessionID: &sid,
	}, now, expires, uuid.NewString())
	if err != nil {
		return "", fmt.Errorf("%w: %w", errIssue, err)
	}
	return token, nil
}

// newTestIssuer is an EdDSA issuer over a file signer; its verifier reads go-api's HS256 token under testSecret.
func newTestIssuer(t *testing.T) *Issuer {
	t.Helper()
	signer, _, _ := fileSigner(t)
	return newEdDSAIssuer(t, signer, testAudience)
}

// legacyAccess is go-api's HS256 access token for grant under testSecret, the bearer the session routes still verify.
func legacyAccess(t *testing.T, grant Grant) string {
	t.Helper()
	minter, err := newLegacyMinter(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	token, err := minter.Access(grant)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// eddsaBearer reads the principal and session an EdDSA access token names, as the Verify of the retired class did.
func eddsaBearer(t *testing.T, compact string) (Bearer, string, error) {
	t.Helper()
	claims := claimsOf(t, compact)
	sessionID, err := session.ParseWireSessionID(claims.Sid)
	if err != nil {
		return Bearer{}, "", err
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(claims.Sub, "prn_"))
	if err != nil {
		return Bearer{}, "", err
	}
	principal, err := uuid.FromBytes(raw)
	if err != nil {
		return Bearer{}, "", err
	}
	return Bearer{PrincipalID: principal, SessionID: sessionID}, "", nil
}
