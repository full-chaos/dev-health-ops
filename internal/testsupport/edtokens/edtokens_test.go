package edtokens_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

func userRowProfile(t *testing.T) string {
	t.Helper()
	rows, err := accessrouting.Rows()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.AcceptsUser {
			return row.Profile
		}
	}
	t.Fatal("no go-api row accepts the user access token")
	return ""
}

func TestMintedTokenVerifiesWithItsSessionLive(t *testing.T) {
	issuer := edtokens.New(t)
	user, org, actor := uuid.New(), uuid.NewString(), uuid.New()
	token, err := issuer.Mint(edtokens.Claims{UserID: user, OrgID: org, Actor: &actor})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := issuer.Verifier(t).Verify(token, time.Now())
	if err != nil {
		t.Fatalf("a minted token does not verify: %v", err)
	}
	if claims.Subject != edtokens.PrincipalID(user) || claims.OrgID == nil || *claims.OrgID != org ||
		claims.ActorSubject == nil || *claims.ActorSubject != edtokens.PrincipalID(actor) {
		t.Fatalf("claims %+v do not name the minted user, organization and actor", claims)
	}
	sid, err := sessionid.Parse(claims.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	store := issuer.Store(policy.PGStore{})
	row, found, err := store.SessionState(context.Background(), sid)
	if err != nil || !found || row.PrincipalID != user || row.Revoked {
		t.Fatalf("the minted session is not live for its user: %+v found=%v err=%v", row, found, err)
	}
	if row.LegacyTokenVersion == nil || *row.LegacyTokenVersion != 0 {
		t.Fatalf("the minted session pins token_version %v, want 0 (the api refuses a session without a pin)", row.LegacyTokenVersion)
	}
	pinned, err := issuer.Mint(edtokens.Claims{UserID: user, TokenVersion: 3})
	if err != nil {
		t.Fatal(err)
	}
	pinnedClaims, err := issuer.Verifier(t).Verify(pinned, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pinnedSID, err := sessionid.Parse(pinnedClaims.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if row, _, _ := store.SessionState(context.Background(), pinnedSID); row.LegacyTokenVersion == nil || *row.LegacyTokenVersion != 3 {
		t.Fatalf("a token minted at token_version 3 pins %v", row.LegacyTokenVersion)
	}
	if _, found, _ := store.SessionState(context.Background(), uuid.New()); found {
		t.Fatal("a session this issuer never minted is found")
	}
	if _, err := issuer.Mint(edtokens.Claims{}); err == nil {
		t.Fatal("a token without a user was minted")
	}
}

// Sessions lists every minted session with its user, token_version pin and expiry: the rows a venue writes as
// auth-service sessions.
func TestSessionsListEveryMintedSession(t *testing.T) {
	issuer := edtokens.New(t)
	user, at := uuid.New(), time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, version := range []int64{0, 7} {
		if _, err := issuer.Mint(edtokens.Claims{UserID: user, TokenVersion: version, IssuedAt: at, Lifetime: time.Minute}); err != nil {
			t.Fatal(err)
		}
	}
	sessions := issuer.Sessions()
	if len(sessions) != 2 {
		t.Fatalf("%d sessions, want 2", len(sessions))
	}
	pins := map[int64]bool{}
	for _, session := range sessions {
		if session.Principal != user || !session.ExpiresAt.Equal(at.Add(time.Minute)) || session.ID == uuid.Nil {
			t.Errorf("session %+v, want user %s expiring %s", session, user, at.Add(time.Minute))
		}
		pins[session.TokenVersion] = true
	}
	if !pins[0] || !pins[7] {
		t.Errorf("pins %v, want 0 and 7", pins)
	}
}

// The Authenticator verifies this Issuer's key only: an HS256 bearer and another issuer's token are refused on
// a row that takes the user access token, before any store read (the store has no pool).
func TestAuthenticatorRefusesWhatThisIssuerDidNotMint(t *testing.T) {
	issuer := edtokens.New(t)
	auth := issuer.Authenticator(t, policy.PGStore{}, nil)
	ctx := policy.WithRouteProfile(context.Background(), userRowProfile(t))
	other, err := edtokens.New(t).Mint(edtokens.Claims{UserID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	hs256, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": uuid.NewString(), "type": "access", "exp": time.Now().Add(time.Hour).Unix()}).SignedString([]byte(strings.Repeat("k", 40)))
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"another issuer": other, "hs256": hs256} {
		user, err := auth.Authenticate(ctx, token)
		if user != nil || err == nil || !policy.IsRefusal(err) {
			t.Errorf("%s: user %v err %v, want a refusal", name, user, err)
		}
	}
}
