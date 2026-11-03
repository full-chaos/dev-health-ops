package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func payloadOf(t *testing.T, compact string) map[string]any {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("not a compact JWS: %q", compact)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

func delegatedGrant(until time.Time) Grant {
	org, actor := uuid.New(), uuid.New()
	return Grant{
		PrincipalID: uuid.New(), OrganizationID: &org, Role: "member", SessionID: uuid.New(),
		User: UserState{Email: "ada@example.com", IsActive: true, TokenVersion: 9}, ExpiresAt: &until, Actor: &actor,
	}
}

func plainHS256Issuer(t *testing.T) *Issuer {
	t.Helper()
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}

func TestDelegatedHS256ExpStopsAtTheGrantEnd(t *testing.T) {
	until := time.Now().Add(10 * time.Minute)
	tokens, err := plainHS256Issuer(t).Tokens(context.Background(), delegatedGrant(until))
	if err != nil {
		t.Fatal(err)
	}
	claims := payloadOf(t, tokens.Access)
	exp, iat := int64(claims["exp"].(float64)), int64(claims["iat"].(float64))
	if exp != until.Unix() {
		t.Fatalf("exp = %d, want %d", exp, until.Unix())
	}
	if int64(tokens.ExpiresIn) != exp-iat {
		t.Fatalf("expires_in = %d, want %d", tokens.ExpiresIn, exp-iat)
	}
}

func TestDelegatedEdDSAExpStopsAtTheGrantEndOnBothTokens(t *testing.T) {
	signer, _, _ := fileSigner(t)
	until := time.Now().Add(10 * time.Minute)
	tokens, err := newEdDSAIssuer(t, signer, testAudience).Tokens(context.Background(), delegatedGrant(until))
	if err != nil {
		t.Fatal(err)
	}
	for name, compact := range map[string]string{"access": tokens.Access, "session": tokens.Session} {
		claims := payloadOf(t, compact)
		exp, iat := int64(claims["exp"].(float64)), int64(claims["iat"].(float64))
		if exp != until.Unix() {
			t.Errorf("%s exp = %d, want %d", name, exp, until.Unix())
		}
		if int64(tokens.ExpiresIn) != exp-iat {
			t.Errorf("%s expires_in = %d, want %d", name, tokens.ExpiresIn, exp-iat)
		}
	}
}

func TestAGrantAtItsEndMintsNothing(t *testing.T) {
	signer, _, _ := fileSigner(t)
	for name, issuer := range map[string]*Issuer{"hs256": plainHS256Issuer(t), "eddsa": newEdDSAIssuer(t, signer, testAudience)} {
		for _, end := range []time.Duration{-time.Minute, 0} {
			tokens, err := issuer.Tokens(context.Background(), delegatedGrant(time.Now().Add(end)))
			if !errors.Is(err, errIssue) || tokens.Access != "" || tokens.Session != "" {
				t.Fatalf("%s end %v: tokens %+v, err %v; want errIssue and no token", name, end, tokens, err)
			}
		}
	}
}

func TestAGrantEndAfterTheLifetimeLeavesTheLifetime(t *testing.T) {
	grant := delegatedGrant(time.Now().Add(24 * time.Hour))
	tokens, err := plainHS256Issuer(t).Tokens(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	if tokens.ExpiresIn != AccessExpiresIn {
		t.Fatalf("expires_in = %d, want %d", tokens.ExpiresIn, AccessExpiresIn)
	}
}

func TestDelegatedHS256ClaimShape(t *testing.T) {
	grant := delegatedGrant(time.Now().Add(10 * time.Minute))
	tokens, err := plainHS256Issuer(t).Tokens(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	claims := payloadOf(t, tokens.Access)
	want := map[string]any{
		"sub": grant.PrincipalID.String(), "impersonating_user_id": grant.Actor.String(),
		"sid": session.WireSessionID(grant.SessionID), "tv": float64(9), "org_id": grant.OrganizationID.String(),
	}
	for name, value := range want {
		if claims[name] != value {
			t.Errorf("%s = %v, want %v", name, claims[name], value)
		}
	}
	if _, ok := claims["act"]; ok {
		t.Error("an HS256 token carries act")
	}
}

func TestDelegatedEdDSAClaimShapeOnBothTokens(t *testing.T) {
	signer, _, _ := fileSigner(t)
	grant := delegatedGrant(time.Now().Add(10 * time.Minute))
	tokens, err := newEdDSAIssuer(t, signer, testAudience).Tokens(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	actor := session.WirePrincipalID(*grant.Actor)
	for name, compact := range map[string]string{"access": tokens.Access, "session": tokens.Session} {
		claims := payloadOf(t, compact)
		act, _ := claims["act"].(map[string]any)
		if act["sub"] != actor || len(act) != 1 {
			t.Errorf("%s act = %v, want sub %s only", name, claims["act"], actor)
		}
		if claims["sub"] != session.WirePrincipalID(grant.PrincipalID) || claims["sid"] != session.WireSessionID(grant.SessionID) {
			t.Errorf("%s sub %v sid %v", name, claims["sub"], claims["sid"])
		}
		if _, ok := claims["impersonating_user_id"]; ok {
			t.Errorf("%s carries the HS256 claim", name)
		}
	}
}

func TestReadersTakeTheDelegatedEdDSAToken(t *testing.T) {
	signer, _, _ := fileSigner(t)
	set, err := signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	grant := delegatedGrant(time.Now().Add(10 * time.Minute))
	tokens, err := newEdDSAIssuer(t, signer, testAudience).Tokens(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	actor := session.WirePrincipalID(*grant.Actor)
	parsed, err := contracts.ParseAccessToken(repoRoot(t), accessDocument(t, verifyEdDSA(t, tokens.Access, set, goldenIssuer, testAudience, time.Now())))
	if err != nil {
		t.Fatalf("ParseAccessToken: %v", err)
	}
	if parsed.Claims.Act == nil || parsed.Claims.Act.Sub != actor {
		t.Fatalf("Act = %+v, want %s", parsed.Claims.Act, actor)
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: goldenIssuer, Audience: testAudience, Keys: publishedKeys{source: signer}})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(tokens.Access, time.Now())
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.ActorSubject == nil || *claims.ActorSubject != actor {
		t.Fatalf("ActorSubject = %v, want %s", claims.ActorSubject, actor)
	}
}

func TestAPlainGrantCarriesNoActorClaim(t *testing.T) {
	signer, _, _ := fileSigner(t)
	grant := delegatedGrant(time.Time{})
	grant.ExpiresAt, grant.Actor = nil, nil
	for name, issuer := range map[string]*Issuer{"hs256": plainHS256Issuer(t), "eddsa": newEdDSAIssuer(t, signer, testAudience)} {
		tokens, err := issuer.Tokens(context.Background(), grant)
		if err != nil {
			t.Fatal(err)
		}
		claims := payloadOf(t, tokens.Access)
		for _, member := range []string{"act", "impersonating_user_id"} {
			if _, ok := claims[member]; ok {
				t.Errorf("%s: a plain token carries %s", name, member)
			}
		}
		if tokens.ExpiresIn != issuer.ExpiresIn() {
			t.Errorf("%s: expires_in %d, want %d", name, tokens.ExpiresIn, issuer.ExpiresIn())
		}
	}
}
