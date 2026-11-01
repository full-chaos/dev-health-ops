package contracts

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v2TokenClaims decodes the claims segment of the named token member of a v2
// document.
func v2TokenClaims(t *testing.T, document map[string]any, member string) AccessTokenClaims {
	t.Helper()
	token, ok := document[member].(string)
	if !ok {
		t.Fatalf("%s is not a string", member)
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		t.Fatalf("%s is not a compact JWS", member)
	}
	raw, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims AccessTokenClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

// The session routes accept an EdDSA token of audience auth-service only, and go-api and query-api one of audience
// dev-health-api only, so a v2 answer carries one token of each audience, both of the response's session and both
// living expires_in.
func TestRefreshResponseV2CarriesTheSessionRouteToken(t *testing.T) {
	for _, name := range []string{"valid-full-lifetime.json", "valid-short-lifetime.json"} {
		raw := v2Fixture(t, name)
		response, err := ParseRefreshResponse(testRoot(t), raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if response.SessionAccessToken.Reveal() == "" {
			t.Fatalf("%s: no session_access_token parsed", name)
		}
		var document map[string]any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		api, session := v2TokenClaims(t, document, "access_token"), v2TokenClaims(t, document, "session_access_token")
		if api.Aud != PlatformAPIAudience || session.Aud != SessionRouteAudience {
			t.Errorf("%s: audiences %q and %q, want %q and %q", name, api.Aud, session.Aud, PlatformAPIAudience, SessionRouteAudience)
		}
		if session.Sid != response.SessionID || session.Exp-session.Iat != int64(response.ExpiresIn) {
			t.Errorf("%s: the session token is not of the response's session and lifetime", name)
		}
		if session.Jti == api.Jti {
			t.Errorf("%s: both tokens carry jti %q", name, api.Jti)
		}
	}
}

func TestRefreshResponseV2SessionTokenRefusalsNameTheirRule(t *testing.T) {
	cases := map[string]struct{ stage, rule string }{
		"invalid-missing-session-access-token.json":                {StageSchema, ""},
		"invalid-session-access-token-not-jws.json":                {StageSchema, ""},
		"client-rejected-access-token-session-audience.json":       {StageClientRule, "access_token_audience"},
		"client-rejected-session-token-api-audience.json":          {StageClientRule, "session_access_token_audience"},
		"client-rejected-session-token-sid.json":                   {StageClientRule, "session_id_not_session_access_token_sid"},
		"client-rejected-session-token-lifetime.json":              {StageClientRule, "expires_in_not_session_access_token_lifetime"},
		"client-rejected-session-token-header-alg-hs256.json":      {StageSchema, ""},
		"client-rejected-session-token-payload-trailing-bits.json": {StageClientRule, "session_access_token_segment_encoding"},
	}
	for name, want := range cases {
		_, err := ParseRefreshResponse(testRoot(t), v2Fixture(t, name))
		var refusal *CredentialRefusal
		if !errors.As(err, &refusal) {
			t.Errorf("%s: err %v, want a CredentialRefusal", name, err)
			continue
		}
		if refusal.Surface != RefreshResponseV2Surface || refusal.Stage != want.stage || refusal.Rule != want.rule {
			t.Errorf("%s: refused %s at %s %q, want %s %s %q", name, refusal.Surface, refusal.Stage, refusal.Rule,
				RefreshResponseV2Surface, want.stage, want.rule)
		}
	}
}

// Swapping the two tokens is refused: each member is bound to its audience.
func TestRefreshResponseV2RefusesSwappedTokens(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(v2Fixture(t, "valid-full-lifetime.json"), &document); err != nil {
		t.Fatal(err)
	}
	document["access_token"], document["session_access_token"] = document["session_access_token"], document["access_token"]
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ParseRefreshResponse(testRoot(t), raw)
	var refusal *CredentialRefusal
	if !errors.As(err, &refusal) || refusal.Rule != "access_token_audience" {
		t.Fatalf("swapped tokens: err %v, want the access_token_audience refusal", err)
	}
}

// A v1 answer has no session_access_token: the HS256 token is accepted at the session routes as it is.
func TestRefreshResponseV1RefusesASessionAccessToken(t *testing.T) {
	var document map[string]any
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "examples", "refresh-response", "valid-full-lifetimes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["session_access_token"] = document["access_token"]
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseRefreshResponse(testRoot(t), raw); err == nil {
		t.Fatal("a v1 answer with a session_access_token parsed")
	}
}
