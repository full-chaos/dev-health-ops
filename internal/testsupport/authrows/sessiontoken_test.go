package authrows

import (
	"encoding/base64"
	"strings"
	"testing"
)

const venueIssuer = "https://auth.dev-health.internal:8443"

// mintSessionToken is a compact JWS built at test time, so no token-shaped text is in this source.
func mintSessionToken(header, claims string) string {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return enc(header) + "." + enc(claims) + "." + enc("signature")
}

func sessionAnswer(member string) string {
	access := mintSessionToken(`{"alg":"EdDSA","typ":"at+jwt"}`, `{"iss":"`+venueIssuer+`","aud":"dev-health-api"}`)
	return `{"access_token":"` + access + `","refresh_token":"dhr1_x","token_type":"bearer"` + member + `}`
}

func sessionMember(token string) string { return `,"session_access_token":"` + token + `"` }

// CHAOS-13107: the golden does not hold session_access_token by value (a fresh token per run), so the corpus checks
// every target answer that carries an access token for it: present, not empty, alg EdDSA, iss the venue issuer.
func TestSessionAccessTokenCellRefusesAMissingEmptyOrForeignToken(t *testing.T) {
	good := mintSessionToken(`{"alg":"EdDSA","typ":"at+jwt","kid":"k1"}`, `{"iss":"`+venueIssuer+`","aud":"auth-service"}`)
	if err := SessionAccessTokenErr(sessionAnswer(sessionMember(good)), venueIssuer); err != nil {
		t.Fatalf("a well-formed session_access_token was refused: %v", err)
	}
	for name, c := range map[string]struct{ body, want string }{
		"no session_access_token":  {sessionAnswer(""), "no session_access_token"},
		"an empty one":             {sessionAnswer(sessionMember("")), "empty"},
		"a null one":               {sessionAnswer(`,"session_access_token":null`), "not a string"},
		"not a compact JWS":        {sessionAnswer(sessionMember("opaque-value")), "not a compact JWS"},
		"alg HS256":                {sessionAnswer(sessionMember(mintSessionToken(`{"alg":"HS256","typ":"at+jwt"}`, `{"iss":"`+venueIssuer+`"}`))), `alg "HS256"`},
		"alg none":                 {sessionAnswer(sessionMember(mintSessionToken(`{"alg":"none"}`, `{"iss":"`+venueIssuer+`"}`))), `alg "none"`},
		"no alg":                   {sessionAnswer(sessionMember(mintSessionToken(`{"typ":"at+jwt"}`, `{"iss":"`+venueIssuer+`"}`))), `alg ""`},
		"another issuer":           {sessionAnswer(sessionMember(mintSessionToken(`{"alg":"EdDSA"}`, `{"iss":"dev-health-ops"}`))), `iss "dev-health-ops"`},
		"no issuer":                {sessionAnswer(sessionMember(mintSessionToken(`{"alg":"EdDSA"}`, `{"aud":"auth-service"}`))), `iss ""`},
		"an issuer that is no str": {sessionAnswer(sessionMember(mintSessionToken(`{"alg":"EdDSA"}`, `{"iss":7}`))), "iss"},
		"a header that is no JSON": {sessionAnswer(sessionMember("bm90IGpzb24." + strings.Split(good, ".")[1] + ".c2ln")), "header"},
	} {
		t.Run(name, func(t *testing.T) {
			err := SessionAccessTokenErr(c.body, venueIssuer)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a refusal naming %q, got %v", c.want, err)
			}
			if err != nil && strings.Contains(err.Error(), strings.Split(good, ".")[1]) {
				t.Fatal("the refusal prints the token")
			}
		})
	}
	// An answer without an access token needs none; one that carries a session token alone is still checked.
	for _, body := range []string{`{"message":"Check your email to continue"}`, `{"detail":"Invalid credentials"}`, `[]`, `not json`} {
		if err := SessionAccessTokenErr(body, venueIssuer); err != nil {
			t.Errorf("%s: an answer that carries no token was refused: %v", body, err)
		}
	}
	if err := SessionAccessTokenErr(`{"session_access_token":""}`, venueIssuer); err == nil {
		t.Error("an empty session_access_token outside a token answer was accepted")
	}
	if err := SessionAccessTokenErr(sessionAnswer(sessionMember(good)), ""); err == nil || !strings.Contains(err.Error(), "venue issuer") {
		t.Errorf("a check without the venue issuer ran: %v", err)
	}
}

// The comparison drops the projected session_access_token, the last member of the target's answer, and nothing else:
// the golden never held it, and SessionAccessTokenErr checked the raw value.
func TestTargetResponseBodyDropsOnlyTheProjectedSessionAccessTokenMember(t *testing.T) {
	golden := goldenBody(t, "accept: ivan joins org A")
	target := targetOf(t, golden, false)
	want := GoldenResponseBody("accept: ivan joins org A", golden)
	projected := `"session_access_token":"<jwt|aud=string:auth-service|hdr.alg=string:EdDSA|iss=string:https%3A%2F%2Fauth.dev-health.internal%3A8443>"`
	withSession := strings.TrimSuffix(target, "}") + "," + projected + "}"
	if got := TargetResponseBody(withSession); got != want {
		t.Fatalf("the projected session_access_token was not dropped:\n got  %s\n want %s", got, want)
	}
	for name, body := range map[string]string{
		"not the last member": strings.Replace(target, `"token_type":"bearer"`, projected+`,"token_type":"bearer"`, 1),
		"not projected":       strings.TrimSuffix(target, "}") + `,"session_access_token":"opaque"}`,
		"undecodable":         strings.TrimSuffix(target, "}") + `,"session_access_token":"<jwt:undecodable:header>"}`,
		"another member":      strings.TrimSuffix(target, "}") + `,"session_access_tokens":"<jwt|hdr.alg=string:EdDSA>"}`,
	} {
		if TargetResponseBody(body) == want {
			t.Errorf("%s was classified away", name)
		}
	}
}
