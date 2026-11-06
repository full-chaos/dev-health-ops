package authrows

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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

// sessionsLeg is the GoldenSpec.GoBlanked of a corpus leg that forwards sessions (TestAuthFlowVenueOracle).
var sessionsLeg = map[string]string{SessionAccessTokenPattern: SessionAccessTokenReason}

const projectedSession = `"session_access_token":"<jwt|aud=string:auth-service|hdr.alg=string:EdDSA|iss=string:https%3A%2F%2Fauth.dev-health.internal%3A8443>"`

// withLast is body with member as its last member.
func withLast(body, member string) string { return strings.TrimSuffix(body, "}") + "," + member + "}" }

// The comparison drops session_access_token by key, wherever it sits, and only the key the leg's GoldenSpec.GoBlanked
// names ("body $.<key>"): the golden never held it, and SessionAccessTokenErr checked the raw value. A leg with no
// GoBlanked drops nothing.
func TestTargetResponseBodyDropsOnlyTheGoBlankedKey(t *testing.T) {
	golden := goldenBody(t, "accept: ivan joins org A")
	target := targetOf(t, golden, false)
	want := GoldenResponseBody("accept: ivan joins org A", golden)
	for name, body := range map[string]string{
		"the last member":  withLast(target, projectedSession),
		"a middle member":  strings.Replace(target, `"token_type":"bearer"`, projectedSession+`,"token_type":"bearer"`, 1),
		"the first member": "{" + projectedSession + "," + strings.TrimPrefix(target, "{"),
	} {
		if body == target {
			t.Fatalf("%s: nothing planted", name)
		}
		if got := TargetResponseBody(body, sessionsLeg); got != want {
			t.Errorf("%s: the GoBlanked key was not dropped:\n got  %s\n want %s", name, got, want)
		}
	}
	for name, c := range map[string]struct {
		body      string
		goBlanked map[string]string
	}{
		"a leg with no GoBlanked":             {withLast(target, projectedSession), nil},
		"a GoBlanked naming another key":      {withLast(target, projectedSession), map[string]string{"body $.other_token": "a reason"}},
		"a GoBlanked naming a deeper path":    {withLast(target, projectedSession), map[string]string{"body $.user.session_access_token": "a reason"}},
		"a GoBlanked naming a header":         {withLast(target, projectedSession), map[string]string{"header session_access_token": "a reason"}},
		"not projected":                       {withLast(target, `"session_access_token":"opaque"`), sessionsLeg},
		"undecodable":                         {withLast(target, `"session_access_token":"<jwt:undecodable:header>"`), sessionsLeg},
		"another key":                         {withLast(target, `"session_access_tokens":"<jwt|hdr.alg=string:EdDSA>"`), sessionsLeg},
		"the key in a nested object":          {withLast(target, `"session":{`+projectedSession+`}`), sessionsLeg},
		"the key's text inside another value": {strings.Replace(target, `"org_name":"Org A"`, `"org_name":"Org A,`+strings.ReplaceAll(projectedSession, `"`, `'`)+`"`, 1), sessionsLeg},
	} {
		if TargetResponseBody(c.body, c.goBlanked) == want {
			t.Errorf("%s was classified away", name)
		}
	}
}

// On a GoBlanked leg every other member is still compared, and in its order: a member missing, a planted extra
// member (wherever it sits, the last place included) and a reordered body each still fail Compare.
func TestGoBlankedLegStillComparesEveryOtherMember(t *testing.T) {
	golden := goldenBody(t, "accept: ivan joins org A")
	target := targetOf(t, golden, false)
	want := GoldenResponseBody("accept: ivan joins org A", golden)
	compare := func(body string) bool {
		headers := map[string]string{"content-type": "application/json"}
		same, _, _, _ := venueoracle.Compare(venueoracle.Request{Method: http.MethodPost, Path: "/api/v1/auth/accept-invite"},
			venueoracle.Response{Status: http.StatusOK, Headers: headers, Body: want},
			venueoracle.Response{Status: http.StatusOK, Headers: headers, Body: TargetResponseBody(body, sessionsLeg)},
			venueoracle.DiffOptions{})
		return same
	}
	if !compare(withLast(target, projectedSession)) {
		t.Fatal("the clean pair on a GoBlanked leg does not compare equal")
	}
	const tokenType, expiresIn = `"token_type":"bearer"`, `"expires_in":3600`
	if !strings.Contains(target, tokenType+","+expiresIn) {
		t.Fatalf("the target body changed shape: %s", target)
	}
	for name, body := range map[string]string{
		"a planted extra member, last":             withLast(withLast(target, projectedSession), `"extra":"x"`),
		"a planted extra member, before the token": withLast(withLast(target, `"extra":"x"`), projectedSession),
		"a planted extra member, first":            "{" + `"extra":"x",` + strings.TrimPrefix(withLast(target, projectedSession), "{"),
		"a member missing":                         strings.Replace(withLast(target, projectedSession), tokenType+",", "", 1),
		"a member missing, the token in its place": strings.Replace(target, tokenType, projectedSession, 1),
		"a reordered body":                         strings.Replace(withLast(target, projectedSession), tokenType+","+expiresIn, expiresIn+","+tokenType, 1),
		"a reordered body, the token between":      strings.Replace(withLast(target, projectedSession), tokenType+","+expiresIn, expiresIn+","+projectedSession+","+tokenType, 1),
	} {
		if compare(body) {
			t.Errorf("%s was hidden by the drop", name)
		}
	}
}
