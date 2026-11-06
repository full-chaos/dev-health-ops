package authrows

import (
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The corpus users of the six sign-in steps (sessionscenario and the authflowvenue seed). The cells derive each
// principal from the user id with PrincipalID, as the runner derives it from the seeded row: never a literal prn_.
const (
	ivanID = "20000000-0000-4000-8000-000000000001"
	piaID  = "20000000-0000-4000-8000-000000000005"
)

func principalOf(t *testing.T, userID string) string {
	t.Helper()
	principal, err := PrincipalID(userID)
	if err != nil || principal == "" {
		t.Fatalf("PrincipalID(%s) = %q, %v", userID, principal, err)
	}
	return principal
}

// eddsaAnswer is a sign-in answer whose access token has the given header and claims.
func eddsaAnswer(header, claims string) string {
	return `{"access_token":"` + mintSessionToken(header, claims) + `","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`
}

// The eddsa class auth-service mints, as the local corpus legs recorded it (CHAOS-13107): no email, role or
// is_superuser claim, sub the user's principal.
func eddsaClaims(principal string, extra string) string {
	return `{"aud":"dev-health-api","exp":1,"iat":1,"iss":"` + venueIssuer + `","jti":"j","nbf":1,"org_id":"a0000000-0000-4000-8000-00000000000a","principal_type":"human","scope":"api:access","sub":"` + principal + `"` + extra + `}`
}

const eddsaHeader = `{"alg":"EdDSA","kid":"auth-signing-local","typ":"at+jwt"}`

// CHAOS-13107: the access token of a sign-in step is checked by value on the target's raw answer: present, a compact
// JWS, alg EdDSA, iss the venue issuer, sub the principal of the step's own user, and the eddsa class shape (no
// email, role or is_superuser claim).
func TestAccessTokenCellChecksTheEdDSAClassOfTheStepsUser(t *testing.T) {
	ivan := principalOf(t, ivanID)
	good := eddsaAnswer(eddsaHeader, eddsaClaims(ivan, ""))
	if err := AccessTokenErr(good, venueIssuer, ivan); err != nil {
		t.Fatalf("a well-formed eddsa access token was refused: %v", err)
	}
	for name, c := range map[string]struct{ body, want string }{
		"another user's principal (pia)": {eddsaAnswer(eddsaHeader, eddsaClaims(principalOf(t, piaID), "")), "sub"},
		"the user id, not its principal": {eddsaAnswer(eddsaHeader, eddsaClaims(ivanID, "")), "sub"},
		"no sub":                         {eddsaAnswer(eddsaHeader, strings.Replace(eddsaClaims(ivan, ""), `,"sub":"`+ivan+`"`, "", 1)), "sub"},
		"alg HS256":                      {eddsaAnswer(`{"alg":"HS256","typ":"JWT"}`, eddsaClaims(ivan, "")), `alg "HS256"`},
		"alg none":                       {eddsaAnswer(`{"alg":"none"}`, eddsaClaims(ivan, "")), `alg "none"`},
		"an email claim":                 {eddsaAnswer(eddsaHeader, eddsaClaims(ivan, `,"email":"ivan@example.com"`)), "email"},
		"a role claim":                   {eddsaAnswer(eddsaHeader, eddsaClaims(ivan, `,"role":"admin"`)), "role"},
		"an is_superuser claim":          {eddsaAnswer(eddsaHeader, eddsaClaims(ivan, `,"is_superuser":false`)), "is_superuser"},
		"another issuer":                 {eddsaAnswer(eddsaHeader, strings.Replace(eddsaClaims(ivan, ""), venueIssuer, "dev-health-ops", 1)), `iss "dev-health-ops"`},
		"no access_token":                {`{"refresh_token":"dhr1_x","token_type":"bearer"}`, "no access_token"},
		"an empty one":                   {`{"access_token":""}`, "empty"},
		"a null one":                     {`{"access_token":null}`, "not a string"},
		"not a compact JWS":              {`{"access_token":"opaque"}`, "not a compact JWS"},
		"not a JSON object":              {`[]`, "no access_token"},
	} {
		t.Run(name, func(t *testing.T) {
			err := AccessTokenErr(c.body, venueIssuer, ivan)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a refusal naming %q, got %v", c.want, err)
			}
			if payload := strings.Split(strings.Split(c.body, `"access_token":"`)[len(strings.Split(c.body, `"access_token":"`))-1], "."); len(payload) == 3 && strings.Contains(err.Error(), payload[1]) {
				t.Fatal("the refusal prints the token")
			}
		})
	}
	for name, c := range map[string]struct{ issuer, principal string }{
		"no venue issuer": {"", ivan},
		"no principal":    {venueIssuer, ""},
	} {
		if err := AccessTokenErr(good, c.issuer, c.principal); err == nil {
			t.Errorf("%s: the check ran", name)
		}
	}
}

// The principal is session.WirePrincipalID of the user id, the form auth-service writes into sub.
func TestPrincipalIDIsTheSessionWireForm(t *testing.T) {
	for _, id := range []string{aliceID, ivanID, piaID, "20000000-0000-4000-8000-000000000002", "20000000-0000-4000-8000-000000000003"} {
		if got, want := principalOf(t, id), session.WirePrincipalID(uuid.MustParse(id)); got != want {
			t.Errorf("PrincipalID(%s) = %q, want %q", id, got, want)
		}
	}
	for _, bad := range []string{"", "alice", "not-a-uuid", "<id>"} {
		if got, err := PrincipalID(bad); err == nil {
			t.Errorf("PrincipalID(%q) = %q, want an error", bad, got)
		}
	}
}

// Only the six sign-in steps the local legs showed (CHAOS-13107) name a user; every other step compares its access
// token by value.
func TestAccessTokenUserNamesExactlyTheSixSignInSteps(t *testing.T) {
	want := map[string]string{
		aliceLogin1: "alice", aliceLogin2: "alice", "accept: ivan joins org A": "ivan",
		"onboard: olive creates an org": "olive", "onboard: oscar joins org D": "oscar", "login: pia, new password": "pia",
	}
	for _, s := range sixSteps {
		user, ok := AccessTokenUser(s.step)
		if !ok || user != want[s.step] {
			t.Errorf("%s: user %q (%v), want %q", s.step, user, ok, want[s.step])
		}
	}
	for _, step := range []string{"", "login: alice", "refresh: alice", "login: pia, new password ", "onboard: olive"} {
		if user, ok := AccessTokenUser(step); ok {
			t.Errorf("%q names user %q", step, user)
		}
	}
}

// eddsaProjected is the projected eddsa token of a local leg (CHAOS-13107), as a JSON string value.
const eddsaProjected = `"<jwt|aud=string:dev-health-api|exp=number:|exp-iat=number:3600|hdr.alg=string:EdDSA|hdr.kid=string:auth-signing-local|hdr.typ=string:at%2Bjwt|iat=number:|iss=string:https%3A%2F%2Fauth.dev-health.internal%3A8443|jti=string:|nbf=number:|nbf-iat=number:0|org_id=string:a0000000-0000-4000-8000-00000000000a|principal_type=string:human|scope=string:api%3Aaccess|sub=string:prn_IAAAAAAAQACAAAAAAAAAAQ>"`

var projectedAccess = regexp.MustCompile(`"access_token":"<jwt\|[^"]*>"`)

// withEdDSAToken is a projected answer with its access token replaced by the eddsa projection.
func withEdDSAToken(t *testing.T, body string) string {
	t.Helper()
	if len(projectedAccess.FindAllString(body, -1)) != 1 {
		t.Fatalf("want one projected access token in %s", body)
	}
	return projectedAccess.ReplaceAllLiteralString(body, `"access_token":`+eddsaProjected)
}

// The two projections are compared as members, not by value: the place holder replaces the value of the top-level
// access_token on both planes, and every other member, the place and presence of access_token included, still compares.
func TestAccessTokenUnvaluedComparesTheMemberAndNotItsValue(t *testing.T) {
	golden := GoldenResponseBody("accept: ivan joins org A", goldenBody(t, "accept: ivan joins org A"))
	target := withEdDSAToken(t, golden)
	compare := func(py, gv string) bool {
		pyU, pyOK := AccessTokenUnvalued(py)
		goU, goOK := AccessTokenUnvalued(gv)
		if !pyOK || !goOK {
			return false
		}
		headers := map[string]string{"content-type": "application/json"}
		same, _, _, _ := venueoracle.Compare(venueoracle.Request{Method: http.MethodPost, Path: "/api/v1/auth/accept-invite"},
			venueoracle.Response{Status: http.StatusOK, Headers: headers, Body: pyU},
			venueoracle.Response{Status: http.StatusOK, Headers: headers, Body: goU}, venueoracle.DiffOptions{})
		return same
	}
	if !compare(golden, target) {
		t.Fatal("the recording's HS256 projection and the eddsa projection do not compare equal once unvalued")
	}
	if got, _ := AccessTokenUnvalued(golden); got == golden || strings.Contains(got, "<jwt|") {
		t.Fatalf("the access token's value stayed: %s", got)
	}
	for name, gv := range map[string]string{
		"a member missing":     strings.Replace(target, `"token_type":"bearer",`, "", 1),
		"an extra member":      strings.TrimSuffix(target, "}") + `,"extra":"x"}`,
		"another role":         strings.Replace(target, `"role":"admin"`, `"role":"member"`, 1),
		"a reordered body":     strings.Replace(target, `"token_type":"bearer","expires_in":3600`, `"expires_in":3600,"token_type":"bearer"`, 1),
		"no access token":      strings.Replace(target, `"access_token":`+eddsaProjected+`,`, "", 1),
		"an unprojected token": strings.Replace(target, eddsaProjected, `"opaque"`, 1),
	} {
		if gv == target {
			t.Fatalf("%s: nothing planted", name)
		}
		if compare(golden, gv) {
			t.Errorf("%s was hidden by the place holder", name)
		}
	}
	// Only the top-level member, and only a projected token: a nested access_token, or a body that is not one JSON
	// object, is not unvalued.
	for name, body := range map[string]string{
		"nested":            `{"session":{"access_token":` + eddsaProjected + `}}`,
		"not projected":     `{"access_token":"opaque"}`,
		"not a JSON object": `[` + eddsaProjected + `]`,
		"twice":             `{"access_token":` + eddsaProjected + `,"access_token":` + eddsaProjected + `}`,
		"trailing text":     `{"access_token":` + eddsaProjected + `} x`,
	} {
		if got, ok := AccessTokenUnvalued(body); ok || got != body {
			t.Errorf("%s: unvalued (%v): %s", name, ok, got)
		}
	}
}
