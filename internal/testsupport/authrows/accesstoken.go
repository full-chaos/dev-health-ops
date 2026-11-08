package authrows

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
)

// CHAOS-13107: with sessions forwarded, auth-service mints the access token of every sign-in under its eddsa class, as
// production does: hdr.alg EdDSA, the issuer URL as iss and the principal (prn_) as sub, with none of the HS256
// recording's user claims. The projected claims therefore differ from the frozen golden's by design on the six sign-in
// steps (AccessTokenUser). On a leg whose GoldenSpec.GoBlanked names AccessTokenPattern, the runner compares the member
// by one place holder on both planes (AccessTokenUnvalued) and checks the target's raw value here (AccessTokenErr).

// AccessTokenPattern is the leaf whose projected value the Go plane's eddsa class makes differ from the recording.
const AccessTokenPattern = "body $.access_token"

// AccessTokenReason is why AccessTokenPattern is not compared by value on a sessions leg.
const AccessTokenReason = "EdDSA class: issuer URL iss + prn_ sub differ from the HS256 recording by design (13107)"

// accessTokenSteps are the sign-in steps whose access token differed only by the eddsa class on the local legs
// (CHAOS-13107), each to the corpus user (the seed's key) it signs in. No other step is unvalued.
var accessTokenSteps = map[string]string{
	"login: alice (a refresh token the reset revokes)": "alice",
	"login: alice, new password":                       "alice",
	"accept: ivan joins org A":                         "ivan",
	"onboard: olive creates an org":                    "olive",
	"onboard: oscar joins org D":                       "oscar",
	"login: pia, new password":                         "pia",
}

// AccessTokenUser is the corpus user (the seed's key) whose sign-in step answers an eddsa access token, for the six
// sign-in steps of the corpus only.
func AccessTokenUser(step string) (string, bool) {
	user, ok := accessTokenSteps[step]
	return user, ok
}

// PrincipalID is the session.v1 wire form of a user id, as session.WirePrincipalID writes it into sub: "prn_" and the
// unpadded base64url of the id's 16 bytes. A text that is not one UUID is an error.
func PrincipalID(userID string) (string, error) {
	id, err := uuid.Parse(userID)
	if err != nil || id.String() != strings.ToLower(userID) {
		return "", fmt.Errorf("authrows: %q is not a user id", userID)
	}
	return "prn_" + base64.RawURLEncoding.EncodeToString(id[:]), nil
}

// hs256OnlyClaims are claims of the HS256 recording the eddsa class never carries.
var hs256OnlyClaims = []string{"email", "role", "is_superuser"}

// AccessTokenErr checks a target answer's raw body (before any projection): it carries access_token, a non-empty
// compact JWS whose header alg is EdDSA, whose iss is issuer and whose sub is principal (the step's user, PrincipalID),
// and which has none of hs256OnlyClaims. The error never prints the token.
func AccessTokenErr(body, issuer, principal string) error {
	if issuer == "" {
		return errors.New("authrows: no venue issuer to check access_token against")
	}
	if principal == "" {
		return errors.New("authrows: no principal to check access_token's sub against")
	}
	var members map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &members) != nil {
		return errors.New("authrows: the answer is not a JSON object: no access_token")
	}
	raw, ok := members["access_token"]
	if !ok {
		return errors.New("authrows: the answer carries no access_token")
	}
	var token string
	if string(raw) == "null" || json.Unmarshal(raw, &token) != nil {
		return errors.New("authrows: access_token is not a string")
	}
	if token == "" {
		return errors.New("authrows: access_token is empty")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return errors.New("authrows: access_token is not a compact JWS")
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if !decodeSegment(parts[0], &header) {
		return errors.New("authrows: access_token's header does not decode")
	}
	if header.Alg != "EdDSA" {
		return fmt.Errorf("authrows: access_token has alg %q, want \"EdDSA\"", header.Alg)
	}
	var claims map[string]any
	if !decodeSegment(parts[1], &claims) {
		return errors.New("authrows: access_token's claims do not decode")
	}
	if iss, _ := claims["iss"].(string); iss != issuer {
		return fmt.Errorf("authrows: access_token has iss %q, want the venue issuer %q", fmt.Sprint(claims["iss"]), issuer)
	}
	if sub, _ := claims["sub"].(string); sub != principal {
		return fmt.Errorf("authrows: access_token has sub %q, want the step user's principal %q", fmt.Sprint(claims["sub"]), principal)
	}
	for _, claim := range hs256OnlyClaims {
		if _, has := claims[claim]; has {
			return fmt.Errorf("authrows: access_token carries the %s claim: the eddsa class has none", claim)
		}
	}
	return nil
}

// unvaluedAccessToken replaces the projected access token on both planes: the member is compared, not its value.
const unvaluedAccessToken = `"<access token: value checked by authrows.AccessTokenErr>"`

// AccessTokenUnvalued is body, one JSON object, with the value of its top-level access_token, a projected token,
// replaced by one place holder; every other byte stays. ok is false, and body is returned as it is, when body is not
// one JSON object or has no such member, or has it more than once.
func AccessTokenUnvalued(body string) (string, bool) {
	decoder := json.NewDecoder(strings.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return body, false
	}
	start, end := -1, -1
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return body, false
		}
		key, _ := token.(string)
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return body, false
		}
		if key != "access_token" {
			continue
		}
		if start >= 0 || !projectedJWT.Match(value) {
			return body, false
		}
		end = int(decoder.InputOffset())
		start = end - len(value)
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || start < 0 {
		return body, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return body, false
	}
	return body[:start] + unvaluedAccessToken + body[end:], true
}

// SignInBodies is the pair of bodies the runner compares for one step on a sessions leg: the golden's projected
// answer and the target's, each ruled difference classified (GoldenResponseBody, TargetResponseBody). On a sign-in
// step whose access token the leg compares unvalued (goBlanked names AccessTokenPattern), both bodies carry the one
// place holder in its place and unvalued is true: the runner then reports Golden.Unvalued(AccessTokenPattern).
func SignInBodies(step, golden, target string, goBlanked map[string]string) (pyBody, goBody string, unvalued bool) {
	return SignInBodiesLogin(step, golden, target, goBlanked, true)
}

// SignInBodiesLogin is SignInBodies for a leg that says who answers the sign-ins: with loginForwarded false
// (local_login not forwarded) go-api itself chooses alice's organization on both planes, so her organization is not
// rewritten (GoldenRefreshBody); the other ruled differences are classified as SignInBodies classifies them.
func SignInBodiesLogin(step, golden, target string, goBlanked map[string]string, loginForwarded bool) (pyBody, goBody string, unvalued bool) {
	pyBody, goBody = GoldenRefreshBody(golden), TargetResponseBody(target, goBlanked)
	if loginForwarded {
		pyBody = GoldenResponseBody(step, golden)
	}
	if _, ok := AccessTokenUser(step); !ok || goBlanked[AccessTokenPattern] == "" {
		return pyBody, goBody, false
	}
	pyUnvalued, pyOK := AccessTokenUnvalued(pyBody)
	goUnvalued, goOK := AccessTokenUnvalued(goBody)
	if !pyOK || !goOK {
		return pyBody, goBody, false
	}
	return pyUnvalued, goUnvalued, true
}
