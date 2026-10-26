package authrows

import (
	"regexp"
	"strings"
)

// The answers of the corpus's sign-in steps are compared as projected bodies (tokens as their claims). Three
// differences between go-api's golden and auth-service are ruled, each classified exactly and nowhere else; a body
// that differs in any other way still differs.
//
//   - The refresh token is an opaque credential, "dhr1_" and 43 base64url characters (internal/auth/session/credential.go,
//     credentialLength), where go-api's is a JWT. Each side must have its own shape; the field is never ignored.
//   - The access token carries the sid claim (a session id, "ses_" and 22 base64url characters) that go-api's does not.
//   - Alice's two sign-ins answer her earliest organization where go-api answers the one with activity data (CHAOS-10228).

const opaqueRefresh = `"refresh_token":"<opaque refresh credential>"`

var (
	goldenRefreshJWT    = regexp.MustCompile(`"refresh_token":"<jwt\|(?:[^"<>]|<id>)*\|type=string:refresh>"`)
	targetRefreshOpaque = regexp.MustCompile(`"refresh_token":"dhr1_[A-Za-z0-9_-]{43}"`)
	accessTokenSID      = regexp.MustCompile(`("access_token":"<jwt\|(?:[^"<>]|<id>)*)\|sid=string:ses_[A-Za-z0-9_-]{22}((?:\|(?:[^"<>|]|<id>)*)*>")`)
)

// aliceSteps are alice's two successful sign-ins of the corpus, by exact step name.
var aliceSteps = map[string]bool{
	"login: alice (a refresh token the reset revokes)": true,
	"login: alice, new password":                       true,
}

const (
	aliceID        = "10000000-0000-4000-8000-000000000001"
	orgAID         = "a0000000-0000-4000-8000-00000000000a"
	orgBID         = "b0000000-0000-4000-8000-00000000000b"
	goldenClaims   = "|org_id=string:" + orgBID + "|role=string:member|"
	targetClaims   = "|org_id=string:" + orgAID + "|role=string:owner|"
	goldenUserOrg  = `"org_id":"` + orgBID + `","role":"member"`
	targetUserOrg  = `"org_id":"` + orgAID + `","role":"owner"`
	aliceUserStart = `"user":{"id":"` + aliceID + `"`
	aliceSubClaim  = "|sub=string:" + aliceID + "|"
)

// GoldenResponseBody is a golden (go-api) sign-in answer's body, projected for comparison with the ACP target's:
// its refresh JWT as an opaque credential's place holder, and, on alice's two sign-in steps, her organization A as
// owner (the organization the target answers).
func GoldenResponseBody(step, body string) string {
	body = goldenRefreshJWT.ReplaceAllString(body, opaqueRefresh)
	if !aliceSteps[step] || !strings.Contains(body, aliceUserStart) || strings.Count(body, aliceSubClaim) != 1 ||
		strings.Count(body, goldenClaims) != 1 || strings.Count(body, goldenUserOrg) != 1 {
		return body
	}
	body = strings.Replace(body, goldenClaims, targetClaims, 1)
	return strings.Replace(body, goldenUserOrg, targetUserOrg, 1)
}

// TargetResponseBody is the ACP target's (auth-service) answer's body, projected for comparison with the golden's:
// a well-formed opaque refresh credential as the same place holder, and the access token without its sid claim.
// A refresh token of any other shape, and any other claim, stay as they are.
func TargetResponseBody(body string) string {
	body = targetRefreshOpaque.ReplaceAllString(body, opaqueRefresh)
	return accessTokenSID.ReplaceAllString(body, "${1}${2}")
}
