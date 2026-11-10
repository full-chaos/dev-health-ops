package authrows

import (
	"encoding/json"
	"io"
	"regexp"
	"strings"
)

// The answers of the corpus's sign-in steps are compared as projected bodies (tokens as their claims). Four
// differences between go-api's golden and auth-service are ruled, each classified exactly and nowhere else; a body
// that differs in any other way still differs.
//
//   - The refresh token is an opaque credential, "dhr1_" and 43 base64url characters (internal/auth/session/credential.go,
//     credentialLength), where go-api's is a JWT. Each side must have its own shape; the field is never ignored.
//   - The access token carries the sid claim (a session id, "ses_" and 22 base64url characters) that go-api's does not.
//   - Alice's two sign-ins answer her earliest organization where go-api answers the one with activity data (CHAOS-10228).
//   - Under the eddsa class the answer carries session_access_token, which go-api's golden never held (CHAOS-13107):
//     on a leg whose GoldenSpec.GoBlanked names it, the projected member is dropped by its key, after
//     SessionAccessTokenErr checked its raw value (sessiontoken.go). Every other member is compared, in its order.

const opaqueRefresh = `"refresh_token":"<opaque refresh credential>"`

var (
	goldenRefreshJWT    = regexp.MustCompile(`"refresh_token":"<jwt\|(?:[^"<>]|<id>)*\|type=string:refresh>"`)
	targetRefreshOpaque = regexp.MustCompile(`"refresh_token":"dhr1_[A-Za-z0-9_-]{43}"`)
	accessTokenSID      = regexp.MustCompile(`("access_token":"<jwt\|(?:[^"<>]|<id>)*)\|sid=string:ses_[A-Za-z0-9_-]{22}((?:\|(?:[^"<>|]|<id>)*)*>")`)
	// projectedJWT is a projected token as a JSON string value.
	projectedJWT = regexp.MustCompile(`^"<jwt\|(?:[^"<>]|<id>)*>"$`)
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
	body = GoldenRefreshBody(body)
	if !aliceSteps[step] || !strings.Contains(body, aliceUserStart) || strings.Count(body, aliceSubClaim) != 1 ||
		strings.Count(body, goldenClaims) != 1 || strings.Count(body, goldenUserOrg) != 1 {
		return body
	}
	body = strings.Replace(body, goldenClaims, targetClaims, 1)
	return strings.Replace(body, goldenUserOrg, targetUserOrg, 1)
}

// GoldenRefreshBody is a golden sign-in answer's body with its refresh JWT as an opaque credential's place holder,
// and nothing else changed.
func GoldenRefreshBody(body string) string {
	return goldenRefreshJWT.ReplaceAllString(body, opaqueRefresh)
}

// TargetResponseBody is the ACP target's (auth-service) answer's body, projected for comparison with the golden's:
// a well-formed opaque refresh credential as the same place holder, and the access token without its sid claim.
// A refresh token of any other shape, and any other claim, stay as they are. goBlanked is the leg's
// GoldenSpec.GoBlanked: a top-level member whose key it names ("body $.<key>") and whose value is a projected token is
// dropped (the golden never held it; SessionAccessTokenErr checks its raw value). A nil goBlanked drops nothing.
func TargetResponseBody(body string, goBlanked map[string]string) string {
	body = withoutGoBlankedMembers(body, goBlanked)
	body = targetRefreshOpaque.ReplaceAllString(body, opaqueRefresh)
	return accessTokenSID.ReplaceAllString(body, "${1}${2}")
}

// goBlankedKeys maps each "body $.<key>" pattern of goBlanked to its top-level key. A deeper path, a header or any
// other pattern names no key. AccessTokenPattern names none either: the recording holds access_token, so it is
// compared unvalued on both planes (AccessTokenUnvalued), never dropped from one (CHAOS-13107, leg acp).
func goBlankedKeys(goBlanked map[string]string) map[string]bool {
	keys := map[string]bool{}
	for pattern := range goBlanked {
		if pattern == AccessTokenPattern {
			continue
		}
		if key, ok := strings.CutPrefix(pattern, "body $."); ok && key != "" && !strings.ContainsAny(key, ".[") {
			keys[key] = true
		}
	}
	return keys
}

// withoutGoBlankedMembers is body, a JSON object, without its top-level members whose key goBlanked names and whose
// value is a projected token. The other members keep their text and their order. A body that is not one JSON object,
// or that has no such member, is returned as it is.
func withoutGoBlankedMembers(body string, goBlanked map[string]string) string {
	keys := goBlankedKeys(goBlanked)
	if len(keys) == 0 {
		return body
	}
	decoder := json.NewDecoder(strings.NewReader(body))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return body
	}
	open := decoder.InputOffset()
	var kept []string
	dropped := false
	for decoder.More() {
		start := decoder.InputOffset()
		token, err := decoder.Token()
		if err != nil {
			return body
		}
		key, _ := token.(string)
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return body
		}
		if keys[key] && projectedJWT.Match(value) {
			dropped = true
			continue
		}
		kept = append(kept, strings.TrimLeft(body[start:decoder.InputOffset()], ", \t\r\n"))
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || !dropped {
		return body
	}
	end := decoder.InputOffset()
	if _, err := decoder.Token(); err != io.EOF {
		return body
	}
	return body[:open] + strings.Join(kept, ",") + body[end-1:]
}
