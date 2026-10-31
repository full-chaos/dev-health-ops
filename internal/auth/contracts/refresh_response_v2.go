package contracts

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// RefreshResponseV2Surface names the refresh-response.v2 wire contract: the
// refresh answer whose access token is the EdDSA access-token.v1 JWS.
const RefreshResponseV2Surface = "refresh-response.v2"

// refreshResponseVersion is the schema_version member of raw, or "" when raw
// is not a JSON object carrying a string there. The version only picks the
// parser; the chosen parser validates the whole document and refuses a
// duplicate member before any value is trusted.
func refreshResponseVersion(raw []byte) string {
	var head struct {
		SchemaVersion string `json:"schema_version"`
	}
	if json.Unmarshal(raw, &head) != nil {
		return ""
	}
	return head.SchemaVersion
}

// parseRefreshResponseV2 validates and parses a refresh-response.v2 document.
// Beyond the schema it holds the rules a schema cannot say, for each of the two
// tokens: the header and claims are the access-token.v1 rules
// (ParseAccessToken, the one copy), the audience is the member's own
// (access_token for go-api and query-api, session_access_token for
// auth-service's session routes), its sid is the response's session_id, and
// expires_in is its exp - iat. The signatures are not checked; a reader that
// acts on a token verifies it against jwks.v1 first.
func parseRefreshResponseV2(root string, raw []byte) (*RefreshResponse, error) {
	var r RefreshResponse
	if err := decodeWire(root, RefreshResponseV2Surface, raw, &r); err != nil {
		return nil, refuseCredential(RefreshResponseV2Surface, err)
	}
	for _, member := range []refreshV2Token{
		{token: r.AccessToken, audience: PlatformAPIAudience, rule: "access_token", ofToken: "token"},
		{token: r.SessionAccessToken, audience: SessionRouteAudience, rule: "session_access_token", ofToken: "session_access_token"},
	} {
		if err := checkRefreshResponseV2Token(root, &r, member); err != nil {
			return nil, refuseCredential(RefreshResponseV2Surface, err)
		}
	}
	return &r, nil
}

// refreshV2Token is one token member of a v2 answer: the audience it must
// carry and the names its refusals use (rule for the member's own rules,
// ofToken in the session_id and expires_in rules).
type refreshV2Token struct {
	token    SecretString
	audience string
	rule     string
	ofToken  string
}

func checkRefreshResponseV2Token(root string, r *RefreshResponse, member refreshV2Token) error {
	segments := strings.Split(member.token.Reveal(), ".")
	if len(segments) != 3 {
		return refreshV2Rule(member.rule + "_not_jws")
	}
	header, headerErr := strictSegment(segments[0])
	claims, claimsErr := strictSegment(segments[1])
	_, signatureErr := strictSegment(segments[2])
	if headerErr != nil || claimsErr != nil || signatureErr != nil {
		return refreshV2Rule(member.rule + "_segment_encoding")
	}
	if !isJSONObject(header) || !isJSONObject(claims) {
		return refreshV2Rule(member.rule + "_segment_not_object")
	}
	document := make([]byte, 0, len(header)+len(claims)+24)
	document = append(document, `{"header":`...)
	document = append(document, header...)
	document = append(document, `,"claims":`...)
	document = append(document, claims...)
	document = append(document, '}')
	token, err := ParseAccessToken(root, document)
	if err != nil {
		return err
	}
	if token.Claims.Aud != member.audience {
		return refreshV2Rule(member.rule + "_audience")
	}
	if token.Claims.Sid != r.SessionID {
		return refreshV2Rule("session_id_not_" + member.ofToken + "_sid")
	}
	if int64(r.ExpiresIn) != token.Claims.Exp-token.Claims.Iat {
		return refreshV2Rule("expires_in_not_" + member.ofToken + "_lifetime")
	}
	return nil
}

// strictSegment decodes one JWS segment as RFC 7515 base64url: the URL
// alphabet only, no padding, no line breaks, and no non-zero trailing bits
// (Strict), so one byte string has one spelling.
func strictSegment(segment string) ([]byte, error) {
	return base64.RawURLEncoding.Strict().DecodeString(segment)
}

// isJSONObject reports whether b is exactly one JSON object. Embedding a
// segment that is anything else could add or hide a member of the assembled
// document.
func isJSONObject(b []byte) bool {
	trimmed := strings.TrimLeft(string(b), " \t\r\n")
	return strings.HasPrefix(trimmed, "{") && json.Valid(b)
}

func refreshV2Rule(rule string) error {
	return &CredentialRefusal{Surface: RefreshResponseV2Surface, Stage: StageClientRule, Rule: rule}
}
