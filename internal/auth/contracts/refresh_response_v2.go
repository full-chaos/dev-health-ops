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
// Beyond the schema it holds the rules a schema cannot say: the access token's
// header and claims are the access-token.v1 rules (ParseAccessToken, the one
// copy), its sid is the response's session_id, and expires_in is exp - iat.
// The signature is not checked; a reader that acts on the token verifies it
// against jwks.v1 first.
func parseRefreshResponseV2(root string, raw []byte) (*RefreshResponse, error) {
	var r RefreshResponse
	if err := decodeWire(root, RefreshResponseV2Surface, raw, &r); err != nil {
		return nil, refuseCredential(RefreshResponseV2Surface, err)
	}
	if err := checkRefreshResponseV2Token(root, &r); err != nil {
		return nil, refuseCredential(RefreshResponseV2Surface, err)
	}
	return &r, nil
}

func checkRefreshResponseV2Token(root string, r *RefreshResponse) error {
	segments := strings.Split(r.AccessToken.Reveal(), ".")
	if len(segments) != 3 {
		return refreshV2Rule("access_token_not_jws")
	}
	header, headerErr := strictSegment(segments[0])
	claims, claimsErr := strictSegment(segments[1])
	_, signatureErr := strictSegment(segments[2])
	if headerErr != nil || claimsErr != nil || signatureErr != nil {
		return refreshV2Rule("access_token_segment_encoding")
	}
	if !isJSONObject(header) || !isJSONObject(claims) {
		return refreshV2Rule("access_token_segment_not_object")
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
	if token.Claims.Sid != r.SessionID {
		return refreshV2Rule("session_id_not_token_sid")
	}
	if int64(r.ExpiresIn) != token.Claims.Exp-token.Claims.Iat {
		return refreshV2Rule("expires_in_not_token_lifetime")
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
