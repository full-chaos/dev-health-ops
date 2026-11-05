package authrows

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// CHAOS-13107: with sessions forwarded, auth-service mints the eddsa class, and every answer that carries an access
// token carries session_access_token too: the token the session routes accept. go-api's frozen golden never held
// that member, and its value is a fresh token per run, so the golden does not hold it by value: the test declares the
// one path on its GoldenSpec (SessionAccessTokenPattern, with SessionAccessTokenReason). Blanked is not unchecked:
// SessionAccessTokenErr checks the target's raw answer, and TargetResponseBody drops the projected member by the key
// the leg's GoldenSpec.GoBlanked names from the body it compares.

// EnvSessionIssuer is the environment variable ci/auth_corpus_external.sh sets to the issuer auth-service signs under
// (its AUTH_TOKEN_ISSUER_URL as compose renders it for the leg).
const EnvSessionIssuer = "DEVHEALTH_AUTH_ORACLE_SESSION_ISSUER"

// SessionAccessTokenPattern is the leaf the Go plane blanks and the recording never did.
const SessionAccessTokenPattern = "body $.session_access_token"

// SessionAccessTokenReason is why SessionAccessTokenPattern is not compared by value.
const SessionAccessTokenReason = "with sessions forwarded, the token is a fresh EdDSA at+jwt per run, so its value is not stable."

// SessionIssuerFromEnv reads EnvSessionIssuer; unset or empty is an error.
func SessionIssuerFromEnv() (string, error) {
	issuer := os.Getenv(EnvSessionIssuer)
	if issuer == "" {
		return "", fmt.Errorf("authrows: %s is not set; the run script exports auth-service's issuer", EnvSessionIssuer)
	}
	return issuer, nil
}

// SessionAccessTokenErr checks a target answer's raw body (before any projection). A JSON object that carries
// access_token or session_access_token must carry a session_access_token that is a non-empty compact JWS whose header
// alg is EdDSA and whose iss is issuer. Any other body needs none. The error never prints the token.
func SessionAccessTokenErr(body, issuer string) error {
	if issuer == "" {
		return errors.New("authrows: no venue issuer to check session_access_token against")
	}
	var members map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &members) != nil {
		return nil // not a JSON object: no token answer
	}
	_, hasAccess := members["access_token"]
	raw, hasSession := members["session_access_token"]
	switch {
	case !hasAccess && !hasSession:
		return nil
	case !hasSession:
		return errors.New("authrows: the answer carries access_token and no session_access_token")
	}
	var token string
	if string(raw) == "null" || json.Unmarshal(raw, &token) != nil {
		return errors.New("authrows: session_access_token is not a string")
	}
	if token == "" {
		return errors.New("authrows: session_access_token is empty")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return errors.New("authrows: session_access_token is not a compact JWS")
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if !decodeSegment(parts[0], &header) {
		return errors.New("authrows: session_access_token's header does not decode")
	}
	if header.Alg != "EdDSA" {
		return fmt.Errorf("authrows: session_access_token has alg %q, want \"EdDSA\"", header.Alg)
	}
	var claims map[string]any
	if !decodeSegment(parts[1], &claims) {
		return errors.New("authrows: session_access_token's claims do not decode")
	}
	iss, isString := claims["iss"].(string)
	if !isString && claims["iss"] != nil {
		return fmt.Errorf("authrows: session_access_token's iss is not a string, want the venue issuer %q", issuer)
	}
	if iss != issuer {
		return fmt.Errorf("authrows: session_access_token has iss %q, want the venue issuer %q", iss, issuer)
	}
	return nil
}

func decodeSegment(segment string, into any) bool {
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	return err == nil && json.Unmarshal(raw, into) == nil
}
