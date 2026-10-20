package oidc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
)

const (
	maxIDTokenBytes  = 16 << 10
	maxHeaderBytes   = 2 << 10
	maxSubjectBytes  = 1024
	maxJSONDepth     = 16
	clockSkew        = 60 * time.Second // Python OIDC_JWT_LEEWAY_SECONDS
	maxIDTokenMaxAge = stateTTL
)

// allowedAlgs is the closed set of ID token signature algorithms: asymmetric
// only. "none" and every HS* algorithm are refused before any key is used.
var allowedAlgs = []string{
	gooidc.RS256, gooidc.RS384, gooidc.RS512,
	gooidc.PS256, gooidc.PS384, gooidc.PS512,
	gooidc.ES256, gooidc.ES384, gooidc.ES512,
}

type joseHeader struct {
	Alg string
	Kid string
}

var (
	errMalformedToken = errors.New("oidc: malformed ID token")
	errAlgNotAllowed  = errors.New("oidc: ID token algorithm not allowed")
)

// parseJOSEHeader checks the compact form and the protected header before any
// signature work: three non-empty parts, a strict base64url JSON object header
// with no duplicate members, an allowed alg, and no member that would let the
// token name its own key (jwk, jku, x5u, x5c) or demand extensions (crit).
func parseJOSEHeader(raw string) (joseHeader, error) {
	if raw == "" || len(raw) > maxIDTokenBytes {
		return joseHeader{}, errMalformedToken
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return joseHeader{}, errMalformedToken
	}
	if base64.RawURLEncoding.DecodedLen(len(parts[0])) > maxHeaderBytes {
		return joseHeader{}, errMalformedToken
	}
	headerJSON, err := base64.RawURLEncoding.Strict().DecodeString(parts[0])
	if err != nil {
		return joseHeader{}, errMalformedToken
	}
	if err := checkJSONObject(headerJSON); err != nil {
		return joseHeader{}, errMalformedToken
	}
	var header map[string]any
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return joseHeader{}, errMalformedToken
	}
	for _, refused := range []string{"crit", "jwk", "jku", "x5u", "x5c"} {
		if _, present := header[refused]; present {
			return joseHeader{}, errMalformedToken
		}
	}
	alg, ok := header["alg"].(string)
	if !ok {
		return joseHeader{}, errMalformedToken
	}
	if !algAllowed(alg) {
		return joseHeader{}, errAlgNotAllowed
	}
	out := joseHeader{Alg: alg}
	if kid, present := header["kid"]; present {
		text, ok := kid.(string)
		if !ok {
			return joseHeader{}, errMalformedToken
		}
		out.Kid = text
	}
	return out, nil
}

func algAllowed(alg string) bool {
	for _, allowed := range allowedAlgs {
		if alg == allowed {
			return true
		}
	}
	return false
}

// checkJSONObject refuses anything but one JSON object, with no duplicate
// member at any depth and bounded nesting.
func checkJSONObject(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return errors.New("not a JSON object")
	}
	if err := walkObject(dec, 1); err != nil {
		return err
	}
	if _, err := dec.Token(); err == nil {
		return errors.New("trailing data after the JSON object")
	}
	return nil
}

func walkObject(dec *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON nested too deep")
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := tok.(string)
		if !ok {
			return errors.New("object key is not a string")
		}
		if seen[key] {
			return fmt.Errorf("duplicate member %q", key)
		}
		seen[key] = true
		if err := walkValue(dec, depth); err != nil {
			return err
		}
	}
	_, err := dec.Token()
	return err
}

func walkValue(dec *json.Decoder, depth int) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		return walkObject(dec, depth+1)
	case '[':
		if depth+1 > maxJSONDepth {
			return errors.New("JSON nested too deep")
		}
		for dec.More() {
			if err := walkValue(dec, depth+1); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	}
	return errors.New("unexpected delimiter")
}

// claimRules are what an ID token must satisfy for one flow.
type claimRules struct {
	Issuer   string
	ClientID string
	Nonce    string
	Now      time.Time
}

// idClaims is a validated ID token payload.
type idClaims struct {
	Issuer  string
	Subject string
	All     map[string]any
}

type claimError struct {
	reason Reason
	msg    string
}

func (e *claimError) Error() string { return "oidc: ID token " + e.msg }

func claimFail(reason Reason, msg string) error { return &claimError{reason: reason, msg: msg} }

// reasonOf maps a validation error to its outcome code.
func reasonOf(err error) Reason {
	var ce *claimError
	if errors.As(err, &ce) {
		return ce.reason
	}
	if errors.Is(err, errMalformedToken) {
		return ReasonMalformed
	}
	return ReasonBadSignature
}

// checkClaims validates a payload whose signature already verified. Every
// check is explicit here; none relies on a library default.
func checkClaims(payload []byte, rules claimRules) (idClaims, error) {
	if err := checkJSONObject(payload); err != nil {
		return idClaims{}, claimFail(ReasonMalformed, "payload is not a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var all map[string]any
	if err := dec.Decode(&all); err != nil {
		return idClaims{}, claimFail(ReasonMalformed, "payload does not decode")
	}

	iss, ok := all["iss"].(string)
	if !ok || rules.Issuer == "" || iss != rules.Issuer {
		return idClaims{}, claimFail(ReasonIssuer, "issuer mismatch")
	}
	audiences, ok := stringOrStrings(all["aud"])
	if !ok || len(audiences) == 0 || rules.ClientID == "" || !contains(audiences, rules.ClientID) {
		return idClaims{}, claimFail(ReasonAudience, "audience mismatch")
	}
	azp, azpPresent := all["azp"]
	if azpPresent {
		text, ok := azp.(string)
		if !ok || text != rules.ClientID {
			return idClaims{}, claimFail(ReasonAudience, "authorized party mismatch")
		}
	} else if len(audiences) > 1 {
		return idClaims{}, claimFail(ReasonAudience, "several audiences and no authorized party")
	}

	exp, ok := numericDate(all["exp"])
	if !ok {
		return idClaims{}, claimFail(ReasonMalformed, "exp missing or not a number")
	}
	iat, ok := numericDate(all["iat"])
	if !ok {
		return idClaims{}, claimFail(ReasonMalformed, "iat missing or not a number")
	}
	now := rules.Now
	if !now.Before(exp.Add(clockSkew)) {
		return idClaims{}, claimFail(ReasonExpired, "expired")
	}
	if iat.After(now.Add(clockSkew)) {
		return idClaims{}, claimFail(ReasonNotYetValid, "issued in the future")
	}
	if now.Sub(iat) > maxIDTokenMaxAge+clockSkew {
		return idClaims{}, claimFail(ReasonExpired, "issued too long ago")
	}
	if raw, present := all["nbf"]; present {
		nbf, ok := numericDate(raw)
		if !ok {
			return idClaims{}, claimFail(ReasonMalformed, "nbf not a number")
		}
		if nbf.After(now.Add(clockSkew)) {
			return idClaims{}, claimFail(ReasonNotYetValid, "not yet valid")
		}
	}

	nonce, ok := all["nonce"].(string)
	if !ok || rules.Nonce == "" || !constantTimeEqual(nonce, rules.Nonce) {
		return idClaims{}, claimFail(ReasonNonceMismatch, "nonce mismatch")
	}
	sub, ok := all["sub"].(string)
	if !ok || !validSubject(sub) {
		return idClaims{}, claimFail(ReasonMissingSub, "subject missing or invalid")
	}
	return idClaims{Issuer: iss, Subject: sub, All: all}, nil
}

func stringOrStrings(value any) ([]string, bool) {
	switch v := value.(type) {
	case string:
		return []string{v}, true
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, text)
		}
		return out, true
	}
	return nil, false
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// numericDate reads a JSON NumericDate: a finite, non-negative number of
// seconds below year 10000.
func numericDate(value any) (time.Time, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return time.Time{}, false
	}
	f, err := number.Float64()
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f >= 253402300800 {
		return time.Time{}, false
	}
	sec, frac := math.Modf(f)
	return time.Unix(int64(sec), int64(frac*1e9)).UTC(), true
}

// validSubject is the external identity key rule for a subject: non-empty,
// bounded, valid UTF-8, no control character, no white space at either end.
// A subject is compared exactly; it is never normalized.
func validSubject(v string) bool {
	if v == "" || len(v) > maxSubjectBytes || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || r == utf8.RuneError {
			return false
		}
	}
	first, _ := utf8.DecodeRuneInString(v)
	last, _ := utf8.DecodeLastRuneInString(v)
	return !unicode.IsSpace(first) && !unicode.IsSpace(last)
}

// verifyIDToken checks the header, the signature against the issuer's JWKS,
// and then every claim.
func verifyIDToken(ctx context.Context, keys gooidc.KeySet, raw string, rules claimRules) (idClaims, error) {
	if _, err := parseJOSEHeader(raw); err != nil {
		return idClaims{}, err
	}
	verifier := gooidc.NewVerifier(rules.Issuer, keys, &gooidc.Config{
		SupportedSigningAlgs: allowedAlgs,
		// The claim checks below are this package's own (checkClaims); the
		// library is used for the signature only.
		SkipClientIDCheck: true,
		SkipIssuerCheck:   true,
		SkipExpiryCheck:   true,
	})
	token, err := verifier.Verify(ctx, raw)
	if err != nil {
		return idClaims{}, fmt.Errorf("oidc: ID token signature: %w", err)
	}
	var payload json.RawMessage
	if err := token.Claims(&payload); err != nil {
		return idClaims{}, claimFail(ReasonMalformed, "payload does not decode")
	}
	return checkClaims(payload, rules)
}
