package shadowvenue

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// CallerSuperuserOrg labels the cell of a platform superuser whose token carries an org claim and who has no membership
// row there. The shadow logs it as kind "superuser" (callerKind).
const CallerSuperuserOrg = "superuser_org"

// MinTokenLife is the least time a token must have left when its slot is sent.
const MinTokenLife = 120 * time.Second

var cellCallers = append(slices.Clone(callerOrder), CallerSuperuserOrg)

// callerKind is the shadow caller kind of a cell label.
func callerKind(caller string) string {
	if caller == CallerSuperuserOrg {
		return "superuser"
	}
	return caller
}

// WirePrincipal is the wire form of a user id in a token's sub: "prn_" and the base64url of its 16 bytes.
func WirePrincipal(id string) string {
	raw := uuidBytes(id)
	if raw == nil {
		return ""
	}
	return "prn_" + base64.RawURLEncoding.EncodeToString(raw)
}

// PrincipalUUID reads a wire principal id back to its UUID.
func PrincipalUUID(sub string) (string, bool) {
	rest, ok := strings.CutPrefix(sub, "prn_")
	if !ok {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil || len(raw) != 16 {
		return "", false
	}
	h := hex.EncodeToString(raw)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], true
}

func uuidBytes(id string) []byte {
	if !isUUID(id) {
		return nil
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(raw) != 16 {
		return nil
	}
	return raw
}

// TokenClaims are the three claims of a user access token the run reads back: sub, org_id ("-" when absent) and exp.
type TokenClaims struct {
	Sub, Org string
	Exp      time.Time
}

// DecodeClaims reads the claims of a user access token (access-token.v1: header alg EdDSA, typ at+jwt) without
// verifying it: a local read-back, nothing is sent. The token never reaches an error.
func DecodeClaims(token string) (TokenClaims, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return TokenClaims{}, errors.New("not a three-part token")
	}
	headerRaw, err1 := base64.RawURLEncoding.DecodeString(parts[0])
	payloadRaw, err2 := base64.RawURLEncoding.DecodeString(parts[1])
	if err1 != nil || err2 != nil {
		return TokenClaims{}, errors.New("a segment is not base64url")
	}
	var header struct{ Alg, Typ string }
	if json.Unmarshal(headerRaw, &header) != nil || header.Alg != "EdDSA" || header.Typ != "at+jwt" {
		return TokenClaims{}, errors.New("it is not a user access token (alg EdDSA, typ at+jwt)")
	}
	var payload struct {
		Sub string  `json:"sub"`
		Org *string `json:"org_id"`
		Exp *int64  `json:"exp"`
	}
	if json.Unmarshal(payloadRaw, &payload) != nil || payload.Sub == "" || payload.Exp == nil {
		return TokenClaims{}, errors.New("the claims lack a sub or an exp")
	}
	claims := TokenClaims{Sub: payload.Sub, Org: none, Exp: time.Unix(*payload.Exp, 0).UTC()}
	if payload.Org != nil {
		if !isUUID(*payload.Org) {
			return TokenClaims{}, errors.New("the org_id claim is not a UUID")
		}
		claims.Org = strings.ToLower(*payload.Org)
	}
	return claims, nil
}

// ReadTokenFile reads one user access token from an owner-only regular file. The value never reaches an error.
func ReadTokenFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("token file %s: %w", path, errors.Unwrap(err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 16<<10 {
		return "", fmt.Errorf("token file %s: want an owner-only (0600) regular file of at most 16 KiB", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("token file %s: unreadable", path)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("token file %s: want one non-empty line", path)
	}
	if _, err := DecodeClaims(value); err != nil {
		return "", fmt.Errorf("token file %s: %w", path, err)
	}
	return value, nil
}
