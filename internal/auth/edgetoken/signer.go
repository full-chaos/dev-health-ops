package edgetoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// RefreshType is the "type" claim of a refresh token.
const RefreshType = "refresh"

// AccessLifetime and RefreshLifetime are JWT_ACCESS_TOKEN_EXPIRE_MINUTES
// and JWT_REFRESH_TOKEN_EXPIRE_DAYS.
const (
	AccessLifetime  = 60 * time.Minute
	RefreshLifetime = 7 * 24 * time.Hour
)

// headerSegment is base64url of the header PyJWT writes for HS256:
// {"typ": "JWT", "alg": "HS256"} dumped with sort_keys and compact
// separators.
var headerSegment = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Signer mints the tokens AuthService mints: HS256 over the same secret,
// with the same issuer and audience. It is safe for concurrent use.
type Signer struct {
	secret   []byte
	issuer   string
	audience string
}

// NewSigner builds a Signer under the same rules as New.
func NewSigner(secret, issuer, audience string) (*Signer, error) {
	if _, err := New(secret, issuer, audience); err != nil {
		return nil, err
	}
	return &Signer{secret: []byte(secret), issuer: issuer, audience: audience}, nil
}

// AccessClaims are create_access_token's arguments.
type AccessClaims struct {
	UserID       string
	Email        string
	OrgID        string
	Role         string
	IsSuperuser  bool
	Username     *string
	FullName     *string
	TokenVersion int64
}

// RefreshClaims are create_refresh_token's (and
// create_refresh_token_with_jti's) arguments.
type RefreshClaims struct {
	UserID   string
	OrgID    string
	FamilyID string
}

// Access mints create_access_token(...) at now with jti: the claims in
// the order the Python dict holds them, username and full_name only when
// non-empty.
func (s *Signer) Access(claims AccessClaims, now time.Time, jti string) (string, error) {
	var payload claimWriter
	payload.str("sub", claims.UserID)
	payload.str("email", claims.Email)
	payload.str("org_id", claims.OrgID)
	payload.str("role", claims.Role)
	payload.boolean("is_superuser", claims.IsSuperuser)
	payload.str("type", AccessType)
	payload.str("iss", s.issuer)
	payload.str("aud", s.audience)
	payload.integer("exp", now.Add(AccessLifetime).Unix())
	payload.integer("iat", now.Unix())
	payload.str("jti", jti)
	payload.integer("tv", claims.TokenVersion)
	if claims.Username != nil && *claims.Username != "" {
		payload.str("username", *claims.Username)
	}
	if claims.FullName != nil && *claims.FullName != "" {
		payload.str("full_name", *claims.FullName)
	}
	return s.sign(payload.close())
}

// Refresh mints create_refresh_token(...) at now with jti, expiring
// RefreshLifetime later.
func (s *Signer) Refresh(claims RefreshClaims, now time.Time, jti string) (string, error) {
	return s.RefreshUntil(claims, now, now.Add(RefreshLifetime), jti)
}

// RefreshUntil mints create_refresh_token_with_jti(...): a refresh token
// with a given jti and expiry, issued at now.
func (s *Signer) RefreshUntil(claims RefreshClaims, now, expiresAt time.Time, jti string) (string, error) {
	var payload claimWriter
	payload.str("sub", claims.UserID)
	payload.str("org_id", claims.OrgID)
	payload.str("family_id", claims.FamilyID)
	payload.str("type", RefreshType)
	payload.str("iss", s.issuer)
	payload.str("aud", s.audience)
	payload.integer("exp", expiresAt.Unix())
	payload.integer("iat", now.Unix())
	payload.str("jti", jti)
	return s.sign(payload.close())
}

func (s *Signer) sign(payload []byte) (string, error) {
	if len(s.secret) == 0 {
		return "", fmt.Errorf("edgetoken: signer has no key")
	}
	signingInput := headerSegment + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// claimWriter writes a JSON object as PyJWT does: json.dumps with compact
// separators and the default ensure_ascii=True, keys in insertion order.
type claimWriter struct{ buffer []byte }

func (w *claimWriter) key(name string) {
	if len(w.buffer) == 0 {
		w.buffer = append(w.buffer, '{')
	} else {
		w.buffer = append(w.buffer, ',')
	}
	w.buffer = pythonparity.AppendPythonJSONString(w.buffer, name)
	w.buffer = append(w.buffer, ':')
}

func (w *claimWriter) str(name, value string) {
	w.key(name)
	w.buffer = pythonparity.AppendPythonJSONString(w.buffer, value)
}

func (w *claimWriter) boolean(name string, value bool) {
	w.key(name)
	w.buffer = strconv.AppendBool(w.buffer, value)
}

func (w *claimWriter) integer(name string, value int64) {
	w.key(name)
	w.buffer = strconv.AppendInt(w.buffer, value, 10)
}

func (w *claimWriter) close() []byte { return append(w.buffer, '}') }
