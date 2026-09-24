// Package edgetoken verifies the edge access token: the HS256 JWT the Python
// api's AuthService.create_access_token mints and validate_token verifies
// (src/dev_health_ops/api/services/auth.py). It is the one Go copy of that
// JWT-level decision; query-api's principal.EdgeVerifier and the dho api's
// principal both call it, so the two planes cannot drift apart.
//
// It decides the token only. Whether the user row is still active and the
// token version is current is the caller's database check.
package edgetoken

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// Algorithm is the one signing method accepted (JWT_ALGORITHM). Passed to
	// jwt.WithValidMethods, so an alg-confusion token ("none", RS256) is
	// refused before any key is considered.
	Algorithm = "HS256"
	// AccessType is the "type" claim every REST auth path requires
	// (validate_token called with token_type="access"). A refresh token is
	// refused.
	AccessType = "access"
	// MinSecretLength is _get_jwt_secret's floor.
	MinSecretLength = 32
)

// Rejection reasons. They name the class of failure, never the token.
const (
	ReasonMalformed    = "malformed"
	ReasonBadSignature = "bad_signature"
	ReasonAudience     = "audience"
	ReasonIssuer       = "issuer"
	ReasonExpired      = "expired"
	ReasonNotYetValid  = "not_yet_valid"
	ReasonMissingSub   = "missing_sub"
	ReasonMissingType  = "missing_type"
	ReasonTypeMismatch = "type_mismatch"
)

// Rejection is the error Verify returns. Reason is one of the Reason
// constants.
type Rejection struct {
	Reason string
	Err    error
}

func (r *Rejection) Error() string { return fmt.Sprintf("edgetoken: %s: %v", r.Reason, r.Err) }
func (r *Rejection) Unwrap() error { return r.Err }

// Verifier holds the key material. It is safe for concurrent use.
type Verifier struct {
	secret   []byte
	issuer   string
	audience string
}

// New builds a Verifier. secret must hold at least MinSecretLength
// characters, as Python refuses to sign with less. issuer and audience must
// be non-empty: jwt.WithIssuer("")/WithAudience("") switch the check off
// instead of failing closed.
func New(secret, issuer, audience string) (*Verifier, error) {
	if len([]rune(secret)) < MinSecretLength {
		return nil, fmt.Errorf("edgetoken: secret must be at least %d characters", MinSecretLength)
	}
	if issuer == "" {
		return nil, errors.New("edgetoken: issuer must not be empty")
	}
	if audience == "" {
		return nil, errors.New("edgetoken: audience must not be empty")
	}
	return &Verifier{secret: []byte(secret), issuer: issuer, audience: audience}, nil
}

// Verify reproduces validate_token(token, "access"):
//
//   - an unverified peek decides whether aud and iss are checked at all: a
//     claim that is present must match, an absent one is not checked;
//   - HS256 only; exp, sub and type are required (pyjwt "require");
//   - exp, nbf and iat are checked with no leeway, as pyjwt does by default
//     (pyjwt refuses an iat in the future);
//   - type must equal "access".
//
// It returns the verified claims. On a rejection it returns the claims it
// could decode (possibly nil) with a *Rejection, so a caller can log which
// principal was refused without logging the token.
func (v *Verifier) Verify(tokenString string) (jwt.MapClaims, error) {
	return v.VerifyType(tokenString, AccessType)
}

// VerifyType is validate_token(token, tokenType): Verify's rules with the
// "type" claim required to equal tokenType (RefreshType for a refresh
// token).
func (v *Verifier) VerifyType(tokenString, tokenType string) (jwt.MapClaims, error) {
	var peeked jwt.MapClaims
	if _, _, err := jwt.NewParser().ParseUnverified(tokenString, &peeked); err != nil {
		return nil, &Rejection{Reason: ReasonMalformed, Err: err}
	}
	_, hasAudience := peeked["aud"]
	_, hasIssuer := peeked["iss"]

	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{Algorithm}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
	}
	if hasAudience {
		opts = append(opts, jwt.WithAudience(v.audience))
	}
	if hasIssuer {
		opts = append(opts, jwt.WithIssuer(v.issuer))
	}

	var claims jwt.MapClaims
	// golang-jwt returns a nil error only for a valid token.
	if _, err := jwt.ParseWithClaims(tokenString, &claims, func(*jwt.Token) (interface{}, error) {
		return v.secret, nil
	}, opts...); err != nil {
		return claims, &Rejection{Reason: classify(err), Err: err}
	}
	// pyjwt's "require" is key presence: a present zero value counts.
	if _, ok := claims["sub"]; !ok {
		return claims, &Rejection{Reason: ReasonMissingSub, Err: errors.New("missing sub claim")}
	}
	typeClaim, ok := claims["type"]
	if !ok {
		return claims, &Rejection{Reason: ReasonMissingType, Err: errors.New("missing type claim")}
	}
	if typeValue, _ := typeClaim.(string); typeValue != tokenType {
		return claims, &Rejection{Reason: ReasonTypeMismatch, Err: fmt.Errorf("type claim is %q, want %q", typeValue, tokenType)}
	}
	return claims, nil
}

// ReasonOf returns the rejection reason carried by err, or ReasonMalformed.
func ReasonOf(err error) string {
	var rejection *Rejection
	if errors.As(err, &rejection) {
		return rejection.Reason
	}
	return ReasonMalformed
}

// classify maps a golang-jwt error to a reason. Anything unnamed (an
// unsupported alg, a malformed body, a missing exp) is "malformed": the
// token could not be processed as valid at all.
func classify(err error) string {
	switch {
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return ReasonBadSignature
	case errors.Is(err, jwt.ErrTokenInvalidAudience):
		return ReasonAudience
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return ReasonIssuer
	case errors.Is(err, jwt.ErrTokenExpired):
		return ReasonExpired
	case errors.Is(err, jwt.ErrTokenNotValidYet), errors.Is(err, jwt.ErrTokenUsedBeforeIssued):
		return ReasonNotYetValid
	default:
		return ReasonMalformed
	}
}
