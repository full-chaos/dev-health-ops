package principal

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-go/authverify"
)

// ErrUnsupportedSchemaVersion is returned when an otherwise
// validly-signed envelope carries a `v` this verifier was not written to
// handle. See SupportedSchemaVersion's doc comment.
var ErrUnsupportedSchemaVersion = errors.New("principal: unsupported envelope schema version")

// Verifier verifies effective-principal envelopes minted by the Python
// edge's principal_envelope.issue_effective_principal_envelope, using
// dev-health-go's authverify.Ed25519JWKSVerifier for key material (CHAOS-4377).
type Verifier struct {
	jwks     *authverify.Ed25519JWKSVerifier
	issuer   string
	audience string
}

// NewVerifier builds a Verifier that loads its JWKS document from
// jwksPath on every Verify call (never cached -- matches
// Ed25519JWKSVerifier's own no-cache contract, so a rotated JWKS is picked
// up without a restart) and requires envelopes issued by issuer for
// audience.
func NewVerifier(jwksPath, issuer, audience string) *Verifier {
	return &Verifier{
		jwks:     authverify.NewEd25519JWKSVerifier(jwksPath),
		issuer:   issuer,
		audience: audience,
	}
}

// Verify parses and verifies tokenString as an effective-principal
// envelope: EdDSA signature against the JWKS (looked up by the token's
// `kid` header -- an alg-confusion attempt using any other signing method
// is rejected by jwt.WithValidMethods before a key is even resolved),
// issuer, audience, and expiration (jwt.WithExpirationRequired rejects an
// envelope with no `exp` at all, not just an expired one). On success,
// also enforces the claim-schema-version contract: an envelope whose `v`
// this verifier was not written to handle is rejected even though its
// signature is valid -- see SupportedSchemaVersion's doc comment.
func (v *Verifier) Verify(tokenString string) (*Claims, error) {
	claims := &Claims{}

	keyFunc := func(token *jwt.Token) (interface{}, error) {
		kid, _ := token.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("principal: token has no kid header")
		}
		keys, err := v.jwks.Keys()
		if err != nil {
			return nil, fmt.Errorf("principal: loading jwks: %w", err)
		}
		key, ok := keys[kid]
		if !ok {
			return nil, fmt.Errorf("principal: no jwks key for kid %q", kid)
		}
		return key, nil
	}

	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		keyFunc,
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		recordVerifyOutcome("rejected")
		return nil, fmt.Errorf("principal: verify: %w", err)
	}
	if !token.Valid {
		recordVerifyOutcome("rejected")
		return nil, errors.New("principal: token invalid")
	}

	if claims.SchemaVersion != SupportedSchemaVersion {
		recordVerifyOutcome("unsupported_schema_version")
		return nil, fmt.Errorf(
			"%w: got %d, want %d",
			ErrUnsupportedSchemaVersion, claims.SchemaVersion, SupportedSchemaVersion,
		)
	}

	recordVerifyOutcome("verified")
	return claims, nil
}
