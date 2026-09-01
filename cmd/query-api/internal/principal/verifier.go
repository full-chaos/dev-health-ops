package principal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-go/authverify"
)

// ErrUnsupportedSchemaVersion is returned when an otherwise
// validly-signed envelope carries a `v` this verifier was not written to
// handle. See SupportedSchemaVersion's doc comment.
var ErrUnsupportedSchemaVersion = errors.New("principal: unsupported envelope schema version")

// readJWKSFileForCheck is CheckJWKS's ONLY read of the live jwksPath --
// a seam so a test can prove that invariant deterministically (codex
// round 2, P2: two independent reads of a mutable file can observe two
// different, individually-plausible snapshots and produce an
// internally-inconsistent verdict; see CheckJWKS's doc comment) without
// depending on OS-level FIFO/timing races to force the interleaving.
// Production code always uses os.ReadFile; only tests swap this.
var readJWKSFileForCheck = os.ReadFile

// Verifier verifies effective-principal envelopes minted by the Python
// edge's principal_envelope.issue_effective_principal_envelope, using
// dev-health-go's authverify.Ed25519JWKSVerifier for key material (CHAOS-4377).
type Verifier struct {
	jwks     *authverify.Ed25519JWKSVerifier
	jwksPath string
	issuer   string
	audience string
}

// NewVerifier builds a Verifier that loads its JWKS document from
// jwksPath on every Verify call (never cached -- matches
// Ed25519JWKSVerifier's own no-cache contract, so a rotated JWKS is picked
// up without a restart) and requires envelopes issued by issuer for
// audience.
//
// issuer and audience must both be non-empty. jwt.WithIssuer("") does not
// fail closed -- an empty expectedIss disables issuer checking entirely
// (see golang-jwt/jwt/v5's Validator.expectedIss doc comment), so a caller
// that constructs a Verifier from an unset/misconfigured env var would
// silently accept a validly-signed envelope from ANY issuer. Failing fast
// here, once, at construction time is cheaper than relying on every future
// caller to remember that footgun.
func NewVerifier(jwksPath, issuer, audience string) (*Verifier, error) {
	if issuer == "" {
		return nil, errors.New("principal: issuer must not be empty")
	}
	if audience == "" {
		return nil, errors.New("principal: audience must not be empty")
	}
	return &Verifier{
		jwks:     authverify.NewEd25519JWKSVerifier(jwksPath),
		jwksPath: jwksPath,
		issuer:   issuer,
		audience: audience,
	}, nil
}

// CheckJWKS proves the verifier's configured JWKS document can currently
// produce usable Ed25519 key material -- CHAOS-4708: /readyz needs a
// bounded, live way to answer "will this instance verify a token"
// distinct from "will it 500/401 on the first request", and Verify alone
// cannot answer that without a real signed token to run through it.
//
// Delegates entirely to authverify.Ed25519JWKSVerifier.Keys(), which:
//   - reads jwksPath fresh on every call (never cached) -- calling this
//     from a /readyz probe therefore preserves the no-restart rotation
//     contract NewVerifier's doc comment describes: a JWKS that goes bad
//     AFTER boot (deleted, truncated, rotated-wrong) is caught on the
//     very next probe, not hidden behind a stale cached success.
//   - already guarantees at least one structurally-valid Ed25519 signing
//     key on a nil return (an empty or all-invalid key set is
//     ErrInvalidJWKS, never a nil error with zero keys) -- so a nil
//     return from this method already means "yields >=1 usable key",
//     the strongest of the three checks CHAOS-4708 considered (file
//     exists / parses / yields a usable key). See that method's own doc
//     comment for the exact validation it performs.
//   - costs one local os.ReadFile plus an in-memory JSON decode and
//     per-key base64/size validation -- no network I/O, so it is cheap
//     enough to run uncached on every probe, the same cost model
//     readinessCheck already applies to ClickHouse/Postgres Ping.
//
// codex round 1 (gpt-5.6-terra, xhigh, chaos-4708-20260901T065704), P1,
// CONFIRMED (re-executed independently before fixing, not taken on the
// review's word): Keys() parses the JWKS file with a streaming
// json.Decoder.Decode, which reads only the FIRST JSON value in the file
// and silently ignores any bytes after it -- it does NOT require the
// decoder to reach EOF. A file containing two concatenated JWKS
// documents (e.g. a rotation script that appended a new document instead
// of replacing the old one, or any other non-atomic-write mishap) still
// returns a nil error from Keys(), with ONLY the first document's keys.
// Reproduced: a two-document file with kid "old" then kid "new" makes
// Keys() return {"old": <key>} and nil error -- Verify() for a token
// signed with "new" then fails with "no jwks key for kid \"new\"", while
// this check alone would have reported the instance healthy. A JWKS
// document is defined (RFC 7517) as exactly one JSON object; trailing
// content of ANY kind is therefore untrustworthy, whether or not
// authverify's own load path happens to tolerate it.
//
// codex round 2 (gpt-5.6-terra, xhigh, chaos-4708-round2-20260901T073251),
// P2, CONFIRMED (re-executed independently with a FIFO-backed path
// forcing the exact interleaving, before fixing): round 1's fix called
// Keys() (which does its OWN os.ReadFile internally) and THEN a second,
// separate os.ReadFile for the trailing-content check -- two independent
// reads of a mutable file. A rotation landing between them (e.g.
// replacing a valid JWKS with a syntactically-valid-but-empty-keys one)
// let the first read see the OLD valid content (Keys() succeeds) while
// the second saw the NEW content (json.Valid on {"keys":[]} is also
// true), so CheckJWKS returned nil from an internally-inconsistent
// verdict, even though neither snapshot alone was both current AND
// usable. Fixed by reading the file exactly ONCE and validating BOTH
// properties (single well-formed JSON value, AND yields >=1 usable key)
// against that SAME byte slice: the trailing-content check runs directly
// on the read bytes, and the key-material check runs authverify's own
// Keys() against a private temp-file snapshot of those exact bytes
// (rather than re-implementing authverify's OKP/Ed25519/EdDSA/kid/size
// validation locally, which would drift from the vendored package's own
// rules over time) -- so both checks now see identical content by
// construction, closing the race rather than narrowing its window.
func (v *Verifier) CheckJWKS() error {
	raw, err := readJWKSFileForCheck(v.jwksPath)
	if err != nil {
		return err
	}
	if !json.Valid(raw) {
		return fmt.Errorf("jwks document at %s is not a single well-formed JSON value "+
			"(trailing or malformed content after what may be a valid document)", v.jwksPath)
	}

	snapshot, err := os.CreateTemp("", "jwks-readyz-check-*.json")
	if err != nil {
		return fmt.Errorf("jwks: create snapshot temp file: %w", err)
	}
	snapshotPath := snapshot.Name()
	defer os.Remove(snapshotPath)
	if _, err := snapshot.Write(raw); err != nil {
		snapshot.Close()
		return fmt.Errorf("jwks: write snapshot: %w", err)
	}
	if err := snapshot.Close(); err != nil {
		return fmt.Errorf("jwks: close snapshot: %w", err)
	}

	if _, err := authverify.NewEd25519JWKSVerifier(snapshotPath).Keys(); err != nil {
		return err
	}
	return nil
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
