package goapiproof

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// This file exists because ONE credential cannot reach both planes, and a
// run that discovered that at request time had already refused fifteen
// operations for the wrong reason.
//
// Measured from inside the api container on the deployed stack
// (lane-stack-owner, JOB 4, 2026-09-09): an access token gets HTTP 200 on
// the Python edge's /graphql and 401 on /buildinfo; an effective-principal
// envelope gets 200 on /buildinfo and 401 on the edge. They are different
// credential KINDS checked by different verifiers, not two spellings of
// one thing. The runner previously applied a single Config.Headers map to
// every request, so whichever token it was given, one leg was always
// rejected -- and a 401 on the candidate leg reads as "the edge refused",
// which is indistinguishable in a report from a real authorization
// failure.
//
// The second half is lifetime. ENVELOPE_DEFAULT_TTL_SECONDS is 60
// (principal_envelope.py:96) and the envelope was read ONCE at startup,
// while a 15-operation run plus the closing VerifyBuildStable takes
// longer than that. JOB 4's attempt B died on a ~90-second-old envelope
// at the final /buildinfo -- after every measurement had already been
// taken, which is the most expensive possible moment to fail.
//
// So a credential here is a SOURCE, not a string: it is asked for a value
// per request and may mint a fresh one. Go cannot mint an envelope itself
// -- issue_effective_principal_envelope needs the signing private key and
// a database-authenticated user, and no endpoint hands one to an external
// caller -- so the minting stays outside this process, behind a function
// the operator supplies.

// Credential supplies one leg's authorization header.
//
// Values are NEVER logged, and no method on this type renders one: the
// String method is deliberately absent so a %v of a Config or a Runner
// cannot spill a token.
type Credential struct {
	// header is the request header the value is set on, e.g.
	// "Authorization".
	header string
	// kind names the credential in operator-facing errors ("access
	// token", "envelope"), so a 401 says WHICH credential was refused
	// without printing it.
	kind string

	mint     func(context.Context) (string, error)
	freshFor time.Duration
	// validate, when set, checks the SHAPE of a minted value before it is
	// sent. A minter that prints a shell error, a prompt, or an empty
	// string would otherwise be sent as a bearer token and come back 401 --
	// the same indistinguishable-401 this file exists to eliminate, one
	// level further out.
	validate func(string) error

	mu       sync.Mutex
	cached   string
	mintedAt time.Time
	mints    int
}

// StaticCredential is a value that does not expire within a run -- the
// edge's access token.
func StaticCredential(header, kind, value string) *Credential {
	return &Credential{header: header, kind: kind, cached: value}
}

// MintedCredential re-mints its value once it is older than freshFor.
//
// freshFor is a FRESHNESS floor, not the credential's lifetime, and it
// must be comfortably below the real TTL: the value is minted before a
// request is sent and has to still be valid when the server checks it. At
// a 60-second envelope TTL, 25 seconds leaves 35 for the request to
// arrive -- the point is that no request is ever sent with a value that
// was already near expiry, which is exactly how attempt B failed.
//
// A mint error is returned to the caller, never swallowed in favour of a
// stale value. A run that cannot authenticate must refuse by name, not
// quietly retry with something the server will reject.
func MintedCredential(header, kind string, freshFor time.Duration, mint func(context.Context) (string, error)) *Credential {
	return &Credential{header: header, kind: kind, mint: mint, freshFor: freshFor}
}

// WithShapeValidator returns c with a shape check applied to every minted
// value. Returns c so it can be chained at construction.
func (c *Credential) WithShapeValidator(validate func(string) error) *Credential {
	c.validate = validate
	return c
}

// Mints is how many times this credential has been minted. A COUNT, never
// a value: it belongs in the run report, where it shows an operator that
// the refresh is working without putting a credential anywhere near a log.
func (c *Credential) Mints() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mints
}

// ValidateEnvelopeShape rejects a value that cannot be an
// effective-principal envelope.
//
// The envelope is a JWT (principal_envelope.py:266 calls jwt.encode), so
// it is three non-empty base64url segments separated by dots. This does
// NOT verify the signature -- only the issuer's key could, and the server
// does that anyway. It catches the failure the operator actually hits: a
// minting command that printed a usage line, an error, a JSON blob, or a
// shell prompt instead of a token. Sending one of those produces a 401
// that reads exactly like a rejected credential.
func ValidateEnvelopeShape(value string) error {
	token := strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return fmt.Errorf("expected a JWT (three dot-separated segments), got %d segment(s)", len(segments))
	}
	for i, segment := range segments {
		if segment == "" {
			return fmt.Errorf("JWT segment %d is empty", i+1)
		}
		if strings.ContainsAny(segment, " \t\n\r") {
			return fmt.Errorf("JWT segment %d contains whitespace, so this is not a token", i+1)
		}
		if _, err := base64.RawURLEncoding.DecodeString(segment); err != nil {
			return fmt.Errorf("JWT segment %d is not base64url", i+1)
		}
	}
	return nil
}

// Kind names the credential for an error message. Safe to print.
func (c *Credential) Kind() string {
	if c == nil {
		return "none"
	}
	return c.kind
}

// value returns a usable credential, minting a new one when the cached
// one has aged past freshFor.
func (c *Credential) value(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mint == nil {
		return c.cached, nil
	}
	if c.cached != "" && c.freshFor > 0 && time.Since(c.mintedAt) < c.freshFor {
		return c.cached, nil
	}
	minted, err := c.mint(ctx)
	if err != nil {
		return "", fmt.Errorf("mint %s credential: %w", c.kind, err)
	}
	if minted == "" {
		// An empty value would be sent as a bare "Bearer ", which the
		// server answers 401 -- indistinguishable in the report from a
		// real authorization failure. Refuse at the source instead.
		return "", fmt.Errorf("the %s minter returned an empty value", c.kind)
	}
	if c.validate != nil {
		if err := c.validate(minted); err != nil {
			// The VALUE is never included -- only what is wrong with its
			// shape. A malformed credential is still a credential.
			return "", fmt.Errorf("the %s minter returned something that is not a %s: %w", c.kind, c.kind, err)
		}
	}
	c.cached, c.mintedAt = minted, time.Now()
	c.mints++
	return minted, nil
}

// Apply sets this credential on a request, minting a fresh value if the
// cached one has aged out.
//
// A nil Credential is an ERROR rather than a no-op request. Sending a
// request with no credential produces a 401 that reads exactly like a
// rejected one, and the whole point of this file is that those two must
// never be confused again.
func (c *Credential) Apply(ctx context.Context, request *http.Request) error {
	if c == nil {
		return fmt.Errorf("goapiproof: no credential configured for %s: this leg cannot be measured, and sending it unauthenticated would produce a 401 indistinguishable from a rejected credential", request.URL.Path)
	}
	value, err := c.value(ctx)
	if err != nil {
		return fmt.Errorf("goapiproof: %w", err)
	}
	request.Header.Set(c.header, value)
	return nil
}
