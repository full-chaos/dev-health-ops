package goapiproof

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
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
// Values are NEVER logged. An earlier version of this comment claimed the
// ABSENCE of a String method achieved that; r2 proved the opposite with an
// executed repro -- with no String method, fmt falls back to printing the
// struct, so `%v` of a Credential rendered the cached token in full. The
// redaction has to be written, not assumed, which is why String, GoString
// and Format are all implemented below: %v, %+v, %#v and Sprint each take
// a different path through fmt and each one had to be closed.
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

	// state holds everything MUTABLE, behind a pointer, and that shape is
	// load-bearing twice over.
	//
	// r3 found that a VALUE copy of a Credential leaked: the redaction
	// methods had pointer receivers, so `%v` of a copy fell through to
	// fmt's struct printer and rendered the cached token in full. My own
	// tests passed because they only ever formatted the pointer.
	//
	// Value receivers fix that -- their method set covers both forms --
	// but a value receiver on a struct containing a sync.Mutex copies the
	// lock, which `go vet` correctly refuses. Moving the mutable state
	// behind a pointer solves both at once: Credential itself is a small
	// immutable descriptor that is safe to copy, copies share one state,
	// and the only field fmt could print for it is an address.
	state *credentialState
}

// credentialState is the mutable half. Unexported, reachable only through
// the methods below, and never rendered by any of them.
type credentialState struct {
	mu       sync.Mutex
	cached   string
	mintedAt time.Time
	mints    int
}

// StaticCredential is a value that does not expire within a run -- the
// edge's access token.
//
// An EMPTY value is legal to construct and refused at Apply. Refusing here
// would be tidier, but the constructor has no error return and the CLI is
// not the only caller a future change might add; refusing at the moment of
// use means no path can send a bare "Bearer ", whoever built it.
func StaticCredential(header, kind, value string) *Credential {
	return &Credential{header: header, kind: kind, state: &credentialState{cached: value}}
}

// redacted is what every formatting verb renders instead of the value.
const redacted = "goapiproof.Credential{kind:%q, header:%q, value:REDACTED}"

// String, GoString and Format together close every fmt path to the cached
// value. r2 found `%v` printing a token in full; %+v, %#v and Sprint reach
// fmt differently, so all four are pinned by test.
func (c Credential) String() string {
	return fmt.Sprintf(redacted, c.kind, c.header)
}

// GoString covers %#v, which ignores String and would otherwise render the
// struct literally, cached token included.
func (c Credential) GoString() string { return c.String() }

// Format covers the remaining verbs -- %+v, %s, %q and anything else a
// future caller reaches for -- so no verb falls through to the default
// struct printer. A credential must not depend on the caller choosing a
// safe verb.
func (c Credential) Format(f fmt.State, verb rune) {
	_, _ = io.WriteString(f, c.String())
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
	return &Credential{header: header, kind: kind, mint: mint, freshFor: freshFor, state: &credentialState{}}
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
	if c == nil || c.state == nil {
		return 0
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return c.state.mints
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
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.mint == nil {
		if isEmptyCredential(c.state.cached) {
			// A bare "Bearer " is answered 401, which is
			// indistinguishable in a report from a rejected credential --
			// the confusion this whole file exists to remove. Refuse at
			// the source (r2 P2).
			// TrimSpace, not == "": r3 installed `Authorization: "Bearer    "`
			// from GO_API_PROVE_BEARER="   ", which the server answers 401 --
			// the same indistinguishable-401 an empty one produces. Whitespace
			// is an empty credential wearing a disguise.
			return "", fmt.Errorf("the %s credential is empty or whitespace", c.kind)
		}
		return c.state.cached, nil
	}
	if c.state.cached != "" && c.freshFor > 0 && time.Since(c.state.mintedAt) < c.freshFor {
		return c.state.cached, nil
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
	c.state.cached, c.state.mintedAt = minted, time.Now()
	c.state.mints++
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

// isEmptyCredential reports whether a header value carries no actual
// credential.
//
// Not simply == "": r3 installed `Authorization: "Bearer    "` from
// GO_API_PROVE_BEARER="   ", which the server answers 401 -- the same
// indistinguishable-401 an empty value produces. And not simply
// TrimSpace either, because by the time the value reaches here it already
// carries the "Bearer " scheme, so the whitespace that matters is AFTER
// the scheme. Whitespace is an empty credential wearing a disguise, and it
// has to be undressed at the same place the disguise was put on.
func isEmptyCredential(value string) bool {
	if strings.TrimSpace(value) == "" {
		return true
	}
	// Cut the RAW value, not a trimmed copy. Trimming first removes the
	// separator that "Bearer    " consists of -- TrimSpace turns it into
	// "Bearer", Cut then finds no space, and the whitespace-only token
	// reads as a whole credential. That was this function's first version,
	// and the probe that caught it is why the case list below is in a
	// test rather than in my head.
	scheme, token, found := strings.Cut(value, " ")
	if !found {
		// A bare token with no scheme -- non-empty by the check above.
		return false
	}
	if strings.EqualFold(strings.TrimSpace(scheme), "Bearer") {
		return strings.TrimSpace(token) == ""
	}
	return false
}
