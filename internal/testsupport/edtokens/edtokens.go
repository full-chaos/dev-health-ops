// Package edtokens is test support for the user access token (access-token.v1, EdDSA): one throwaway
// Ed25519 key per Issuer, tokens minted under it, and the go-api Authenticator a test api uses to verify
// them by the route's endpoint-profile row, as production does with AUTH_ACCESS_TOKEN_ISSUER set.
//
// The user access token names a session (sid) the api checks live; Store answers that check for every
// session this Issuer minted, and reads everything else (users, memberships) from Postgres.
package edtokens

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const (
	// IssuerURL, Audience and KeyID are the iss, aud and kid of every token minted here.
	IssuerURL = "https://edtokens.test"
	Audience  = "dev-health-api"
	KeyID     = "edtokens-test"
	// Lifetime is a minted token's lifetime unless Claims.Lifetime says otherwise.
	Lifetime = time.Hour
)

// Issuer mints user access tokens under its own key and remembers each token's session.
type Issuer struct {
	priv     ed25519.PrivateKey
	keys     authverify.StaticKeys
	mu       sync.Mutex
	sessions map[uuid.UUID]session
}

// session is a minted token's session: its user and the users.token_version it began at.
type session struct {
	principal    uuid.UUID
	tokenVersion int64
	expiresAt    time.Time
}

// New returns an Issuer with a fresh key.
func New(t testing.TB) *Issuer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &Issuer{priv: priv, keys: authverify.StaticKeys{KeyID: pub}, sessions: map[uuid.UUID]session{}}
}

// Fresh is New for a package-level fixture (no test at hand); it panics when no key can be made.
func Fresh() *Issuer {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return &Issuer{priv: priv, keys: authverify.StaticKeys{KeyID: pub}, sessions: map[uuid.UUID]session{}}
}

// MustMint is Mint that fails t.
func (i *Issuer) MustMint(t testing.TB, c Claims) string {
	t.Helper()
	token, err := i.Mint(c)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// Claims are the inputs of one token. OrgID "" mints a token without an organization. TokenVersion is the
// users.token_version the token's session begins at: the api refuses the token once the users row moves past it.
type Claims struct {
	UserID       uuid.UUID
	OrgID        string
	Actor        *uuid.UUID
	TokenVersion int64
	IssuedAt     time.Time
	Lifetime     time.Duration
	// Extra are more claims (for example a spoofed is_superuser); they never replace a standard claim.
	Extra map[string]any
	// Subject, when set, replaces the wire sub (a refusal cell's malformed subject).
	Subject string
	// SessionID, when set, is the token's session (an auth-service session the test made) instead of a new one.
	SessionID *uuid.UUID
	// NoSession mints a token without sid (a refusal cell): no session is remembered.
	NoSession bool
}

// PrincipalID is the wire form of a user id in a token's sub and act.sub.
func PrincipalID(id uuid.UUID) string {
	return "prn_" + base64.RawURLEncoding.EncodeToString(id[:])
}

// Mint signs a token for c under a new live session of c.UserID.
func (i *Issuer) Mint(c Claims) (string, error) {
	if c.UserID == uuid.Nil {
		return "", errors.New("edtokens: a token needs a user id")
	}
	now := c.IssuedAt
	if now.IsZero() {
		now = time.Now()
	}
	lifetime := c.Lifetime
	if lifetime == 0 {
		lifetime = Lifetime
	}
	if c.NoSession && c.SessionID != nil {
		return "", errors.New("edtokens: a token without a session names no session")
	}
	sid := uuid.New()
	if c.SessionID != nil {
		sid = *c.SessionID
	}
	claims := map[string]any{
		"iss": IssuerURL, "aud": Audience, "sub": PrincipalID(c.UserID), "principal_type": "human",
		"sid": sessionid.Wire(sid), "jti": strings.ReplaceAll(uuid.NewString(), "-", ""),
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(lifetime).Unix(), "scope": "api:access",
	}
	if c.NoSession {
		delete(claims, "sid")
	}
	if c.OrgID != "" {
		claims["org_id"] = c.OrgID
	}
	if c.Subject != "" {
		claims["sub"] = c.Subject
	}
	if c.Actor != nil {
		claims["act"] = map[string]any{"sub": PrincipalID(*c.Actor)}
	}
	for key, value := range c.Extra {
		if _, standard := claims[key]; !standard {
			claims[key] = value
		}
	}
	header, err := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": KeyID})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	if !c.NoSession {
		i.mu.Lock()
		i.sessions[sid] = session{principal: c.UserID, tokenVersion: c.TokenVersion, expiresAt: now.Add(lifetime)}
		i.mu.Unlock()
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(i.priv, []byte(signing))), nil
}

// Session is the session of a minted token: the auth-service session row a production token names.
type Session struct {
	ID, Principal uuid.UUID
	TokenVersion  int64
	ExpiresAt     time.Time
}

// Sessions are the sessions of every token this Issuer minted, ordered by id.
func (i *Issuer) Sessions() []Session {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]Session, 0, len(i.sessions))
	for id, minted := range i.sessions {
		out = append(out, Session{ID: id, Principal: minted.principal, TokenVersion: minted.tokenVersion, ExpiresAt: minted.expiresAt})
	}
	slices.SortFunc(out, func(a, b Session) int { return strings.Compare(a.ID.String(), b.ID.String()) })
	return out
}

// Verifier verifies a token of this Issuer as the api does.
func (i *Issuer) Verifier(t testing.TB) *authverify.Verifier {
	t.Helper()
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: IssuerURL, Audience: Audience, Keys: i.keys})
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

// Store is a Postgres policy store whose session check answers from the sessions the Issuer minted.
type Store struct {
	policy.PGStore
	issuer *Issuer
}

// SessionState is the live session row of a minted token; an unknown session is not found.
func (s Store) SessionState(_ context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	row, ok := s.issuer.session(id)
	return row, ok, nil
}

func (i *Issuer) session(id uuid.UUID) (policy.SessionRow, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	minted, ok := i.sessions[id]
	if !ok {
		return policy.SessionRow{}, false
	}
	version := minted.tokenVersion
	return policy.SessionRow{PrincipalID: minted.principal, LegacyTokenVersion: &version}, true
}

// Store wraps base so the session check sees this Issuer's sessions.
func (i *Issuer) Store(base policy.PGStore) Store { return Store{PGStore: base, issuer: i} }

// Authenticator is the api's Authenticator over base, routed by the go-api endpoint-profile rows with
// this Issuer's key as the only key: a bearer this Issuer did not mint is refused.
func (i *Issuer) Authenticator(t testing.TB, base policy.PGStore, logger *slog.Logger) *policy.Authenticator {
	t.Helper()
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return i.route(t, i.Store(base), logger)
}

// AuthenticatorOver is Authenticator over any policy store (a unit test's in-memory double): the session
// check answers this Issuer's sessions first, then base's own when it reads sessions; the users read is
// base's own AccessUser when it has one, else its UserState.
func (i *Issuer) AuthenticatorOver(t testing.TB, base policy.Store, logger *slog.Logger) *policy.Authenticator {
	t.Helper()
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return i.route(t, overStore{Store: base, issuer: i}, logger)
}

func (i *Issuer) route(t testing.TB, store policy.Store, logger *slog.Logger) *policy.Authenticator {
	t.Helper()
	auth, err := policy.NewAuthenticator(store, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := accessrouting.RouteUserTokens(auth, i.Verifier(t), noop.NewMeterProvider().Meter("edtokens"), logger); err != nil {
		t.Fatal(err)
	}
	return auth
}

// overStore is base with this Issuer's sessions.
type overStore struct {
	policy.Store
	issuer *Issuer
}

func (s overStore) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	if row, ok := s.issuer.session(id); ok {
		return row, true, nil
	}
	if own, ok := s.Store.(policy.SessionStore); ok {
		return own.SessionState(ctx, id)
	}
	return policy.SessionRow{}, false, nil
}

func (s overStore) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	if own, ok := s.Store.(policy.AccessUserStore); ok {
		return own.AccessUser(ctx, id)
	}
	state, found, err := s.Store.UserState(ctx, id)
	return policy.AccessUserRow{State: state}, found, err
}
