package edgetokenmint

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/acraudience"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
	"github.com/full-chaos/dev-health-ops/internal/signingkey"
)

// The EdDSA mode signs an access-token.v1 user token with an Ed25519 key read from a file, the way signingkey
// reads its key. The token is accepted only for a live session of its principal, so the caller names a session
// that already exists: this package reads it, never writes one.
const (
	SigningKeyFileEnvVar = "EDGE_TOKEN_SIGNING_KEY_FILE"
	KeyIDEnvVar          = "EDGE_TOKEN_KID"
	SessionIDEnvVar      = "EDGE_TOKEN_SESSION_ID"
	// IssuerURLEnvVar is the variable go-api and query-api read their issuer from; the token's iss is the same URL.
	IssuerURLEnvVar = "AUTH_ACCESS_TOKEN_ISSUER"
)

var (
	ErrSessionMissing   = fmt.Errorf("edgetokenmint: %s is not set", SessionIDEnvVar)
	ErrSessionNotFound  = errors.New("edgetokenmint: the session has no row")
	ErrSessionNotLive   = errors.New("edgetokenmint: the session is revoked or expired")
	ErrSessionPrincipal = errors.New("edgetokenmint: the session belongs to another principal")
	ErrSessionPin       = errors.New("edgetokenmint: the session predates the principal's token_version")
)

// EdDSASigner is what the EdDSA mode signs with. Zero values are refused.
type EdDSASigner struct {
	Key       ed25519.PrivateKey
	KeyID     string
	Issuer    string
	SessionID uuid.UUID
}

// EdDSASignerFromEnv reads the signer from the EDGE_TOKEN_* and AUTH_ACCESS_TOKEN_ISSUER variables by name. The
// session id may be absent: MintForProveEdDSA refuses that after the principal checks, so a missing membership is
// still named first.
func EdDSASignerFromEnv(lookup func(string) (string, bool)) (EdDSASigner, error) {
	get := func(name string) string {
		v, _ := lookup(name)
		return strings.TrimSpace(v)
	}
	key, err := signingkey.LoadPrivateKeyFromFile(get(SigningKeyFileEnvVar))
	if err != nil {
		return EdDSASigner{}, err
	}
	signer := EdDSASigner{Key: key, KeyID: get(KeyIDEnvVar), Issuer: get(IssuerURLEnvVar)}
	if signer.KeyID == "" {
		return EdDSASigner{}, fmt.Errorf("%s is not set", KeyIDEnvVar)
	}
	if signer.Issuer == "" {
		return EdDSASigner{}, fmt.Errorf("%s is not set", IssuerURLEnvVar)
	}
	if raw := get(SessionIDEnvVar); raw != "" {
		if signer.SessionID, err = uuid.Parse(raw); err != nil {
			return EdDSASigner{}, fmt.Errorf("%s is not a UUID", SessionIDEnvVar)
		}
	}
	return signer, nil
}

// sessionLiveSQL reads the session the way go-api's policy.PGStore.SessionState does, plus expiry.
const sessionLiveSQL = `SELECT principal_id, revoked_at IS NOT NULL, expires_at <= now(), legacy_token_version FROM %s.sessions WHERE id = $1`

// CheckSession refuses a session the verifier would refuse for principal: no row, revoked or expired, another
// principal's, or pinned to another token_version than the principal's current one.
func CheckSession(ctx context.Context, db RowQuerier, sessionID uuid.UUID, principal Principal, authSchema string) error {
	if sessionID == uuid.Nil {
		return ErrSessionMissing
	}
	if authSchema == "" {
		authSchema = "auth"
	}
	if pgident.Check(authSchema) != pgident.OK {
		return fmt.Errorf("edgetokenmint: auth schema %q is not a usable schema name", authSchema)
	}
	var (
		owner            uuid.UUID
		revoked, expired bool
		pin              *int64
	)
	err := db.QueryRow(ctx, fmt.Sprintf(sessionLiveSQL, pgx.Identifier{authSchema}.Sanitize()), sessionID).Scan(&owner, &revoked, &expired, &pin)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrSessionNotFound
	case err != nil:
		return fmt.Errorf("edgetokenmint: read the session: %w", err)
	case owner.String() != principal.UserID:
		return ErrSessionPrincipal
	case revoked || expired:
		return ErrSessionNotLive
	case pin == nil || *pin != int64(principal.TokenVersion):
		return ErrSessionPin
	}
	return nil
}

// MintEdDSA signs an access-token.v1 token for principal in the session signer names. It does not read the
// database: MintForProveEdDSA checks the session first.
func MintEdDSA(signer EdDSASigner, principal Principal, opts Options) (string, error) {
	if len(signer.Key) != ed25519.PrivateKeySize || signer.KeyID == "" || signer.Issuer == "" || signer.SessionID == uuid.Nil {
		return "", errors.New("edgetokenmint: the EdDSA signer needs a key, a kid, an issuer and a session")
	}
	userID, err := uuid.Parse(principal.UserID)
	if err != nil {
		return "", errors.New("edgetokenmint: principal user id is not a UUID")
	}
	if _, err := uuid.Parse(principal.OrgID); err != nil {
		return "", errors.New("edgetokenmint: org id is not a UUID")
	}
	if !RoleAllowedFor(principal.UserID, principal.Role) {
		return "", ErrRoleNotAllowed
	}
	if opts.Audience == "" {
		opts.Audience = DefaultAudience
	}
	if err := acraudience.Refuse("edgetokenmint", opts.Audience); err != nil {
		return "", err
	}
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	if opts.TTL < 0 || opts.TTL > MaxTTL {
		return "", fmt.Errorf("edgetokenmint: TTL must be between 0 and %s", MaxTTL)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	now := opts.Now()
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "at+jwt", "kid": signer.KeyID})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss": signer.Issuer, "aud": opts.Audience, "sub": "prn_" + base64.RawURLEncoding.EncodeToString(userID[:]),
		"principal_type": "human", "sid": sessionid.Wire(signer.SessionID), "jti": uuid.NewString(),
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(opts.TTL).Unix(),
		"org_id": principal.OrgID, "scope": "api:access",
	})
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(signer.Key, []byte(signing))), nil
}

// MintForProveEdDSA looks up the proof service principal for orgID, checks signer's session for it and mints its
// EdDSA token.
func MintForProveEdDSA(ctx context.Context, db RowQuerier, signer EdDSASigner, orgID string, opts Options) (string, error) {
	principal, err := LookupPrincipal(ctx, db, orgID, opts)
	if err != nil {
		return "", err
	}
	return mintChecked(ctx, db, signer, principal, opts)
}

// MintForAdminProveEdDSA is MintForProveEdDSA for the org-admin proof service principal.
func MintForAdminProveEdDSA(ctx context.Context, db RowQuerier, signer EdDSASigner, orgID string, opts Options) (string, error) {
	principal, err := LookupAdminPrincipal(ctx, db, orgID, opts)
	if err != nil {
		return "", err
	}
	return mintChecked(ctx, db, signer, principal, opts)
}

func mintChecked(ctx context.Context, db RowQuerier, signer EdDSASigner, principal Principal, opts Options) (string, error) {
	if err := CheckSession(ctx, db, signer.SessionID, principal, opts.AuthSchema); err != nil {
		return "", err
	}
	return MintEdDSA(signer, principal, opts)
}
