package serviceaccount

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	KeyClass         = "service_account_key"
	KeyPrefix        = "fcsak_"
	KeyHashAlgorithm = "sha256"

	KeyExpiryDefault  = 90 * 24 * time.Hour
	KeyExpiryCeiling  = 365 * 24 * time.Hour
	KeyOverlapCeiling = 24 * time.Hour
)

const (
	KeyReasonCompromised  = "compromised"
	KeyReasonOwnerRequest = "owner_request"
)

func ValidKeyShape(string) bool { return false }

func KeyDigest(string) [sha256.Size]byte { return [sha256.Size]byte{} }

func MintKey() (string, error) { return "", nil }

func CanonicalKey(string) bool { return false }

type Key struct {
	ID        uuid.UUID
	Principal uuid.UUID
	ExpiresAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	ParentID  *uuid.UUID
}

type IssuedKey struct {
	Secret          contracts.SecretString
	Key             Key
	ParentExpiresAt time.Time
}

type ResolvedKey struct {
	CredentialID uuid.UUID
	Principal    uuid.UUID
	OrgID        uuid.UUID
	ExpiresAt    time.Time
	Revision     int64
	Audiences    []string
	Scopes       []string
}

func (k ResolvedKey) HasAudience(string) bool { return false }

func (k ResolvedKey) HasScope(string) bool { return false }

type KeyRefusal struct{ Reason string }

func (e *KeyRefusal) Error() string { return "serviceaccount: key refused: " + e.Reason }

func faultOf(err error) error { return err }

type keyState struct {
	accountActive bool
	principalOff  bool
	orgActive     bool
	owner         ownerState
	live          int
}

func decideIssue(keyState) error { return nil }

func decideRotate(keyState) error { return nil }

func validKeyExpiry(time.Duration) bool { return false }

func validKeyOverlap(time.Duration) bool { return false }

func (s *Store) IssueKey(context.Context, uuid.UUID, time.Duration, Actor) (IssuedKey, error) {
	return IssuedKey{}, ErrStore
}

func (s *Store) RotateKey(context.Context, uuid.UUID, time.Duration, time.Duration, Actor) (IssuedKey, error) {
	return IssuedKey{}, ErrStore
}

func (s *Store) RevokeKey(context.Context, uuid.UUID, string, Actor) error { return ErrStore }

func (s *Store) ListKeys(context.Context, uuid.UUID) ([]Key, error) { return nil, ErrStore }

type keyRow struct {
	id, principal, org uuid.UUID
	class, domainOwner string
	revoked            bool
	expiresAt          time.Time
	audiences, scopes  []string
	kind               string
	accountDisabled    bool
	principalDisabled  bool
	orgSuspended       bool
	revision           int64
}

func (row keyRow) decide(time.Time) (ResolvedKey, string) { return ResolvedKey{}, "" }

func (s *Store) ResolveKey(context.Context, [sha256.Size]byte, time.Time) (ResolvedKey, error) {
	return ResolvedKey{}, ErrStore
}
