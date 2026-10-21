package keystore

import (
	"context"
	"crypto/ed25519"
	"regexp"
)

// State is a key's place in its rotation life cycle.
type State string

const (
	// StatePending: published, so consumers cache it before it signs.
	StatePending State = "pending"
	// StateActive: the one key that signs.
	StateActive State = "active"
	// StateRetiring: published until the tokens it signed expire; never signs.
	StateRetiring State = "retiring"
	// StateRetired: neither published nor signing.
	StateRetired State = "retired"
	// StateCompromised: neither published nor signing, and never again in
	// this process (see FileCustody).
	StateCompromised State = "compromised"
)

// Valid reports whether s is one of the five states.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateActive, StateRetiring, StateRetired, StateCompromised:
		return true
	}
	return false
}

const (
	ReasonManifestUnparseable Reason = "keyset_manifest_unparseable"
	ReasonManifestInvalid     Reason = "keyset_manifest_invalid"
	ReasonKeyUnknown          Reason = "key_id_unknown"
	ReasonKeyNotActive        Reason = "key_not_active"
)

// MaxManifestBytes bounds a key-set manifest file.
const MaxManifestBytes = int64(16 << 10)

// KeyIDPattern is the kid syntax: authconfig's AUTH_SIGNING_KEY_ID rule.
var KeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Key is one custodied key's public description.
type Key struct {
	KeyID     string
	State     State
	Algorithm string
	PublicKey ed25519.PublicKey
}

// Custody holds signing keys and exposes sign and public-key operations only
// (ACP-ADR-02 §1). No method returns private material.
type Custody interface {
	// Keys returns every configured key's public description and state. It
	// performs I/O and must be called with a bounded context.
	Keys(ctx context.Context) ([]Key, error)
	// Sign returns the Ed25519 signature of message by the key kid. Only an
	// active key signs.
	Sign(ctx context.Context, kid string, message []byte) ([]byte, error)
}

// Keys is not implemented yet.
func (s FileSource) Keys(ctx context.Context) ([]Key, error) { return nil, nil }

// Sign is not implemented yet.
func (s FileSource) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	return nil, nil
}

// FileCustody is file custody of a key set; not implemented yet.
type FileCustody struct{}

// NewFileCustody returns custody for the manifest at path.
func NewFileCustody(path string) *FileCustody { return &FileCustody{} }

// Keys is not implemented yet.
func (c *FileCustody) Keys(ctx context.Context) ([]Key, error) { return nil, nil }

// Sign is not implemented yet.
func (c *FileCustody) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	return nil, nil
}

type manifestEntry struct {
	KeyID string
	State State
	File  string
}

func parseManifest(raw []byte) ([]manifestEntry, error) { return nil, nil }

func bareFileName(name string) bool { return true }
