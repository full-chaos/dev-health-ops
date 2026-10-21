// Package token owns platform token signing. It is the only package that may
// import internal/auth/keystore (ACP-ADR-02 §1; TestOnlyTokenImportsKeystore).
// It holds no private key: custody signs, and this package builds what custody
// signs over.
package token

import (
	"context"
	"crypto/ed25519"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

// State is a key's rotation state.
type State = keystore.State

// The five rotation states.
const (
	StatePending     = keystore.StatePending
	StateActive      = keystore.StateActive
	StateRetiring    = keystore.StateRetiring
	StateRetired     = keystore.StateRetired
	StateCompromised = keystore.StateCompromised
)

// Key is one signing key's public description.
type Key struct {
	KeyID     string
	State     State
	Algorithm string
	PublicKey ed25519.PublicKey
}

// Reason is a bounded, path-free classification of a key-set failure.
type Reason string

const (
	ReasonNoKeys         Reason = "keyset_empty"
	ReasonNoActiveKey    Reason = "keyset_no_active_key"
	ReasonManyActiveKeys Reason = "keyset_many_active_keys"
	ReasonDuplicateKeyID Reason = "keyset_duplicate_key_id"
	ReasonInvalidKey     Reason = "keyset_invalid_key"
	ReasonProbeMismatch  Reason = "signing_probe_mismatch"
	ReasonInvalidInput   Reason = "token_input_invalid"
)

// Error is a key-set or signing failure carrying only its Reason outwards.
type Error struct {
	Reason Reason
}

func (e *Error) Error() string { return string(e.Reason) }

func failure(reason Reason) error { return &Error{Reason: reason} }

// ReasonOf returns the bounded reason of an error from this package or from
// custody, and a generic label otherwise. It never returns err.Error().
func ReasonOf(err error) string {
	var own *Error
	if errors.As(err, &own) {
		return string(own.Reason)
	}
	return keystore.ReasonOf(err)
}

// KeySet is a set of signing keys; not implemented yet.
type KeySet struct{}

// NewKeySet is not implemented yet.
func NewKeySet(keys []Key) (KeySet, error) { return KeySet{}, nil }

// LoadKeySet is not implemented yet.
func LoadKeySet(ctx context.Context, custody keystore.Custody) (KeySet, error) { return KeySet{}, nil }

// Active is not implemented yet.
func (s KeySet) Active() Key { return Key{} }

// Published is not implemented yet.
func (s KeySet) Published() []Key { return nil }

// Keys is not implemented yet.
func (s KeySet) Keys() []Key { return nil }
