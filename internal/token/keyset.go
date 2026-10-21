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
	// ReasonDuplicatePublicKey: one key listed under two kids.
	ReasonDuplicatePublicKey Reason = "keyset_duplicate_public_key"
	ReasonInvalidKey         Reason = "keyset_invalid_key"
	ReasonProbeMismatch      Reason = "signing_probe_mismatch"
	ReasonInvalidInput       Reason = "token_input_invalid"
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

// KeySet is a validated set of signing keys: exactly one active key (G-61:
// one active issuer per token class), unique kids.
type KeySet struct {
	keys   []Key
	active int
}

// NewKeySet validates keys.
func NewKeySet(keys []Key) (KeySet, error) {
	if len(keys) == 0 {
		return KeySet{}, failure(ReasonNoKeys)
	}
	seen := map[string]bool{}
	active := -1
	for i, key := range keys {
		if !keystore.KeyIDPattern.MatchString(key.KeyID) || !key.State.Valid() ||
			key.Algorithm != keystore.Algorithm || len(key.PublicKey) != ed25519.PublicKeySize {
			return KeySet{}, failure(ReasonInvalidKey)
		}
		if seen[key.KeyID] {
			return KeySet{}, failure(ReasonDuplicateKeyID)
		}
		seen[key.KeyID] = true
		if key.State == StateActive {
			if active >= 0 {
				return KeySet{}, failure(ReasonManyActiveKeys)
			}
			active = i
		}
	}
	if active < 0 {
		return KeySet{}, failure(ReasonNoActiveKey)
	}
	return KeySet{keys: append([]Key(nil), keys...), active: active}, nil
}

// LoadKeySet reads and validates custody's key set.
func LoadKeySet(ctx context.Context, custody keystore.Custody) (KeySet, error) {
	custodied, err := custody.Keys(ctx)
	if err != nil {
		return KeySet{}, err
	}
	keys := make([]Key, len(custodied))
	for i, key := range custodied {
		keys[i] = Key{KeyID: key.KeyID, State: key.State, Algorithm: key.Algorithm, PublicKey: key.PublicKey}
	}
	return NewKeySet(keys)
}

// Active is the key that signs.
func (s KeySet) Active() Key { return s.keys[s.active] }

// Published is the keys a JWKS serves: pending, active and retiring, in set
// order. Retired and compromised keys are never published.
func (s KeySet) Published() []Key {
	var out []Key
	for _, key := range s.keys {
		switch key.State {
		case StatePending, StateActive, StateRetiring:
			out = append(out, key)
		}
	}
	return out
}

// Keys is every key in the set, in set order.
func (s KeySet) Keys() []Key { return append([]Key(nil), s.keys...) }
