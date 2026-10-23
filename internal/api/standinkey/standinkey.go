// Package standinkey is the verification material an intake handler checks a delivery against.
// A provider whose credential is not set gets a per-process random key no caller can know, so its
// deliveries fail verification after the same work as a delivery to a configured provider.
package standinkey

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"sync/atomic"
)

const keyLength = 32

// standIn is made once at startup; a failed random read stops the process, it never yields an empty
// or guessable key (an empty key would let anyone sign). It is never logged or exported.
var standIn = MustRandomKey(rand.Reader)

// MustRandomKey reads a key from source and panics when it cannot read all of it.
func MustRandomKey(source io.Reader) string {
	key := make([]byte, keyLength)
	if _, err := io.ReadFull(source, key); err != nil {
		panic("standinkey: no random bytes for the stand-in verification key: " + err.Error())
	}
	return string(key)
}

// StandIn is the process-wide stand-in key (for tests of its shape; production code uses Secret).
func StandIn() string { return standIn }

// Secret is the credential a delivery is checked against: the configured one, else the stand-in.
func Secret(configured string) string {
	if configured != "" {
		return configured
	}
	return standIn
}

// Key is the fixed-size material a handler verifies with. It is made once when the handler is built
// (startup), so the work of a request does not depend on the length of the credential or on whether
// one is set. HMACKey is at most one SHA-256 block (a longer credential is hashed first, which is what
// HMAC does with it); Digest is the SHA-256 of the credential, for a plain token compare.
type Key struct {
	HMACKey []byte
	Digest  [sha256.Size]byte
}

const sha256BlockSize = 64

var derivations atomic.Int64

// Derivations counts New calls; a test pins that no request makes one.
func Derivations() int64 { return derivations.Load() }

// New derives the material of a configured credential, or of the stand-in when it is empty.
func New(configured string) Key {
	derivations.Add(1)
	secret := []byte(Secret(configured))
	key := secret
	if len(key) > sha256BlockSize {
		hashed := sha256.Sum256(key)
		key = hashed[:]
	}
	return Key{HMACKey: key, Digest: sha256.Sum256(secret)}
}
