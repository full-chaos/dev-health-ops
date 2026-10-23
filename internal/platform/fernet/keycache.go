package fernet

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/crypto/pbkdf2"
)

// cacheLimit bounds the derived keys held at once. A process uses one or two
// (key, salt) pairs; the bound only stops growth when callers feed many.
const cacheLimit = 8

// derivation is one derived key. It never prints: a formatted value of this
// type carries no key bytes.
type derivation struct {
	once sync.Once
	key  []byte
	err  error
}

func (*derivation) String() string   { return "fernet.derivation" }
func (*derivation) GoString() string { return "fernet.derivation" }

// cacheID is the digest naming a cache entry; it never prints.
type cacheID [sha256.Size]byte

func (cacheID) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("fernet.cacheID")) }

var (
	cacheMu      sync.Mutex
	cacheEntries = map[cacheID]*derivation{}
	cacheOrder   []cacheID
	derivations  atomic.Uint64

	// deriveKey is the real derivation; tests replace it to count or fail.
	deriveKey = func(key, salt []byte) ([]byte, error) {
		return pbkdf2.Key(key, salt, pbkdf2Rounds, 32, sha256.New), nil
	}
)

// Derivations counts the real key derivations this process has run.
func Derivations() uint64 { return derivations.Load() }

// cacheKey identifies a (key, salt) pair without ambiguity: the key's length
// is part of the hashed input, so ("ab","c") and ("a","bc") differ.
func cacheKey(key, salt []byte) cacheID {
	hash := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	hash.Write(length[:])
	hash.Write(key)
	hash.Write(salt)
	var out cacheID
	copy(out[:], hash.Sum(nil))
	return out
}

// derivedKey returns the PBKDF2 key for (key, salt), deriving it once per
// pair: concurrent first uses wait for one derivation, and a derivation error
// is never kept.
func derivedKey(key, salt []byte) ([]byte, error) {
	id := cacheKey(key, salt)
	cacheMu.Lock()
	entry, ok := cacheEntries[id]
	if !ok {
		entry = &derivation{}
		cacheEntries[id] = entry
		cacheOrder = append(cacheOrder, id)
		for len(cacheOrder) > cacheLimit {
			delete(cacheEntries, cacheOrder[0])
			cacheOrder = cacheOrder[1:]
		}
	}
	cacheMu.Unlock()
	entry.once.Do(func() {
		derivations.Add(1)
		entry.key, entry.err = deriveKey(key, salt)
	})
	if entry.err != nil {
		cacheMu.Lock()
		if cacheEntries[id] == entry {
			delete(cacheEntries, id)
			for index, held := range cacheOrder {
				if held == id {
					cacheOrder = append(cacheOrder[:index], cacheOrder[index+1:]...)
					break
				}
			}
		}
		cacheMu.Unlock()
		return nil, entry.err
	}
	return entry.key, nil
}
