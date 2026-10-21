package keystore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
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

// Keys describes the single key as an active one-key set.
func (s FileSource) Keys(ctx context.Context) ([]Key, error) {
	metadata, err := s.Describe(ctx)
	if err != nil {
		return nil, err
	}
	return []Key{{KeyID: metadata.KeyID, State: StateActive, Algorithm: Algorithm, PublicKey: metadata.PublicKey}}, nil
}

// Sign signs with the single key, re-reading it under the file contract.
func (s FileSource) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Path == "" {
		return nil, failure(ReasonPathUnconfigured, nil)
	}
	if s.KeyID == "" {
		return nil, failure(ReasonKeyIDUnconfigured, nil)
	}
	if kid != s.KeyID {
		return nil, failure(ReasonKeyUnknown, nil)
	}
	return signWithFile(s.Path, message)
}

func signWithFile(path string, message []byte) ([]byte, error) {
	pemBytes, err := readCustodiedFile(path, MaxKeyFileBytes)
	if err != nil {
		return nil, err
	}
	private, err := parseEd25519Private(pemBytes)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(private, message), nil
}

// FileCustody is file custody of a key set (ACP-ADR-02 §3). A manifest names
// the keys:
//
//	{"keys": [{"kid": "auth-2026-10", "state": "active", "file": "auth-2026-10.pem"}]}
//
// Each file is a bare name in the manifest's directory. The manifest and every
// key file are re-read under the file contract on each call, so an atomic
// rename of the manifest rotates the set with no restart. A key file is
// immutable per kid: a new key is a new file.
//
// A kid this value has seen compromised stays compromised for its life, even
// when a later manifest names it in another state. The durable deny list is
// the rotation state machine's.
type FileCustody struct {
	manifestPath string
	compromised  *latch
}

// NewFileCustody returns custody for the manifest at path.
func NewFileCustody(path string) *FileCustody {
	return &FileCustody{manifestPath: path, compromised: &latch{seen: map[string]bool{}}}
}

var _ Custody = (*FileCustody)(nil)

type latch struct {
	mu   sync.Mutex
	seen map[string]bool
}

// apply records the compromised kids of entries and returns each entry's
// effective state.
func (l *latch) apply(entries []manifestEntry) []State {
	l.mu.Lock()
	defer l.mu.Unlock()
	states := make([]State, len(entries))
	for i, entry := range entries {
		if entry.State == StateCompromised {
			l.seen[entry.KeyID] = true
		}
		states[i] = entry.State
		if l.seen[entry.KeyID] {
			states[i] = StateCompromised
		}
	}
	return states
}

type manifestEntry struct {
	KeyID string `json:"kid"`
	State State  `json:"state"`
	File  string `json:"file"`
}

type manifest struct {
	Keys []manifestEntry `json:"keys"`
}

// Keys returns the manifest's keys with their public halves.
func (c *FileCustody) Keys(ctx context.Context) ([]Key, error) {
	entries, states, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	keys := make([]Key, 0, len(entries))
	for i, entry := range entries {
		pemBytes, err := readCustodiedFile(c.keyPath(entry), MaxKeyFileBytes)
		if err != nil {
			return nil, err
		}
		public, err := parseEd25519Public(pemBytes)
		if err != nil {
			return nil, err
		}
		keys = append(keys, Key{KeyID: entry.KeyID, State: states[i], Algorithm: Algorithm, PublicKey: public})
	}
	return keys, nil
}

// Sign signs with kid when the manifest names it active.
func (c *FileCustody) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	entries, states, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	for i, entry := range entries {
		if entry.KeyID != kid {
			continue
		}
		if states[i] != StateActive {
			return nil, failure(ReasonKeyNotActive, nil)
		}
		return signWithFile(c.keyPath(entry), message)
	}
	return nil, failure(ReasonKeyUnknown, nil)
}

func (c *FileCustody) keyPath(entry manifestEntry) string {
	return filepath.Join(filepath.Dir(c.manifestPath), entry.File)
}

func (c *FileCustody) load(ctx context.Context) ([]manifestEntry, []State, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if c.manifestPath == "" {
		return nil, nil, failure(ReasonPathUnconfigured, nil)
	}
	raw, err := readCustodiedFile(c.manifestPath, MaxManifestBytes)
	if err != nil {
		return nil, nil, err
	}
	entries, err := parseManifest(raw)
	if err != nil {
		return nil, nil, err
	}
	return entries, c.compromised.apply(entries), nil
}

// parseManifest decodes a closed manifest: an unknown member, trailing data,
// an empty set, an invalid kid, state or file name, or a repeated kid or file
// is refused.
func parseManifest(raw []byte) ([]manifestEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document manifest
	if err := decoder.Decode(&document); err != nil {
		return nil, failure(ReasonManifestUnparseable, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, failure(ReasonManifestUnparseable, nil)
	}
	if len(document.Keys) == 0 {
		return nil, failure(ReasonManifestInvalid, nil)
	}
	kids, files := map[string]bool{}, map[string]bool{}
	for _, entry := range document.Keys {
		if !KeyIDPattern.MatchString(entry.KeyID) || !entry.State.Valid() || !bareFileName(entry.File) ||
			kids[entry.KeyID] || files[entry.File] {
			return nil, failure(ReasonManifestInvalid, nil)
		}
		kids[entry.KeyID], files[entry.File] = true, true
	}
	return document.Keys, nil
}

// bareFileName admits a name in the manifest's own directory only.
func bareFileName(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 255 &&
		!strings.ContainsAny(name, "/\\\x00") && filepath.Base(name) == name
}
