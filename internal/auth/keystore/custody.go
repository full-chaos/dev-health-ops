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
	ReasonNotOneActiveKey     Reason = "keyset_not_one_active_key"
	ReasonDuplicatePublicKey  Reason = "keyset_duplicate_public_key"
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

// StateOverride replaces each key's state with its effective state from a
// durable state source (the rotation state machine). It returns the keys in
// the same order with only State changed, or an error that refuses the set.
type StateOverride func(ctx context.Context, keys []Key) ([]Key, error)

// Keys describes the single key as an active one-key set, in its effective
// state when States is set.
func (s FileSource) Keys(ctx context.Context) ([]Key, error) {
	metadata, err := s.Describe(ctx)
	if err != nil {
		return nil, err
	}
	keys := []Key{{KeyID: metadata.KeyID, State: StateActive, Algorithm: Algorithm, PublicKey: metadata.PublicKey}}
	if s.States == nil {
		return keys, nil
	}
	return s.States(ctx, keys)
}

// Sign signs with the single key, re-reading it under the file contract. With
// States set, the key signs only while its effective state is active, both
// before and after the signature is made.
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
	if s.States == nil {
		return signWithFile(s.Path, message)
	}
	key, err := s.effectiveActive(ctx)
	if err != nil {
		return nil, err
	}
	signature, signer, err := signWithFileKey(s.Path, message)
	if err != nil {
		return nil, err
	}
	if !signer.Equal(key.PublicKey) {
		return nil, failure(ReasonKeyNotActive, nil)
	}
	if _, err := s.effectiveActive(ctx); err != nil {
		return nil, err
	}
	return signature, nil
}

func (s FileSource) effectiveActive(ctx context.Context) (Key, error) {
	keys, err := s.Keys(ctx)
	if err != nil {
		return Key{}, err
	}
	if len(keys) != 1 || keys[0].State != StateActive {
		return Key{}, failure(ReasonKeyNotActive, nil)
	}
	return keys[0], nil
}

func signWithFile(path string, message []byte) ([]byte, error) {
	signature, _, err := signWithFileKey(path, message)
	return signature, err
}

// signWithFileKey signs and also returns the public half of the key that
// signed, so a caller can check it is the key it read before.
func signWithFileKey(path string, message []byte) ([]byte, ed25519.PublicKey, error) {
	pemBytes, err := readCustodiedFile(path, MaxKeyFileBytes)
	if err != nil {
		return nil, nil, err
	}
	private, err := parseEd25519Private(pemBytes)
	if err != nil {
		return nil, nil, err
	}
	public, ok := private.Public().(ed25519.PublicKey)
	if !ok {
		return nil, nil, failure(ReasonWrongAlgorithm, nil)
	}
	return ed25519.Sign(private, message), public, nil
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
// the rotation state machine's: with a StateOverride, each key's state is the
// one it returns, and a compromise it reports latches here too.
type FileCustody struct {
	manifestPath string
	states       StateOverride
	compromised  *latch
}

// NewFileCustody returns custody for the manifest at path.
func NewFileCustody(path string) *FileCustody {
	return NewFileCustodyWithStates(path, nil)
}

// NewFileCustodyWithStates returns custody for the manifest at path whose key
// states come from states (nil: the manifest's own).
func NewFileCustodyWithStates(path string, states StateOverride) *FileCustody {
	return &FileCustody{manifestPath: path, states: states, compromised: &latch{kids: map[string]bool{}, publics: map[string]bool{}}}
}

var _ Custody = (*FileCustody)(nil)

type latch struct {
	mu      sync.Mutex
	kids    map[string]bool
	publics map[string]bool
}

// apply records the compromised kids and public keys of keys and returns the
// keys with the effective state: a key whose kid or public key was ever seen
// compromised is compromised.
func (l *latch) apply(keys []Key) []Key {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		if key.State == StateCompromised {
			l.kids[key.KeyID] = true
			l.publics[string(key.PublicKey)] = true
		}
	}
	out := make([]Key, len(keys))
	for i, key := range keys {
		out[i] = key
		if l.kids[key.KeyID] || l.publics[string(key.PublicKey)] {
			out[i].State = StateCompromised
		}
	}
	return out
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
	_, keys, err := c.readSet(ctx)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// readSet loads the manifest and reads every key it names under the file
// contract: a set with any key that is not custodied, or with one key under two
// kids, is refused whole. States are the effective ones (see latch).
func (c *FileCustody) readSet(ctx context.Context) ([]manifestEntry, []Key, error) {
	entries, err := c.load(ctx)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]Key, 0, len(entries))
	publics := map[string]bool{}
	for _, entry := range entries {
		pemBytes, err := readCustodiedFile(c.keyPath(entry), MaxKeyFileBytes)
		if err != nil {
			return nil, nil, err
		}
		public, err := parseEd25519Public(pemBytes)
		if err != nil {
			return nil, nil, err
		}
		if publics[string(public)] {
			return nil, nil, failure(ReasonDuplicatePublicKey, nil)
		}
		publics[string(public)] = true
		keys = append(keys, Key{KeyID: entry.KeyID, State: entry.State, Algorithm: Algorithm, PublicKey: public})
	}
	// Latch custody's own compromises before the override can fail.
	keys = c.compromised.apply(keys)
	if c.states != nil {
		if keys, err = c.states(ctx, keys); err != nil {
			return nil, nil, err
		}
	}
	return entries, c.compromised.apply(keys), nil
}

// Sign signs with kid when every key the manifest names is custodied and kid
// is its one active key, both before and after the signature is made. A rotation that lands while the key is read
// and used (the kid retired, replaced or compromised) discards the signature
// and returns ReasonKeyNotActive: a signature is returned only when the kid
// is still active at the manifest read that follows it.
func (c *FileCustody) Sign(ctx context.Context, kid string, message []byte) ([]byte, error) {
	entry, key, err := c.activeEntry(ctx, kid)
	if err != nil {
		return nil, err
	}
	if beforeSign != nil {
		beforeSign()
	}
	signature, signer, err := signWithFileKey(c.keyPath(entry), message)
	if err != nil {
		return nil, err
	}
	if !signer.Equal(key.PublicKey) {
		return nil, failure(ReasonKeyNotActive, nil)
	}
	if afterSign != nil {
		afterSign()
	}
	again, againKey, err := c.activeEntry(ctx, kid)
	if err != nil {
		return nil, err
	}
	if again.File != entry.File || !againKey.PublicKey.Equal(key.PublicKey) {
		return nil, failure(ReasonKeyNotActive, nil)
	}
	return signature, nil
}

// beforeSign is a test hook run between the first manifest read and the
// signature.
var beforeSign func()

// afterSign is a test hook run between the signature and the second manifest
// read.
var afterSign func()

// activeEntry returns kid's manifest entry and key when every key the
// manifest names is custodied, exactly one key is in effective state active,
// and that key is kid.
func (c *FileCustody) activeEntry(ctx context.Context, kid string) (manifestEntry, Key, error) {
	entries, keys, err := c.readSet(ctx)
	if err != nil {
		return manifestEntry{}, Key{}, err
	}
	active := 0
	for _, key := range keys {
		if key.State == StateActive {
			active++
		}
	}
	if active != 1 {
		return manifestEntry{}, Key{}, failure(ReasonNotOneActiveKey, nil)
	}
	for i, entry := range entries {
		if entry.KeyID != kid {
			continue
		}
		if keys[i].State != StateActive {
			return manifestEntry{}, Key{}, failure(ReasonKeyNotActive, nil)
		}
		return entry, keys[i], nil
	}
	return manifestEntry{}, Key{}, failure(ReasonKeyUnknown, nil)
}

func (c *FileCustody) keyPath(entry manifestEntry) string {
	return filepath.Join(filepath.Dir(c.manifestPath), entry.File)
}

func (c *FileCustody) load(ctx context.Context) ([]manifestEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.manifestPath == "" {
		return nil, failure(ReasonPathUnconfigured, nil)
	}
	raw, err := readCustodiedFile(c.manifestPath, MaxManifestBytes)
	if err != nil {
		return nil, err
	}
	return parseManifest(raw)
}

// parseManifest decodes a closed manifest: an unknown member, trailing data,
// an empty set, an invalid kid, state or file name, or a repeated kid or file
// is refused.
func parseManifest(raw []byte) ([]manifestEntry, error) {
	if err := exactMembers(raw); err != nil {
		return nil, err
	}
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

// exactMembers walks raw token by token: the document is {"keys": [...]} and
// each entry an object of "kid", "state" and "file", every member name exact
// (encoding/json matches names without case) and none repeated (encoding/json
// keeps the last).
func exactMembers(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	bad := func() error { return failure(ReasonManifestUnparseable, nil) }
	expect := func(want json.Delim) error {
		token, err := decoder.Token()
		if err != nil || token != want {
			return bad()
		}
		return nil
	}
	object := func(allowed map[string]bool, value func(name string) error) error {
		if err := expect('{'); err != nil {
			return err
		}
		seen := map[string]bool{}
		for decoder.More() {
			token, err := decoder.Token()
			name, ok := token.(string)
			if err != nil || !ok || !allowed[name] || seen[name] {
				return bad()
			}
			seen[name] = true
			if err := value(name); err != nil {
				return err
			}
		}
		return expect('}')
	}
	skip := func(string) error {
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return bad()
		}
		return nil
	}
	entryNames := map[string]bool{"kid": true, "state": true, "file": true}
	return object(map[string]bool{"keys": true}, func(string) error {
		if err := expect('['); err != nil {
			return err
		}
		for decoder.More() {
			if err := object(entryNames, skip); err != nil {
				return err
			}
		}
		return expect(']')
	})
}
