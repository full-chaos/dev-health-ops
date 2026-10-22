package token

import (
	"context"
	"crypto/ed25519"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

// JWKSCacheTTL is the longest a consumer may cache the published key set: the
// JWKS Cache-Control max-age. A pending key is published at least this long
// before it signs, and a key state snapshot is used at most this long after
// its last good read.
const JWKSCacheTTL = 5 * time.Minute

// KeyStateRefresh is how often a KeyStates snapshot is read again.
const KeyStateRefresh = 15 * time.Second

// Reasons of the durable key state overlay.
const (
	// ReasonKeyStatesUnavailable: no key state snapshot newer than the
	// staleness bound, so nothing signs and nothing is published.
	ReasonKeyStatesUnavailable Reason = "key_states_unavailable"
	// ReasonKeyStateMismatch: custody holds a key under a registered kid whose
	// registered public key is another.
	ReasonKeyStateMismatch Reason = "key_state_public_key_mismatch"
)

// KeyStateRow is one key's durable rotation state (the signing_keys table).
type KeyStateRow struct {
	KeyID     string
	State     State
	PublicKey ed25519.PublicKey
}

// KeyStateSource reads every key's durable rotation state.
type KeyStateSource interface {
	KeyStates(ctx context.Context) ([]KeyStateRow, error)
}

// KeyStatesConfig configures KeyStates. Zero durations take the defaults
// (KeyStateRefresh, JWKSCacheTTL); a nil Now is time.Now.
type KeyStatesConfig struct {
	Source       KeyStateSource
	RefreshEvery time.Duration
	MaxStale     time.Duration
	Now          func() time.Time
}

// KeyStates is a cached snapshot of the durable key states. A failed read
// keeps the last good snapshot until it is MaxStale old; after that the
// snapshot is unavailable and custody fails closed.
type KeyStates struct {
	source       KeyStateSource
	refreshEvery time.Duration
	maxStale     time.Duration
	now          func() time.Time

	mu        sync.Mutex
	rows      []KeyStateRow
	readAt    time.Time
	attemptAt time.Time
	have      bool
	attempted bool
}

// NewKeyStates returns a snapshot cache over config.Source.
func NewKeyStates(config KeyStatesConfig) *KeyStates {
	states := &KeyStates{source: config.Source, refreshEvery: config.RefreshEvery, maxStale: config.MaxStale, now: config.Now}
	if states.refreshEvery <= 0 {
		states.refreshEvery = KeyStateRefresh
	}
	if states.maxStale <= 0 {
		states.maxStale = JWKSCacheTTL
	}
	if states.now == nil {
		states.now = time.Now
	}
	return states
}

// Rows returns the current snapshot. It reads the source at most once per
// refresh interval.
func (s *KeyStates) Rows(ctx context.Context) ([]KeyStateRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.attempted || now.Sub(s.attemptAt) >= s.refreshEvery {
		s.attempted, s.attemptAt = true, now
		if rows, err := s.source.KeyStates(ctx); err == nil {
			s.rows, s.readAt, s.have = rows, now, true
		}
	}
	if !s.have || now.Sub(s.readAt) > s.maxStale {
		return nil, failure(ReasonKeyStatesUnavailable)
	}
	return s.rows, nil
}

// override is the custody StateOverride over this snapshot.
func (s *KeyStates) override(ctx context.Context, keys []keystore.Key) ([]keystore.Key, error) {
	rows, err := s.Rows(ctx)
	if err != nil {
		return nil, err
	}
	return applyKeyStates(keys, rows)
}

// applyKeyStates gives custody's keys their effective states. With no rows
// the custody states stand (a deployment that has not adopted the rotation
// state machine). Otherwise the rows are authoritative: a key with no row is
// neither published nor signing (retired), and a key is compromised when its
// row, a compromised row's public key, or custody says so.
func applyKeyStates(keys []keystore.Key, rows []KeyStateRow) ([]keystore.Key, error) {
	if len(rows) == 0 {
		return keys, nil
	}
	byKid := make(map[string]KeyStateRow, len(rows))
	denied := map[string]bool{}
	for _, row := range rows {
		byKid[row.KeyID] = row
		if row.State == StateCompromised {
			denied[string(row.PublicKey)] = true
		}
	}
	out := make([]keystore.Key, len(keys))
	for i, key := range keys {
		out[i] = key
		row, registered := byKid[key.KeyID]
		switch {
		case key.State == StateCompromised, denied[string(key.PublicKey)],
			registered && row.State == StateCompromised:
			out[i].State = StateCompromised
		case !registered:
			out[i].State = StateRetired
		case !row.PublicKey.Equal(key.PublicKey):
			return nil, failure(ReasonKeyStateMismatch)
		default:
			out[i].State = row.State
		}
	}
	return out, nil
}
