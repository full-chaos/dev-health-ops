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
	// ReasonKeyStateInvalid: a row carries a state outside the five rotation
	// states.
	ReasonKeyStateInvalid Reason = "key_state_invalid"
)

// KeyStateRow is one key's durable rotation state (the signing_keys table).
type KeyStateRow struct {
	KeyID     string
	State     State
	PublicKey ed25519.PublicKey
}

// KeyStateSource reads every key's durable rotation state. On an error it
// returns the rows it could read (a compromised row whose public key it could
// not read carries its kid only); they are latched, never used.
type KeyStateSource interface {
	KeyStates(ctx context.Context) ([]KeyStateRow, error)
}

// KeyStateReadTimeout bounds one read of the durable key states. The read runs
// apart from any request, so a stalled database holds no request for it.
const KeyStateReadTimeout = 5 * time.Second

// KeyStateInlineWait is how long the one request that starts a refresh waits
// for it before answering from the snapshot it has: a healthy database lands
// the new states in that call, a stalled one costs that request this much.
const KeyStateInlineWait = 100 * time.Millisecond

// KeyStatesConfig configures KeyStates. Zero durations take the defaults
// (KeyStateRefresh, JWKSCacheTTL, KeyStateReadTimeout, KeyStateInlineWait); a
// nil Now is time.Now.
type KeyStatesConfig struct {
	Source       KeyStateSource
	RefreshEvery time.Duration
	MaxStale     time.Duration
	ReadTimeout  time.Duration
	InlineWait   time.Duration
	Now          func() time.Time
}

// KeyStates is a cached snapshot of the durable key states. A failed read
// keeps the last good snapshot until it is MaxStale old; after that the
// snapshot is unavailable and custody fails closed. A read never runs under
// the lock or on a request's context: one read at a time runs on its own
// bounded context, and a request with a usable snapshot never waits for it.
type KeyStates struct {
	source       KeyStateSource
	refreshEvery time.Duration
	maxStale     time.Duration
	readTimeout  time.Duration
	inlineWait   time.Duration
	now          func() time.Time

	mu        sync.Mutex
	rows      []KeyStateRow
	readAt    time.Time
	attemptAt time.Time
	have      bool
	attempted bool
	// flight is the read in progress, nil when none.
	flight chan struct{}
	// base ends every read when the service stops.
	base context.Context
	stop context.CancelFunc

	// deniedKids and deniedPublics latch every compromise this process has
	// seen, from a row or from custody, for its life: no later snapshot (a
	// restored backup, an emptied table) and no custody adapter re-enables it.
	latchMu       sync.Mutex
	deniedKids    map[string]bool
	deniedPublics map[string]bool
}

// NewKeyStates returns a snapshot cache over config.Source.
func NewKeyStates(config KeyStatesConfig) *KeyStates {
	states := &KeyStates{
		source: config.Source, refreshEvery: config.RefreshEvery, maxStale: config.MaxStale,
		readTimeout: config.ReadTimeout, inlineWait: config.InlineWait, now: config.Now,
		deniedKids: map[string]bool{}, deniedPublics: map[string]bool{},
	}
	if states.refreshEvery <= 0 {
		states.refreshEvery = KeyStateRefresh
	}
	if states.maxStale <= 0 {
		states.maxStale = JWKSCacheTTL
	}
	states.base, states.stop = context.WithCancel(context.Background())
	if states.readTimeout <= 0 {
		states.readTimeout = KeyStateReadTimeout
	}
	if states.inlineWait <= 0 {
		states.inlineWait = KeyStateInlineWait
	}
	if states.now == nil {
		states.now = time.Now
	}
	return states
}

// Rows returns the current snapshot. A read starts at most once per refresh
// interval and runs apart from the caller: with a snapshot inside the
// staleness bound Rows returns it at once, and with none (first call, or older
// than the bound) it waits for the read in progress, never longer than ctx.
// Every compromised row of a read is latched as the read lands.
func (s *KeyStates) Rows(ctx context.Context) ([]KeyStateRow, error) {
	s.mu.Lock()
	now := s.now()
	started := false
	if (!s.attempted || now.Sub(s.attemptAt) >= s.refreshEvery) && s.flight == nil {
		s.attempted, s.attemptAt, started = true, now, true
		s.flight = make(chan struct{})
		go s.read(s.flight, now)
	}
	flight := s.flight
	rows, usable := s.snapshot(now)
	s.mu.Unlock()
	if usable && !started {
		return rows, nil
	}
	if flight != nil {
		var timer <-chan time.Time
		if usable {
			t := time.NewTimer(s.inlineWait)
			defer t.Stop()
			timer = t.C
		}
		select {
		case <-flight:
		case <-timer:
			return rows, nil
		case <-ctx.Done():
			if usable {
				return rows, nil
			}
			return nil, failure(ReasonKeyStatesUnavailable)
		}
		s.mu.Lock()
		rows, usable = s.snapshot(now)
		s.mu.Unlock()
	}
	if usable {
		return rows, nil
	}
	return nil, failure(ReasonKeyStatesUnavailable)
}

// Close ends the read in flight and every later one; a caller with a usable
// snapshot keeps getting it, one without is refused.
func (s *KeyStates) Close() { s.stop() }

// snapshot is the held rows and whether they are inside the staleness bound.
func (s *KeyStates) snapshot(now time.Time) ([]KeyStateRow, bool) {
	return s.rows, s.have && now.Sub(s.readAt) <= s.maxStale
}

// read is the one read in flight. A failed read may still return rows: their
// compromises latch before the read is discarded.
func (s *KeyStates) read(flight chan struct{}, at time.Time) {
	ctx, cancel := context.WithTimeout(s.base, s.readTimeout)
	defer cancel()
	rows, err := s.source.KeyStates(ctx)
	s.latch(nil, rows)
	s.mu.Lock()
	if err == nil {
		s.rows, s.readAt, s.have = rows, at, true
	}
	s.flight = nil
	s.mu.Unlock()
	close(flight)
}

// override is the custody StateOverride over this snapshot, with every
// compromise seen so far latched. The latch always comes first: a compromise
// shown by custody or by the snapshot is recorded before anything in this call
// can fail, so a call that fails closed still latches it.
func (s *KeyStates) override(ctx context.Context, keys []keystore.Key) ([]keystore.Key, error) {
	s.latch(keys, nil)
	rows, err := s.Rows(ctx)
	if err != nil {
		return nil, err
	}
	applied, err := applyKeyStates(keys, rows)
	if err != nil {
		return nil, err
	}
	out := append([]keystore.Key(nil), applied...)
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	for i := range out {
		if s.deniedKids[out[i].KeyID] || s.deniedPublics[string(out[i].PublicKey)] {
			out[i].State = StateCompromised
		}
	}
	return out, nil
}

// latch records every compromised key and row.
func (s *KeyStates) latch(keys []keystore.Key, rows []KeyStateRow) {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	for _, key := range keys {
		if key.State == StateCompromised {
			s.deniedKids[key.KeyID], s.deniedPublics[string(key.PublicKey)] = true, true
		}
	}
	for _, row := range rows {
		if row.State == StateCompromised {
			s.deniedKids[row.KeyID], s.deniedPublics[string(row.PublicKey)] = true, true
		}
	}
}

// applyKeyStates gives custody's keys their effective states. With no rows
// the custody states stand (a deployment that has not adopted the rotation
// state machine). Otherwise the rows are authoritative: a key with no row is
// neither published nor signing (retired), and a key is compromised when its
// row, a compromised row's public key, or custody says so. A row in an
// unknown state, then a registered kid with another public key, refuse the
// whole set; neither depends on row order.
func applyKeyStates(keys []keystore.Key, rows []KeyStateRow) ([]keystore.Key, error) {
	if len(rows) == 0 {
		return keys, nil
	}
	byKid := make(map[string]KeyStateRow, len(rows))
	denied := map[string]bool{}
	for _, row := range rows {
		if !row.State.Valid() {
			return nil, failure(ReasonKeyStateInvalid)
		}
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
