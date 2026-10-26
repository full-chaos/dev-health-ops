package token

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
)

// fakeStates is a KeyStateSource whose rows and failure a test sets.
type fakeStates struct {
	mu    sync.Mutex
	rows  []KeyStateRow
	err   error
	reads int
}

func (f *fakeStates) KeyStates(context.Context) ([]KeyStateRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	return append([]KeyStateRow(nil), f.rows...), nil
}

func (f *fakeStates) set(rows []KeyStateRow, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows, f.err = rows, err
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func reasonIs(err error, reason Reason) bool {
	var own *Error
	return errors.As(err, &own) && own.Reason == reason
}

func newStates(source *fakeStates, clock *fakeClock) *KeyStates {
	return NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL, Now: clock.Now})
}

// A refresh that fails inside the staleness bound serves the last good
// snapshot.
func TestKeyStatesKeepTheLastGoodSnapshotInsideTheBound(t *testing.T) {
	source := &fakeStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := newStates(source, clock)
	ctx := context.Background()
	if rows, err := states.Rows(ctx); err != nil || len(rows) != 1 {
		t.Fatalf("first read = %v, %v", rows, err)
	}
	source.set(nil, errors.New("database unreachable"))
	clock.advance(JWKSCacheTTL)
	rows, err := states.Rows(ctx)
	if err != nil || len(rows) != 1 || rows[0].KeyID != "a" {
		t.Fatalf("read at the bound with the source down = %v, %v; want the last good snapshot", rows, err)
	}
	if source.reads != 2 {
		t.Fatalf("the source was read %d times, want 2 (the refresh was attempted)", source.reads)
	}
}

// Past the staleness bound with no good read, the snapshot is unavailable:
// custody signs and publishes nothing.
func TestKeyStatesFailClosedPastTheBound(t *testing.T) {
	source := &fakeStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := newStates(source, clock)
	ctx := context.Background()
	if _, err := states.Rows(ctx); err != nil {
		t.Fatal(err)
	}
	source.set(nil, errors.New("database unreachable"))
	clock.advance(JWKSCacheTTL + time.Second)
	if rows, err := states.Rows(ctx); !reasonIs(err, ReasonKeyStatesUnavailable) || rows != nil {
		t.Fatalf("read past the bound = %v, %v; want %s", rows, err, ReasonKeyStatesUnavailable)
	}
	source.set([]KeyStateRow{{KeyID: "a", State: StateActive}}, nil)
	clock.advance(10 * time.Second)
	if _, err := states.Rows(ctx); err != nil {
		t.Fatalf("a good read after the outage did not recover: %v", err)
	}
}

// With no read ever good there is no snapshot to fall back on.
func TestKeyStatesFailClosedBeforeTheFirstGoodRead(t *testing.T) {
	source := &fakeStates{err: errors.New("database unreachable")}
	states := newStates(source, &fakeClock{now: time.Unix(0, 0)})
	if _, err := states.Rows(context.Background()); !reasonIs(err, ReasonKeyStatesUnavailable) {
		t.Fatalf("Rows = %v, want %s", err, ReasonKeyStatesUnavailable)
	}
}

// A compromised row is denied from the next refresh on: the signer stops
// signing with it and the published set drops it.
func TestACompromisedRowIsDeniedOnTheNextRefresh(t *testing.T) {
	dir := t.TempDir()
	publicA, publicB := writeKey(t, dir, "a"), writeKey(t, dir, "b")
	source := &fakeStates{rows: []KeyStateRow{
		{KeyID: "a", State: StateActive, PublicKey: publicA}, {KeyID: "b", State: StatePending, PublicKey: publicB},
	}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := newStates(source, clock)
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "pending"), States: states})
	ctx := context.Background()
	if _, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"x"}`)); err != nil || kid != "a" {
		t.Fatalf("Sign before the compromise = %q, %v", kid, err)
	}

	source.set([]KeyStateRow{
		{KeyID: "a", State: StateCompromised, PublicKey: publicA}, {KeyID: "b", State: StateActive, PublicKey: publicB},
	}, nil)
	if _, kid, _ := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"x"}`)); kid != "a" {
		t.Fatalf("inside the refresh interval the snapshot changed early: kid %q", kid)
	}
	clock.advance(10 * time.Second)
	_, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"x"}`))
	if err != nil || kid != "b" {
		t.Fatalf("Sign after the refresh = %q, %v; want the successor b", kid, err)
	}
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range set.Published() {
		if key.KeyID == "a" || key.PublicKey.Equal(publicA) {
			t.Fatalf("the compromised key is still published: %+v", set.Published())
		}
	}

	// The durable source "un-compromising" the key (a restored backup) does
	// not bring it back in this process.
	source.set([]KeyStateRow{
		{KeyID: "a", State: StateActive, PublicKey: publicA}, {KeyID: "b", State: StateRetiring, PublicKey: publicB},
	}, nil)
	clock.advance(10 * time.Second)
	if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"x"}`)); err == nil {
		t.Fatal("a key seen compromised signed again")
	}
}

func TestApplyKeyStates(t *testing.T) {
	pa, pb, pc, px := ed25519.PublicKey(make32(1)), ed25519.PublicKey(make32(2)), ed25519.PublicKey(make32(3)), ed25519.PublicKey(make32(4))
	custody := []keystore.Key{
		{KeyID: "a", State: StatePending, PublicKey: pa},
		{KeyID: "b", State: StateActive, PublicKey: pb},
		{KeyID: "c", State: StateRetiring, PublicKey: pc},
	}
	states := func(keys []keystore.Key) []State {
		var out []State
		for _, key := range keys {
			out = append(out, key.State)
		}
		return out
	}
	cases := []struct {
		name    string
		keys    []keystore.Key
		rows    []KeyStateRow
		want    []State
		failure Reason
	}{
		{name: "an empty table keeps custody's states", keys: custody, rows: nil,
			want: []State{StatePending, StateActive, StateRetiring}},
		{name: "rows are authoritative", keys: custody, rows: []KeyStateRow{
			{KeyID: "a", State: StateActive, PublicKey: pa}, {KeyID: "b", State: StateRetiring, PublicKey: pb}, {KeyID: "c", State: StateRetired, PublicKey: pc}},
			want: []State{StateActive, StateRetiring, StateRetired}},
		{name: "a key with no row is neither published nor signing", keys: custody, rows: []KeyStateRow{
			{KeyID: "b", State: StateActive, PublicKey: pb}},
			want: []State{StateRetired, StateActive, StateRetired}},
		{name: "a compromised row denies its kid", keys: custody, rows: []KeyStateRow{
			{KeyID: "a", State: StatePending, PublicKey: pa}, {KeyID: "b", State: StateCompromised, PublicKey: pb}, {KeyID: "c", State: StateActive, PublicKey: pc}},
			want: []State{StatePending, StateCompromised, StateActive}},
		{name: "a compromised row with another public key still denies its kid", keys: custody, rows: []KeyStateRow{
			{KeyID: "b", State: StateCompromised, PublicKey: px}, {KeyID: "c", State: StateActive, PublicKey: pc}},
			want: []State{StateRetired, StateCompromised, StateActive}},
		{name: "a compromised public key is denied under every kid", keys: custody, rows: []KeyStateRow{
			{KeyID: "old", State: StateCompromised, PublicKey: pa}, {KeyID: "a", State: StateActive, PublicKey: pa}},
			want: []State{StateCompromised, StateRetired, StateRetired}},
		{name: "custody's compromised state wins over a row", keys: []keystore.Key{{KeyID: "a", State: StateCompromised, PublicKey: pa}},
			rows: []KeyStateRow{{KeyID: "a", State: StateActive, PublicKey: pa}}, want: []State{StateCompromised}},
		{name: "a registered kid with another public key refuses the set", keys: custody, rows: []KeyStateRow{
			{KeyID: "b", State: StateActive, PublicKey: px}}, failure: ReasonKeyStateMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := applyKeyStates(c.keys, c.rows)
			if c.failure != "" {
				if !reasonIs(err, c.failure) {
					t.Fatalf("applyKeyStates = %v, %v; want %s", got, err, c.failure)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if gotStates := states(got); len(gotStates) != len(c.want) || !equalStates(gotStates, c.want) {
				t.Fatalf("states = %v, want %v", gotStates, c.want)
			}
		})
	}
}

func equalStates(a, b []State) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func make32(seed byte) []byte {
	out := make([]byte, ed25519.PublicKeySize)
	for i := range out {
		out[i] = seed
	}
	return out
}

// A single key file under a durable state: compromised means it never signs.
func TestSingleKeyFileFollowsTheDurableState(t *testing.T) {
	dir := t.TempDir()
	public := writeKey(t, dir, "one")
	source := &fakeStates{rows: []KeyStateRow{{KeyID: "one", State: StateActive, PublicKey: public}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	signer := NewFileSigner(FileConfig{KeyPath: dir + "/one.pem", KeyID: "one", States: newStates(source, clock)})
	ctx := context.Background()
	if err := signer.Check(ctx); err != nil {
		t.Fatalf("Check with an active row: %v", err)
	}
	source.set([]KeyStateRow{{KeyID: "one", State: StateCompromised, PublicKey: public}}, nil)
	clock.advance(10 * time.Second)
	if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatal("a compromised single key signed")
	}
	if _, err := signer.KeySet(ctx); !reasonIs(err, ReasonNoActiveKey) {
		t.Fatalf("KeySet = %v, want %s", err, ReasonNoActiveKey)
	}
}

// CustodyKeys lists custody's own states, never the durable overlay: what an
// operator adopts.
func TestCustodyKeysIgnoresTheOverlay(t *testing.T) {
	dir := t.TempDir()
	writeKey(t, dir, "a")
	writeKey(t, dir, "b")
	source := &fakeStates{err: errors.New("database unreachable")}
	keys, err := CustodyKeys(context.Background(), FileConfig{
		KeySetPath: writeSet(t, dir, "a", "active", "b", "pending"),
		States:     newStates(source, &fakeClock{now: time.Unix(0, 0)}),
	})
	if err != nil || len(keys) != 2 || keys[0].State != StateActive || keys[1].State != StatePending {
		t.Fatalf("CustodyKeys = %+v, %v", keys, err)
	}
	if source.reads != 0 {
		t.Fatal("CustodyKeys read the durable states")
	}
}

// A compromise latches in the process whatever custody adapter signs: a
// single key file seen compromised never signs again, when the source later
// names it active (a restored backup) or holds no rows at all.
func TestACompromiseLatchesForEveryCustody(t *testing.T) {
	for _, layout := range []string{"single key file", "key-set manifest"} {
		for _, after := range []string{"restored active", "empty table"} {
			t.Run(layout+"/"+after, func(t *testing.T) {
				dir := t.TempDir()
				public := writeKey(t, dir, "one")
				source := &fakeStates{rows: []KeyStateRow{{KeyID: "one", State: StateActive, PublicKey: public}}}
				clock := &fakeClock{now: time.Unix(0, 0)}
				config := FileConfig{KeyPath: dir + "/one.pem", KeyID: "one", States: newStates(source, clock)}
				if layout == "key-set manifest" {
					config = FileConfig{KeySetPath: writeSet(t, dir, "one", "active"), States: newStates(source, clock)}
				}
				signer := NewFileSigner(config)
				ctx := context.Background()
				if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err != nil {
					t.Fatalf("Sign while active: %v", err)
				}
				source.set([]KeyStateRow{{KeyID: "one", State: StateCompromised, PublicKey: public}}, nil)
				clock.advance(10 * time.Second)
				if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
					t.Fatal("a compromised key signed")
				}
				rows := []KeyStateRow{{KeyID: "one", State: StateActive, PublicKey: public}}
				if after == "empty table" {
					rows = nil
				}
				source.set(rows, nil)
				clock.advance(10 * time.Second)
				if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
					t.Fatalf("a key seen compromised signed again after the source showed %s", after)
				}
				if set, err := signer.KeySet(ctx); err == nil {
					for _, key := range set.Published() {
						if key.PublicKey.Equal(public) {
							t.Fatal("a key seen compromised is published again")
						}
					}
				}
			})
		}
	}
}

// A compromised row latches its kid and public key even when custody does not
// hold that key yet: a later manifest that adds it finds it denied.
func TestACompromisedRowLatchesBeforeCustodyHoldsTheKey(t *testing.T) {
	dir := t.TempDir()
	publicA, publicB := writeKey(t, dir, "a"), writeKey(t, dir, "b")
	source := &fakeStates{rows: []KeyStateRow{
		{KeyID: "a", State: StateActive, PublicKey: publicA}, {KeyID: "b", State: StateCompromised, PublicKey: publicB},
	}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := newStates(source, clock)
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active"), States: states})
	ctx := context.Background()
	if _, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err != nil || kid != "a" {
		t.Fatalf("Sign = %q, %v", kid, err)
	}
	source.set(nil, nil)
	clock.advance(10 * time.Second)
	writeSet(t, dir, "a", "retiring", "b", "active")
	if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatal("a key the table once marked compromised signed after the table emptied")
	}
}

// Every early return of the overlay comes after the latch: a compromise seen
// in custody or in a read snapshot is latched even when that same call fails
// closed, and a later snapshot naming the key active does not re-enable it.
func TestEveryOverlayEarlyReturnLatchesFirst(t *testing.T) {
	pa, pb, px := ed25519.PublicKey(make32(1)), ed25519.PublicKey(make32(2)), ed25519.PublicKey(make32(9))
	custody := func(bState State) []keystore.Key {
		return []keystore.Key{{KeyID: "a", State: StateActive, PublicKey: pa}, {KeyID: "b", State: bState, PublicKey: pb}}
	}
	restored := []KeyStateRow{{KeyID: "a", State: StateRetiring, PublicKey: pa}, {KeyID: "b", State: StateActive, PublicKey: pb}}
	cases := map[string]struct {
		firstCustody []keystore.Key
		firstRows    []KeyStateRow
		firstErr     error
		want         Reason
	}{
		// applyKeyStates refuses the set (a's row names another public key)
		// in the same call that first shows b compromised.
		"a public key mismatch before a compromised row": {
			firstCustody: custody(StatePending),
			firstRows:    []KeyStateRow{{KeyID: "a", State: StateActive, PublicKey: px}, {KeyID: "b", State: StateCompromised, PublicKey: pb}},
			want:         ReasonKeyStateMismatch,
		},
		// The snapshot is unavailable in the call where custody shows b
		// compromised.
		"no snapshot while custody shows the compromise": {
			firstCustody: custody(StateCompromised),
			firstErr:     errors.New("database unreachable"),
			want:         ReasonKeyStatesUnavailable,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			source := &fakeStates{rows: c.firstRows, err: c.firstErr}
			clock := &fakeClock{now: time.Unix(0, 0)}
			states := newStates(source, clock)
			ctx := context.Background()
			if _, err := states.override(ctx, c.firstCustody); !reasonIs(err, c.want) {
				t.Fatalf("first call = %v, want %s", err, c.want)
			}
			source.set(restored, nil)
			clock.advance(10 * time.Second)
			out, err := states.override(ctx, custody(StatePending))
			if err != nil {
				t.Fatal(err)
			}
			if out[1].State != StateCompromised {
				t.Fatalf("b after the restored snapshot = %s; a compromise seen in a failing call was not latched", out[1].State)
			}
		})
	}
}

// The review repro end to end: a mismatch in the snapshot that first shows a
// compromise, then a restored snapshot, through the signer.
func TestAMismatchDoesNotSkipTheCompromiseLatch(t *testing.T) {
	dir := t.TempDir()
	publicA, publicB := writeKey(t, dir, "a"), writeKey(t, dir, "b")
	source := &fakeStates{rows: []KeyStateRow{
		{KeyID: "a", State: StateActive, PublicKey: ed25519.PublicKey(make32(9))}, {KeyID: "b", State: StateCompromised, PublicKey: publicB},
	}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "pending"), States: newStates(source, clock)})
	ctx := context.Background()
	if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatal("a snapshot with a public key mismatch signed")
	}
	source.set([]KeyStateRow{{KeyID: "a", State: StateRetiring, PublicKey: publicA}, {KeyID: "b", State: StateActive, PublicKey: publicB}}, nil)
	clock.advance(10 * time.Second)
	if _, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatalf("previously compromised key %q signed after a mismatch then a restored snapshot", kid)
	}
}

// permutations returns every order of rows.
func permutations(rows []KeyStateRow) [][]KeyStateRow {
	if len(rows) <= 1 {
		return [][]KeyStateRow{append([]KeyStateRow(nil), rows...)}
	}
	var out [][]KeyStateRow
	for i := range rows {
		rest := append(append([]KeyStateRow(nil), rows[:i]...), rows[i+1:]...)
		for _, tail := range permutations(rest) {
			out = append(out, append([]KeyStateRow{rows[i]}, tail...))
		}
	}
	return out
}

// Over every order of a mismatch row, a compromised row, an unknown-state row
// and a good row, the overlay latches the same set and fails with the same
// reason.
func TestRowOrderDoesNotChangeTheLatchOrTheError(t *testing.T) {
	pa, pb, pc, pd, px := ed25519.PublicKey(make32(1)), ed25519.PublicKey(make32(2)), ed25519.PublicKey(make32(3)), ed25519.PublicKey(make32(4)), ed25519.PublicKey(make32(9))
	custody := []keystore.Key{
		{KeyID: "a", State: StateActive, PublicKey: pa}, {KeyID: "b", State: StatePending, PublicKey: pb},
		{KeyID: "c", State: StatePending, PublicKey: pc}, {KeyID: "d", State: StatePending, PublicKey: pd},
	}
	mismatch := KeyStateRow{KeyID: "a", State: StateActive, PublicKey: px}
	compromisedRow := KeyStateRow{KeyID: "b", State: StateCompromised, PublicKey: pb}
	revoked := KeyStateRow{KeyID: "c", State: "revoked", PublicKey: pc}
	ok := KeyStateRow{KeyID: "d", State: StatePending, PublicKey: pd}
	for name, c := range map[string]struct {
		rows []KeyStateRow
		want Reason
	}{
		"mismatch, compromised, revoked, ok": {[]KeyStateRow{mismatch, compromisedRow, revoked, ok}, ReasonKeyStateInvalid},
		"mismatch, compromised, ok":          {[]KeyStateRow{mismatch, compromisedRow, ok}, ReasonKeyStateMismatch},
		"compromised, revoked, ok":           {[]KeyStateRow{compromisedRow, revoked, ok}, ReasonKeyStateInvalid},
	} {
		orders := permutations(c.rows)
		for _, rows := range orders {
			source := &fakeStates{rows: rows}
			states := newStates(source, &fakeClock{now: time.Unix(0, 0)})
			if _, err := states.override(context.Background(), custody); !reasonIs(err, c.want) {
				t.Fatalf("%s, order %v: err = %v, want %s", name, kidsOf(rows), err, c.want)
			}
			if len(states.deniedKids) != 1 || !states.deniedKids["b"] || len(states.deniedPublics) != 1 || !states.deniedPublics[string(pb)] {
				t.Fatalf("%s, order %v: latched kids %v, %d public keys; want exactly b", name, kidsOf(rows), states.deniedKids, len(states.deniedPublics))
			}
		}
		if want := map[int]int{4: 24, 3: 6}[len(c.rows)]; len(orders) != want {
			t.Fatalf("%s: %d orders, want %d", name, len(orders), want)
		}
	}
}

func kidsOf(rows []KeyStateRow) []string {
	var out []string
	for _, row := range rows {
		out = append(out, row.KeyID)
	}
	return out
}

// An overlay error after the latch fails closed: no signature, no key set,
// no readiness, never a partial success.
func TestAnOverlayErrorFailsClosed(t *testing.T) {
	for name, rows := range map[string]func(publicA, publicB ed25519.PublicKey) []KeyStateRow{
		"public key mismatch": func(_, publicB ed25519.PublicKey) []KeyStateRow {
			return []KeyStateRow{{KeyID: "a", State: StateActive, PublicKey: ed25519.PublicKey(make32(9))}, {KeyID: "b", State: StateCompromised, PublicKey: publicB}}
		},
		"unknown state": func(publicA, publicB ed25519.PublicKey) []KeyStateRow {
			return []KeyStateRow{{KeyID: "a", State: StateActive, PublicKey: publicA}, {KeyID: "b", State: "revoked", PublicKey: publicB}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			publicA, publicB := writeKey(t, dir, "a"), writeKey(t, dir, "b")
			source := &fakeStates{rows: rows(publicA, publicB)}
			signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "a", "active", "b", "pending"), States: newStates(source, &fakeClock{now: time.Unix(0, 0)})})
			ctx := context.Background()
			if compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil || compact != "" || kid != "" {
				t.Fatalf("Sign = %q, %q, %v; want no token and an error", compact, kid, err)
			}
			if set, err := signer.KeySet(ctx); err == nil || len(set.Keys()) != 0 {
				t.Fatalf("KeySet = %d keys, %v; want none and an error", len(set.Keys()), err)
			}
			if err := signer.Check(ctx); err == nil {
				t.Fatal("Check passed on a refused snapshot")
			}
		})
	}
}

// partialStates is a source whose read fails after returning some rows.
type partialStates struct {
	fakeStates
	partial []KeyStateRow
}

func (p *partialStates) KeyStates(ctx context.Context) ([]KeyStateRow, error) {
	p.mu.Lock()
	partial := p.partial
	p.mu.Unlock()
	if partial != nil {
		return partial, errors.New("row unreadable")
	}
	return p.fakeStates.KeyStates(ctx)
}

// A read that fails after reading a compromised row latches that row before
// the read is discarded.
func TestAFailedReadStillLatchesTheRowsItRead(t *testing.T) {
	pa, pb := ed25519.PublicKey(make32(1)), ed25519.PublicKey(make32(2))
	for name, partial := range map[string][]KeyStateRow{
		"a compromised row with its public key": {{KeyID: "b", State: StateCompromised, PublicKey: pb}},
		"a compromised row with its kid only":   {{KeyID: "b", State: StateCompromised}},
	} {
		t.Run(name, func(t *testing.T) {
			source := &partialStates{partial: partial}
			clock := &fakeClock{now: time.Unix(0, 0)}
			states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL, Now: clock.Now})
			custody := []keystore.Key{{KeyID: "a", State: StateActive, PublicKey: pa}, {KeyID: "b", State: StatePending, PublicKey: pb}}
			ctx := context.Background()
			if _, err := states.override(ctx, custody); !reasonIs(err, ReasonKeyStatesUnavailable) {
				t.Fatalf("override over a failed first read = %v", err)
			}
			source.mu.Lock()
			source.partial = nil
			source.rows = []KeyStateRow{{KeyID: "a", State: StateRetiring, PublicKey: pa}, {KeyID: "b", State: StateActive, PublicKey: pb}}
			source.mu.Unlock()
			clock.advance(10 * time.Second)
			out, err := states.override(ctx, custody)
			if err != nil {
				t.Fatal(err)
			}
			if out[1].State != StateCompromised {
				t.Fatalf("b = %s after a failed read showed it compromised", out[1].State)
			}
		})
	}
}

// hungStates blocks every read until its context ends, as a database behind a
// blackhole does.
type hungStates struct {
	mu    sync.Mutex
	reads int
	rows  []KeyStateRow
	hang  bool
}

func (h *hungStates) KeyStates(ctx context.Context) ([]KeyStateRow, error) {
	h.mu.Lock()
	h.reads++
	hang, rows := h.hang, append([]KeyStateRow(nil), h.rows...)
	h.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return rows, nil
}

func (h *hungStates) setHang(v bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hang = v
}

func (h *hungStates) readCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reads
}

func (s *KeyStates) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flight == nil
}

// A refresh against a hung source never holds a request: every concurrent
// caller answers from the last good snapshot within the inline wait, the
// source is read once, and the read ends at its own timeout.
func TestKeyStatesRefreshAgainstAHungSourceHoldsNoRequest(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL,
		ReadTimeout: 300 * time.Millisecond, InlineWait: 20 * time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.setHang(true)
	clock.advance(11 * time.Second)

	const callers = 200
	took := make([]time.Duration, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			rows, err := states.Rows(context.Background())
			took[i] = time.Since(start)
			if err != nil || len(rows) != 1 {
				t.Errorf("caller %d = %v, %v; want the last good snapshot", i, rows, err)
			}
		}()
	}
	wg.Wait()
	for i, d := range took {
		if d > 250*time.Millisecond {
			t.Errorf("caller %d waited %s on a hung source, want well under the read timeout 300ms", i, d)
		}
	}
	if got := source.readCount(); got != 2 {
		t.Errorf("the source was read %d times, want 2 (first read + one single-flight refresh)", got)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !states.idle() {
		if time.Now().After(deadline) {
			t.Fatal("the refresh goroutine did not end after its read timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// With no snapshot at all, a caller waits for the read but never longer than
// its own context, and is refused (fail closed), not served nothing.
func TestKeyStatesColdStartAgainstAHungSourceFailsClosedAtTheCallersDeadline(t *testing.T) {
	source := &hungStates{hang: true}
	states := NewKeyStates(KeyStatesConfig{Source: source, ReadTimeout: 2 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	rows, err := states.Rows(ctx)
	if !reasonIs(err, ReasonKeyStatesUnavailable) || rows != nil {
		t.Fatalf("cold read = %v, %v; want %s", rows, err, ReasonKeyStatesUnavailable)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("cold read took %s, want about the caller's 50ms", took)
	}
}

// A compromise that lands in a background refresh is applied by the very next
// call, and a later hung refresh does not bring the pre-compromise state back.
func TestKeyStatesBackgroundRefreshLatchesACompromiseAtOnce(t *testing.T) {
	pa := ed25519.PublicKey(make32(1))
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive, PublicKey: pa}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL, Now: clock.Now})
	ctx := context.Background()
	custody := func() []keystore.Key {
		return []keystore.Key{{KeyID: "a", State: StateActive, Algorithm: keystore.Algorithm, PublicKey: pa}}
	}
	if out, err := states.override(ctx, custody()); err != nil || out[0].State != StateActive {
		t.Fatalf("before = %v, %v", out, err)
	}
	source.mu.Lock()
	source.rows = []KeyStateRow{{KeyID: "a", State: StateCompromised, PublicKey: pa}}
	source.mu.Unlock()
	clock.advance(11 * time.Second)
	if out, err := states.override(ctx, custody()); err != nil || out[0].State != StateCompromised {
		t.Fatalf("after the refresh = %v, %v; want compromised in that call", out, err)
	}
	source.setHang(true)
	clock.advance(11 * time.Second)
	if out, err := states.override(ctx, custody()); err != nil || out[0].State != StateCompromised {
		t.Fatalf("during a hung refresh = %v, %v; want compromised", out, err)
	}
}

// Close ends a read stalled on the source at once (the goroutine leaves), and
// the caller that held a snapshot still gets it.
func TestKeyStatesCloseEndsTheReadInFlight(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL,
		ReadTimeout: time.Minute, InlineWait: time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.setHang(true)
	clock.advance(11 * time.Second)
	if rows, err := states.Rows(context.Background()); err != nil || len(rows) != 1 {
		t.Fatalf("during the hung refresh = %v, %v", rows, err)
	}
	if states.idle() {
		t.Fatal("no read in flight to end")
	}
	states.Close()
	deadline := time.Now().Add(2 * time.Second)
	for !states.idle() {
		if time.Now().After(deadline) {
			t.Fatal("Close did not end the read in flight")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// With a healthy database a state change (here a revocation) takes effect at
// the next refresh, in the call that starts it: the bound is the refresh
// period, as before the read moved off the request.
func TestKeyStatesRevokedStateTakesEffectAtTheNextRefresh(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL, Now: clock.Now})
	ctx := context.Background()
	if rows, _ := states.Rows(ctx); rows[0].State != StateActive {
		t.Fatalf("first = %v", rows)
	}
	source.mu.Lock()
	source.rows = []KeyStateRow{{KeyID: "a", State: StateRetired}}
	source.mu.Unlock()
	clock.advance(9 * time.Second)
	if rows, _ := states.Rows(ctx); rows[0].State != StateActive {
		t.Fatalf("inside the refresh period = %v, want the held snapshot", rows)
	}
	clock.advance(time.Second)
	if rows, _ := states.Rows(ctx); rows[0].State != StateRetired {
		t.Fatalf("at the refresh = %v, want the revoked state in that call", rows)
	}
}

// A snapshot that was inside the bound when a stalled refresh began is not
// served once the wait leaves it past the bound.
func TestKeyStatesDoNotServePastMaxStaleAfterAnInlineWait(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: time.Millisecond, MaxStale: 100 * time.Millisecond,
		ReadTimeout: time.Second, InlineWait: 30 * time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.setHang(true)
	clock.advance(90 * time.Millisecond)
	done := make(chan error, 1)
	go func() {
		_, err := states.Rows(context.Background())
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	clock.advance(20 * time.Millisecond)
	if err := <-done; !reasonIs(err, ReasonKeyStatesUnavailable) {
		t.Fatalf("Rows = %v, want %s once the wait left the snapshot past MaxStale", err, ReasonKeyStatesUnavailable)
	}
}

// steppedClock answers its first armed call with early and every later call
// with late, so a test can move the clock between two reads inside one call.
type steppedClock struct {
	mu          sync.Mutex
	base        time.Time
	armed       bool
	calls       int
	early, late time.Duration
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed {
		return c.base
	}
	c.calls++
	if c.calls == 1 {
		return c.base.Add(c.early)
	}
	return c.base.Add(c.late)
}

func (c *steppedClock) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = true
}

// The cached-snapshot fast path judges the age with the clock at return: a
// snapshot inside the bound at the start of the call is not served when the
// clock has left the bound by the return.
func TestKeyStatesFastPathRechecksTheAgeAtReturn(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &steppedClock{base: time.Unix(0, 0), early: 90 * time.Millisecond, late: 110 * time.Millisecond}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: time.Hour, MaxStale: 100 * time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.arm()
	if rows, err := states.Rows(context.Background()); !reasonIs(err, ReasonKeyStatesUnavailable) || rows != nil {
		t.Fatalf("fast path = %v, %v; want %s once the return-time clock is past MaxStale", rows, err, ReasonKeyStatesUnavailable)
	}
}

// slowExitStates honors cancellation but takes a while to return, as a driver
// unwinding a cancelled query does.
type slowExitStates struct {
	exited atomic.Bool
	rows   []KeyStateRow
	hang   atomic.Bool
}

func (h *slowExitStates) KeyStates(ctx context.Context) ([]KeyStateRow, error) {
	if !h.hang.Load() {
		return h.rows, nil
	}
	<-ctx.Done()
	time.Sleep(80 * time.Millisecond)
	h.exited.Store(true)
	return nil, ctx.Err()
}

// Close returns only after the read in flight has returned, and is safe to
// call again and with no read in flight.
func TestKeyStatesCloseWaitsForTheReadToEnd(t *testing.T) {
	source := &slowExitStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL,
		ReadTimeout: 5 * time.Second, InlineWait: time.Millisecond, Now: clock.Now})
	states.Close()
	states = NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL,
		ReadTimeout: 5 * time.Second, InlineWait: time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.hang.Store(true)
	clock.advance(11 * time.Second)
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	if states.idle() {
		t.Fatal("no read in flight")
	}
	states.Close()
	if !source.exited.Load() {
		t.Fatal("Close returned while the cancelled read was still running")
	}
	states.Close()
}

// After Close no read starts: a caller with a usable snapshot gets it, one
// without is refused, and the source is not read again.
func TestKeyStatesRowsAfterCloseStartNoRead(t *testing.T) {
	source := &hungStates{rows: []KeyStateRow{{KeyID: "a", State: StateActive}}}
	clock := &fakeClock{now: time.Unix(0, 0)}
	states := NewKeyStates(KeyStatesConfig{Source: source, RefreshEvery: 10 * time.Second, MaxStale: JWKSCacheTTL,
		InlineWait: time.Millisecond, Now: clock.Now})
	if _, err := states.Rows(context.Background()); err != nil {
		t.Fatal(err)
	}
	states.Close()
	reads := source.readCount()
	clock.advance(11 * time.Second)
	if rows, err := states.Rows(context.Background()); err != nil || len(rows) != 1 {
		t.Fatalf("Rows after Close with a usable snapshot = %v, %v", rows, err)
	}
	clock.advance(JWKSCacheTTL)
	if rows, err := states.Rows(context.Background()); !reasonIs(err, ReasonKeyStatesUnavailable) || rows != nil {
		t.Fatalf("Rows after Close past the bound = %v, %v; want %s", rows, err, ReasonKeyStatesUnavailable)
	}
	time.Sleep(20 * time.Millisecond)
	if got := source.readCount(); got != reads {
		t.Fatalf("the source was read %d more times after Close, want 0", got-reads)
	}
	if !states.idle() {
		t.Fatal("a read is in flight after Close")
	}
}
