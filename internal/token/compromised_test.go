package token

import (
	"context"
	"crypto/ed25519"
	"testing"
)

type compromiseLatch interface {
	Compromised(kid string, public ed25519.PublicKey) bool
}

func latchOf(t *testing.T, signer *Signer) compromiseLatch {
	t.Helper()
	latch, ok := any(signer).(compromiseLatch)
	if !ok {
		t.Fatal("Signer has no in-memory Compromised(kid, public) check")
	}
	return latch
}

// Compromised answers from custody's latch: a kid and a public key the
// manifest named compromised stay compromised; other keys are not.
func TestSignerCompromisedReadsTheCustodyLatch(t *testing.T) {
	dir := t.TempDir()
	active := writeKey(t, dir, "auth-a")
	bad := writeKey(t, dir, "auth-b")
	signer := NewFileSigner(FileConfig{KeySetPath: writeSet(t, dir, "auth-a", "active", "auth-b", "compromised")})
	if _, err := signer.KeySet(context.Background()); err != nil {
		t.Fatalf("key set: %v", err)
	}
	latch := latchOf(t, signer)
	if !latch.Compromised("auth-b", bad) || !latch.Compromised("other-kid", bad) || !latch.Compromised("auth-b", nil) {
		t.Fatal("a kid or public key custody latched compromised is not reported")
	}
	if latch.Compromised("auth-a", active) {
		t.Fatal("the active key is reported compromised")
	}
}

// Compromised answers from the durable key states' latch too: a row that a
// read landed compromised is reported by kid and by public key.
func TestSignerCompromisedReadsTheKeyStatesLatch(t *testing.T) {
	dir := t.TempDir()
	active := writeKey(t, dir, "auth-a")
	revoked := writeKey(t, t.TempDir(), "auth-c")
	source := &fakeStates{rows: []KeyStateRow{
		{KeyID: "auth-a", State: StateActive, PublicKey: active},
		{KeyID: "auth-c", State: StateCompromised, PublicKey: revoked},
	}}
	states := NewKeyStates(KeyStatesConfig{Source: source})
	t.Cleanup(states.Close)
	signer := NewFileSigner(FileConfig{KeyPath: dir + "/auth-a.pem", KeyID: "auth-a", States: states})
	if _, err := signer.KeySet(context.Background()); err != nil {
		t.Fatalf("key set: %v", err)
	}
	latch := latchOf(t, signer)
	if !latch.Compromised("auth-c", nil) || !latch.Compromised("other-kid", revoked) {
		t.Fatal("a row latched compromised is not reported")
	}
	if latch.Compromised("auth-a", active) {
		t.Fatal("the active key is reported compromised")
	}
}
