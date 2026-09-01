package principal

import (
	"crypto/ed25519"
	"testing"
)

// TestCheckJWKS_ReadsJWKSFileExactlyOnce is codex round 2's P2 (gpt-5.6-terra,
// xhigh, chaos-4708-round2-20260901T073251), re-executed and CONFIRMED
// before this fix (via a Unix-FIFO repro forcing the exact interleaving,
// not included here -- see verifier.go's CheckJWKS doc comment for the
// full writeup) before landing: the pre-round-2 CheckJWKS called Keys()
// (which does its OWN internal os.ReadFile) and THEN a second, separate
// os.ReadFile for the trailing-content check -- two independent reads of
// a mutable file. A rotation landing between them could make the first
// read see valid content (Keys() succeeds) while the second saw
// DIFFERENT content (e.g. a syntactically-valid-but-empty-keys
// replacement), producing an internally-inconsistent nil verdict from
// two snapshots that were never simultaneously true.
//
// This test proves the fix's actual invariant -- CheckJWKS reads
// v.jwksPath exactly ONCE per call -- deterministically, via the
// readJWKSFileForCheck seam, rather than depending on OS-level file
// descriptor race timing (which a FIFO-based version of this test proved
// to be its own source of flakiness: a mismatched read/write cardinality
// lets the OS coalesce or fail to coalesce multiple writer sessions
// unpredictably). The swapped-in reader still serves the FIRST call with
// the real, valid document (proving CheckJWKS's happy path is unaffected)
// but would serve a DIFFERENT, unusable document to any second call --
// exactly codex's shape -- so a regression that reintroduces a second
// read is caught by the call-count assertion even in the (unlikely)
// event the second document also happened to look superficially valid.
func TestCheckJWKS_ReadsJWKSFileExactlyOnce(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	path := writeJWKS(t, pub, testKID)

	callCount := 0
	originalReader := readJWKSFileForCheck
	t.Cleanup(func() { readJWKSFileForCheck = originalReader })
	readJWKSFileForCheck = func(name string) ([]byte, error) {
		callCount++
		if callCount > 1 {
			// codex round 2's exact shape: a second read observes a
			// DIFFERENT, syntactically-valid-but-unusable document (a
			// rotation to zero keys), simulating the file having changed
			// between two reads.
			return []byte(`{"keys":[]}`), nil
		}
		return originalReader(name)
	}

	v := mustVerifier(t, path, testIssuer, testAudience)
	if err := v.CheckJWKS(); err != nil {
		t.Fatalf("CheckJWKS: unexpected error on the first (valid) read: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("CheckJWKS read the JWKS file %d times in one call, want exactly 1 -- "+
			"codex round 2's P2: a second read can observe a different, individually-plausible "+
			"snapshot and produce an internally-inconsistent verdict", callCount)
	}
}
