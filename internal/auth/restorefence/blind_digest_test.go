package restorefence

import (
	"context"
	"regexp"
	"testing"
)

// Follow-up (round1128 P2): every blind margin shares the digest "blind_margin", so a retry with a larger margin is a
// successful no-op (AlreadyApplied). The digest names the margin; the state table accepts a 64-hex digest.
func TestDifferentBlindMarginsHaveDifferentDigests(t *testing.T) {
	a, err := resolveInputs(context.Background(), nil, Options{BlindMargin: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolveInputs(context.Background(), nil, Options{BlindMargin: 2000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.digest == b.digest {
		t.Fatalf("margins 1000 and 2000 share the digest %q: a retry with a larger margin would be a no-op", a.digest)
	}
	again, _ := resolveInputs(context.Background(), nil, Options{BlindMargin: 1000}, nil)
	if again.digest != a.digest {
		t.Error("the same margin must keep the same digest (idempotent retry)")
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(a.digest) {
		t.Errorf("the digest %q must satisfy the restore_fence_state CHECK (64 hex)", a.digest)
	}
}
