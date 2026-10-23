package usertokens

import (
	"context"
	"strings"
	"testing"
)

func TestAllMintsEveryKindOfUserToken(t *testing.T) {
	kinds := map[string]int{}
	for _, tok := range All(t) {
		if strings.Count(tok.Value, ".") != 2 {
			t.Errorf("%s is not a compact JWS: %q", tok.Kind, tok.Value)
		}
		kinds[strings.SplitN(tok.Kind, ":", 2)[0]]++
	}
	for _, want := range []string{"hs256-access", "legacy-refresh", "eddsa-corpus"} {
		if kinds[want] == 0 {
			t.Errorf("no %s token", want)
		}
	}
}

func TestCountingPoolCountsEveryAttemptAndRefusesThem(t *testing.T) {
	pool, counter := CountingPool(t)
	if counter.Count() != 0 {
		t.Fatalf("count %d before any query", counter.Count())
	}
	if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(new(int)); err == nil {
		t.Fatal("a query succeeded on the counting pool")
	}
	if counter.Count() == 0 {
		t.Fatal("a query left the count at zero")
	}
}

func TestNoLeakSeesATokenAndItsSignature(t *testing.T) {
	tokens := All(t)
	probe := &fakeT{}
	NoLeak(probe, "body", "ok "+tokens[0].Value, tokens)
	if !probe.failed {
		t.Fatal("a body holding a token passed")
	}
	probe = &fakeT{}
	NoLeak(probe, "body", "sig "+tokens[0].Signature(), tokens)
	if !probe.failed {
		t.Fatal("a body holding a token's signature passed")
	}
	probe = &fakeT{}
	NoLeak(probe, "body", `{"detail":"Unauthorized"}`, tokens)
	if probe.failed {
		t.Fatal("a clean body failed")
	}
}

type fakeT struct {
	testing.TB
	failed bool
}

func (f *fakeT) Helper()                           {}
func (f *fakeT) Errorf(format string, args ...any) { f.failed = true }
