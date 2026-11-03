//go:build integration

package social

import (
	"context"
	"errors"
	"testing"
)

// Under users-owned, a provider email that holds a lone surrogate escape
// (refused as unverified) or the character U+FFFD itself (refused as an
// unusable address) reaches no write.
func TestOwnedExchangeRefusesAReplacementCharacterEmailWithoutWrites(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	owned := e.service(t, true)
	for i, tc := range []struct {
		raw  string
		want error
	}{{`e\ud800@example.com`, ErrEmailUnverified}, {`\ud800@example.com`, ErrEmailUnverified}, {"e\xef\xbf\xbd@example.com", ErrUnusableEmail}} {
		raw := tc.raw
		token := e.providers.token(t.Name()+raw, account{provider: Google, googleID: "fffd-" + string(rune('a'+i)), verified: true, rawEmail: raw})
		before, publicBefore := e.state(t, ctx), e.count(t, ctx, `SELECT count(*) FROM public.users`)
		if _, err := owned.Exchange(ctx, Google, token, nil); !errors.Is(err, tc.want) {
			t.Errorf("%q: err = %v, want %v", raw, err, tc.want)
		}
		e.unchanged(t, ctx, before)
		if n := e.count(t, ctx, `SELECT count(*) FROM public.users`); n != publicBefore {
			t.Errorf("%q: public.users %d -> %d", raw, publicBefore, n)
		}
	}
}
