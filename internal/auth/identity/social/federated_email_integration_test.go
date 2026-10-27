//go:build integration

package social

import (
	"context"
	"errors"
	"testing"
)

// Under users-owned, a provider email that holds U+FFFD (a lone surrogate
// escape decoded, or the character itself) reaches no write.
func TestOwnedExchangeRefusesAReplacementCharacterEmailWithoutWrites(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	owned := e.service(t, true)
	for i, raw := range []string{`e\ud800@example.com`, `\ud800@example.com`, "e\xef\xbf\xbd@example.com"} {
		token := e.providers.token(t.Name()+raw, account{provider: Google, googleID: "fffd-" + string(rune('a'+i)), verified: true, rawEmail: raw})
		before, publicBefore := e.state(t, ctx), e.count(t, ctx, `SELECT count(*) FROM public.users`)
		if _, err := owned.Exchange(ctx, Google, token, nil); !errors.Is(err, ErrEmailUnverified) {
			t.Errorf("%q: err = %v, want ErrEmailUnverified", raw, err)
		}
		e.unchanged(t, ctx, before)
		if n := e.count(t, ctx, `SELECT count(*) FROM public.users`); n != publicBefore {
			t.Errorf("%q: public.users %d -> %d", raw, publicBefore, n)
		}
	}
}
