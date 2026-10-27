//go:build integration

package oidc

import (
	"encoding/json"
	"net/http"
	"testing"
)

// Under users-owned, an ID token email that holds U+FFFD (a lone surrogate
// escape, which encoding/json decodes to it, or the character itself) is
// answered 400 malformed and reaches no write and no session.
func TestOwnedCallbackRefusesAReplacementCharacterEmailWithoutWrites(t *testing.T) {
	for _, raw := range []string{`"e\ud800@example.com"`, `"\ud800@example.com"`, "\"e\xef\xbf\xbd@example.com\""} {
		h := newHarness(t, harnessOpts{signup: true})
		before := h.snapshot(t)
		outboxBefore := h.count(t, `SELECT count(*) FROM auth.auth_outbox_events`)
		usersBefore := h.count(t, `SELECT count(*) FROM auth.users`)
		s := h.start(t)
		claims := h.claims(s.nonce, "sub-fffd", "unused@example.com")
		claims["email"] = json.RawMessage(raw)
		_, failure := h.finish(t, s, h.sign(t, claims))
		wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
		wantUnchanged(t, h, before)
		if n := h.count(t, `SELECT count(*) FROM auth.auth_outbox_events`); n != outboxBefore {
			t.Errorf("%s: outbox %d -> %d", raw, outboxBefore, n)
		}
		if n := h.count(t, `SELECT count(*) FROM auth.users`); n != usersBefore {
			t.Errorf("%s: auth.users %d -> %d", raw, usersBefore, n)
		}
	}
}
