//go:build integration

package saml_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

// Under users-owned, an asserted email that holds U+FFFD is refused as an
// invalid address (malformed) and writes no account, membership or identity.
func TestSAMLReplacementCharacterEmailIsRefusedWithoutWrites(t *testing.T) {
	h := newHarness(t, true)
	h.ownUsers()
	for _, email := range []string{"e�@example.com", "�@example.com"} {
		before := h.writeCounts()
		_, err := h.consume(h.respond(resp{nameID: nid("fffd-" + uuid.NewString()), email: email}), nil)
		if !errors.Is(err, saml.ErrInvalidEmailAddress) || saml.OutcomeReason(err) != "malformed" {
			t.Errorf("%q: err = %v, want ErrInvalidEmailAddress (malformed)", email, err)
		}
		if after := h.writeCounts(); after != before {
			t.Errorf("%q: rows %v -> %v, want no write", email, before, after)
		}
	}
}
