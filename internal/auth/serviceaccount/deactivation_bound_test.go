package serviceaccount

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// A key is read from its rows on every call, so a deactivation of the account, the key or the principal
// refuses at once, however far away the key's own expiry is. The expiry is set past the longest bound in
// the contracts so that no refusal can come from the clock.
func TestDeactivationBound_ServiceAccount_ResolveKey_EachFlagAloneRefusesAtOnce(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	far := now.Add(2 * contracts.AccessTokenLifetimeCeiling)
	live := func() keyRow {
		return keyRow{id: uuid.New(), principal: uuid.New(), org: uuid.New(), class: "service_account_key", domainOwner: "platform",
			expiresAt: far, kind: "service_account"}
	}
	if lr := live(); true {
		if _, why := lr.decide(now); why != "" {
			t.Fatalf("the live row refused: %q", why)
		}
	}
	for _, tc := range []struct {
		name string
		set  func(*keyRow)
		want string
	}{
		{"key revoked", func(r *keyRow) { r.revoked = true }, "revoked"},
		{"service account disabled", func(r *keyRow) { r.accountDisabled = true }, "account_disabled"},
		{"principal disabled", func(r *keyRow) { r.principalDisabled = true }, "principal_disabled"},
		{"organization suspended", func(r *keyRow) { r.orgSuspended = true }, "org_suspended"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := live()
			tc.set(&row)
			if _, got := row.decide(now); got != tc.want {
				t.Fatalf("refusal %q, want %q", got, tc.want)
			}
			if _, got := row.decide(now.Add(-time.Hour)); got != tc.want {
				t.Fatalf("an earlier instant: refusal %q, want %q", got, tc.want)
			}
		})
	}
}

// A refusal never carries a key: the caller gets no principal, org or scope to act on.
func TestDeactivationBound_ServiceAccount_ResolveKey_RefusalCarriesNoKey(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	row := keyRow{id: uuid.New(), principal: uuid.New(), org: uuid.New(), class: "service_account_key", domainOwner: "platform",
		expiresAt: now.Add(time.Hour), kind: "service_account", scopes: []string{"api:access"}, audiences: []string{"a"}, accountDisabled: true}
	key, why := row.decide(now)
	if why == "" {
		t.Fatal("a disabled account resolved")
	}
	if key.Principal != uuid.Nil || key.OrgID != uuid.Nil || key.CredentialID != uuid.Nil || key.Scopes != nil || key.Audiences != nil {
		t.Fatalf("a refusal carried %+v", key)
	}
}
