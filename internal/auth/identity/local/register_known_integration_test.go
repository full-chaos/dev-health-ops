//go:build integration

package local

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// A register for a known address in another case runs the decoy provision on the real store: it answers like a new
// address, with and without an organization, stores no account, organization, outbox event or audit row, and mails
// only the account-exists notice to the known address, never to the decoy address. Its outcome is a new address's.
func TestRegisterForAKnownAddressInAnotherCaseAnswersOKAndStoresNothing(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	email := "known-" + uuid.NewString() + "@example.com"
	hash, err := cheapHash("Known-pass-2026")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: email, PasswordHash: hash}); err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	const stored = `SELECT (SELECT count(*) FROM auth.principals) + (SELECT count(*) FROM auth.users) + (SELECT count(*) FROM public.users)
 + (SELECT count(*) FROM public.organizations) + (SELECT count(*) FROM auth.auth_outbox_events) + (SELECT count(*) FROM auth.security_audit_events)`
	mail := &liveMail{}
	flows, err := NewFlows(FlowsConfig{Accounts: rig.accounts, Links: rig.links, Sessions: rig.sessions, Mail: mail,
		Locker: rig.links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	org := "Known Co " + uuid.NewString()[:8]
	for _, name := range []*string{nil, &org} {
		before := rig.env.count(t, ctx, stored)
		mail.sent = nil
		out, err := flows.Register(ctx, RegisterRequest{Email: strings.ToUpper(email), Password: "Another-pass-2026", OrganizationName: name})
		afterKnown, sent := rig.env.count(t, ctx, stored), append([]Message(nil), mail.sent...)
		if err != nil || out.Kind != FlowOK {
			t.Fatalf("org=%v: Register(known address, other case) = %+v, %v; want FlowOK", name != nil, out, err)
		}
		fresh, err := flows.Register(ctx, RegisterRequest{Email: "new-" + uuid.NewString() + "@example.com", Password: "Another-pass-2026", OrganizationName: name})
		if err != nil || !reflect.DeepEqual(out, fresh) {
			t.Fatalf("org=%v: known address outcome %+v, new address %+v, %v; want the same", name != nil, out, fresh, err)
		}
		if afterKnown != before {
			t.Errorf("org=%v: a register for a known address changed the stored row count from %d to %d", name != nil, before, afterKnown)
		}
		if len(sent) != 1 || sent[0].To != email || sent[0].Purpose != PurposeAccountExists || !sent[0].Deliver {
			t.Errorf("org=%v: mail = %+v, want one deliverable account-exists notice to %s", name != nil, sent, email)
		}
	}
}
