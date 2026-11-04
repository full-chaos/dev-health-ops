package local

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
)

func registerSequences(t *testing.T) (map[string][]string, map[string]*flowRig) {
	t.Helper()
	seqs, rigs := map[string][]string{}, map[string]*flowRig{}
	for _, name := range []string{"new", "known", "taken-on-write"} {
		rig := newFlowRig(t, true)
		email := name + "@example.com"
		switch name {
		case "known":
			rig.user(email, true)
		case "taken-on-write":
			rig.accounts.taken = true
		}
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: email, Password: flowPassword})
		if err != nil || out.Kind != FlowOK {
			t.Fatalf("%s: Register = %+v, %v", name, out, err)
		}
		seqs[name], rigs[name] = rig.rec.names(), rig
	}
	return seqs, rigs
}

// A known address, a new address and an address taken between the read and the write make the same calls.
func TestRegisterMakesOneCallSequenceForNewKnownAndTakenAddresses(t *testing.T) {
	seqs, _ := registerSequences(t)
	want := []string{"ByEmail", "ProvisionLocal", "Issue", "Send"}
	for name, got := range seqs {
		if len(got) != len(want) {
			t.Errorf("%s: calls = %v, want %v", name, got, want)
		}
	}
	callseq.Same(t, "register", seqs)
}

// The known address stores nothing and mails the one notice; its link credential is the decoy of the new one.
func TestRegisterKnownAddressStoresNothingAndSendsTheNoticeOnly(t *testing.T) {
	_, rigs := registerSequences(t)
	known, fresh := rigs["known"], rigs["new"]
	if len(known.accounts.provided) != 0 {
		t.Errorf("a known address provisioned %d accounts", len(known.accounts.provided))
	}
	if len(known.links.issued) != 1 || len(fresh.links.issued) != 1 {
		t.Fatalf("link requests: known %d, new %d, want 1 and 1", len(known.links.issued), len(fresh.links.issued))
	}
	k, n := known.links.issued[0], fresh.links.issued[0]
	if k.PrincipalID != uuid.Nil || n.PrincipalID == uuid.Nil {
		t.Errorf("principals: known %v (want nil), new %v", k.PrincipalID, n.PrincipalID)
	}
	if k.Purpose != n.Purpose || k.TTL != n.TTL {
		t.Errorf("link request shapes differ: known %+v, new %+v", k, n)
	}
	if len(known.mail.sent) != 1 || known.mail.sent[0].Purpose != PurposeAccountExists || !known.mail.sent[0].Deliver {
		t.Errorf("known address mail = %+v, want one deliverable account_exists notice", known.mail.sent)
	}
	if known.accounts.decoyed != 1 || fresh.accounts.decoyed != 0 {
		t.Errorf("decoy provisions: known %d, new %d, want 1 and 0", known.accounts.decoyed, fresh.accounts.decoyed)
	}
}

// With the organization slug taken, every address state still makes the calls of a new sign-up; only a known
// address delivers the notice, and no state stores a credential.
func TestRegisterSlugCollisionMakesOneCallSequenceForEveryAddressState(t *testing.T) {
	seqs := map[string][]string{}
	for _, name := range []string{"new", "known", "taken-on-write"} {
		for _, collision := range []bool{false, true} {
			rig := newFlowRig(t, true)
			email := name + "@example.com"
			switch name {
			case "known":
				rig.user(email, true)
			case "taken-on-write":
				rig.accounts.taken = true
			}
			if collision {
				rig.f.accounts = recordedCollision{Accounts: rig.f.accounts, rec: rig.rec}
			}
			out, err := rig.f.Register(context.Background(), RegisterRequest{Email: email, Password: flowPassword})
			if err != nil || out.Kind != FlowOK {
				t.Fatalf("%s collision=%v: Register = %+v, %v", name, collision, out, err)
			}
			key := fmt.Sprintf("%s/collision=%v", name, collision)
			seqs[key] = rig.rec.names()
			for _, issued := range rig.links.issued {
				if collision && issued.PrincipalID != uuid.Nil {
					t.Errorf("%s: a credential was stored for %v", key, issued.PrincipalID)
				}
			}
			delivered := 0
			for _, msg := range rig.mail.sent {
				if msg.Deliver {
					delivered++
				}
			}
			if want := map[bool]int{false: 1, true: 0}[collision && name != "known"]; delivered != want {
				t.Errorf("%s: %d mails delivered, want %d", key, delivered, want)
			}
		}
	}
	callseq.Same(t, "register with a slug collision", seqs)
}

// recordedCollision refuses the provision with a slug collision, as the store does when the organization name's slug is
// held, and records the call as the other fakes do.
type recordedCollision struct {
	Accounts
	rec *flowRecorder
}

func (c recordedCollision) ProvisionLocal(context.Context, ProvisionLocalRequest) (uuid.UUID, error) {
	c.rec.add("ProvisionLocal")
	return uuid.Nil, identity.ErrOrganizationSlugCollision
}
