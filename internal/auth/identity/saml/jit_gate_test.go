package saml

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type gateMembers struct{ member bool }

func (m *gateMembers) Member(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return m.member, nil
}

type gateJIT struct {
	joins int
	owner *gateMembers
}

func (j *gateJIT) JoinOnSignIn(context.Context, uuid.UUID, Provider) error {
	j.joins++
	j.owner.member = true
	return nil
}
func (j *gateJIT) ProvisionOnSignIn(context.Context, NewAccount, Provider) (uuid.UUID, error) {
	return uuid.Nil, nil
}

// The provider row's auto-provision setting gates the join of a known account.
func TestRequireMemberJoinsOnlyWhenTheProviderAllowsIt(t *testing.T) {
	for _, allow := range []bool{false, true} {
		members := &gateMembers{}
		jit := &gateJIT{owner: members}
		svc := &Service{Memberships: members, JIT: jit}
		err := svc.requireMember(context.Background(), Provider{ID: uuid.New(), OrgID: uuid.New(), AutoProvision: allow}, uuid.New())
		switch {
		case allow && (err != nil || jit.joins != 1):
			t.Fatalf("allowed: err %v joins %d, want a join", err, jit.joins)
		case !allow && (err != ErrNotOrgMember || jit.joins != 0):
			t.Fatalf("not allowed: err %v joins %d, want ErrNotOrgMember and no join", err, jit.joins)
		}
	}
}
