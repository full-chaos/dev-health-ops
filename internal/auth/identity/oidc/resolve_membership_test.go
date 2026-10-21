package oidc

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

type resolveIdentities struct {
	principal Principal
	err       error
	inUse     bool
	provision int
}

func (r *resolveIdentities) IdentityBySubject(context.Context, ExternalKey, LegacyMatch) (Principal, error) {
	return r.principal, r.err
}
func (r *resolveIdentities) EmailInUse(context.Context, string) (bool, error) { return r.inUse, nil }
func (r *resolveIdentities) ProvisionFederated(context.Context, FederatedAccount) (Principal, error) {
	r.provision++
	return Principal{ID: uuid.New(), Active: true}, nil
}

type resolveMembers struct {
	found bool
	role  string
}

func (m resolveMembers) Role(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return m.role, m.found, nil
}

type resolveSessions struct{ issued []SessionRequest }

func (s *resolveSessions) Issue(_ context.Context, req SessionRequest) (Tokens, error) {
	s.issued = append(s.issued, req)
	return Tokens{AccessToken: "a"}, nil
}

type resolveJIT struct {
	calls []SessionRequest
	new   []FederatedAccount
	err   error
	// writes counts rows that would be committed: none when err is set.
	writes int
}

func (j *resolveJIT) JoinAndIssue(_ context.Context, _, _ uuid.UUID, req SessionRequest) (Tokens, error) {
	j.calls = append(j.calls, req)
	if j.err != nil {
		return Tokens{}, j.err
	}
	j.writes += 2
	return Tokens{AccessToken: "j"}, nil
}

func (j *resolveJIT) ProvisionJoinAndIssue(_ context.Context, account FederatedAccount, _ uuid.UUID, req SessionRequest) (Principal, Tokens, error) {
	j.new = append(j.new, account)
	j.calls = append(j.calls, req)
	if j.err != nil {
		return Principal{}, Tokens{}, j.err
	}
	j.writes += 3
	return Principal{ID: uuid.New(), Email: account.Email, Active: true}, Tokens{AccessToken: "n"}, nil
}

func resolveRun(protocol string, ids *resolveIdentities, members resolveMembers, jit MembershipJIT) (Login, *Error, *resolveSessions) {
	sessions := &resolveSessions{}
	svc := &Service{cfg: Config{Identities: ids, Memberships: members, Sessions: sessions, MembershipJIT: jit}}
	row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true}
	v := validated{profile: profile{Email: "p@example.com", EmailVerified: true}}
	login, failure := svc.resolve(context.Background(), row, v, nil)
	return login, failure, sessions
}

func TestResolveKeyHitWithoutMembershipIsRefusedBeforeASession(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		t.Run(protocol, func(t *testing.T) {
			ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
			_, failure, sessions := resolveRun(protocol, ids, resolveMembers{}, nil)
			wantRefused(t, failure, http.StatusForbidden, ReasonMembershipRequired)
			if len(sessions.issued) != 0 {
				t.Fatalf("sessions issued = %d, want 0", len(sessions.issued))
			}
		})
	}
}

func TestResolveKeyHitWithJITJoinsAndIssuesInOneStep(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		t.Run(protocol, func(t *testing.T) {
			ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
			jit := &resolveJIT{}
			login, failure, sessions := resolveRun(protocol, ids, resolveMembers{}, jit)
			if failure != nil {
				t.Fatalf("resolve: %v", failure)
			}
			if len(jit.calls) != 1 || len(sessions.issued) != 0 {
				t.Fatalf("join calls %d, separate sessions %d; want 1 and 0", len(jit.calls), len(sessions.issued))
			}
			if login.OrgID == nil || jit.calls[0].OrgID == nil || *jit.calls[0].OrgID != *login.OrgID || login.Role != "member" {
				t.Fatalf("login %+v request %+v", login, jit.calls[0])
			}
		})
	}
}

func TestResolveJITFailureWritesNothingAndIssuesNoSession(t *testing.T) {
	ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
	jit := &resolveJIT{err: errors.New("tx failed")}
	_, failure, sessions := resolveRun("oidc", ids, resolveMembers{}, jit)
	if failure == nil || failure.Status != http.StatusInternalServerError {
		t.Fatalf("failure %+v, want 500", failure)
	}
	if jit.writes != 0 || len(sessions.issued) != 0 {
		t.Fatalf("writes %d sessions %d, want 0 and 0", jit.writes, len(sessions.issued))
	}
}

func TestResolveMemberGetsAnOrgSession(t *testing.T) {
	ids := &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}}
	login, failure, sessions := resolveRun("oidc", ids, resolveMembers{found: true, role: "admin"}, nil)
	if failure != nil || len(sessions.issued) != 1 || login.OrgID == nil || login.Role != "admin" ||
		sessions.issued[0].OrgID == nil || *sessions.issued[0].OrgID != *login.OrgID {
		t.Fatalf("login %+v failure %v sessions %+v", login, failure, sessions.issued)
	}
}

func TestResolveNewAccountWithJITOffIsRefusedWithZeroWrites(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		ids := &resolveIdentities{err: ErrIdentityNotFound}
		_, failure, sessions := resolveRun(protocol, ids, resolveMembers{}, nil)
		wantRefused(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		if ids.provision != 0 || len(sessions.issued) != 0 {
			t.Fatalf("%s: provision %d sessions %d, want 0 and 0", protocol, ids.provision, len(sessions.issued))
		}
	}
}

func TestResolveNewAccountWithJITProvisionsJoinsAndIssuesInOneStep(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		t.Run(protocol, func(t *testing.T) {
			ids := &resolveIdentities{err: ErrIdentityNotFound}
			jit := &resolveJIT{}
			login, failure, sessions := resolveRun(protocol, ids, resolveMembers{}, jit)
			if failure != nil {
				t.Fatalf("resolve: %v", failure)
			}
			if len(jit.new) != 1 || jit.new[0].Email != "p@example.com" || ids.provision != 0 || len(sessions.issued) != 0 {
				t.Fatalf("jit accounts %+v, direct provisions %d, separate sessions %d", jit.new, ids.provision, len(sessions.issued))
			}
			if login.OrgID == nil || jit.calls[0].OrgID == nil || *jit.calls[0].OrgID != *login.OrgID ||
				login.Role != "member" || login.PrincipalID == uuid.Nil {
				t.Fatalf("login %+v request %+v", login, jit.calls[0])
			}
		})
	}
}

func TestResolveNewAccountJITFailureWritesNothing(t *testing.T) {
	for name, cause := range map[string]error{"store": errors.New("tx failed"), "conflict": ErrIdentityConflict} {
		t.Run(name, func(t *testing.T) {
			ids := &resolveIdentities{err: ErrIdentityNotFound}
			jit := &resolveJIT{err: cause}
			_, failure, sessions := resolveRun("oidc", ids, resolveMembers{}, jit)
			if failure == nil || jit.writes != 0 || len(sessions.issued) != 0 {
				t.Fatalf("failure %+v writes %d sessions %d", failure, jit.writes, len(sessions.issued))
			}
			if name == "conflict" {
				wantRefused(t, failure, http.StatusConflict, ReasonEmailCollision)
			}
		})
	}
}

func TestResolveNewAccountChecksPrecedeTheJITStep(t *testing.T) {
	for name, tc := range map[string]struct {
		ids    *resolveIdentities
		row    func(*Provider)
		prof   profile
		status int
		reason Reason
	}{
		"unverified": {&resolveIdentities{err: ErrIdentityNotFound}, func(*Provider) {}, profile{Email: "p@example.com"}, http.StatusForbidden, ReasonEmailUnverified},
		"collision":  {&resolveIdentities{err: ErrIdentityNotFound, inUse: true}, func(*Provider) {}, profile{Email: "p@example.com", EmailVerified: true}, http.StatusConflict, ReasonEmailCollision},
		"disabled":   {&resolveIdentities{err: ErrIdentityNotFound}, func(r *Provider) { r.AutoProvision = false }, profile{Email: "p@example.com", EmailVerified: true}, http.StatusForbidden, ReasonProvisioningDisabled},
	} {
		t.Run(name, func(t *testing.T) {
			jit := &resolveJIT{}
			svc := &Service{cfg: Config{Identities: tc.ids, Memberships: resolveMembers{}, Sessions: &resolveSessions{}, MembershipJIT: jit}}
			row := Provider{ID: uuid.New(), OrgID: uuid.New(), AutoProvision: true}
			tc.row(&row)
			_, failure := svc.resolve(context.Background(), row, validated{profile: tc.prof}, nil)
			wantRefused(t, failure, tc.status, tc.reason)
			if len(jit.calls) != 0 {
				t.Fatalf("the JIT step ran after a refused check")
			}
		})
	}
}

func wantRefused(t *testing.T, failure *Error, status int, reason Reason) {
	t.Helper()
	if failure == nil || failure.Status != status || failure.Reason != reason {
		t.Fatalf("failure %+v, want %d %s", failure, status, reason)
	}
}
