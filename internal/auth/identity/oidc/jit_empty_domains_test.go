package oidc

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// An empty or NULL allowed_domains makes no new account (chris D1 = B on CHAOS-11010): with JIT on, a free key gets
// the answer and the provider failure record of the same row with auto-provisioning off, and nothing is written. An
// unverified or held email keeps the answer it gets before the auto-provisioning check.
func TestJITNewAccountWithNoDomainListIsAutoProvisionOff(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth_gitlab"} {
		for name, list := range map[string][]string{"empty": {}, "null": nil} {
			for email, tc := range map[string]struct {
				verified, inUse bool
				reason          Reason
			}{
				"free":       {true, false, ReasonProvisioningDisabled},
				"unverified": {false, false, ReasonEmailUnverified},
				"held":       {true, true, ReasonEmailCollision},
			} {
				t.Run(protocol+"/"+name+"/"+email, func(t *testing.T) {
					row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AllowedDomains: list}
					run := func(autoProvision bool) (*Error, *resolveJIT, *resolveSessions, *oidcFailures) {
						jit, sessions, failures := &resolveJIT{}, &resolveSessions{}, &oidcFailures{}
						svc := &Service{cfg: Config{Identities: &resolveIdentities{err: ErrIdentityNotFound, inUse: tc.inUse}, Memberships: resolveMembers{},
							Sessions: sessions, MembershipJIT: jit, ProviderFailure: failures}}
						r := row
						r.AutoProvision = autoProvision
						p := profile{Email: "p@unlisted.example", EmailVerified: tc.verified}
						_, failure := svc.resolve(context.Background(), r, validated{profile: p}, nil)
						return failure, jit, sessions, failures
					}
					want, _, _, wantRows := run(false)
					if want == nil || want.Reason != tc.reason {
						t.Fatalf("auto-provisioning off = %+v, want the %s refusal (harness control)", want, tc.reason)
					}
					got, jit, sessions, gotRows := run(true)
					if got == nil {
						t.Fatalf("auto-provisioning on with no domain list made an account: jit accounts %+v", jit.new)
					}
					if got.Status != want.Status || got.Reason != want.Reason || !reflect.DeepEqual(got.Detail, want.Detail) {
						t.Fatalf("answer %d %s %#v, want the auto-provisioning-off answer %d %s %#v",
							got.Status, got.Reason, got.Detail, want.Status, want.Reason, want.Detail)
					}
					if !reflect.DeepEqual(gotRows.rows, wantRows.rows) {
						t.Fatalf("failure records %+v, want the auto-provisioning-off records %+v", gotRows.rows, wantRows.rows)
					}
					if len(jit.new) != 0 || len(jit.calls) != 0 || jit.writes != 0 || len(sessions.issued) != 0 {
						t.Fatalf("jit accounts %d, jit calls %d, writes %d, sessions %d; want none",
							len(jit.new), len(jit.calls), jit.writes, len(sessions.issued))
					}
				})
			}
		}
	}
}

// A key the row already holds still signs in and joins with no domain list: the list decides only new accounts.
func TestJITLinkedNonMemberWithNoDomainListStillJoins(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth_gitlab"} {
		for name, list := range map[string][]string{"empty": {}, "null": nil} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				principal := Principal{ID: uuid.New(), Email: "p@unlisted.example", Active: true}
				jit, sessions := &resolveJIT{}, &resolveSessions{}
				svc := &Service{cfg: Config{Identities: &resolveIdentities{principal: principal}, Memberships: resolveMembers{},
					Sessions: sessions, MembershipJIT: jit}}
				row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: list}
				login, failure := svc.resolve(context.Background(), row, validated{profile: profile{Email: principal.Email, EmailVerified: true}}, nil)
				if failure != nil {
					t.Fatalf("linked non-member refused: %d %s", failure.Status, failure.Reason)
				}
				if len(jit.calls) != 1 || len(jit.new) != 0 || login.PrincipalID != principal.ID ||
					login.OrgID == nil || *login.OrgID != row.OrgID || login.Role != "member" {
					t.Fatalf("jit joins %d, jit accounts %d, login %+v; want one join of the linked account", len(jit.calls), len(jit.new), login)
				}
			})
		}
	}
}

// With JIT off no sign-in makes an account; a free key under a row with no domain list keeps its membership_required
// answer, and nothing is written.
func TestJITOffNewAccountWithNoDomainListKeepsMembershipRequired(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth_gitlab"} {
		for name, list := range map[string][]string{"empty": {}, "null": nil} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				ids, failures := &resolveIdentities{err: ErrIdentityNotFound}, &oidcFailures{}
				svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{}, Sessions: &resolveSessions{}, ProviderFailure: failures}}
				row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: list}
				_, failure := svc.resolve(context.Background(), row, validated{profile: profile{Email: "p@unlisted.example", EmailVerified: true}}, nil)
				wantRefused(t, failure, http.StatusForbidden, ReasonMembershipRequired)
				if ids.provision != 0 || len(failures.rows) != 0 {
					t.Fatalf("provisions %d, failure records %+v; want none", ids.provision, failures.rows)
				}
			})
		}
	}
}
