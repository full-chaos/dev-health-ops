package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func allBut(action string) *rule.Platform {
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != action {
			rest = append(rest, a)
		}
	}
	p := rule.PlatformOf(rest...)
	return &p
}

func only(action string) *rule.Platform {
	p := rule.PlatformOf(action)
	return &p
}

type actionCase struct {
	name      string
	superuser bool
	platform  *rule.Platform
	want      bool
}

func actionCases(action string) []actionCase {
	empty := rule.PlatformOf()
	return []actionCase{
		{"flag only, as before the change", true, nil, true},
		{"only the mapped action", false, only(action), true},
		{"every other action", true, allBut(action), false},
		{"an empty set beside the flag", true, &empty, false},
		{"no flag, no set", false, nil, false},
	}
}

// Mix 9: actorRule. An actor with no owner or admin role in the org passes
// only by admin.orgs; a superuser-only route is theirs only by the same.
func TestPlatformAction_ProvisioningActorNeedsAdminOrgs(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminOrgs) {
		actor, org := uuid.New(), uuid.New()
		h := provisioningHandlers{ProvisioningDeps: ProvisioningDeps{Users: &fakeProvUsers{
			states: map[uuid.UUID]UserState{actor: {IsActive: true, IsSuperuser: c.superuser, Platform: c.platform}},
		}}}
		for _, superuserOnly := range []bool{false, true} {
			_, err := h.actorRule(context.Background(), nil, actor, org, superuserOnly)
			var refused errActorRefused
			if got := err == nil; got != c.want || (err != nil && !errors.As(err, &refused)) {
				t.Errorf("%s (superuserOnly %v): err %v, want allowed %v", c.name, superuserOnly, err, c.want)
			}
		}
	}
}

// Mix 9: targetsRule. An actor may target a superuser only by admin.users.
func TestPlatformAction_ProvisioningTargetsNeedAdminUsers(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminUsers) {
		target := uuid.New()
		h := provisioningHandlers{ProvisioningDeps: ProvisioningDeps{Users: &fakeProvUsers{
			states: map[uuid.UUID]UserState{target: {IsActive: true, IsSuperuser: true}},
		}}}
		actor := UserState{IsActive: true, IsSuperuser: c.superuser, Platform: c.platform}
		err := h.targetsRule(context.Background(), nil, actor, []uuid.UUID{target})
		if got := err == nil; got != c.want {
			t.Errorf("%s: err %v, want allowed %v", c.name, err, c.want)
		}
	}
}

// Mix 4: the refresh routes and their guard keep a session whose org has no
// membership for the user only when the user holds admin.orgs.
func TestPlatformAction_RefreshWithoutMembershipNeedsAdminOrgs(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminOrgs) {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			state := f.users.users[f.principal]
			state.IsSuperuser, state.Platform = c.superuser, c.platform
			f.users.users[f.principal] = state
			f.users.roles = map[[2]uuid.UUID]*string{}
			f.build()
			f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
				Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
			response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
			if kept := response.Code == http.StatusOK && len(f.sessions.revoked) == 0; kept != c.want {
				t.Errorf("refresh: answer %d, revoked %v, kept %v, want %v", response.Code, f.sessions.revoked, kept, c.want)
			}

			err := guardRefresh(context.Background(), f.users, recorder{logger: slog.New(slog.DiscardHandler)}, nil, f.record)
			if got := err == nil; got != c.want {
				t.Fatalf("guardRefresh: err %v, want allowed %v", err, c.want)
			}
		})
	}
}

func TestPlatformAction_CompatRefreshWithoutMembershipNeedsAdminOrgs(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminOrgs) {
		t.Run(c.name, func(t *testing.T) {
			f := newCompatFixture(t)
			state := f.users.users[f.principal]
			state.IsSuperuser, state.Platform = c.superuser, c.platform
			f.users.users[f.principal] = state
			f.users.roles = map[[2]uuid.UUID]*string{}
			f.buildCompat()
			f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
				Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour)}}
			response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
			if kept := response.Code == http.StatusOK && len(f.sessions.revoked) == 0; kept != c.want {
				t.Fatalf("compat refresh: answer %d, revoked %v, kept %v, want %v", response.Code, f.sessions.revoked, kept, c.want)
			}
		})
	}
}
