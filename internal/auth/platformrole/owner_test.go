package platformrole

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

func TestPlatformOwnerIsAPlatformRoleOfTheCatalog(t *testing.T) {
	if PlatformOwner != "platform_owner" || rule.PlatformOwner != "platform_owner" {
		t.Fatalf("PlatformOwner = %q / %q", PlatformOwner, rule.PlatformOwner)
	}
	if !IsPlatformRole(PlatformOwner) {
		t.Fatal("platform_owner is not a platform role")
	}
}

// Q1: the owner role is a marker. Whatever platform_admin allows, platform_owner
// allows, and nothing more; an organization action is never allowed by either.
func TestOwnerAuthorityAllowsNoActionAdminDoesNot(t *testing.T) {
	owner, _ := authorityOf([]string{PlatformOwner})
	admin, _ := authorityOf([]string{PlatformAdmin})
	if !owner.IsOwner() || !owner.Any() {
		t.Fatalf("an owner authority: IsOwner=%v Any=%v", owner.IsOwner(), owner.Any())
	}
	if admin.IsOwner() {
		t.Error("platform_admin alone reads as owner")
	}
	if (Authority{}).IsOwner() {
		t.Error("the zero Authority reads as owner")
	}
	for _, a := range policycatalog.Default().Actions() {
		if owner.Allows(a.ID) && !admin.Allows(a.ID) {
			t.Errorf("the owner role allows %s, which platform_admin does not", a.ID)
		}
		if admin.Allows(a.ID) != owner.Allows(a.ID) {
			t.Errorf("%s: admin %v, owner %v; the owner role must equal the admin actions", a.ID, admin.Allows(a.ID), owner.Allows(a.ID))
		}
	}
	both, _ := authorityOf([]string{PlatformAdmin, PlatformOwner})
	if !both.IsOwner() || !both.Allows(GrantAction) || len(both.Roles()) != 2 {
		t.Errorf("one principal with both roles: roles %v", both.Roles())
	}
}

// Q2: no path grants owner but the CLI verb: the API store refuses owner for a
// grant and for a revoke before it reads or writes anything.
func TestAPIPathRefusesPlatformOwnerBeforeAnyRead(t *testing.T) {
	s := &Store{logger: slog.New(slog.DiscardHandler)}
	ctx := t.Context()
	if _, err := s.Grant(ctx, GrantInput{Actor: uuid.New(), Principal: uuid.New(), Role: PlatformOwner}); !errors.Is(err, ErrPlatformOwnerCLIOnly) {
		t.Errorf("Grant of platform_owner: %v, want ErrPlatformOwnerCLIOnly", err)
	}
	if err := s.Revoke(ctx, RevokeInput{Actor: uuid.New(), Principal: uuid.New(), Role: PlatformOwner}); !errors.Is(err, ErrPlatformOwnerCLIOnly) {
		t.Errorf("Revoke of platform_owner: %v, want ErrPlatformOwnerCLIOnly", err)
	}
}

func TestPlatformOwnerRefusalIsKept(t *testing.T) {
	found := false
	for _, r := range refusals {
		found = found || r == ErrPlatformOwnerCLIOnly
	}
	if !found {
		t.Error("ErrPlatformOwnerCLIOnly is not in the refusals writeErr keeps")
	}
}

// The operator verbs audit with no actor principal and the operator CLI as the
// source; the API refusal audits the actor, the target and a denied outcome.
func TestOwnerAuditEventsNameWhoAndHow(t *testing.T) {
	subject := uuid.New()
	ev := operatorAudit("auth.platform_role.granted", "role_granted", subject.String())
	if ev.ActorPrincipalID != nil || ev.SubjectPrincipalID == nil || *ev.SubjectPrincipalID != subject.String() ||
		ev.Outcome != audit.OutcomeAllowed || ev.Attributes["source"] != audit.SourceOperatorCLI ||
		ev.Attributes["target_id"] != subject.String() || ev.Attributes["reason_code"] != "role_granted" {
		t.Errorf("operator audit event = %+v", ev)
	}
	if err := audit.ValidateAttributes(ev.Attributes); err != nil {
		t.Errorf("operator audit attributes refused: %v", err)
	}
	actor := uuid.New()
	ref := ownerRefusalAudit(audit.EventPlatformOwnerGrantRefused, actor, subject)
	if ref.Outcome != audit.OutcomeDenied || ref.ActorPrincipalID == nil || *ref.ActorPrincipalID != actor.String() ||
		ref.SubjectPrincipalID == nil || *ref.SubjectPrincipalID != subject.String() || ref.EventType != audit.EventPlatformOwnerGrantRefused {
		t.Errorf("refusal audit event = %+v", ref)
	}
	if err := audit.ValidateAttributes(ref.Attributes); err != nil {
		t.Errorf("refusal audit attributes refused: %v", err)
	}
}
