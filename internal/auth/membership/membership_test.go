package membership

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

func ptr[T any](v T) *T { return &v }

func TestEffectiveStateTreatsThePublicRowAsTheRecord(t *testing.T) {
	cases := []struct {
		name       string
		publicRole *string
		authStatus *Status
		authRole   *string
		want       state
	}{
		{"no rows", nil, nil, nil, state{status: statusAbsent}},
		{"public only", ptr("admin"), nil, nil, state{status: StatusActive, role: "admin"}},
		{"both active, public role wins", ptr("member"), ptr(StatusActive), ptr("admin"), state{status: StatusActive, role: "member"}},
		{"public row and a removed auth row", ptr("viewer"), ptr(StatusRemoved), ptr("member"), state{status: StatusActive, role: "viewer"}},
		{"auth active without a public row counts as removed", nil, ptr(StatusActive), ptr("member"), state{status: StatusRemoved, role: "member"}},
		{"auth suspended", nil, ptr(StatusSuspended), ptr("admin"), state{status: StatusSuspended, role: "admin"}},
		{"auth removed", nil, ptr(StatusRemoved), ptr("owner"), state{status: StatusRemoved, role: "owner"}},
		{"auth invited has no role", nil, ptr(StatusInvited), nil, state{status: StatusInvited}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := effective(c.publicRole, c.authStatus, c.authRole); got != c.want {
				t.Fatalf("effective = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestDecideTable(t *testing.T) {
	active := func(role string) state { return state{status: StatusActive, role: role} }
	cases := []struct {
		name   string
		verb   verb
		cur    state
		role   string
		owners int
		want   decision
		err    error
	}{
		{"add to no row", verbAdd, state{}, "member", 1, decision{next: active("member")}, nil},
		{"add re-adds a removed member", verbAdd, state{status: StatusRemoved, role: "admin"}, "viewer", 1, decision{next: active("viewer")}, nil},
		{"add activates an invited row", verbAdd, state{status: StatusInvited}, "member", 1, decision{next: active("member")}, nil},
		{"add of an active member", verbAdd, active("member"), "member", 1, decision{}, ErrAlreadyMember},
		{"add of a suspended member", verbAdd, state{status: StatusSuspended, role: "member"}, "member", 1, decision{}, ErrSuspended},
		{"add with a platform role", verbAdd, state{}, "superuser", 1, decision{}, ErrRoleInvalid},
		{"add with an empty role", verbAdd, state{}, "", 1, decision{}, ErrRoleInvalid},

		{"role change", verbChangeRole, active("member"), "admin", 1, decision{next: active("admin")}, nil},
		{"role change to the same role is a no-op", verbChangeRole, active("admin"), "admin", 1, decision{noop: true, next: active("admin")}, nil},
		{"role change of the only owner", verbChangeRole, active("owner"), "admin", 1, decision{}, ErrLastOwner},
		{"role change of one of two owners", verbChangeRole, active("owner"), "member", 2, decision{next: active("member")}, nil},
		{"role change of the only owner to owner is a no-op", verbChangeRole, active("owner"), "owner", 1, decision{noop: true, next: active("owner")}, nil},
		{"role change of a non-member", verbChangeRole, state{}, "admin", 1, decision{}, ErrNotMember},
		{"role change of a removed member", verbChangeRole, state{status: StatusRemoved, role: "member"}, "admin", 1, decision{}, ErrNotMember},
		{"role change of a suspended member", verbChangeRole, state{status: StatusSuspended, role: "member"}, "admin", 1, decision{}, ErrSuspended},
		{"role change to an unknown role", verbChangeRole, active("member"), "root", 1, decision{}, ErrRoleInvalid},

		{"suspend", verbSuspend, active("member"), "", 1, decision{next: state{status: StatusSuspended, role: "member"}}, nil},
		{"suspend twice is a no-op", verbSuspend, state{status: StatusSuspended, role: "member"}, "", 1,
			decision{noop: true, next: state{status: StatusSuspended, role: "member"}}, nil},
		{"suspend the only owner", verbSuspend, active("owner"), "", 1, decision{}, ErrLastOwner},
		{"suspend one of two owners", verbSuspend, active("owner"), "", 2, decision{next: state{status: StatusSuspended, role: "owner"}}, nil},
		{"suspend a non-member", verbSuspend, state{}, "", 1, decision{}, ErrNotMember},
		{"suspend a removed member", verbSuspend, state{status: StatusRemoved, role: "member"}, "", 1, decision{}, ErrNotMember},

		{"reactivate", verbReactivate, state{status: StatusSuspended, role: "admin"}, "", 1, decision{next: active("admin")}, nil},
		{"reactivate an active member is a no-op", verbReactivate, active("admin"), "", 1, decision{noop: true, next: active("admin")}, nil},
		{"reactivate without a role", verbReactivate, state{status: StatusSuspended}, "", 1, decision{}, ErrRoleMissing},
		{"reactivate a removed member", verbReactivate, state{status: StatusRemoved, role: "member"}, "", 1, decision{}, ErrNotMember},
		{"reactivate a non-member", verbReactivate, state{}, "", 1, decision{}, ErrNotMember},

		{"remove", verbRemove, active("member"), "", 1, decision{next: state{status: StatusRemoved, role: "member"}}, nil},
		{"remove a suspended member", verbRemove, state{status: StatusSuspended, role: "admin"}, "", 1, decision{next: state{status: StatusRemoved, role: "admin"}}, nil},
		{"remove the only owner", verbRemove, active("owner"), "", 1, decision{}, ErrLastOwner},
		{"remove one of two owners", verbRemove, active("owner"), "", 2, decision{next: state{status: StatusRemoved, role: "owner"}}, nil},
		{"remove a removed member", verbRemove, state{status: StatusRemoved, role: "member"}, "", 1, decision{}, ErrNotMember},
		{"remove a non-member", verbRemove, state{}, "", 1, decision{}, ErrNotMember},
		{"remove an invited row", verbRemove, state{status: StatusInvited}, "", 1, decision{}, ErrNotMember},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decide(c.verb, c.cur, c.role, c.owners)
			if !errors.Is(err, c.err) || (c.err == nil && err != nil) {
				t.Fatalf("decide err = %v, want %v", err, c.err)
			}
			if c.err == nil && got != c.want {
				t.Fatalf("decide = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestDecideTransfer(t *testing.T) {
	active := func(role string) state { return state{status: StatusActive, role: role} }
	from, to, err := decideTransfer(active("owner"), active("member"))
	if err != nil || from != active("admin") || to != active("owner") {
		t.Fatalf("transfer = %+v %+v %v, want admin, owner, nil", from, to, err)
	}
	refusals := []struct {
		name     string
		from, to state
		err      error
	}{
		{"source is an admin", active("admin"), active("member"), ErrNotOwner},
		{"source is not a member", state{}, active("member"), ErrNotOwner},
		{"source is suspended", state{status: StatusSuspended, role: "owner"}, active("member"), ErrNotOwner},
		{"target is not a member", active("owner"), state{}, ErrTargetNotMember},
		{"target is removed", active("owner"), state{status: StatusRemoved, role: "member"}, ErrTargetNotMember},
		{"target is suspended", active("owner"), state{status: StatusSuspended, role: "member"}, ErrTargetNotMember},
	}
	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := decideTransfer(c.from, c.to); !errors.Is(err, c.err) {
				t.Fatalf("transfer err = %v, want %v", err, c.err)
			}
		})
	}
}

func TestEventsAreProvisioningEvents(t *testing.T) {
	id := uuid.New()
	cases := []struct {
		verb      verb
		next      state
		eventType string
		change    map[string]any
	}{
		{verbAdd, state{status: StatusActive, role: "member"}, "membership.activated", map[string]any{"status": "active", "role_id": "rol_member"}},
		{verbReactivate, state{status: StatusActive, role: "admin"}, "membership.activated", map[string]any{"status": "active", "role_id": "rol_admin"}},
		{verbChangeRole, state{status: StatusActive, role: "admin"}, "membership.role_changed", map[string]any{"role_id": "rol_admin"}},
		{verbTransfer, state{status: StatusActive, role: "owner"}, "membership.role_changed", map[string]any{"role_id": "rol_owner"}},
		{verbSuspend, state{status: StatusSuspended, role: "member"}, "membership.suspended", map[string]any{"status": "suspended"}},
		{verbRemove, state{status: StatusRemoved, role: "member"}, "membership.removed", map[string]any{"status": "removed"}},
	}
	for _, c := range cases {
		t.Run(string(c.verb), func(t *testing.T) {
			e := eventFor(c.verb, id, c.next)
			if e.AggregateType != "membership" || e.AggregateID != provisioning.WireID("membership", id.String()) || e.EventType != c.eventType {
				t.Fatalf("event = %+v, want membership %s", e, c.eventType)
			}
			if len(e.Change) != len(c.change) {
				t.Fatalf("change = %v, want %v", e.Change, c.change)
			}
			for k, v := range c.change {
				if e.Change[k] != v {
					t.Fatalf("change[%s] = %v, want %v", k, e.Change[k], v)
				}
			}
			if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
				t.Fatalf("CheckEvent: %v", err)
			}
		})
	}
}

func TestAuditRowsPassTheAllowlistAndNameTheMembership(t *testing.T) {
	org, principal, id, actor := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	a := Actor{PrincipalID: &actor}
	cases := []struct {
		verb       verb
		prev, next state
		eventType  string
		reason     string
	}{
		{verbAdd, state{}, state{status: StatusActive, role: "member"}, "auth.membership.added", "membership_added"},
		{verbChangeRole, state{status: StatusActive, role: "member"}, state{status: StatusActive, role: "admin"}, "auth.membership.role_changed", "membership_status_changed"},
		{verbTransfer, state{status: StatusActive, role: "owner"}, state{status: StatusActive, role: "admin"}, "auth.membership.ownership_transferred", "membership_status_changed"},
		{verbSuspend, state{status: StatusActive, role: "member"}, state{status: StatusSuspended, role: "member"}, "auth.membership.suspended", "membership_status_changed"},
		{verbReactivate, state{status: StatusSuspended, role: "member"}, state{status: StatusActive, role: "member"}, "auth.membership.reactivated", "membership_status_changed"},
		{verbRemove, state{status: StatusActive, role: "member"}, state{status: StatusRemoved, role: "member"}, "auth.membership.removed", "membership_removed"},
	}
	for _, c := range cases {
		t.Run(string(c.verb), func(t *testing.T) {
			ev := auditFor(c.verb, org, principal, id, a, c.prev, c.next)
			if ev.EventType != c.eventType || ev.Outcome != audit.OutcomeAllowed {
				t.Fatalf("audit = %s/%s, want %s/allowed", ev.EventType, ev.Outcome, c.eventType)
			}
			if ev.OrganizationID == nil || *ev.OrganizationID != org.String() ||
				ev.SubjectPrincipalID == nil || *ev.SubjectPrincipalID != principal.String() ||
				ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != actor.String() {
				t.Fatalf("audit ids = %+v", ev)
			}
			if ev.Attributes["reason_code"] != c.reason || ev.Attributes["target_kind"] != "membership" || ev.Attributes["target_id"] != id.String() {
				t.Fatalf("audit attributes = %v", ev.Attributes)
			}
			if c.prev.status != c.next.status {
				if ev.Attributes["new_status"] != string(c.next.status) {
					t.Fatalf("new_status = %v, want %s", ev.Attributes["new_status"], c.next.status)
				}
				if c.prev.status != statusAbsent && ev.Attributes["previous_status"] != string(c.prev.status) {
					t.Fatalf("previous_status = %v, want %s", ev.Attributes["previous_status"], c.prev.status)
				}
			}
			if c.prev.role != c.next.role && c.prev.status == c.next.status {
				fields, _ := ev.Attributes["changed_fields"].([]string)
				if len(fields) != 1 || fields[0] != "role_key" {
					t.Fatalf("changed_fields = %v, want [role_key]", ev.Attributes["changed_fields"])
				}
			}
			if err := audit.ValidateAttributes(ev.Attributes); err != nil {
				t.Fatalf("ValidateAttributes: %v", err)
			}
		})
	}
}

func TestOutcomeLabelsAreClosed(t *testing.T) {
	cases := map[error]string{
		nil: "ok", audit.ErrNoStateChange: "noop", ErrNotMember: "not_member", ErrAlreadyMember: "already_member",
		ErrSuspended: "suspended", ErrLastOwner: "last_owner", ErrNotOwner: "not_owner", ErrTargetNotMember: "not_member",
		ErrRoleInvalid: "invalid", ErrRoleMissing: "invalid", ErrRevisionConflict: "conflict", ErrOrgNotFound: "not_found",
		ErrOrgSuspended: "suspended", ErrPrincipalNotFound: "not_found", ErrInviterNotFound: "not_found", ErrDenied: "denied", ErrNotOwned: "not_owned",
		errors.New("anything else"): "error",
	}
	for err, want := range cases {
		if got := outcomeLabel(err); got != want {
			t.Errorf("outcomeLabel(%v) = %q, want %q", err, got, want)
		}
	}
}

// A caller that changes the returned role list changes nothing the store checks.
func TestOrganizationRolesIsACopy(t *testing.T) {
	roles := OrganizationRoles()
	roles[0] = "superuser"
	_ = append(OrganizationRoles(), "superuser")
	if _, err := decide(verbAdd, state{}, "superuser", 1); !errors.Is(err, ErrRoleInvalid) {
		t.Fatalf("decide after a caller changed the role list = %v, want ErrRoleInvalid", err)
	}
	if got := OrganizationRoles(); len(got) != 4 || got[0] != "viewer" {
		t.Fatalf("OrganizationRoles = %v, want the four catalog roles", got)
	}
}
