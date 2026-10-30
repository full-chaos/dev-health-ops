package identity

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func TestDecideOrg(t *testing.T) {
	a := orgState{slug: "acme", name: "Acme", active: true}
	cases := []struct {
		name  string
		found bool
		cur   orgState
		src   orgState
		want  orgDecision
	}{
		{"missing is inserted with a slug check", false, orgState{}, a, orgDecision{action: orgInsert, slugCheck: true}},
		{"equal active writes nothing", true, a, a, orgDecision{}},
		{"equal inactive writes nothing, so disabled_at is not bumped", true, orgState{slug: "acme", name: "Acme"}, orgState{slug: "acme", name: "Acme"}, orgDecision{}},
		{"name change updates without a slug check", true, a, orgState{slug: "acme", name: "Other", active: true}, orgDecision{action: orgUpdate}},
		{"slug change updates with a slug check", true, a, orgState{slug: "acme2", name: "Acme", active: true}, orgDecision{action: orgUpdate, slugCheck: true}},
		{"turning inactive is a suspension", true, a, orgState{slug: "acme", name: "Acme"}, orgDecision{action: orgUpdate, suspended: true}},
		{"turning active is a reactivation", true, orgState{slug: "acme", name: "Acme"}, a, orgDecision{action: orgUpdate, reactivated: true}},
		{"name change while inactive is neither", true, orgState{slug: "acme", name: "Acme"}, orgState{slug: "acme", name: "New"}, orgDecision{action: orgUpdate}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decideOrg(c.found, c.cur, c.src); got != c.want {
				t.Fatalf("decideOrg = %+v, want %+v", got, c.want)
			}
		})
	}
}

// A refresh from go-api's row never reactivates: a suspended organization
// keeps its suspension and takes go-api's name and slug.
func TestKeepSuspended(t *testing.T) {
	active := orgState{slug: "acme", name: "Acme", active: true}
	suspended := orgState{slug: "acme", name: "Acme"}
	for _, c := range []struct {
		name          string
		cur, src, out orgState
	}{
		{"suspended stays suspended against an active go-api row", suspended, orgState{slug: "acme2", name: "New", active: true}, orgState{slug: "acme2", name: "New"}},
		{"suspended against a suspended go-api row", suspended, suspended, suspended},
		{"active follows a suspension", active, suspended, suspended},
		{"active stays active", active, active, active},
	} {
		if got := keepSuspended(c.cur, c.src); got != c.out {
			t.Fatalf("%s: keepSuspended = %+v, want %+v", c.name, got, c.out)
		}
	}
	if d := decideOrg(true, suspended, keepSuspended(suspended, active)); d != (orgDecision{}) {
		t.Fatalf("a suspended organization against an active go-api row decides %+v, want nothing", d)
	}
}

func TestDecideMembership(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name                  string
		org, principal, found bool
		status, role          *string
		want                  memberAction
		err                   error
	}{
		{"missing organization", false, true, false, nil, nil, memberNone, ErrBackfillOrgMissing},
		{"missing principal", true, false, false, nil, nil, memberNone, ErrBackfillPrincipalMissing},
		{"missing membership is inserted", true, true, false, nil, nil, memberInsert, nil},
		{"equal active writes nothing", true, true, true, s("active"), s("admin"), memberNone, nil},
		{"other role on an active membership", true, true, true, s("active"), s("viewer"), memberSetRole, nil},
		{"active without a role gets the role", true, true, true, s("active"), nil, memberSetRole, nil},
		{"removed is only reported", true, true, true, s("removed"), s("viewer"), memberDiverged, nil},
		{"suspended is only reported", true, true, true, s("suspended"), s("admin"), memberDiverged, nil},
		{"invited is only reported", true, true, true, s("invited"), nil, memberDiverged, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decideMembership(c.org, c.principal, c.found, c.status, c.role, "admin")
			if got != c.want || !errors.Is(err, c.err) {
				t.Fatalf("decideMembership = %v, %v; want %v, %v", got, err, c.want, c.err)
			}
		})
	}
}

func TestRoleChangeAuditNamesTheChangedFieldAndIsAccepted(t *testing.T) {
	ev := roleChangeAudit(MembershipBackfillTarget{OrgID: uuid.New(), UserID: uuid.New(), Role: "owner"}, uuid.New())
	if ev.EventType != "auth.membership.role_changed" {
		t.Fatalf("event type = %q", ev.EventType)
	}
	fields, ok := ev.Attributes["changed_fields"].([]string)
	if !ok || len(fields) != 1 || fields[0] != "role_key" {
		t.Fatalf("changed_fields = %v", ev.Attributes["changed_fields"])
	}
	if err := audit.ValidateAttributes(ev.Attributes); err != nil {
		t.Fatalf("the audit allowlist refuses the attributes: %v", err)
	}
	for k, v := range ev.Attributes {
		if s, ok := v.(string); ok && (s == "owner" || s == "admin") {
			t.Fatalf("attribute %s carries a role value", k)
		}
	}
}

func TestDecideFill(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	later := now.Add(time.Hour)
	cases := []struct {
		name       string
		active     bool
		curInviter *uuid.UUID
		curJoined  *time.Time
		want       *uuid.UUID
		resolved   bool
		wantJoined *time.Time
		out        fillDecision
	}{
		{"missing inviter and join are filled", true, nil, nil, &a, true, &now, fillDecision{Inviter: true, Joined: true}},
		{"a set inviter is never overwritten", true, &b, &now, &a, true, &now, fillDecision{}},
		{"a set join is never overwritten", true, nil, &now, nil, false, &later, fillDecision{}},
		{"unresolved inviter is counted, not written", true, nil, &now, &a, false, &now, fillDecision{Unresolved: true}},
		{"a set auth inviter makes an unresolved public one irrelevant", true, &b, &now, &a, false, &now, fillDecision{}},
		{"no public inviter writes nothing", true, nil, &now, nil, false, &now, fillDecision{}},
		{"an inactive row is untouched", false, nil, nil, &a, true, &now, fillDecision{}},
		{"join only", true, &b, nil, &a, true, &now, fillDecision{Joined: true}},
		{"no public join writes no join", true, nil, nil, nil, false, nil, fillDecision{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decideFill(c.active, c.curInviter, c.curJoined, c.want, c.resolved, c.wantJoined); got != c.out {
				t.Fatalf("decideFill = %+v, want %+v", got, c.out)
			}
		})
	}
}

// The fill reuses the projected-membership event the INSERT emits; it is not a role change.
func TestFillAuditReusesTheProjectedEventAndNamesTheFields(t *testing.T) {
	ev := fillAudit(MembershipBackfillTarget{OrgID: uuid.New(), UserID: uuid.New()}, uuid.New(), fillDecision{Inviter: true, Joined: true})
	if ev.EventType != "auth.membership.projected" || ev.Attributes["reason_code"] != "membership_added" {
		t.Fatalf("event = %q reason %v", ev.EventType, ev.Attributes["reason_code"])
	}
	fields, ok := ev.Attributes["changed_fields"].([]string)
	if !ok || len(fields) != 2 || fields[0] != "invited_by" || fields[1] != "joined_at" {
		t.Fatalf("changed_fields = %v", ev.Attributes["changed_fields"])
	}
	if err := audit.ValidateAttributes(ev.Attributes); err != nil {
		t.Fatalf("the audit allowlist refuses the attributes: %v", err)
	}
	one := fillAudit(MembershipBackfillTarget{OrgID: uuid.New(), UserID: uuid.New()}, uuid.New(), fillDecision{Joined: true})
	if f, _ := one.Attributes["changed_fields"].([]string); len(f) != 1 || f[0] != "joined_at" {
		t.Fatalf("joined-only changed_fields = %v", one.Attributes["changed_fields"])
	}
}

// The report takes the flags of what the transaction decided, not of the first read: the unresolved count included.
func TestApplyFillTakesTheFlagsOfWhatTheTransactionChanged(t *testing.T) {
	in := MembershipBackfillResult{RoleUpdated: true, InviterSet: true, JoinedSet: true, InviterUnresolved: true}
	cases := []struct {
		name    string
		in      MembershipBackfillResult
		changed fillDecision
		want    MembershipBackfillResult
	}{
		{"both changed, the inviter now resolved", in, fillDecision{Inviter: true, Joined: true}, MembershipBackfillResult{RoleUpdated: true, InviterSet: true, JoinedSet: true}},
		{"only the join changed, the inviter still unresolved", in, fillDecision{Joined: true, Unresolved: true}, MembershipBackfillResult{RoleUpdated: true, JoinedSet: true, InviterUnresolved: true}},
		{"nothing changed", in, fillDecision{}, MembershipBackfillResult{RoleUpdated: true}},
		{"resolved at the first read, unresolved under the lock", MembershipBackfillResult{InviterSet: true, JoinedSet: true}, fillDecision{Joined: true, Unresolved: true}, MembershipBackfillResult{JoinedSet: true, InviterUnresolved: true}},
	}
	for _, c := range cases {
		if got := applyFill(c.in, c.changed); got != c.want {
			t.Fatalf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

// The inviter is resolved only by the locked read of its principal row, and only as a user principal.
func TestLockedInviterResolved(t *testing.T) {
	cases := []struct {
		found bool
		kind  string
		want  bool
	}{{true, "user", true}, {true, "service_account", false}, {true, "", false}, {false, "user", false}}
	for _, c := range cases {
		if got := lockedInviterResolved(c.found, c.kind); got != c.want {
			t.Fatalf("found=%v kind=%q: got %v, want %v", c.found, c.kind, got, c.want)
		}
	}
}

// The step-1 lock set: the member and the inviter once each, ascending by id (the auth lock order).
func TestFillLockIDsAreUniqueAndAscending(t *testing.T) {
	lo := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	hi := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	cases := []struct {
		name    string
		inviter *uuid.UUID
		member  uuid.UUID
		want    []uuid.UUID
	}{
		{"inviter below the member", &lo, hi, []uuid.UUID{lo, hi}},
		{"inviter above the member", &hi, lo, []uuid.UUID{lo, hi}},
		{"no inviter", nil, hi, []uuid.UUID{hi}},
		{"the inviter is the member", &lo, lo, []uuid.UUID{lo}},
	}
	for _, c := range cases {
		got := fillLockIDs(c.inviter, c.member)
		if len(got) != len(c.want) {
			t.Fatalf("%s: got %v, want %v", c.name, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: got %v, want %v", c.name, got, c.want)
			}
		}
	}
}
