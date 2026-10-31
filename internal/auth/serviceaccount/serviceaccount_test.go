package serviceaccount

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

var okOwner = ownerState{isUser: true, member: true}

func TestValidateName(t *testing.T) {
	for _, n := range []string{"a", "ci-bot", "ci.bot_2", strings.Repeat("a", 63), "0abc"} {
		if err := ValidateName(n); err != nil {
			t.Errorf("%q refused: %v", n, err)
		}
	}
	for _, n := range []string{"", "-a", "A", "a b", "a/b", "a\n", strings.Repeat("a", 64), ".a"} {
		if !errors.Is(ValidateName(n), ErrNameInvalid) {
			t.Errorf("%q accepted", n)
		}
	}
}

func TestDecideCreate(t *testing.T) {
	cases := []struct {
		name      string
		orgActive bool
		owner     ownerState
		want      error
	}{
		{"ok", true, okOwner, nil},
		{"org suspended wins", false, ownerState{}, ErrOrgNotActive},
		{"owner not a user", true, ownerState{member: true}, ErrOwnerNotUser},
		{"owner disabled", true, ownerState{isUser: true, disabled: true, member: true}, ErrOwnerDisabled},
		{"owner not a member", true, ownerState{isUser: true}, ErrOwnerNotMember},
	}
	for _, c := range cases {
		if got := decideCreate(c.orgActive, c.owner); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDecideEnable(t *testing.T) {
	cases := []struct {
		name              string
		active, orgActive bool
		owner             ownerState
		want              error
	}{
		{"ok", false, true, okOwner, nil},
		{"already active", true, true, okOwner, ErrAlreadyInState},
		{"org suspended", false, false, okOwner, ErrOrgNotActive},
		{"owner disabled", false, true, ownerState{isUser: true, disabled: true, member: true}, ErrOwnerDisabled},
		{"owner left the org", false, true, ownerState{isUser: true}, ErrOwnerNotMember},
		{"owner not a user", false, true, ownerState{member: true}, ErrOwnerNotUser},
	}
	for _, c := range cases {
		if got := decideEnable(c.active, c.orgActive, c.owner); !errors.Is(got, c.want) || (c.want == nil && got != nil) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDecideDisable(t *testing.T) {
	if err := decideDisable(true); err != nil {
		t.Fatal(err)
	}
	if err := decideDisable(false); !errors.Is(err, ErrAlreadyInState) {
		t.Fatal(err)
	}
}

func TestDecideSetOwner(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	if err := decideSetOwner(a, b, true, okOwner); err != nil {
		t.Fatal(err)
	}
	if err := decideSetOwner(a, a, true, okOwner); !errors.Is(err, ErrAlreadyInState) {
		t.Fatalf("same owner: %v", err)
	}
	if err := decideSetOwner(a, b, false, okOwner); !errors.Is(err, ErrOrgNotActive) {
		t.Fatalf("org suspended: %v", err)
	}
	if err := decideSetOwner(a, b, true, ownerState{isUser: true, disabled: true, member: true}); !errors.Is(err, ErrOwnerDisabled) {
		t.Fatalf("owner disabled: %v", err)
	}
	if err := decideSetOwner(a, b, true, ownerState{isUser: true}); !errors.Is(err, ErrOwnerNotMember) {
		t.Fatalf("owner not member: %v", err)
	}
}

func TestLockRowsForUseOneAscendingModePerID(t *testing.T) {
	for i := 0; i < 50; i++ {
		sa, owner, actor := uuid.New(), uuid.New(), uuid.New()
		rows := lockRowsFor(&sa, owner, &actor)
		if len(rows) != 3 {
			t.Fatalf("%d rows, want 3", len(rows))
		}
		for j := 1; j < len(rows); j++ {
			if bytes.Compare(rows[j-1].ID[:], rows[j].ID[:]) >= 0 {
				t.Fatalf("rows not ascending: %v", rows)
			}
		}
		want := map[uuid.UUID]membership.LockMode{sa: membership.LockUpdate, owner: membership.LockShare, actor: membership.LockShare}
		for _, r := range rows {
			if want[r.ID] != r.Mode {
				t.Fatalf("id %s mode %v, want %v", r.ID, r.Mode, want[r.ID])
			}
		}
	}
}

func TestLockRowsForMergesIdsKeepingTheStrongestMode(t *testing.T) {
	sa, owner := uuid.New(), uuid.New()
	rows := lockRowsFor(&sa, owner, &owner)
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	rows = lockRowsFor(nil, owner, nil)
	if len(rows) != 1 || rows[0].Mode != membership.LockShare {
		t.Fatalf("create with no actor: %v, want the owner FOR SHARE", rows)
	}
	rows = lockRowsFor(&sa, sa, nil)
	if len(rows) != 1 || rows[0].Mode != membership.LockUpdate {
		t.Fatalf("owner = account: %v, want FOR UPDATE", rows)
	}
}

// captured is a Store whose commit records the mutation and never runs Apply.
func captured(t *testing.T, ownsUsers bool, info *Info) (*Store, *[]audit.Mutation) {
	t.Helper()
	var got []audit.Mutation
	s := &Store{ownsUsers: ownsUsers, logger: nopLogger(), mutations: nopCounter(t),
		commit: func(_ context.Context, m audit.Mutation) error { got = append(got, m); return nil },
		read:   func(context.Context, uuid.UUID) (*Info, error) { return info, nil }}
	return s, &got
}

func TestOwnedGuardRefusesBeforeAnyTransaction(t *testing.T) {
	info := &Info{ID: uuid.New(), OrgID: uuid.New(), Owner: uuid.New()}
	s, got := captured(t, false, info)
	ctx := context.Background()
	if _, err := s.Create(ctx, CreateInput{OrgID: info.OrgID, Name: "bot", Owner: info.Owner}, Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Create: %v", err)
	}
	if err := s.Enable(ctx, info.ID, Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("Enable: %v", err)
	}
	if err := s.SetOwner(ctx, info.ID, uuid.New(), Actor{}); !errors.Is(err, ErrNotOwned) {
		t.Errorf("SetOwner: %v", err)
	}
	if len(*got) != 0 {
		t.Fatalf("%d transactions started, want none", len(*got))
	}
	if err := s.Disable(ctx, info.ID, Actor{}); err != nil || len(*got) != 1 {
		t.Fatalf("Disable must not need users owned: err %v, mutations %d", err, len(*got))
	}
}

func checkMutation(t *testing.T, m audit.Mutation, event, reason string, id uuid.UUID, change map[string]any) {
	t.Helper()
	if err := audit.ValidateAttributes(m.Audit.Attributes); err != nil {
		t.Fatalf("audit attributes refused: %v", err)
	}
	if m.Audit.EventType != event || m.Audit.Attributes["reason_code"] != reason ||
		m.Audit.Attributes["target_kind"] != "service_account" || m.Audit.Attributes["target_id"] != id.String() {
		t.Fatalf("audit row %q %v", m.Audit.EventType, m.Audit.Attributes)
	}
	if m.Audit.SubjectPrincipalID == nil || *m.Audit.SubjectPrincipalID != id.String() || m.Audit.OrganizationID == nil {
		t.Fatalf("audit subject/org %v / %v", m.Audit.SubjectPrincipalID, m.Audit.OrganizationID)
	}
	if len(m.Events) != 1 {
		t.Fatalf("%d outbox events, want 1", len(m.Events))
	}
	e := m.Events[0]
	suffix := strings.TrimPrefix(event, "auth.")
	if e.AggregateType != "service_account" || e.EventType != suffix || e.AggregateID != provisioning.WireID("service_account", id.String()) {
		t.Fatalf("outbox event %+v", e)
	}
	if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
		t.Fatalf("provisioning contract refuses the event: %v", err)
	}
	for k, v := range change {
		if e.Change[k] != v {
			t.Fatalf("change[%s] = %v, want %v", k, e.Change[k], v)
		}
	}
	if len(e.Change) != len(change) {
		t.Fatalf("change %v, want exactly %v", e.Change, change)
	}
}

func TestCreateWritesOneAuditRowAndOneEvent(t *testing.T) {
	org, owner, actor := uuid.New(), uuid.New(), uuid.New()
	rid := uuid.NewString()
	s, got := captured(t, true, nil)
	id, err := s.Create(context.Background(), CreateInput{OrgID: org, Name: "ci-bot", Owner: owner}, Actor{PrincipalID: &actor, RequestID: &rid})
	if err != nil || id == uuid.Nil {
		t.Fatalf("Create: %v %v", id, err)
	}
	m := (*got)[0]
	checkMutation(t, m, "auth.service_account.created", "principal_provisioned", id,
		map[string]any{"status": "active", "owner_principal_id": provisioning.WireID("principal", owner.String())})
	if m.Audit.Attributes["principal_id"] != owner.String() || m.Audit.Attributes["new_status"] != "active" {
		t.Fatalf("attributes %v", m.Audit.Attributes)
	}
	if m.Audit.ActorPrincipalID == nil || *m.Audit.ActorPrincipalID != actor.String() || m.Audit.RequestID == nil || *m.Audit.RequestID != rid {
		t.Fatalf("actor/request %v %v", m.Audit.ActorPrincipalID, m.Audit.RequestID)
	}
	if *m.Audit.OrganizationID != org.String() {
		t.Fatalf("organization %s", *m.Audit.OrganizationID)
	}
}

func TestCreateRefusesABadNameBeforeAnyTransaction(t *testing.T) {
	s, got := captured(t, true, nil)
	if _, err := s.Create(context.Background(), CreateInput{OrgID: uuid.New(), Name: "Bad Name", Owner: uuid.New()}, Actor{}); !errors.Is(err, ErrNameInvalid) {
		t.Fatal(err)
	}
	if len(*got) != 0 {
		t.Fatal("a transaction started")
	}
}

func TestDisableEnableAndSetOwnerWriteTheirAuditRowAndEvent(t *testing.T) {
	info := &Info{ID: uuid.New(), OrgID: uuid.New(), Owner: uuid.New(), Active: true}
	s, got := captured(t, true, info)
	ctx := context.Background()
	next := uuid.New()
	if err := s.Disable(ctx, info.ID, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Enable(ctx, info.ID, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOwner(ctx, info.ID, next, Actor{}); err != nil {
		t.Fatal(err)
	}
	checkMutation(t, (*got)[0], "auth.service_account.disabled", "principal_disabled", info.ID, map[string]any{"status": "disabled"})
	checkMutation(t, (*got)[1], "auth.service_account.enabled", "principal_enabled", info.ID, map[string]any{"status": "active"})
	checkMutation(t, (*got)[2], "auth.service_account.owner_changed", "service_account_owner_changed", info.ID,
		map[string]any{"owner_principal_id": provisioning.WireID("principal", next.String())})
	if (*got)[0].Audit.Attributes["previous_status"] != "active" || (*got)[0].Audit.Attributes["new_status"] != "disabled" {
		t.Fatalf("disable attributes %v", (*got)[0].Audit.Attributes)
	}
	if (*got)[2].Audit.Attributes["principal_id"] != next.String() {
		t.Fatalf("owner change names %v, want the new owner", (*got)[2].Audit.Attributes["principal_id"])
	}
	for _, m := range *got {
		if m.OffStream != "" {
			t.Fatal("a service account event is on the provisioning stream, not off it")
		}
	}
}

func TestRefusalsKeepTheirIdentityThroughRun(t *testing.T) {
	for _, r := range refusals {
		s := &Store{logger: nopLogger(), mutations: nopCounter(t),
			commit: func(context.Context, audit.Mutation) error { return errors.Join(errors.New("tx"), r) }}
		if err := s.run(context.Background(), verbCreate, audit.Mutation{}); !errors.Is(err, r) || errors.Is(err, ErrStore) {
			t.Errorf("%v came back as %v", r, err)
		}
	}
	s := &Store{logger: nopLogger(), mutations: nopCounter(t),
		commit: func(context.Context, audit.Mutation) error { return errors.New("boom") }}
	if err := s.run(context.Background(), verbCreate, audit.Mutation{}); !errors.Is(err, ErrStore) {
		t.Fatalf("a database error came back as %v", err)
	}
}
