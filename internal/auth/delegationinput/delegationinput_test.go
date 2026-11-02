package delegationinput

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	org      = "org-1"
	ordinary = "git.read"
	floor    = "members.invite"
)

type inner struct{ role string }

func (inner) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (i inner) PlatformRole() string                { return i.role }
func (inner) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (inner) Membership(id string) (authorization.Membership, bool) {
	return authorization.Membership{OrgID: id, Role: "owner", Status: authorization.StatusActive}, true
}

type fakeSessions struct {
	rec   session.Record
	err   error
	calls int
}

func (f *fakeSessions) Current(context.Context, uuid.UUID) (session.Record, error) {
	f.calls++
	return f.rec, f.err
}

type fakeRoles struct {
	auth  Authority
	err   error
	calls int
}

func (f *fakeRoles) Resolve(context.Context, uuid.UUID) (Authority, error) {
	f.calls++
	return f.auth, f.err
}

var (
	now     = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	actor   = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	subject = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	orgID   = uuid.MustParse("33333333-3333-3333-3333-333333333333")
	sessID  = uuid.MustParse("44444444-4444-4444-4444-444444444444")
)

func record() session.Record {
	o := orgID
	return session.Record{
		ID: sessID, PrincipalID: subject, OrganizationID: &o, AMR: []string{"pwd"}, ExpiresAt: now.Add(10 * time.Minute),
		Delegation: &session.Delegation{Actor: actor, PermittedActions: []string{ordinary, floor}},
	}
}

func rig(t *testing.T, rec session.Record, auth Authority) (*Adapter, *fakeSessions, *fakeRoles) {
	t.Helper()
	s, r := &fakeSessions{rec: rec}, &fakeRoles{auth: auth}
	a, err := New(Config{Sessions: s, Roles: r, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return a, s, r
}

func in(i authorization.Snapshot) Input {
	return Input{Inner: i, SessionID: sessID, PrincipalID: subject}
}

func eval(s authorization.Snapshot, action string) authorization.Decision {
	return authorization.NewEvaluator().Evaluate(authorization.Request{Action: action, ResourceKind: "organization", OrganizationID: orgID.String()}, s)
}

type fakeAuth struct{ owner, power bool }

func (f fakeAuth) IsOwner() bool { return f.owner }
func (f fakeAuth) Allows(a string) bool {
	return f.power && a == session.ImpersonateAction
}

var (
	owner  = fakeAuth{owner: true, power: true}
	admin  = fakeAuth{power: true}
	nobody = fakeAuth{}
)

// The evaluator reads the target org from Delegation: it is the record's org.
func TestOwnerExemptsTheFloorThroughTheRealEvaluator(t *testing.T) {
	a, _, _ := rig(t, record(), owner)
	s, err := a.Snapshot(context.Background(), in(inner{}))
	if err != nil {
		t.Fatal(err)
	}
	d := eval(s, floor)
	if !d.Allowed || d.Audit == nil {
		t.Fatalf("owner: %+v", d)
	}
	del, _ := s.(authorization.DelegationSnapshot).Delegation()
	if del.ActorPlatformRole != platformrole.PlatformOwner || del.Chain[0].ActorPrincipalID != actor.String() ||
		del.TargetOrganizationID != orgID.String() || del.Assurance != "aal1" {
		t.Fatalf("%+v", del)
	}
	if got := eval(s, ordinary); !got.Allowed || got.Obligations == nil || got.Obligations.MinimumAssurance != "aal1" {
		t.Fatalf("ordinary: %+v", got)
	}
}

func TestAdminWithThePowerIsNotOwner(t *testing.T) {
	a, _, _ := rig(t, record(), admin)
	s, err := a.Snapshot(context.Background(), in(inner{}))
	if err != nil {
		t.Fatal(err)
	}
	if d := eval(s, floor); d.Allowed || d.Reason != authorization.ReasonImpersonationDenied {
		t.Fatalf("%+v", d)
	}
	if d := eval(s, ordinary); !d.Allowed {
		t.Fatalf("%+v", d)
	}
}

func assertInvalid(t *testing.T, s authorization.Snapshot, err error, wantErr bool) {
	t.Helper()
	if (err != nil) != wantErr {
		t.Fatalf("err = %v, want error %v", err, wantErr)
	}
	if d := eval(s, ordinary); d.Allowed || d.Reason != authorization.ReasonDelegationInvalid {
		t.Fatalf("ordinary action: %+v", d)
	}
}

func TestNoPowerIsInvalidWithoutError(t *testing.T) {
	a, _, _ := rig(t, record(), nobody)
	s, err := a.Snapshot(context.Background(), in(inner{}))
	assertInvalid(t, s, err, false)
}

func TestRoleReadErrorDeniesEverythingAndReturnsTheCause(t *testing.T) {
	a, _, r := rig(t, record(), owner)
	cause := errors.New("boom")
	r.err = cause
	s, err := a.Snapshot(context.Background(), in(inner{}))
	assertInvalid(t, s, err, true)
	if !errors.Is(err, ErrDelegationUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("%v", err)
	}
}

func TestRoleIsReadLivePerDecision(t *testing.T) {
	a, s, r := rig(t, record(), owner)
	first, _ := a.Snapshot(context.Background(), in(inner{}))
	if d, _ := first.(authorization.DelegationSnapshot).Delegation(); d.ActorPlatformRole != platformrole.PlatformOwner {
		t.Fatalf("%+v", d)
	}
	r.auth = admin
	second, _ := a.Snapshot(context.Background(), in(inner{}))
	if d, _ := second.(authorization.DelegationSnapshot).Delegation(); d.ActorPlatformRole != "" || len(d.Chain) != 1 {
		t.Fatalf("owner lapsed, admin held: %+v", d)
	}
	r.auth = nobody
	third, err := a.Snapshot(context.Background(), in(inner{}))
	assertInvalid(t, third, err, false)
	if r.calls != 3 || s.calls != 3 {
		t.Fatalf("calls roles=%d sessions=%d, want 3 each", r.calls, s.calls)
	}
}

func TestSubjectRoleNeverFillsTheActorRole(t *testing.T) {
	a, _, _ := rig(t, record(), admin)
	s, _ := a.Snapshot(context.Background(), in(inner{role: platformrole.PlatformOwner}))
	if d, _ := s.(authorization.DelegationSnapshot).Delegation(); d.ActorPlatformRole != "" {
		t.Fatalf("%+v", d)
	}
}

func TestAssuranceFollowsTheRowAmr(t *testing.T) {
	rec := record()
	rec.AMR = []string{"fed"}
	a, _, _ := rig(t, rec, owner)
	s, _ := a.Snapshot(context.Background(), in(inner{}))
	if d, _ := s.(authorization.DelegationSnapshot).Delegation(); d.Assurance != "aal1" {
		t.Fatalf("%+v", d)
	}
	for _, amr := range [][]string{nil, {"junk"}} {
		rec.AMR = amr
		a, _, _ = rig(t, rec, owner)
		s, err := a.Snapshot(context.Background(), in(inner{}))
		if err != nil {
			t.Fatal(err)
		}
		if d := eval(s, ordinary); d.Allowed || d.Reason != authorization.ReasonDelegationInvalid {
			t.Fatalf("%v: %+v", amr, d)
		}
	}
}

func TestRowShapesAreInvalid(t *testing.T) {
	revoked := now.Add(-time.Minute)
	other := uuid.New()
	cases := map[string]func(*session.Record, *Input){
		"nil org":          func(r *session.Record, _ *Input) { r.OrganizationID = nil },
		"revoked":          func(r *session.Record, _ *Input) { r.RevokedAt = &revoked },
		"expired":          func(r *session.Record, _ *Input) { r.ExpiresAt = now },
		"subject mismatch": func(_ *session.Record, i *Input) { i.PrincipalID = other },
		"nil session id":   func(_ *session.Record, i *Input) { i.SessionID = uuid.Nil },
		"claim mismatch":   func(_ *session.Record, i *Input) { i.ClaimedActor = &other },
		"nil actor":        func(r *session.Record, _ *Input) { r.Delegation.Actor = uuid.Nil },
		"claim, plain row": func(r *session.Record, i *Input) { r.Delegation = nil; i.ClaimedActor = &actor },
	}
	for name, mut := range cases {
		rec, i := record(), in(inner{})
		mut(&rec, &i)
		a, _, _ := rig(t, rec, owner)
		s, err := a.Snapshot(context.Background(), i)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if d := eval(s, ordinary); d.Allowed || d.Reason != authorization.ReasonDelegationInvalid {
			t.Errorf("%s: %+v", name, d)
		}
	}
	a, s, _ := rig(t, record(), owner)
	s.err = errors.New("down")
	sn, err := a.Snapshot(context.Background(), in(inner{}))
	assertInvalid(t, sn, err, true)
}

func TestClaimMatchAndPlainRow(t *testing.T) {
	a, _, _ := rig(t, record(), owner)
	i := in(inner{})
	i.ClaimedActor = &actor
	s, err := a.Snapshot(context.Background(), i)
	if err != nil {
		t.Fatal(err)
	}
	if d := eval(s, ordinary); !d.Allowed {
		t.Fatalf("%+v", d)
	}
	rec := record()
	rec.Delegation = nil
	a, _, r := rig(t, rec, owner)
	in0 := inner{}
	s, err = a.Snapshot(context.Background(), in(in0))
	if err != nil || s != authorization.Snapshot(in0) || r.calls != 0 {
		t.Fatalf("%v %v roles=%d", s, err, r.calls)
	}
}

func TestPermittedActionsAreCopied(t *testing.T) {
	rec := record()
	a, _, _ := rig(t, rec, owner)
	s, _ := a.Snapshot(context.Background(), in(inner{}))
	rec.Delegation.PermittedActions[0] = "x"
	if d, _ := s.(authorization.DelegationSnapshot).Delegation(); d.Chain[0].PermittedActions[0] != ordinary {
		t.Fatalf("%+v", d)
	}
}

func TestNewAndNilInner(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("no readers accepted")
	}
	a, _, _ := rig(t, record(), owner)
	if _, err := a.Snapshot(context.Background(), Input{SessionID: sessID}); err == nil {
		t.Fatal("nil inner accepted")
	}
}
