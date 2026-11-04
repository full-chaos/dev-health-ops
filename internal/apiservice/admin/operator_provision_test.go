package admin

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func strp(s string) *string { return &s }

// operatorActor is the --actor user: read by id, and never the target user.
var operatorActor = uuid.MustParse("0f000000-0000-4000-8000-00000000000f")

// operatorRows answer the reads of a membership update: the user by email (provTarget),
// the organization by slug, no membership yet (so the update adds one). The
// actor is an id and is not read.
func operatorRows() map[string][]any {
	return map[string][]any{
		"FROM users WHERE lower(email) = $1":         {zeroFill{}},
		"FROM organizations WHERE lower(slug) = $1":  {zeroFill{}},
		"SELECT is_superuser FROM users WHERE id = ": {false},
	}
}

const operatorActorFlag = "0f000000-0000-4000-8000-00000000000f"

var operatorMembershipUpdates = map[string]UpdateUserInput{
	"add":    {Email: strp("a@example.test"), MembershipOrg: strp("acme"), Role: strp("admin"), Actor: strp(operatorActorFlag)},
	"role":   {Email: strp("a@example.test"), MembershipOrg: strp("acme"), Role: strp("viewer"), Actor: strp(operatorActorFlag)},
	"remove": {Email: strp("a@example.test"), RemoveFromOrg: strp("acme"), Actor: strp(operatorActorFlag)},
}

// operatorRowsFor adds the existing membership the role and remove updates need.
func operatorRowsFor(name string) map[string][]any {
	rows := operatorRows()
	if name != "add" {
		rows["FROM memberships WHERE org_id = $1 AND user_id = $2"] = []any{zeroFill{}}
	}
	return rows
}

var operatorWantCall = map[string]string{"add": "add", "role": "role", "remove": "remove"}

func writesMembership(sql string) bool {
	return strings.Contains(sql, "INSERT INTO memberships") || strings.Contains(sql, "UPDATE memberships") ||
		strings.Contains(sql, "DELETE FROM memberships") || strings.Contains(sql, "refresh_tokens")
}

// A membership change always goes through auth-service's route, with the actor
// the operator named, and the CLI writes no membership row itself: not with
// AUTH_PROXY_ORG_PROVISIONING on, and not with it off (the switch is only for
// an organization create).
func TestOperatorMembershipChangeGoesThroughTheRouteWhateverTheSwitchSays(t *testing.T) {
	for _, switchOn := range []bool{true, false} {
		for name, in := range operatorMembershipUpdates {
			db := &provisionDB{rows: operatorRowsFor(name)}
			fake := &fakeProvisioner{db: db}
			result, err := Operator{db: db, OrgProvisioning: switchOn, Provisioner: fake}.UpdateUser(context.Background(), in)
			if err != nil {
				t.Fatalf("%s (switch %v): %v", name, switchOn, err)
			}
			if !slices.Equal(fake.calls, []string{operatorWantCall[name]}) {
				t.Fatalf("%s (switch %v): auth calls %v, want exactly one %q", name, switchOn, fake.calls, operatorWantCall[name])
			}
			args := fake.args[0]
			if args[1] != provTarget || args[2] != operatorActor {
				t.Fatalf("%s (switch %v): principal %v actor %v, want %v and %v", name, switchOn, args[1], args[2], provTarget, operatorActor)
			}
			if name != "remove" && args[3] != *in.Role {
				t.Fatalf("%s (switch %v): role %v, want %v", name, switchOn, args[3], *in.Role)
			}
			for _, sql := range db.log {
				if writesMembership(sql) || strings.Contains(sql, "FOR UPDATE") {
					t.Fatalf("%s (switch %v): the CLI wrote a membership row itself: %q", name, switchOn, sql)
				}
			}
			if len(result.Changes) != 1 {
				t.Fatalf("%s (switch %v): changes %v", name, switchOn, result.Changes)
			}
		}
	}
}

// A user that is not a member is a no-change answer with no auth call.
func TestOperatorRemoveOfANonMemberCallsNoRoute(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	result, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), operatorMembershipUpdates["remove"])
	if err != nil || len(fake.calls) != 0 || len(result.Changes) != 1 || !strings.Contains(result.Changes[0], "no change") {
		t.Fatalf("err %v calls %v changes %v, want no route call and a no-change line", err, fake.calls, result.Changes)
	}
}

// With no client of the routes, or no --actor, a membership change refuses by
// name before any database call, and the delete the same.
func TestOperatorMembershipAndDeleteRefuseWithoutClientOrActor(t *testing.T) {
	for name, in := range operatorMembershipUpdates {
		db := &provisionDB{rows: operatorRowsFor(name)}
		_, err := Operator{db: db}.UpdateUser(context.Background(), in)
		if !errors.Is(err, ErrNoProvisioner) || len(db.log) != 0 {
			t.Fatalf("%s no client: err %v, database calls %v; want ErrNoProvisioner and none", name, err, db.log)
		}
		noActor := in
		noActor.Actor = nil
		db = &provisionDB{rows: operatorRowsFor(name)}
		fake := &fakeProvisioner{db: db}
		_, err = Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), noActor)
		if !errors.Is(err, ErrActorRequired) || len(db.log) != 0 || len(fake.calls) != 0 {
			t.Fatalf("%s no actor: err %v, database calls %v; want ErrActorRequired and none", name, err, db.log)
		}
	}
	db := &provisionDB{}
	_, err := Operator{db: db}.DeleteOrg(context.Background(), provOrg.String(), false, DeleteOrgConfig{Actor: operatorActorFlag})
	if !errors.Is(err, ErrNoProvisioner) || len(db.log) != 0 {
		t.Fatalf("delete no client: err %v, database calls %v", err, db.log)
	}
	fake := &fakeProvisioner{db: db}
	_, err = Operator{db: db, Provisioner: fake}.DeleteOrg(context.Background(), provOrg.String(), false, DeleteOrgConfig{})
	if !errors.Is(err, ErrActorRequired) || len(db.log) != 0 || len(fake.calls) != 0 {
		t.Fatalf("delete no actor: err %v, database calls %v", err, db.log)
	}
}

// An actor that is neither an id nor an email refuses before any route call; an
// actor id is not read from the database; an actor email is.
func TestOperatorActorForms(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	in := operatorMembershipUpdates["add"]
	in.Actor = strp("not-an-id")
	_, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), in)
	var refusal *OperatorError
	if !errors.As(err, &refusal) || len(fake.calls) != 0 {
		t.Fatalf("err %v calls %v, want an operator refusal and no route call", err, fake.calls)
	}
	db = &provisionDB{rows: operatorRows()}
	fake = &fakeProvisioner{db: db}
	in.Actor = strp("boss@example.test")
	if _, err = (Operator{db: db, Provisioner: fake}).UpdateUser(context.Background(), in); err != nil || fake.args[0][2] != provTarget {
		t.Fatalf("email actor: err %v args %v, want the actor read from the users row", err, fake.args)
	}
}

// Each auth-service reason is the refusal the operator reads; any other
// answer is a failure (not a refusal) that names nothing secret, and the
// update reports no change for the failed step.
func TestOperatorMapsAuthServiceAnswers(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		update string
		want   string
	}{
		{"role invalid", refusedWith(400, "role_invalid"), "add", "Invalid role: admin"},
		{"already member", refusedWith(409, "already_member"), "add", "User is already a member of this organization"},
		{"last owner on role", refusedWith(409, "last_owner"), "role", "Cannot remove the last owner of an organization"},
		{"last owner on remove", refusedWith(409, "last_owner"), "remove", "Cannot remove the last owner of an organization"},
		{"membership missing", refusedWith(404, "membership_not_found"), "role", "Membership not found"},
		{"admin required", refusedWith(403, "admin_required"), "add", "Admin access required for organization"},
		{"superuser required", refusedWith(403, "superuser_required"), "add", "Superuser access required"},
		{"actor deactivated", refusedWith(403, "principal_deactivated"), "add", "The actor is deactivated"},
		{"unnamed refusal", refusedWith(409, "state_changed"), "add", `The authentication service refused the change (status 409, reason "state_changed"); nothing was written.`},
	}
	for _, tc := range cases {
		db := &provisionDB{rows: operatorRowsFor(tc.update)}
		fake := &fakeProvisioner{db: db, err: tc.err}
		result, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), operatorMembershipUpdates[tc.update])
		var refusal *OperatorError
		if result != nil || !errors.As(err, &refusal) || refusal.Message != tc.want {
			t.Errorf("%s: result %v err %v, want the refusal %q", tc.name, result, err, tc.want)
		}
	}
	for _, failure := range []error{authprovision.ErrUnavailable, errors.New("connection refused")} {
		db := &provisionDB{rows: operatorRowsFor("add")}
		fake := &fakeProvisioner{db: db, err: failure}
		_, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), operatorMembershipUpdates["add"])
		var refusal *OperatorError
		if err == nil || errors.As(err, &refusal) || !errors.Is(err, failure) {
			t.Errorf("failure %v: err %v, want a failure that wraps it, not a refusal", failure, err)
		}
		for _, sql := range db.log {
			if writesMembership(sql) {
				t.Errorf("failure %v: the CLI fell back to a membership write: %q", failure, sql)
			}
		}
	}
}

// A profile change with a membership change: the route is called first, in
// no transaction of this call; the profile writes follow, and a refused route
// leaves them unrun.
func TestOperatorMembershipRouteRunsBeforeTheProfileWrites(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	in := operatorMembershipUpdates["add"]
	in.FullName = strp("New Name")
	result, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), in)
	if err != nil || len(result.Changes) != 2 {
		t.Fatalf("err %v changes %v", err, result)
	}
	call := slices.Index(db.log, "auth:add")
	begins := 0
	for i, sql := range db.log {
		if sql == "BEGIN" {
			begins++
			if begins == 2 && i < call {
				t.Fatalf("the profile transaction began before the route call: %q", db.log)
			}
		}
	}
	if call < 0 || begins != 2 {
		t.Fatalf("route call at %d, %d transactions: %q", call, begins, db.log)
	}
	db = &provisionDB{rows: operatorRows()}
	fake = &fakeProvisioner{db: db, err: refusedWith(409, "already_member")}
	if _, err = (Operator{db: db, Provisioner: fake}).UpdateUser(context.Background(), in); err == nil {
		t.Fatal("a refused route left the update to succeed")
	}
	for _, sql := range db.log {
		if strings.Contains(sql, "UPDATE users") || strings.Contains(sql, "password_hash =") {
			t.Fatalf("a refused route was followed by a profile write: %q", db.log)
		}
	}
	if n := strings.Count(strings.Join(db.log, "\n"), "BEGIN"); n != 1 {
		t.Fatalf("%d transactions after a refused route, want only the read one: %q", n, db.log)
	}
}

// A profile-only update makes no route call and needs no actor.
func TestOperatorProfileUpdateNeedsNoRouteOrActor(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	_, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), UpdateUserInput{Email: strp("a@example.test"), FullName: strp("New Name")})
	if err != nil || len(fake.calls) != 0 {
		t.Fatalf("err %v calls %v", err, fake.calls)
	}
	_, err = Operator{db: &provisionDB{rows: operatorRows()}}.UpdateUser(context.Background(), UpdateUserInput{Email: strp("a@example.test"), FullName: strp("New Name")})
	if err != nil {
		t.Fatalf("a profile update with no client: %v", err)
	}
}

// The delete's deprovision call carries the actor read from --actor, and an
// unknown actor refuses without a call.
func TestOperatorDeleteDeprovisionCarriesTheActor(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	operator := Operator{db: db, Provisioner: fake}
	p := actorProvisioner{Provisioner: fake, actor: operatorActorFlag, resolve: operator.resolveActor}
	if err := p.Deprovision(context.Background(), provOrg, uuid.Nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.calls, []string{"deprovision"}) || fake.args[0][0] != provOrg || fake.args[0][1] != operatorActor {
		t.Fatalf("calls %v args %v, want one deprovision of the org by the actor", fake.calls, fake.args)
	}
	missing := &provisionDB{rows: map[string][]any{}}
	fake2 := &fakeProvisioner{db: missing}
	p = actorProvisioner{Provisioner: fake2, actor: "nobody@example.test", resolve: Operator{db: missing}.resolveActor}
	err := p.Deprovision(context.Background(), provOrg, uuid.Nil)
	var refusal *OperatorError
	if !errors.As(err, &refusal) || len(fake2.calls) != 0 {
		t.Fatalf("err %v calls %v, want a refusal and no call", err, fake2.calls)
	}
	wrapped := provisionError((&orgDeletionAuthError{refusedWith(403, "superuser_required")}).err, "")
	if !errors.As(wrapped, &refusal) || refusal.Message != "Superuser access required" {
		t.Fatalf("deprovision refusal maps to %v", wrapped)
	}
}

// A dry run writes nothing, so it needs no client and no actor (here it stops
// at today's ClickHouse refusal).
func TestOperatorDryRunDeleteNeedsNoClientOrActor(t *testing.T) {
	_, err := Operator{db: &provisionDB{}, OrgProvisioning: true}.DeleteOrg(context.Background(), provOrg.String(), true, DeleteOrgConfig{})
	var refusal *OperatorError
	if errors.Is(err, ErrNoProvisioner) || errors.Is(err, ErrActorRequired) || !errors.As(err, &refusal) || refusal.Message != wantOrgDeletionRefusedDetail {
		t.Fatalf("dry run: %v, want today's ClickHouse refusal", err)
	}
}

// An organization create keeps its switch refusal: it would write go-api's rows only.
func TestOperatorCreateOrgStillRefusesWithTheSwitchOn(t *testing.T) {
	db := &provisionDB{}
	_, err := Operator{db: db, OrgProvisioning: true}.CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community"})
	if !errors.Is(err, ErrOrgProvisioningOn) || len(db.log) != 0 {
		t.Fatalf("err %v, database calls %v; want ErrOrgProvisioningOn and none", err, db.log)
	}
}

// --org with --remove-from-org is refused before any database or route call:
// a second step could fail after the first was sent.
func TestOperatorTwoMembershipStepsAreRefusedBeforeAnyWrite(t *testing.T) {
	for name, first := range map[string]string{"add then remove": "add", "role then remove": "role"} {
		db := &provisionDB{rows: operatorRowsFor(first)}
		fake := &fakeProvisioner{db: db}
		in := operatorMembershipUpdates[first]
		in.RemoveFromOrg = strp("other")
		in.FullName = strp("New Name")
		_, err := Operator{db: db, Provisioner: fake}.UpdateUser(context.Background(), in)
		if !errors.Is(err, ErrOneMembershipStep) || len(db.log) != 0 || len(fake.calls) != 0 {
			t.Fatalf("%s: err %v database calls %v route calls %v, want ErrOneMembershipStep and none", name, err, db.log, fake.calls)
		}
	}
}

// A real delete asks auth-service (as the actor) before it purges; a dry run asks nothing.
func TestOperatorDeleteWiresTheDeprovisionCall(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	operator := Operator{db: db, Provisioner: fake}
	if h := operator.orgDeleteHandlers(DeleteOrgConfig{Actor: operatorActorFlag}, nil, true); h.orgProvision != nil {
		t.Fatalf("a dry run holds a provisioner: %v", h.orgProvision)
	}
	h := operator.orgDeleteHandlers(DeleteOrgConfig{Actor: operatorActorFlag}, nil, false)
	if h.orgProvision == nil {
		t.Fatal("a real delete holds no provisioner")
	}
	ctx := policy.WithUser(context.Background(), &policy.User{})
	if err := h.deprovisionInAuth(ctx, provOrg); err != nil {
		t.Fatal(err)
	}
	if len(fake.calls) != 1 || fake.args[0][0] != provOrg || fake.args[0][1] != operatorActor {
		t.Fatalf("calls %v args %v, want one deprovision of the org by the actor", fake.calls, fake.args)
	}
}

// --actor may be the old email of the user the same call renames: it is read,
// with the user, before the route call and before any profile statement.
func TestOperatorActorIsReadBeforeTheProfileUpdate(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	fake := &fakeProvisioner{db: db}
	in := operatorMembershipUpdates["add"]
	in.Actor = strp("a@example.test")
	in.FullName = strp("New Name")
	if _, err := (Operator{db: db, Provisioner: fake}).UpdateUser(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	call := slices.Index(db.log, "auth:add")
	emailReads := 0
	for _, sql := range db.log[:max(call, 0)] {
		if strings.Contains(sql, "FROM users WHERE lower(email) = $1") {
			emailReads++
		}
	}
	profile := slices.IndexFunc(db.log, func(sql string) bool { return strings.Contains(sql, "FROM users WHERE id = $1") })
	if call < 0 || emailReads != 2 || profile < call {
		t.Fatalf("email reads before the route %d (want user and actor), route at %d, profile read at %d: %q", emailReads, call, profile, db.log)
	}
}
