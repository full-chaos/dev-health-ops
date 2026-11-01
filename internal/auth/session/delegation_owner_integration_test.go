//go:build integration

package session

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// roleWithADeniedAction is an organization role that holds an action a plain
// actor's delegation may not exercise, with that action.
func roleWithADeniedAction(t *testing.T) (string, string) {
	t.Helper()
	catalog := policycatalog.Default()
	for _, role := range catalog.Roles() {
		if role.Scope != policycatalog.ScopeOrganization {
			continue
		}
		delegable, err := catalog.DelegatedActions(role.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, action := range role.Actions {
			if !slices.Contains(delegable, action) {
				return role.ID, action
			}
		}
	}
	t.Fatal("no organization role holds an action denied for a delegation")
	return "", ""
}

func (d *delegationEnv) ownerStore(t *testing.T) *platformrole.Store {
	t.Helper()
	store, err := platformrole.New(platformrole.Config{Pool: d.admin, Schema: d.schema})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (d *delegationEnv) fixtureRole(t *testing.T, ctx context.Context, role string) delegationCase {
	t.Helper()
	c := delegationCase{actor: d.user(t, ctx, true), target: d.user(t, ctx, false), org: d.org(t, ctx)}
	d.member(t, ctx, c.org, c.target, role)
	base, err := d.owned.Create(ctx, c.actor, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	c.base = base
	return c
}

func (d *delegationEnv) storedActions(t *testing.T, ctx context.Context, id uuid.UUID) []string {
	t.Helper()
	var actions []string
	if err := d.admin.QueryRow(ctx, `SELECT delegation_permitted_actions FROM auth.sessions WHERE id = $1`, id).Scan(&actions); err != nil {
		t.Fatal(err)
	}
	return actions
}

func TestDelegationOwnerActorStoresTheRoleFullSet(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	role, denied := roleWithADeniedAction(t)
	c := d.fixtureRole(t, ctx, role)
	if _, err := d.ownerStore(t).GrantOwner(ctx, c.actor); err != nil {
		t.Fatalf("GrantOwner: %v", err)
	}
	row := d.start(t, ctx, c)
	full, err := policycatalog.Default().Role(role)
	if err != nil {
		t.Fatal(err)
	}
	got := d.storedActions(t, ctx, row.ID)
	if !slices.Equal(got, full.Actions) || !slices.Contains(got, denied) {
		t.Fatalf("stored %v, want the full set %v with %q", got, full.Actions, denied)
	}
}

func TestDelegationPlainActorKeepsTheDelegableSet(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	role, denied := roleWithADeniedAction(t)
	row := d.start(t, ctx, d.fixtureRole(t, ctx, role))
	want, err := policycatalog.Default().DelegatedActions(role)
	if err != nil {
		t.Fatal(err)
	}
	got := d.storedActions(t, ctx, row.ID)
	if !slices.Equal(got, want) || slices.Contains(got, denied) {
		t.Fatalf("stored %v, want %v without %q", got, want, denied)
	}
}

func TestDelegationOwnerRoleRevokedBeforeStartGivesTheDelegableSet(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	role, denied := roleWithADeniedAction(t)
	c := d.fixtureRole(t, ctx, role)
	owners := d.ownerStore(t)
	if _, err := owners.GrantOwner(ctx, c.actor); err != nil {
		t.Fatalf("GrantOwner: %v", err)
	}
	if err := owners.RevokeOwner(ctx, c.actor); err != nil {
		t.Fatalf("RevokeOwner: %v", err)
	}
	row := d.start(t, ctx, c)
	if got := d.storedActions(t, ctx, row.ID); slices.Contains(got, denied) {
		t.Fatalf("stored %v holds %q after the owner role ended", got, denied)
	}
}

func TestSupersedeRefusesADelegatedPredecessor(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	c := d.fixture(t, ctx)
	row := d.start(t, ctx, c)
	before := d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, c.target)
	_, err := d.owned.CreateSupersedingAudited(ctx, c.target, &c.org, []string{"pwd"}, row.ID, sessionEvent("auth.session.issued"))
	if !errors.Is(err, ErrPredecessorDelegated) {
		t.Fatalf("err = %v, want ErrPredecessorDelegated", err)
	}
	if got := d.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, c.target); got != before {
		t.Fatalf("sessions of the target = %d, want %d", got, before)
	}
	if d.liveRowsOf(t, ctx, c.actor) != 1 {
		t.Fatal("the refused switch ended the delegated row")
	}
}

func TestDelegationCounterHoldsStartedAndRefused(t *testing.T) {
	ctx := testContext(t)
	d := newDelegationEnv(t, ctx)
	reader := sdkmetric.NewManualReader()
	store := d.newStore(t, d.keyText, Config{UsersOwned: true, PlatformRolesOwned: true,
		Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")})
	c := d.fixture(t, ctx)
	if _, err := store.StartDelegation(ctx, c.input()); err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	bad := c.input()
	bad.TargetPrincipalID = c.actor
	if _, err := store.StartDelegation(ctx, bad); !errors.Is(err, ErrTargetIsActor) {
		t.Fatalf("err = %v, want ErrTargetIsActor", err)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "auth.session.delegations" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				outcome, _ := point.Attributes.Value("outcome")
				counts[outcome.AsString()] += point.Value
			}
		}
	}
	if counts["started"] != 1 || counts["refused"] != 1 || len(counts) != 2 {
		t.Fatalf("outcomes = %v, want started 1 and refused 1", counts)
	}
}
