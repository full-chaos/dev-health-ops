//go:build integration

package decisionstate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed"
)

// The admin force delete (identity.AdminForceDeleteUser, PLAN-13380 S5) after a cached allow: the erase's
// principal.disabled event bumps the principal's revision and, through the real consumer, evicts the allow that read
// it; the next read of the erased principal is unknown (a deny), never the cached allow.
func TestAnErasedUsersEventEvictsTheAllowAndTheNextReadIsUnknown(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, actor, org := e.user(t, ctx, true, false), e.user(t, ctx, true, true), e.org(t, ctx)
	prn := "prn_" + user.String()
	marks := &lazyMarks{}
	cache, err := decisioncache.New(decisioncache.Config{Catalog: policycatalog.Default(), Watermarks: marks})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := revisionfeed.NewConsumer(revisionfeed.Config{Invalidator: cache})
	if err != nil {
		t.Fatal(err)
	}
	marks.w = consumer.Watermarks()
	stamp := cache.Begin()
	deps, sets := depsOf(t, e.snapshotWith(t, ctx, e.collecting(t, false), user, org.String()))
	before, ok := revisionOf(deps, "principal", prn)
	if !ok {
		t.Fatalf("no principal dependency in %+v", deps)
	}
	k := lowKey(user, org)
	if got := cache.Put(k, authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole, PolicyRevision: k.PolicyRevision},
		decisioncache.Read{Dependencies: deps, SetTypes: sets, Stamp: stamp}); got != decisioncache.Stored {
		t.Fatalf("Put = %q", got)
	}

	// The erase as the table owner: this package's runtime role is not given the users-owned grants.
	store, err := identity.New(identity.Config{Pool: e.admin, Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	none := func(context.Context, audit.TxOps) error { return nil }
	if _, err := store.AdminForceDeleteUser(ctx, identity.AdminForceDeleteRequest{Principal: user, Actor: actor, Authorize: none, RevokeRoles: none}); err != nil {
		t.Fatalf("erase: %v", err)
	}
	var revision int64
	if err := e.admin.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = 'principal' AND aggregate_id = $1`, prn).Scan(&revision); err != nil {
		t.Fatalf("the principal's revision row after the erase: %v", err)
	}
	if revision <= before {
		t.Fatalf("principal revision %d after the erase, want above the cached read's %d", revision, before)
	}
	var payload []byte
	if err := e.admin.QueryRow(ctx, `SELECT payload FROM auth.auth_outbox_events WHERE aggregate_type = 'principal' AND aggregate_id = $1
AND event_type = 'principal.disabled' ORDER BY id DESC LIMIT 1`, prn).Scan(&payload); err != nil {
		t.Fatalf("no principal.disabled event for the erased principal: %v", err)
	}
	var p contracts.ProvisioningPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Handle(ctx, contracts.ProvisioningEvent{AggregateType: "principal", AggregateID: prn, Payload: p}); err != nil {
		t.Fatalf("the consumer on the erase event: %v", err)
	}
	if _, ok := cache.Get(k); ok {
		t.Fatal("the allow that read the erased principal was served after the erase event")
	}
	if s := e.snapshot(t, ctx, user, org.String()).PrincipalStatus(); s != authorization.StatusUnknown {
		t.Fatalf("the erased principal reads %v, want unknown (a deny)", s)
	}
}
