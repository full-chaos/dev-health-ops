//go:build integration

package lockorder

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The cells below race the writers of public.organizations and org_licenses:
// go-api's admin PATCH, the Stripe subscription webhook and the org delete
// purge. Each holds one party at a statement, runs the other into its lock
// wait, then releases: both must end with no 40P01.

const stripeWebhookPath = "/api/v1/billing/webhooks/stripe"

// Billing's and the PATCH's writes of the two rows: their SQL differs, so a
// barrier holds exactly one of the parties.
const (
	billingOrgWrite  = "UPDATE organizations SET tier = $2, updated_at = $3 WHERE id = $1"
	billingPlanRead  = "SELECT tier FROM billing_plans WHERE id = $1"
	patchLicenseSync = "UPDATE org_licenses SET tier = $2, managed_by = $3"
	purgeMembers     = "DELETE FROM memberships WHERE"
)

// billingOrg is an organization a Stripe event can change and an admin PATCH
// can move off its tier: tier team, managed by stripe, with a license row.
type billingOrg struct {
	id    uuid.UUID
	price string
	sub   string
}

func (e *env) seedBillingOrg(t *testing.T, ctx context.Context) billingOrg {
	t.Helper()
	b := billingOrg{id: e.seedOrg(t, ctx), price: "price_" + uuid.NewString(), sub: "sub_" + uuid.NewString()}
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE public.organizations SET tier = 'team', managed_by = 'stripe' WHERE id = $1`, []any{b.id}},
		{`INSERT INTO public.org_licenses (id, org_id, tier, managed_by, features_override, created_at, updated_at)
VALUES ($1, $2, 'team', 'stripe', '{}'::json, now(), now())`, []any{uuid.New(), b.id}},
		{`INSERT INTO public.billing_plans (id, key, name, tier, is_active, display_order) VALUES ($1, $2, 'lockorder', 'enterprise', true, 0)`,
			[]any{uuid.MustParse(b.price[len("price_"):]), "plan-" + b.price}},
		{`INSERT INTO public.billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id)
VALUES ($1, $2, 'monthly', 100, 'usd', true, $3)`, []any{uuid.New(), uuid.MustParse(b.price[len("price_"):]), b.price}},
	} {
		if _, err := e.admin.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatalf("seed billing: %v", err)
		}
	}
	return b
}

// subscriptionEvent is a signed customer.subscription.created delivery for b:
// the route takes it through the subscription upsert and the license sync
// and calls no Stripe API.
func (e *env) subscriptionEvent(ctx context.Context, b billingOrg, created int64) response {
	start := time.Now().Unix()
	payload, err := json.Marshal(map[string]any{
		"id": "evt_" + uuid.NewString(), "object": "event", "type": "customer.subscription.created", "created": created,
		"data": map[string]any{"object": map[string]any{
			"id": b.sub, "object": "subscription", "customer": "cus_" + b.id.String(), "status": "active",
			"current_period_start": start, "current_period_end": start + 86400, "cancel_at_period_end": false,
			"metadata": map[string]any{"org_id": b.id.String()},
			"items":    map[string]any{"data": []any{map[string]any{"price": map[string]any{"id": b.price}}}},
		}},
	})
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	now := time.Now().Unix()
	mac := hmac.New(sha256.New, []byte(e.webhookKey))
	fmt.Fprintf(mac, "%d.%s", now, payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.server.URL+stripeWebhookPath, bytes.NewReader(payload))
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	request.Header.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", now, hex.EncodeToString(mac.Sum(nil))))
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, raw: string(raw)}
}

// patchTier is go-api's admin PATCH of the tier to community: it changes the
// tier, keeps the org managed by stripe, and syncs the license row.
func (e *env) patchTier(ctx context.Context, org uuid.UUID) response {
	return e.call(ctx, e.root, http.MethodPatch, "/api/v1/admin/orgs/"+org.String(), map[string]any{"tier": "community"})
}

// wantBillingCommitted fails the cell unless the billing event committed and the
// organization and its license ended on one tier.
func (e *env) wantBillingCommitted(t *testing.T, ctx context.Context, cell string, b billingOrg) {
	t.Helper()
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.subscription_events ev
JOIN public.subscriptions s ON s.id = ev.subscription_id WHERE s.stripe_subscription_id = $1`, b.sub); got != "1" {
		t.Fatalf("%s: race answer: %s subscription events, want 1 (the event did not commit)", cell, got)
	}
	if got := e.scalar(t, ctx, `SELECT count(*)::text FROM public.organizations o
JOIN public.org_licenses l ON l.org_id = o.id WHERE o.id = $1 AND o.tier = l.tier`, b.id); got != "1" {
		t.Fatalf("%s: race answer: the organization and its license ended on different tiers", cell)
	}
}

func wantAcceptOrGone(t *testing.T, cell string, p *party) {
	t.Helper()
	if p.resp.status != http.StatusOK && p.resp.status != http.StatusNotFound {
		t.Fatalf("%s: race answer: %s, want 200, or 404 once the organization is gone", cell, p)
	}
}

// C1: the billing event is held at its organizations write, the tier PATCH
// runs into its lock wait.
func TestBillingEventHeldAndTierPatchEndWithoutADeadlock(t *testing.T) {
	const cell = "billing-patch/billing-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	b := e.seedBillingOrg(t, ctx)

	reached := e.goBarrier.Arm(billingOrgWrite)
	w := goAPI("billing webhook", func() response { return e.subscriptionEvent(ctx, b, 1) })
	awaitReached(t, cell, "the billing event's organizations write", reached, w)
	p := goAPI("go-api tier PATCH", func() response { return e.patchTier(ctx, b.id) })
	e.awaitWaiters(t, ctx, cell, 1, w, p)
	e.goBarrier.Release()
	e.finish(t, cell, w, p)

	wantStatus(t, cell, w, http.StatusOK)
	wantStatus(t, cell, p, http.StatusOK)
	e.wantBillingCommitted(t, ctx, cell, b)
}

// C2: the tier PATCH is held at its license sync, the billing event runs into
// its lock wait.
func TestTierPatchHeldAndBillingEventEndWithoutADeadlock(t *testing.T) {
	const cell = "billing-patch/patch-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	b := e.seedBillingOrg(t, ctx)

	reached := e.goBarrier.Arm(patchLicenseSync)
	p := goAPI("go-api tier PATCH", func() response { return e.patchTier(ctx, b.id) })
	awaitReached(t, cell, "the PATCH's license sync", reached, p)
	w := goAPI("billing webhook", func() response { return e.subscriptionEvent(ctx, b, 1) })
	e.awaitWaiters(t, ctx, cell, 1, p, w)
	e.goBarrier.Release()
	e.finish(t, cell, p, w)

	wantStatus(t, cell, p, http.StatusOK)
	wantStatus(t, cell, w, http.StatusOK)
	e.wantBillingCommitted(t, ctx, cell, b)
}

// C4: the purge is held after it deleted the license row, before it deletes
// the organization; the tier PATCH runs into its lock wait.
func TestPurgeHeldAndTierPatchEndWithoutADeadlock(t *testing.T) {
	const cell = "purge-patch/purge-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	b := e.seedBillingOrg(t, ctx)
	e.add(t, ctx, b.id, e.seedUser(t, ctx), "owner", nil)

	reached := e.goBarrier.Arm(purgeMembers)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, b.id) })
	awaitReached(t, cell, "the purge's memberships delete", reached, d)
	p := goAPI("go-api tier PATCH", func() response { return e.patchTier(ctx, b.id) })
	e.awaitWaiters(t, ctx, cell, 1, d, p)
	e.goBarrier.Release()
	e.finish(t, cell, d, p)

	wantStatus(t, cell, d, http.StatusOK)
	wantAcceptOrGone(t, cell, p)
}

// C4b, the reverse: the tier PATCH holds the organization at its license sync,
// the purge runs into its lock wait.
func TestTierPatchHeldAndPurgeEndWithoutADeadlock(t *testing.T) {
	const cell = "purge-patch/patch-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	b := e.seedBillingOrg(t, ctx)
	e.add(t, ctx, b.id, e.seedUser(t, ctx), "owner", nil)

	reached := e.goBarrier.Arm(patchLicenseSync)
	p := goAPI("go-api tier PATCH", func() response { return e.patchTier(ctx, b.id) })
	awaitReached(t, cell, "the PATCH's license sync", reached, p)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, b.id) })
	e.awaitWaiters(t, ctx, cell, 1, p, d)
	e.goBarrier.Release()
	e.finish(t, cell, p, d)

	wantStatus(t, cell, p, http.StatusOK)
	wantStatus(t, cell, d, http.StatusOK)
}

// C5: a second event of a stored subscription is held after its subscriptions
// write, before the license sync; the purge runs into its lock wait. With the
// billing lock taken after that write the two would cycle (billing holds the
// subscription and wants the organization, the purge the reverse).
func TestBillingEventHeldAndPurgeEndWithoutADeadlock(t *testing.T) {
	const cell = "purge-billing/billing-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	b := e.seedBillingOrg(t, ctx)
	e.add(t, ctx, b.id, e.seedUser(t, ctx), "owner", nil)
	if got := e.subscriptionEvent(ctx, b, 1); got.status != http.StatusOK {
		t.Fatalf("%s: fixture: first event answered %d %s", cell, got.status, got.raw)
	}

	reached := e.goBarrier.Arm(billingPlanRead)
	w := goAPI("billing webhook", func() response { return e.subscriptionEvent(ctx, b, 2) })
	awaitReached(t, cell, "the billing event's plan read", reached, w)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, b.id) })
	e.awaitWaiters(t, ctx, cell, 1, w, d)
	e.goBarrier.Release()
	e.finish(t, cell, w, d)

	wantStatus(t, cell, w, http.StatusOK)
	wantStatus(t, cell, d, http.StatusOK)
}

// C6: the invite accept holds its invite and takes the foreign key's KEY SHARE
// on the organization after the purge took its head lock: that lock must let
// the KEY SHARE through, or the two cycle.
func TestAcceptHeldAndPurgeEndWithoutADeadlock(t *testing.T) {
	const cell = "purge-accept/accept-first"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.goBarrier.Arm("INSERT INTO memberships")
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	awaitReached(t, cell, "the accept's membership insert", reached, g)
	d := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
	e.awaitWaiters(t, ctx, cell, 1, g, d)
	e.goBarrier.Release()
	e.finish(t, cell, g, d)

	wantAcceptOrGone(t, cell, g)
	wantStatus(t, cell, d, http.StatusOK)
}
