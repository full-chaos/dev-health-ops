//go:build integration

package operational

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-5353: the completion fence is a claim/release protocol whose whole
// correctness argument rests on PostgreSQL's row-level serialization of
// concurrent UPDATEs. A mock cannot exhibit that, so these run against a real
// database -- an in-memory double asserting "the code called Exec" would prove
// nothing about the property being relied on.
//
// The schema below creates only what the fence touches, matching
// src/dev_health_ops/models/operational_deliveries.py (BillingNotification)
// and users.py (User, Membership, Organization) -- the same "create just what
// this test touches" pattern the neighbouring integration tests use, rather
// than running the full alembic chain.
func applyBillingFenceSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
		`CREATE TABLE public.billing_notifications (
			id UUID PRIMARY KEY,
			org_id UUID NOT NULL,
			notification_type TEXT NOT NULL,
			idempotency_key TEXT NOT NULL UNIQUE,
			attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			claimed_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ
		)`,
		`CREATE TABLE public.organizations (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL
		)`,
		`CREATE TABLE public.users (
			id UUID PRIMARY KEY,
			email TEXT NOT NULL,
			full_name TEXT
		)`,
		`CREATE TABLE public.memberships (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id UUID NOT NULL,
			org_id UUID NOT NULL,
			role TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("schema statement failed: %v\n%s", err, statement)
		}
	}
}

func startFencePostgres(t *testing.T) (context.Context, *pgxpool.Pool, *PostgresStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyBillingFenceSchema(ctx, t, pool)
	return ctx, pool, &PostgresStore{pool: pool}
}

func seedNotification(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool,
	id string, orgID string, emailType string, attributes string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.billing_notifications
			(id, org_id, notification_type, idempotency_key, attributes)
		VALUES ($1, $2, $3, $4, $5::jsonb)`,
		id, orgID, emailType, "billing:"+id, attributes); err != nil {
		t.Fatal(err)
	}
}

func fenceColumns(
	ctx context.Context, t *testing.T, pool *pgxpool.Pool, id string,
) (*time.Time, *time.Time) {
	t.Helper()
	var claimedAt, completedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT claimed_at, completed_at FROM public.billing_notifications WHERE id = $1`,
		id).Scan(&claimedAt, &completedAt); err != nil {
		t.Fatal(err)
	}
	return claimedAt, completedAt
}

const (
	fenceNotificationID = "00000000-0000-4000-8000-0000000000f1"
	fenceOrgID          = "00000000-0000-4000-8000-0000000000a1"
	fenceUserID         = "00000000-0000-4000-8000-0000000000b1"
)

// TestBillingClaimIsExclusiveUnderConcurrency is the core property: with N
// callers racing on one row, EXACTLY ONE may win. A fence that let two callers
// through would send the customer two copies of the same billing email.
func TestBillingClaimIsExclusiveUnderConcurrency(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	seedNotification(ctx, t, pool, fenceNotificationID, fenceOrgID, EmailTypeInvoiceReceipt, `{}`)

	const racers = 16
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	var group sync.WaitGroup
	results := make([]ClaimResult, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for index := 0; index < racers; index++ {
		group.Add(1)
		go func(slot int) {
			defer group.Done()
			<-start
			results[slot], errs[slot] = store.Claim(ctx, fenceNotificationID, now)
		}(index)
	}
	close(start)
	group.Wait()

	winners := 0
	for index := 0; index < racers; index++ {
		if errs[index] != nil {
			t.Fatalf("racer %d failed: %v", index, errs[index])
		}
		if results[index].Claimed {
			winners++
			continue
		}
		// Every loser must be able to explain itself: it saw the winner's
		// claim. A loser reporting neither claimed_at nor completed_at would
		// leave the caller unable to distinguish in-flight from completed.
		if results[index].ClaimedAt == nil {
			t.Errorf("racer %d lost the claim but reported no claimed_at", index)
		}
	}
	if winners != 1 {
		t.Fatalf("%d racers won the claim; exactly 1 may win", winners)
	}
	claimedAt, completedAt := fenceColumns(ctx, t, pool, fenceNotificationID)
	if claimedAt == nil || completedAt != nil {
		t.Fatalf("after the race claimed_at=%v completed_at=%v; want claimed, uncompleted",
			claimedAt, completedAt)
	}
}

// TestBillingClaimReportsCompletionSoDuplicatesAreSuppressed proves the
// already-sent path: once completed, a later claim loses AND reports why.
func TestBillingClaimReportsCompletionSoDuplicatesAreSuppressed(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	seedNotification(ctx, t, pool, fenceNotificationID, fenceOrgID, EmailTypeInvoiceReceipt, `{}`)
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	first, err := store.Claim(ctx, fenceNotificationID, now)
	if err != nil || !first.Claimed {
		t.Fatalf("first claim: result=%+v err=%v", first, err)
	}
	if err := store.MarkCompleted(ctx, fenceNotificationID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	second, err := store.Claim(ctx, fenceNotificationID, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if second.Claimed {
		t.Fatal("a completed notification was claimed again; the email would be duplicated")
	}
	if second.CompletedAt == nil {
		t.Fatal("the lost claim did not report completed_at, so the caller cannot tell " +
			"a real prior send from an in-flight attempt")
	}
	if second.Stale(now.Add(2 * time.Second)) {
		t.Fatal("a COMPLETED claim must never be classified as stale")
	}
}

// TestBillingClaimAgesIntoStaleOnlyPastTheThreshold pins the 900s boundary in
// both directions: just inside is an ordinary in-flight duplicate, just
// outside is the operator-visible stale state.
func TestBillingClaimAgesIntoStaleOnlyPastTheThreshold(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	seedNotification(ctx, t, pool, fenceNotificationID, fenceOrgID, EmailTypeInvoiceReceipt, `{}`)
	claimedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	if first, err := store.Claim(ctx, fenceNotificationID, claimedAt); err != nil || !first.Claimed {
		t.Fatalf("first claim: result=%+v err=%v", first, err)
	}
	for _, test := range []struct {
		name      string
		at        time.Time
		wantStale bool
	}{
		{"inside the window", claimedAt.Add(StaleClaimThreshold - time.Second), false},
		{"exactly at the threshold", claimedAt.Add(StaleClaimThreshold), false},
		{"past the threshold", claimedAt.Add(StaleClaimThreshold + time.Second), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lost, err := store.Claim(ctx, fenceNotificationID, test.at)
			if err != nil {
				t.Fatal(err)
			}
			if lost.Claimed {
				t.Fatal("a held claim was re-claimed")
			}
			if got := lost.Stale(test.at); got != test.wantStale {
				t.Fatalf("Stale=%v, want %v (claimed_at=%v, now=%v)",
					got, test.wantStale, lost.ClaimedAt, test.at)
			}
		})
	}
}

// TestBillingClaimReleaseAllowsExactlyOneRetryToClaimAgain proves release is
// real: after a failed attempt releases, a retry can claim and send, and the
// row is back to a claimable state rather than merely "not completed".
func TestBillingClaimReleaseAllowsExactlyOneRetryToClaimAgain(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	seedNotification(ctx, t, pool, fenceNotificationID, fenceOrgID, EmailTypeInvoiceReceipt, `{}`)
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

	if first, err := store.Claim(ctx, fenceNotificationID, now); err != nil || !first.Claimed {
		t.Fatalf("first claim: result=%+v err=%v", first, err)
	}
	if err := store.ReleaseClaim(ctx, fenceNotificationID); err != nil {
		t.Fatal(err)
	}
	claimedAt, completedAt := fenceColumns(ctx, t, pool, fenceNotificationID)
	if claimedAt != nil || completedAt != nil {
		t.Fatalf("after release claimed_at=%v completed_at=%v; want both NULL",
			claimedAt, completedAt)
	}
	retry, err := store.Claim(ctx, fenceNotificationID, now.Add(time.Minute))
	if err != nil || !retry.Claimed {
		t.Fatalf("the retry could not claim a released row: result=%+v err=%v", retry, err)
	}
	// And a SECOND racer against that retry still loses -- releasing did not
	// weaken exclusivity, it only reset it.
	loser, err := store.Claim(ctx, fenceNotificationID, now.Add(time.Minute))
	if err != nil || loser.Claimed {
		t.Fatalf("exclusivity was lost after a release: result=%+v err=%v", loser, err)
	}
}

// TestBillingClaimOnAMissingRowIsPermanent: a row deleted between load and
// claim must not be retried forever.
func TestBillingClaimOnAMissingRowIsPermanent(t *testing.T) {
	ctx, _, store := startFencePostgres(t)
	if _, err := store.Claim(ctx, fenceNotificationID, time.Now().UTC()); !errors.Is(err, ErrDeliveryNotFound) {
		t.Fatalf("claim on a missing row returned %v, want ErrDeliveryNotFound", err)
	}
}

// TestLoadOrgOwnerPicksTheEarliestOwnerAndOrgName ports the assertion that
// `get_org_owner_email`'s ORDER BY created_at LIMIT 1 actually selects the
// FIRST owner, and that non-owner memberships are never addressed.
func TestLoadOrgOwnerPicksTheEarliestOwnerAndOrgName(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.organizations (id, name) VALUES ($1, $2)`,
		fenceOrgID, "Élan Systèmes"); err != nil {
		t.Fatal(err)
	}
	members := []struct {
		id       string
		email    string
		fullName any
		role     string
		created  time.Time
	}{
		{"00000000-0000-4000-8000-0000000000b2", "admin@example.test", "Admin Person", "admin", base},
		{fenceUserID, "owner@example.test", "Björn Åberg", "owner", base.Add(time.Hour)},
		{"00000000-0000-4000-8000-0000000000b3", "later@example.test", "Later Owner", "owner", base.Add(2 * time.Hour)},
	}
	for _, member := range members {
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.users (id, email, full_name) VALUES ($1, $2, $3)`,
			member.id, member.email, member.fullName); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO public.memberships (user_id, org_id, role, created_at)
			 VALUES ($1, $2, $3, $4)`,
			member.id, fenceOrgID, member.role, member.created); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := store.LoadOrgOwner(ctx, fenceOrgID)
	if err != nil {
		t.Fatal(err)
	}
	if owner.Email != "owner@example.test" {
		t.Errorf("addressed %q; want the EARLIEST owner, not an admin or a later owner", owner.Email)
	}
	if owner.FullName != "Björn Åberg" || owner.OrgName != "Élan Systèmes" {
		t.Errorf("owner = %+v", owner)
	}
}

// TestLoadOrgOwnerFallsBackToThereForABlankName pins Python's greeting
// fallback for a NULL full_name.
func TestLoadOrgOwnerFallsBackToThereForABlankName(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.users (id, email, full_name) VALUES ($1, $2, NULL)`,
		fenceUserID, "owner@example.test"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.memberships (user_id, org_id, role) VALUES ($1, $2, 'owner')`,
		fenceUserID, fenceOrgID); err != nil {
		t.Fatal(err)
	}
	owner, err := store.LoadOrgOwner(ctx, fenceOrgID)
	if err != nil {
		t.Fatal(err)
	}
	if owner.FullName != "there" {
		t.Errorf("FullName = %q, want the Python \"there\" fallback", owner.FullName)
	}
	// An organization row that does not exist renders an empty name rather
	// than failing the send, exactly as Python did.
	if owner.OrgName != "" {
		t.Errorf("OrgName = %q, want empty for a missing organization row", owner.OrgName)
	}
}

// TestLoadOrgOwnerReportsAnOrgWithNoOwner: the condition Python logged and
// returned silently on. It must be its own error, not an empty address.
func TestLoadOrgOwnerReportsAnOrgWithNoOwner(t *testing.T) {
	ctx, _, store := startFencePostgres(t)
	_, err := store.LoadOrgOwner(ctx, fenceOrgID)
	if !errors.Is(err, ErrOrgOwnerNotFound) {
		t.Fatalf("LoadOrgOwner on an ownerless org returned %v, want ErrOrgOwnerNotFound", err)
	}
}

// TestLoadBillingReturnsStoredAttributesForRendering closes the loop from the
// durable row to a rendered email, on a real row, with a real JSONB value.
func TestLoadBillingReturnsStoredAttributesForRendering(t *testing.T) {
	ctx, pool, store := startFencePostgres(t)
	seedNotification(ctx, t, pool, fenceNotificationID, fenceOrgID, EmailTypeInvoiceReceipt,
		`{"amount_cents": 4999, "currency": "usd", "invoice_url": "https://invoice.example.test/in_1"}`)

	notification, err := store.LoadBilling(ctx, fenceNotificationID)
	if err != nil {
		t.Fatal(err)
	}
	attributes, err := DecodeBillingAttributes(notification.Attributes)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderBillingEmail(notification.NotificationType, attributes,
		OwnerContact{Email: "o@example.test", FullName: "Dana Reed", OrgName: "Northwind Analytics"},
		"https://app.example.test")
	if err != nil {
		t.Fatal(err)
	}
	if rendered.Subject != "Invoice receipt" {
		t.Errorf("subject = %q", rendered.Subject)
	}
	for _, want := range []string{"49.99 USD", "https://invoice.example.test/in_1", "Dana Reed"} {
		if !strings.Contains(rendered.HTML, want) {
			t.Errorf("rendered body is missing %q:\n%s", want, rendered.HTML)
		}
	}
}
