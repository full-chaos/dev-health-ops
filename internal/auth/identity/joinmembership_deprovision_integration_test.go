//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// A join into a suspended organization writes nothing.
func TestJoinMembershipRefusedInASuspendedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	user, orgID := e.joinFixture(t, ctx)
	if err := e.orgStore(t).Suspend(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	err := joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: orgID, Role: "member"}, nil)
	if !errors.Is(err, ErrOrganizationSuspended) {
		t.Fatalf("join into a suspended organization = %v, want ErrOrganizationSuspended", err)
	}
	e.requireNoMemberRows(t, ctx, user, orgID)
}

// A join reaches the organization row before any membership row: while an
// organization deprovision holds the row FOR UPDATE the join waits there, and
// once the deprovision commits the suspension, the join is refused and writes
// nothing.
func TestJoinMembershipWaitsForAnOrganizationDeprovision(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	user, orgID := e.joinFixture(t, ctx)
	orgs := e.orgStore(t)
	locked, release, deprovisioned := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	id := orgID.String()
	go func() {
		deprovisioned <- audit.Commit(ctx, e.runtime, e.store.schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				if _, err := orgs.LockForUpdate(ctx, tx, orgID); err != nil {
					return err
				}
				close(locked)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				return orgs.ApplySuspend(ctx, tx, orgID)
			},
			Audit: audit.AuditEvent{EventType: "auth.organization.deprovisioned", Outcome: audit.OutcomeAllowed, OrganizationID: &id,
				Attributes: map[string]any{"reason_code": "organization_updated", "target_kind": "organization", "target_id": id}},
			Events: []audit.OutboxEvent{org.SuspendedEvent(orgID)},
		})
	}()
	select {
	case <-locked:
	case err := <-deprovisioned:
		t.Fatalf("the deprovision ended (%v) before it held the organization row", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: orgID, Role: "member"}, nil)
	}()
	role := e.runtime.Config().ConnConfig.User
	for waiting := 0; waiting == 0; {
		select {
		case err := <-done:
			t.Fatalf("the join returned (%v) while the deprovision held the organization row", err)
		case <-ctx.Done():
			t.Fatal("the join never waited on the organization row")
		default:
		}
		waiting = e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity
WHERE usename = $1 AND wait_event_type = 'Lock' AND query LIKE '%.organizations WHERE id = $1 FOR SHARE%'`, role)
		if waiting == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	close(release)
	if err := <-deprovisioned; err != nil {
		t.Fatalf("deprovision: %v", err)
	}
	if err := <-done; !errors.Is(err, ErrOrganizationSuspended) {
		t.Fatalf("join after the deprovision = %v, want ErrOrganizationSuspended", err)
	}
	e.requireNoMemberRows(t, ctx, user, orgID)
}
