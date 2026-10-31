//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// verbOut runs users-reconcile against the env's database with the given flags.
func (e *reconEnv) verbOut(ctx context.Context, args ...string) (int, string) {
	dsn := e.runtime.Config().ConnString()
	var stdout, stderr bytes.Buffer
	code := reconcileVerbForTest().Run(ctx, cli.Env{Args: args,
		Lookup: func(k string) (string, bool) {
			if k == dsnEnv {
				return dsn, true
			}
			return "", false
		}, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String() + stderr.String()
}

func (e *reconEnv) seedSuperuser(t *testing.T, ctx context.Context, prefix string, active bool) uuid.UUID {
	t.Helper()
	id := e.seed(t, ctx, uniq(prefix), active, true)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_superuser = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func wantPlatformAdmin(t *testing.T, step string, code int, out string, wantCode int, wantLine string) {
	t.Helper()
	if code != wantCode || !strings.Contains(out, wantLine) {
		t.Fatalf("%s: exit %d, want %d with %q\n%s", step, code, wantCode, wantLine, out)
	}
}

func privateEnv(t *testing.T) (*reconEnv, context.Context) {
	t.Helper()
	e, err := newReconEnv(t)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return e, ctx
}

// Each gate cell has a private database: no other test's superuser or assignment is in it.
func TestVerbNoActivePlatformAdminGate(t *testing.T) {
	const gated, info = "diff.no_active_platform_admin=", "info.no_active_platform_admin="
	t.Run("none held", func(t *testing.T) {
		e, ctx := privateEnv(t)
		e.backfill(t, ctx)
		code, out := e.verbOut(ctx, "-platform-roles-gate")
		wantPlatformAdmin(t, "no admin, flag", code, out, cli.ExitFailure, gated+"1")
		code, out = e.verbOut(ctx)
		wantPlatformAdmin(t, "no admin, no flag", code, out, cli.ExitOK, info+"1")
		if strings.Contains(out, gated) {
			t.Fatalf("no flag printed the gated line\n%s", out)
		}
	})
	t.Run("disabled only", func(t *testing.T) {
		e, ctx := privateEnv(t)
		e.seedSuperuser(t, ctx, "disabledonly", false)
		e.backfill(t, ctx)
		code, out := e.verbOut(ctx, "-platform-roles-gate")
		wantPlatformAdmin(t, "disabled only, flag", code, out, cli.ExitFailure, gated+"1")
	})
	t.Run("one held", func(t *testing.T) {
		e, ctx := privateEnv(t)
		e.seedSuperuser(t, ctx, "held", true)
		e.backfill(t, ctx)
		code, out := e.verbOut(ctx, "-platform-roles-gate")
		wantPlatformAdmin(t, "one held, flag", code, out, cli.ExitOK, gated+"0")
		code, out = e.verbOut(ctx)
		wantPlatformAdmin(t, "one held, no flag", code, out, cli.ExitOK, info+"0")
	})
}

func TestVerbNoActivePlatformAdminCountsOnlyExpiredAsNone(t *testing.T) {
	e, ctx := privateEnv(t)
	id := e.seedSuperuser(t, ctx, "expiredonly", true)
	e.backfill(t, ctx)
	code, out := e.verbOut(ctx, "-platform-roles-gate")
	wantPlatformAdmin(t, "held", code, out, cli.ExitOK, "diff.no_active_platform_admin=0")

	var assignment uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM auth.platform_role_assignments WHERE principal_id = $1`, id).Scan(&assignment); err != nil {
		t.Fatal(err)
	}
	expired := uuid.New()
	subject := id.String()
	err := audit.Commit(ctx, e.runtime, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `DELETE FROM auth.platform_role_assignments WHERE id = $1`, assignment); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, expires_at)
VALUES ($1, $2, 'platform_admin', statement_timestamp() - interval '1 minute')`, expired, id)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{
			{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", assignment.String()),
				EventType: "platform_role_assignment.revoked", Change: map[string]any{"status": "revoked"}},
			{AggregateType: "platform_role_assignment", AggregateID: provisioning.WireID("platform_role_assignment", expired.String()),
				EventType: "platform_role_assignment.granted", Change: map[string]any{"status": "active"}},
		},
	})
	if err != nil {
		t.Fatalf("plant an expired assignment: %v", err)
	}
	code, out = e.verbOut(ctx, "-platform-roles-gate")
	wantPlatformAdmin(t, "expired only, flag", code, out, cli.ExitFailure, "diff.no_active_platform_admin=1")
}
