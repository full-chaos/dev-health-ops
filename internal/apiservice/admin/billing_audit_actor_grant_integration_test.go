//go:build integration

package admin_test

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// A role granted exactly what the api posture declares for billing_audit_log can set actor_id to null (the permanent user
// delete of CHAOS-10019) and can update no other column of that table.
func TestAPIRoleCanUpdateOnlyTheBillingAuditActor(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	orgID, userID, rowID := uuid.New(), uuid.New(), uuid.New()
	for _, query := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'grant-org', 'grant-org', 'community', 'stripe', true, now(), now())`, []any{orgID}},
		{`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'grant-actor@example.com', 'x', true, true, false, 0, now(), now())`, []any{userID}},
		{`INSERT INTO billing_audit_log (id, org_id, actor_id, action, resource_type, resource_id, description, created_at)
VALUES ($1, $2, $3, 'checkout.created', 'checkout', $2, 'Checkout', now())`, []any{rowID, orgID, userID}},
	} {
		if _, err := pool.Exec(ctx, query.sql, query.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query.sql)
		}
	}
	const role = "zz_api_grant_probe"
	statements := []string{
		`CREATE ROLE ` + role + ` LOGIN PASSWORD 'probe'`,
		`GRANT USAGE ON SCHEMA public TO ` + role,
		`GRANT SELECT ON public.billing_audit_log TO ` + role,
	}
	for _, table := range postgres.APIPosture().RequiredTables {
		if table.TableName != "billing_audit_log" {
			continue
		}
		if table.AllowInsert {
			statements = append(statements, `GRANT INSERT ON public.billing_audit_log TO `+role)
		}
		if table.AllowUpdate {
			statements = append(statements, `GRANT UPDATE ON public.billing_audit_log TO `+role)
		}
		if table.AllowDelete {
			statements = append(statements, `GRANT DELETE ON public.billing_audit_log TO `+role)
		}
	}
	for _, column := range postgres.APIPosture().ColumnScoped {
		if column.TableName == "billing_audit_log" {
			statements = append(statements, `GRANT `+column.Privilege+` (`+column.ColumnName+`) ON public.billing_audit_log TO `+role)
		}
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, "probe")
	conn, err := pgx.Connect(ctx, parsed.String())
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	defer conn.Close(ctx)

	for _, column := range []string{"description", "action", "org_id", "resource_type", "local_state", "reconciliation_status"} {
		value := "'x'"
		if column == "org_id" {
			value = "'" + uuid.NewString() + "'::uuid"
		} else if column == "local_state" {
			value = "'{}'::json"
		}
		_, err := conn.Exec(ctx, `UPDATE billing_audit_log SET `+column+` = `+value+` WHERE id = $1`, rowID)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("UPDATE of %s as the api role: %v, want a 42501 permission denial", column, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE billing_audit_log SET description = 'x', actor_id = NULL WHERE id = $1`, rowID); err == nil {
		t.Error("an UPDATE that touches actor_id and description together was allowed")
	}
	tag, err := conn.Exec(ctx, `UPDATE billing_audit_log SET actor_id = NULL WHERE id = $1`, rowID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("UPDATE of actor_id as the api role: %v, rows %d; want it allowed", err, tag.RowsAffected())
	}
}
