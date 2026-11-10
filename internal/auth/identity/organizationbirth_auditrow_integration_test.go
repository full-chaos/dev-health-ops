//go:build integration

package identity

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// execModeTx sends every statement in pgx's exec mode, as audit.TxOps does.
type execModeTx struct{ tx pgx.Tx }

func (e execModeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return e.tx.Exec(ctx, sql, append([]any{pgx.QueryExecModeExec}, args...)...)
}

func (e execModeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return e.tx.QueryRow(ctx, sql, append([]any{pgx.QueryExecModeExec}, args...)...)
}

// The routed row and the direct INSERT's row, written in one exec-mode transaction on the same input (read back
// before a rollback, so as the admin role: the runtime role reads no audit_logs), store byte-equal text in every
// column but id, and both take the transaction's now(). The runtime role's grant is TestOnboardOrgOneCommit's.
func TestOrganizationCreatedLogStoredRowIsTheDirectInsertsRow(t *testing.T) {
	e, ctx := getEnv(t), birthContext(t)
	user := e.orgless(t, ctx)
	born, err := e.owned.OnboardOrganization(ctx, OnboardOrganizationRequest{Principal: user, Name: "Olive's Ünicode Org!!"})
	if err != nil {
		t.Fatal(err)
	}
	var onboardRow string
	if err := e.admin.QueryRow(ctx, `SELECT id::text FROM public.audit_logs WHERE org_id = $1`, born.ID).Scan(&onboardRow); err != nil {
		t.Fatal(err)
	}
	description := "Organization created during onboarding (parity)"
	for name, req := range map[string]OnboardOrganizationRequest{
		"defaults":    {Principal: user, Name: "Olive's Ünicode Org!!"},
		"request set": {Principal: user, Name: "Olive's Ünicode Org!!", Description: &description, RequestMetadata: []byte(`{"ip_address":"192.0.2.1", "user_agent": "ua"}`)},
	} {
		b := organizationBirth{ID: born.ID, Name: req.Name}
		tx, err := e.admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ops := execModeTx{tx}
		if err := legacyOrganizationCreatedLog(ctx, ops, b, req); err != nil {
			t.Fatalf("%s: direct INSERT: %v", name, err)
		}
		if err := insertOrganizationCreatedLog(ctx, ops, b, req); err != nil {
			t.Fatalf("%s: routed: %v", name, err)
		}
		rows, err := tx.Query(ctx, `SELECT org_id::text, COALESCE(user_id::text, '<null>'), action, resource_type, resource_id,
  COALESCE(description, '<null>'), COALESCE(changes::text, '<null>'), COALESCE(request_metadata::text, '<null>'), status,
  COALESCE(error_message, '<null>'), (created_at = now())::text, id::text
FROM public.audit_logs WHERE org_id = $1 AND id <> $2 ORDER BY id`, born.ID, onboardRow)
		if err != nil {
			t.Fatal(err)
		}
		var written [][]string
		for rows.Next() {
			cols := make([]string, 12)
			ptrs := make([]any, len(cols))
			for i := range cols {
				ptrs[i] = &cols[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			written = append(written, cols)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = tx.Rollback(ctx)
		if len(written) != 2 {
			t.Fatalf("%s: %d rows in the transaction, want 2", name, len(written))
		}
		if written[0][11] == written[1][11] {
			t.Errorf("%s: both rows have id %s", name, written[0][11])
		}
		for i, column := range []string{"org_id", "user_id", "action", "resource_type", "resource_id", "description",
			"changes", "request_metadata", "status", "error_message", "created_at = now()"} {
			if written[0][i] != written[1][i] {
				t.Errorf("%s: %s %q vs %q", name, column, written[0][i], written[1][i])
			}
		}
		if written[0][10] != "true" {
			t.Errorf("%s: created_at is not the transaction's now()", name)
		}
	}
}
