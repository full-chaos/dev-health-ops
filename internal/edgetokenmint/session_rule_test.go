package edgetokenmint

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
)

type recordingDB struct {
	sql *string
	row sessionRow
}

func (d recordingDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	*d.sql = sql
	return d.row
}

// The mint pre-check takes revoke and the absolute cap from the session rule, at the
// auth plane's one refresh lifetime (Cap reads it only through a web parent).
func TestCheckSessionReadsTheSharedRule(t *testing.T) {
	principal := testPrincipal()
	for schema, quoted := range map[string]string{"": `"auth"`, "tenant_auth": `"tenant_auth"`} {
		rule, err := sessionrule.New(quoted, session.DefaultRefreshTTL)
		if err != nil {
			t.Fatal(err)
		}
		want := `SELECT s.principal_id, ` + rule.Revoked("s") + ` IS NOT NULL, ` + rule.Cap("s") + ` <= now(), s.legacy_token_version FROM ` + quoted + `.sessions s WHERE s.id = $1`
		var got string
		_ = CheckSession(context.Background(), recordingDB{sql: &got, row: sessionRow{err: pgx.ErrNoRows}}, eddsaSession, principal, schema)
		if got != want {
			t.Errorf("schema %q:\n got %q\nwant %q", schema, got, want)
		}
	}
}
