package platformrole

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

type sqlCapture struct{ sql string }

func (c *sqlCapture) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	c.sql = sql
	return heldRow{}
}

type heldRow struct{}

func (heldRow) Scan(dest ...any) error {
	*(dest[0].(*bool)) = true
	return nil
}

// HeldOn (the auth-service update's compare) and rule.StoredHoldsSQL (go-api's admin users view) are one condition:
// the same table, the principal and role clauses and the expiry clause, with no principal state.
func TestHeldOnIsTheStoredHoldsCondition(t *testing.T) {
	capture := &sqlCapture{}
	s := &Store{q: `"auth"`}
	if held, err := s.HeldOn(context.Background(), capture, uuid.New(), PlatformAdmin); err != nil || !held {
		t.Fatalf("HeldOn = %v, %v", held, err)
	}
	norm := func(sql string) string {
		return strings.Join(strings.Fields(strings.ReplaceAll(strings.ReplaceAll(sql, "pra_stored.", ""), " pra_stored", "")), " ")
	}
	if got, want := norm(capture.sql), "SELECT "+norm(rule.StoredHoldsSQL(`"auth"`, "$1", "$2")); got != want {
		t.Errorf("HeldOn reads %q, want rule.StoredHoldsSQL's condition %q", got, want)
	}
}
