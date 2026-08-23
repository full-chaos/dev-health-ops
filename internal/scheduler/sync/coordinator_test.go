package sync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type coordinatorRow struct {
	values []any
	err    error
}

func (row coordinatorRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index, value := range row.values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *time.Time:
			*target = value.(time.Time)
		default:
			return errors.New("unsupported coordinator scan destination")
		}
	}
	return nil
}

type coordinatorTransaction struct {
	rows       []pgx.Row
	statements []string
	args       [][]any
}

func (*coordinatorTransaction) Exec(
	context.Context,
	string,
	...any,
) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unexpected Exec")
}

// eligibilityRow answers the Coordinator's two pre-mint eligibility lookups
// (organization existence and the schedule's sync targets). It exists so the
// occurrence-handoff tests below can keep asserting on the OCCURRENCE
// statements positionally: the eligibility queries are answered here and are
// deliberately not recorded, because those tests are about the handoff, and the
// gates have their own coverage in eligibility_gate_integration_test.go.
type eligibilityRow struct{ targets []byte }

func (row eligibilityRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("unexpected eligibility scan arity")
	}
	switch target := dest[0].(type) {
	case *string:
		// organizations lookup: a row exists.
		*target = "00000000-0000-4000-8000-000000000001"
	case *[]byte:
		*target = row.targets
	default:
		return errors.New("unsupported eligibility scan destination")
	}
	return nil
}

func (transaction *coordinatorTransaction) QueryRow(
	_ context.Context,
	statement string,
	args ...any,
) pgx.Row {
	if strings.Contains(statement, "public.organizations") {
		return eligibilityRow{}
	}
	if strings.Contains(statement, "sync_targets") {
		// Ungated targets, so the canonical-incident gate does not apply and
		// the handoff proceeds to the statements these tests assert on.
		return eligibilityRow{targets: []byte(`["git"]`)}
	}
	transaction.statements = append(transaction.statements, statement)
	transaction.args = append(transaction.args, args)
	row := transaction.rows[0]
	transaction.rows = transaction.rows[1:]
	return row
}

func coordinatorOccurrence() Occurrence {
	return newOccurrence(
		"00000000-0000-4000-8000-000000003038",
		"org-a",
		"00000000-0000-4000-8000-000000003039",
		at("2026-01-01T11:00:00Z"),
		at("2026-01-01T12:00:00Z"),
		at("2026-01-01T13:00:00Z"),
	)
}

func TestOccurrenceCoordinatorInsertsStableHandoff(t *testing.T) {
	occurrence := coordinatorOccurrence()
	transaction := &coordinatorTransaction{
		rows: []pgx.Row{coordinatorRow{values: []any{occurrence.ID}}},
	}
	outcome, err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OccurrenceMinted {
		t.Fatalf("outcome = %q, want %q", outcome, OccurrenceMinted)
	}
	if len(transaction.statements) != 1 || len(transaction.args) != 1 {
		t.Fatalf("queries=%d args=%d", len(transaction.statements), len(transaction.args))
	}
	args := transaction.args[0]
	if args[0] != occurrence.ID || args[1] != occurrence.IdentityVersion ||
		args[2] != occurrence.OrgID || args[3] != occurrence.ConfigID ||
		args[4] != occurrence.JobID ||
		!args[5].(time.Time).Equal(occurrence.ScheduledFor) {
		t.Fatalf("insert args = %#v", args)
	}
}

func TestOccurrenceCoordinatorAcceptsMatchingExistingHandoff(t *testing.T) {
	occurrence := coordinatorOccurrence()
	transaction := &coordinatorTransaction{
		rows: []pgx.Row{
			coordinatorRow{err: pgx.ErrNoRows},
			coordinatorRow{values: []any{
				occurrence.IdentityVersion,
				occurrence.OrgID,
				occurrence.ConfigID,
				occurrence.JobID,
				occurrence.ScheduledFor,
			}},
		},
	}
	outcome, err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	)
	if err != nil {
		t.Fatal(err)
	}
	// An already-present row is still a success, but it must be reported as a
	// repeat: an idle scheduler that re-confirms one frozen instant every tick
	// is otherwise indistinguishable from a productive one (CHAOS-3936).
	if outcome != OccurrenceRepeated {
		t.Fatalf("outcome = %q, want %q", outcome, OccurrenceRepeated)
	}
	if len(transaction.statements) != 2 {
		t.Fatalf("queries = %d, want 2", len(transaction.statements))
	}
}

func TestOccurrenceCoordinatorRejectsConflictingExistingHandoff(t *testing.T) {
	occurrence := coordinatorOccurrence()
	transaction := &coordinatorTransaction{
		rows: []pgx.Row{
			coordinatorRow{err: pgx.ErrNoRows},
			coordinatorRow{values: []any{
				occurrence.IdentityVersion,
				occurrence.OrgID,
				occurrence.ConfigID,
				"00000000-0000-4000-8000-000000003040",
				occurrence.ScheduledFor,
			}},
		},
	}
	_, err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	)
	if !errors.Is(err, ErrOccurrenceConflict) {
		t.Fatalf("Handoff() err = %v", err)
	}
}
