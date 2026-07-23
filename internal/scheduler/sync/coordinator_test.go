package sync

import (
	"context"
	"errors"
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

func (transaction *coordinatorTransaction) QueryRow(
	_ context.Context,
	statement string,
	args ...any,
) pgx.Row {
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
	if err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	); err != nil {
		t.Fatal(err)
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
	if err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	); err != nil {
		t.Fatal(err)
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
	err := NewOccurrenceCoordinator().Handoff(
		context.Background(),
		transaction,
		occurrence,
	)
	if !errors.Is(err, ErrOccurrenceConflict) {
		t.Fatalf("Handoff() err = %v", err)
	}
}
