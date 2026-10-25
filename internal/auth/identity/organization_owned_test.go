package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/metric/noop"
)

type recordingOrgDB struct {
	mu       sync.Mutex
	statemts []string
	args     [][]any
	rowErr   error
}

func (r *recordingOrgDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statemts = append(r.statemts, sql)
	r.args = append(r.args, args)
	return recordedRow{err: r.rowErr}
}

func (r *recordingOrgDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statemts = append(r.statemts, sql)
	r.args = append(r.args, args)
	return pgconn.CommandTag{}, nil
}

type recordedRow struct{ err error }

func (r recordedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*bool)) = true
	return nil
}

func ownedTestStore(db *recordingOrgDB) *Store {
	counter, _ := noop.NewMeterProvider().Meter("test").Int64Counter("x")
	return &Store{ownsOrgs: true, orgDB: db, q: `"auth"`, logger: slog.Default(), projections: counter}
}

func assertExactOwnedRead(t *testing.T, db *recordingOrgDB, id uuid.UUID) {
	t.Helper()
	if len(db.statemts) != 1 {
		t.Fatalf("the owned branch ran %d statements, want exactly 1: %q", len(db.statemts), db.statemts)
	}
	const want = `SELECT true FROM "auth".organizations WHERE id = $1`
	if got := fmt.Sprintf(ownedOrgExistsSQL, `"auth"`); db.statemts[0] != got || got != want {
		t.Fatalf("the owned branch ran %q (constant renders %q), want exactly %q", db.statemts[0], fmt.Sprintf(ownedOrgExistsSQL, `"auth"`), want)
	}
	if len(db.args[0]) != 1 || db.args[0][0] != any(id) {
		t.Fatalf("the owned branch args = %v, want exactly [%v]", db.args[0], id)
	}
}

func TestProjectOrganizationOwnedTouchesNoPublicTableAndWritesNothing(t *testing.T) {
	db := &recordingOrgDB{}
	id := uuid.New()
	if err := ownedTestStore(db).ProjectOrganization(context.Background(), id); err != nil {
		t.Fatalf("ProjectOrganization = %v", err)
	}
	assertExactOwnedRead(t, db, id)
	for _, sql := range db.statemts {
		low := strings.ToLower(sql)
		if strings.Contains(low, "public.") {
			t.Errorf("the owned branch touched a public table: %s", sql)
		}
		for _, verb := range []string{"insert", "update", "delete", "merge", "truncate", "copy"} {
			if strings.Contains(low, verb) {
				t.Errorf("the owned branch wrote (%s): %s", verb, sql)
			}
		}
		if !strings.HasPrefix(low, "select ") || !strings.Contains(low, `"auth".organizations`) {
			t.Errorf("the owned branch ran something other than a read of auth.organizations: %s", sql)
		}
	}
}

func TestProjectOrganizationOwnedAnswersNotFoundForAMissingAuthRowAndWritesNothing(t *testing.T) {
	db := &recordingOrgDB{rowErr: pgx.ErrNoRows}
	id := uuid.New()
	err := ownedTestStore(db).ProjectOrganization(context.Background(), id)
	if !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("missing auth row = %v, want ErrOrganizationNotFound", err)
	}
	assertExactOwnedRead(t, db, id)
}

func TestOwnedProjectionOutcomeLabelIsInTheClosedSet(t *testing.T) {
	if ownedOutcome != "owned" {
		t.Fatalf("ownedOutcome = %q, want owned", ownedOutcome)
	}
}
