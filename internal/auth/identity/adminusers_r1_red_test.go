package identity

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Every audit row an admin update writes passes the audit attribute vocabulary: the commit refuses a row outside it
// (round r1 P1-1: username, full_name and avatar_url were refused, so every profile update failed).
func TestAdminUpdateAuditRowsPassTheVocabulary(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	name, full, avatar, email, yes := "Neo", "Thomas A.", "https://example.test/a.png", "new@example.test", true
	cur := AdminUpdateCurrent{Email: "old@example.test"}
	for label, req := range map[string]AdminUpdateRequest{
		"email":                {Email: &email},
		"verified":             {Verified: &yes},
		"profile only":         {Username: &name, FullName: &full, AvatarURL: &avatar},
		"email and profile":    {Email: &email, Username: &name, FullName: &full, AvatarURL: &avatar},
		"verified and profile": {Verified: &yes, AvatarURL: &avatar},
		"profile and role":     {Username: &name, Superuser: none},
	} {
		var rows []audit.AuditEvent
		oldMutation, oldProjection := commitMutation, commitPublicProjection
		commitMutation = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, m audit.Mutation) error {
			rows = append(rows, m.Audit)
			return nil
		}
		commitPublicProjection = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, event audit.AuditEvent,
			_ func(context.Context, audit.TxOps) (int64, error)) error {
			rows = append(rows, event)
			return nil
		}
		req.Principal, req.Actor, req.Authorize = uuid.New(), uuid.New(), none
		err := store.AdminUpdateUser(context.Background(), req, cur)
		commitMutation, commitPublicProjection = oldMutation, oldProjection
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s: err %v, %d audit rows", label, err, len(rows))
		}
		if err := audit.ValidateAttributes(rows[0].Attributes); err != nil {
			t.Errorf("%s: the audit row is refused by the vocabulary: %v", label, err)
		}
	}
}

// failingExecTx answers every Exec with err.
type failingExecTx struct{ err error }

func (f failingExecTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.err
}
func (f failingExecTx) QueryRow(context.Context, string, ...any) pgx.Row { return falseRow{} }

// A username race (the unique ix_users_username on public.users) is a username refusal, not an email one (round r1
// P2).
func TestAdminPublicUsernameRaceIsUsernameTaken(t *testing.T) {
	race := &pgconn.PgError{Code: "23505", SchemaName: "public", TableName: "users", ConstraintName: "ix_users_username"}
	name := "neo"
	err := updatePublicUserRow(context.Background(), failingExecTx{race}, `UPDATE public.users SET username = $2, updated_at = now() WHERE id = $1`, uuid.New(), &name)
	if !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("a username race on update = %v, want ErrUsernameTaken", err)
	}
	email := &pgconn.PgError{Code: "23505", SchemaName: "public", TableName: "users", ConstraintName: "ix_users_email"}
	if err := updatePublicUserRow(context.Background(), failingExecTx{email}, `UPDATE public.users SET email = $2, updated_at = now() WHERE id = $1`, uuid.New(), &name); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("an email race on update = %v, want ErrEmailTaken (control)", err)
	}
}

// The create path maps a username race the same way (round r1 P2): the commit's unique violation on
// ix_users_username is ErrUsernameTaken, one on ix_users_email stays ErrEmailTaken.
func TestAdminCreateUsernameRaceIsUsernameTaken(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	for index, want := range map[string]error{"ix_users_username": ErrUsernameTaken, "ix_users_email": ErrEmailTaken} {
		race := &pgconn.PgError{Code: "23505", SchemaName: "public", TableName: "users", ConstraintName: index}
		old := commitMutation
		commitMutation = func(context.Context, *pgxpool.Pool, authschema.ValidatedIdentifier, audit.Mutation) error {
			return fmt.Errorf("%w: applying the state mutation: %w", audit.ErrMutationFailed, race)
		}
		_, err := store.AdminCreateUser(context.Background(), AdminCreateRequest{Email: "a@example.test", Actor: uuid.New(), Authorize: none})
		commitMutation = old
		if !errors.Is(err, want) {
			t.Errorf("a create race on %s = %v, want %v", index, err, want)
		}
	}
}
