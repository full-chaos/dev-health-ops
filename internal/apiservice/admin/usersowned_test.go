package admin

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// failingDB fails the test on any database call.
type failingDB struct{ t *testing.T }

func (d failingDB) Begin(context.Context) (pgx.Tx, error) {
	d.t.Error("a refused users write opened a transaction")
	return nil, errors.New("unexpected database call")
}

func (d failingDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	d.t.Error("a refused users write ran a statement")
	return pgconn.CommandTag{}, errors.New("unexpected database call")
}

func (d failingDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	d.t.Error("a refused users write ran a query")
	return nil, errors.New("unexpected database call")
}

func (d failingDB) QueryRow(context.Context, string, ...any) pgx.Row {
	d.t.Error("a refused users write ran a query")
	return failingRow{}
}

type failingRow struct{}

func (failingRow) Scan(...any) error { return errors.New("unexpected database call") }

// The operator verbs that change a user refuse before they open a
// transaction (the pool is nil: any database work panics).
func TestUsersOwnedOperatorVerbsRefuseBeforeAnyDatabaseWork(t *testing.T) {
	operator := Operator{UsersOwned: true}
	ctx := context.Background()
	email, password, newName := "a@example.test", "long-enough-password", "new name"
	for name, call := range map[string]func() error{
		"create": func() error {
			_, err := operator.CreateUser(ctx, CreateUserInput{Email: email, Password: password})
			return err
		},
		"delete": func() error {
			_, err := operator.DeleteUser(ctx, DeleteUserInput{Email: &email, Confirm: true})
			return err
		},
		"update profile": func() error {
			_, err := operator.UpdateUser(ctx, UpdateUserInput{Email: &email, FullName: &newName})
			return err
		},
		"update password": func() error {
			_, err := operator.UpdateUser(ctx, UpdateUserInput{Email: &email, Password: &password})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						err = fmt.Errorf("went past the switch to the database: %v", recovered)
					}
				}()
				err = call()
			}()
			if !errors.Is(err, ErrUsersOwned) {
				t.Fatalf("err = %v, want ErrUsersOwned", err)
			}
		})
	}
}

// While auth-service owns users, every public.users writer of the store
// refuses before it uses the pool (nil here: a write would panic), so the
// admin routes and the operator verbs share one guard.
func TestUsersOwnedRefusesEveryAdminUsersWriter(t *testing.T) {
	store := pgStore{UsersOwned: true}
	ctx := context.Background()
	id := uuid.New()
	email := "a@example.test"
	for name, call := range map[string]func() error{
		"insertUser":      func() error { _, err := store.insertUser(ctx, userCreateInput{Email: &email}); return err },
		"updateUser":      func() error { _, err := store.updateUser(ctx, id, userUpdate{}); return err },
		"setUserPassword": func() error { _, err := store.setUserPassword(ctx, nil, id, "hash"); return err },
		"softDeleteUser":  func() error { _, err := store.softDeleteUser(ctx, nil, id); return err },
		"forceDeleteUser": func() error { _, _, err := store.forceDeleteUser(ctx, nil, id); return err },
	} {
		if err := call(); !errors.Is(err, ErrUsersOwned) {
			t.Errorf("%s = %v, want ErrUsersOwned", name, err)
		}
	}
}

func TestOperatorHandsTheSwitchToEveryStoreItOpens(t *testing.T) {
	if !(Operator{UsersOwned: true}).store(nil).UsersOwned {
		t.Fatal("the operator's store does not carry the users-owned switch")
	}
	if (Operator{}).store(nil).UsersOwned {
		t.Fatal("the switch is on by default")
	}
}
