package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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

// Every admin users route refuses with the moved-operation answer before it
// does any database work, reads included: a database failure cannot replace
// the refusal. A panic means the handler went on to the request's own state.
func TestUsersOwnedAdminRoutesRefuseBeforeAnyDatabaseWork(t *testing.T) {
	for name, route := range map[string]func(*handlers) http.HandlerFunc{
		"create":       func(h *handlers) http.HandlerFunc { return h.createUser },
		"update":       func(h *handlers) http.HandlerFunc { return h.updateUser },
		"set-password": func(h *handlers) http.HandlerFunc { return h.setUserPassword },
		"delete":       func(h *handlers) http.HandlerFunc { return h.deleteUser },
	} {
		t.Run(name, func(t *testing.T) {
			h := &handlers{store: pgStore{Pool: failingDB{t}, UsersOwned: true}, logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))}
			recorder := httptest.NewRecorder()
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Errorf("the handler went past the switch: %v", recovered)
					}
				}()
				request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users/"+uuid.NewString(), nil)
				request.SetPathValue("user_id", uuid.NewString())
				route(h)(recorder, request)
			}()
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"operation_moved"`) {
				t.Fatalf("status %d body %q, want 409 operation_moved", recorder.Code, recorder.Body.String())
			}
		})
	}
}

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
