package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

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
