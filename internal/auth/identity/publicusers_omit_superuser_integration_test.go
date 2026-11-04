//go:build integration

package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// ProvisionLocal writes its public.users row (insertPublicUser) while the
// runtime role holds no INSERT of is_superuser (the grants pass gives none,
// CHAOS-10993), and the row reads false (the column default). The statement
// does not branch on a switch, so one run covers both states.
func TestProvisionLocalWritesPublicUserWithoutIsSuperuserPrivilege(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	var held bool
	if err := e.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', 'is_superuser', 'INSERT')`, role).Scan(&held); err != nil || held {
		t.Fatalf("the runtime role holds INSERT (is_superuser): %v %v", held, err)
	}
	hash := testBcrypt
	id, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "omit-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &hash, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	var superuser bool
	if err := e.admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, id).Scan(&superuser); err != nil || superuser {
		t.Errorf("is_superuser %v err %v, want false", superuser, err)
	}
}
