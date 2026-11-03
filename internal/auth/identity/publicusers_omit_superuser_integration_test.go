//go:build integration

package identity

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// ProvisionLocal writes its public.users row (insertPublicUser) while the
// runtime role holds no INSERT of is_superuser, and the row reads false (the
// column default). The statement does not branch on a switch, so one run
// covers both states.
func TestProvisionLocalWritesPublicUserWithoutIsSuperuserPrivilege(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`REVOKE INSERT (is_superuser) ON public.users FROM %q`, role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := e.admin.Exec(context.Background(), fmt.Sprintf(`GRANT INSERT (is_superuser) ON public.users TO %q`, role)); err != nil {
			t.Errorf("restore the grant: %v", err)
		}
	})
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
