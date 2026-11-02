//go:build integration

package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/superuserwriter"
)

// A create with a false is_superuser runs as a role that cannot write the
// column (it omits it), with the switch on and off; a true value is still
// written when the roles are not owned, and refused when they are.
func TestAdminInsertUserOmitsIsSuperuserWhenFalse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	writer := superuserwriter.Pool(ctx, t, env.Admin, env.URI)
	read := func(id uuid.UUID) (superuser bool) {
		if err := env.Admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, id).Scan(&superuser); err != nil {
			t.Fatal(err)
		}
		return superuser
	}
	for _, owned := range []bool{true, false} {
		email := uuid.NewString() + "@example.test"
		u, err := pgStore{Pool: writer, PlatformRolesOwned: owned}.insertUser(ctx, userCreateInput{Email: &email})
		if err != nil {
			t.Fatalf("owned %v: false create as a role without the column: %v", owned, err)
		}
		if read(u.ID) {
			t.Errorf("owned %v: row is_superuser = true, want false", owned)
		}
	}
	email := uuid.NewString() + "@example.test"
	u, err := pgStore{Pool: env.Admin}.insertUser(ctx, userCreateInput{Email: &email, IsSuperuser: true})
	if err != nil {
		t.Fatalf("true create, not owned: %v", err)
	}
	if !read(u.ID) {
		t.Error("true create, not owned: row is_superuser = false, want true")
	}
	if _, err := (pgStore{Pool: writer}).insertUser(ctx, userCreateInput{Email: ptr(uuid.NewString() + "@example.test"), IsSuperuser: true}); err == nil {
		t.Error("true create as a role without the column succeeded")
	}
}

func ptr(s string) *string { return &s }
