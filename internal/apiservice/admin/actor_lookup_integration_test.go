//go:build integration

package admin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// The actor-only lookups read the acting user's id and password_hash on a real database, in both platform role
// modes; the target read of the same disabled stored holder still shows is_superuser true.
func TestActorLookupsReadIDAndPasswordHashOnRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)

	holder := env.User(ctx, t, false, true)
	pgseed.SetUserState(ctx, t, env.Admin, holder.String(), false, false, 0)
	env.Disable(ctx, t, holder)
	const hash = "actor-lookup-cell-password-hash"
	if _, err := env.Admin.Exec(ctx, `UPDATE users SET password_hash = $2 WHERE id = $1`, holder, hash); err != nil {
		t.Fatal(err)
	}
	var email string
	if err := env.Admin.QueryRow(ctx, `SELECT email FROM users WHERE id = $1`, holder).Scan(&email); err != nil {
		t.Fatal(err)
	}

	for _, owned := range []bool{true, false} {
		store := pgStore{Pool: env.Admin, PlatformRolesOwned: owned}
		if owned {
			target, err := store.fullUserByID(ctx, holder)
			if err != nil || target == nil {
				t.Fatalf("owned: target read %v, %v", target, err)
			}
			if !target.IsSuperuser {
				t.Error("owned: the target read of a disabled stored holder lost is_superuser true")
			}
		}
		byID, err := store.actorByID(ctx, holder)
		if err != nil || byID == nil {
			t.Fatalf("owned %v: actorByID %v, %v", owned, byID, err)
		}
		if byID.ID != holder || byID.PasswordHash == nil || *byID.PasswordHash != hash {
			t.Errorf("owned %v: actorByID read %v, want the holder's id and password_hash", owned, byID.ID)
		}
		byEmail, err := store.actorByEmail(ctx, strings.ToUpper(email))
		if err != nil || byEmail == nil {
			t.Fatalf("owned %v: actorByEmail %v, %v", owned, byEmail, err)
		}
		if byEmail.ID != holder {
			t.Errorf("owned %v: actorByEmail id %v, want %v", owned, byEmail.ID, holder)
		}
		if missing, err := store.actorByID(ctx, uuid.New()); err != nil || missing != nil {
			t.Errorf("owned %v: actorByID of no user = %v, %v; want nil, nil", owned, missing, err)
		}
		if missing, err := store.actorByEmail(ctx, "no-such-actor@example.test"); err != nil || missing != nil {
			t.Errorf("owned %v: actorByEmail of no user = %v, %v; want nil, nil", owned, missing, err)
		}
	}
}
