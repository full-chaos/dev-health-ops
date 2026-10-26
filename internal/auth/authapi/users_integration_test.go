//go:build integration

package authapi

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

func TestPGUsersStateCarriesTheAuthProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	seed := func(email, provider string) uuid.UUID {
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, false, 0, now(), now())`, id, email, provider); err != nil {
			t.Fatal(err)
		}
		return id
	}
	local := seed("local-"+uuid.NewString()+"@example.test", "local")
	service := seed("service-"+uuid.NewString()+"@example.test", edgetokenmint.ServiceAuthProvider)

	users := PGUsers{Pool: pool}
	state, found, err := users.State(ctx, local)
	if err != nil || !found || identity.IsServiceProvider(state.AuthProvider) {
		t.Fatalf("local user: %+v found=%v err=%v, want found and not a service provider", state, found, err)
	}
	state, found, err = users.State(ctx, service)
	if err != nil || !found || !identity.IsServiceProvider(state.AuthProvider) {
		t.Fatalf("service user: %+v found=%v err=%v, want found and a service provider", state, found, err)
	}
}
