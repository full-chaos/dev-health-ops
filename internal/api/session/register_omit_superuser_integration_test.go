//go:build integration

package session_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/superuserwriter"
)

// register runs on a role that cannot write users.is_superuser: the sign-up
// is accepted and the new row reads is_superuser = false. The statement does
// not branch on a switch, so one run covers both states.
func TestRegisterWritesNoIsSuperuser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	st := startStackWith(t, ctx, nil, func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri string) *pgxpool.Pool {
		return superuserwriter.Pool(ctx, t, admin, uri)
	})
	status, body := registerPost(t, ctx, st, `{"email": "omit.person@example.com", "password": "Fresh-pass-2026"}`)
	if status != http.StatusAccepted {
		t.Fatalf("register: %d %s, want 202", status, body)
	}
	var superuser bool
	if err := st.pool.QueryRow(ctx, `SELECT is_superuser FROM users WHERE email = 'omit.person@example.com'`).Scan(&superuser); err != nil || superuser {
		t.Errorf("is_superuser %v err %v, want a row with false", superuser, err)
	}
}
