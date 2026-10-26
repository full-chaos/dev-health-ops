package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

// recordingTx answers Exec and records the statements; any other method
// panics through the nil embedded pgx.Tx.
type recordingTx struct {
	pgx.Tx
	statements []string
}

func (tx *recordingTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.statements = append(tx.statements, sql)
	return pgconn.CommandTag{}, nil
}

// Every go-api route that writes public.users answers the moved operation
// while auth-service owns users, before it opens a transaction or reads the
// request: the handlers here have no Pool, so any write or read would panic.
func TestUsersOwnedRefusesEverySessionWriterBeforeTouchingTheDatabase(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")
	h := handlers{}
	for name, serve := range map[string]http.HandlerFunc{
		"change-password":     h.changePassword,
		"reset-password":      h.resetPassword,
		"verify":              h.verify,
		"resend-verification": h.resendVerification,
		"forgot-password":     h.forgotPassword,
		"register":            h.register,
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			serve(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+name, nil))
			if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"reason":"operation_moved"`) {
				t.Fatalf("%s: status %d body %q, want 409 operation_moved", name, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestUsersOwnedRefusesTheSocialAccountCreate(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "true")
	h := handlers{}
	if _, err := h.createSocialUser(nil, nil, nil, "a@example.test", "github", "1"); !errors.Is(err, usersowned.ErrRefused) {
		t.Fatalf("createSocialUser = %v, want usersowned.ErrRefused before any write", err)
	}
}

// A sign-in keeps working while auth-service owns users, but it stamps no
// last login on public.users: touchLastLogin writes nothing with the switch
// on, and exactly one UPDATE with it off.
func TestTouchLastLoginWritesNothingWhileUsersOwned(t *testing.T) {
	for _, tc := range []struct {
		env        string
		statements int
	}{{"true", 0}, {"false", 1}, {"", 1}} {
		t.Run("AUTH_USERS_OWNED="+tc.env, func(t *testing.T) {
			t.Setenv("AUTH_USERS_OWNED", tc.env)
			tx := &recordingTx{}
			if err := touchLastLogin(context.Background(), tx, uuid.New(), time.Now()); err != nil {
				t.Fatalf("touchLastLogin: %v", err)
			}
			if len(tx.statements) != tc.statements {
				t.Fatalf("statements = %q, want %d", tx.statements, tc.statements)
			}
		})
	}
}
