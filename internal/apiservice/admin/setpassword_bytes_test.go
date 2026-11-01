package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// probeDB counts the calls a handler makes to the database.
type probeDB struct{ calls *int }

func (d probeDB) Begin(context.Context) (pgx.Tx, error) { *d.calls++; return nil, errors.New("probe") }
func (d probeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	*d.calls++
	return pgconn.CommandTag{}, errors.New("probe")
}
func (d probeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	*d.calls++
	return nil, errors.New("probe")
}
func (d probeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	*d.calls++
	return failingRow{}
}

func setPasswordWith(password string) (*httptest.ResponseRecorder, int) {
	calls := 0
	h := &handlers{store: pgStore{Pool: probeDB{&calls}}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rec := answerWith(h.setUserPassword, http.MethodPost, func(req *http.Request) *http.Request {
		req.SetPathValue("user_id", uuid.NewString())
		return req.WithContext(context.WithValue(req.Context(), setPasswordInputKey{}, setPasswordInput{AdminPassword: "Admin-current-pass-1", Password: password}))
	})
	return rec, calls
}

func TestSetUserPasswordRefusesAPasswordOverTheByteLimitBeforeAnyStoreCall(t *testing.T) {
	for name, password := range map[string]string{
		"73 bytes":                       "Aa1" + strings.Repeat("x", 70),
		"multi-byte under 72 characters": "Aa1" + strings.Repeat("é", 35),
	} {
		rec, calls := setPasswordWith(password)
		if calls != 0 {
			t.Fatalf("%s: set-password made %d store calls", name, calls)
		}
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "Password must be no more than 72 bytes long") {
			t.Fatalf("%s: status %d body %s, want 422 with the byte violation", name, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Body.String(), password) {
			t.Errorf("%s: body echoes the password", name)
		}
	}
}

func TestSetUserPasswordAccepts72BytesPastThePolicy(t *testing.T) {
	_, calls := setPasswordWith("Aa1" + strings.Repeat("x", 69))
	if calls == 0 {
		t.Fatal("a 72-byte password was refused before the store")
	}
}
